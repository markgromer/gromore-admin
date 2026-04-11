"""
Stripe Billing - manages customer creation, subscriptions, and webhook handling
for GroMore agency billing.

Stores the Stripe secret key + webhook secret in the agency settings table.
Each brand gets a stripe_customer_id and stripe_subscription_id.
"""
import json
import logging
import stripe

log = logging.getLogger(__name__)


def _configure(db):
    """Load Stripe API key from agency settings."""
    key = (db.get_setting("stripe_secret_key", "") or "").strip()
    if not key:
        return False
    stripe.api_key = key
    return True


# ── Customer Management ──

def create_customer(db, brand):
    """Create a Stripe customer for a brand. Returns customer ID or None."""
    if not _configure(db):
        return None
    existing = (brand.get("stripe_customer_id") or "").strip()
    if existing:
        return existing
    try:
        customer = stripe.Customer.create(
            name=brand.get("display_name", ""),
            email=brand.get("upgrade_contact_emails", "") or brand.get("email", ""),
            metadata={
                "brand_id": str(brand["id"]),
                "slug": brand.get("slug", ""),
            },
        )
        db.update_brand_stripe(brand["id"], stripe_customer_id=customer.id)
        log.info("Created Stripe customer %s for brand %s", customer.id, brand["id"])
        return customer.id
    except stripe.StripeError as e:
        log.error("Stripe create customer failed: %s", e)
        return None


def get_customer(db, brand):
    """Retrieve the Stripe customer object."""
    if not _configure(db):
        return None
    cid = (brand.get("stripe_customer_id") or "").strip()
    if not cid:
        return None
    try:
        return stripe.Customer.retrieve(cid)
    except stripe.StripeError as e:
        log.error("Stripe get customer failed: %s", e)
        return None


# ── Subscription Management ──

def create_subscription(db, brand, price_id, trial_days=None):
    """Create a subscription for a brand. Auto-creates customer if needed.

    Args:
        db: WebDB instance
        brand: brand dict
        price_id: Stripe Price ID (e.g. price_xxx)
        trial_days: optional trial period in days

    Returns:
        dict with subscription info, or None on failure
    """
    if not _configure(db):
        return None

    customer_id = brand.get("stripe_customer_id") or create_customer(db, brand)
    if not customer_id:
        return None

    params = {
        "customer": customer_id,
        "items": [{"price": price_id}],
        "metadata": {"brand_id": str(brand["id"])},
    }
    if trial_days:
        params["trial_period_days"] = trial_days

    try:
        sub = stripe.Subscription.create(**params)
        db.update_brand_stripe(
            brand["id"],
            stripe_subscription_id=sub.id,
            stripe_plan=price_id,
            stripe_status=sub.status,
            stripe_mrr=_sub_mrr(sub),
            stripe_trial_end=sub.trial_end or "",
        )
        log.info("Created subscription %s for brand %s", sub.id, brand["id"])
        return {
            "subscription_id": sub.id,
            "status": sub.status,
            "client_secret": sub.latest_invoice.payment_intent.client_secret if hasattr(sub, "latest_invoice") and sub.latest_invoice else None,
        }
    except stripe.StripeError as e:
        log.error("Stripe create subscription failed: %s", e)
        return None


def update_subscription(db, brand, new_price_id):
    """Change a brand's subscription to a different plan (upgrade/downgrade)."""
    if not _configure(db):
        return None
    sub_id = (brand.get("stripe_subscription_id") or "").strip()
    if not sub_id:
        return None
    try:
        sub = stripe.Subscription.retrieve(sub_id)
        stripe.Subscription.modify(
            sub_id,
            items=[{
                "id": sub["items"]["data"][0].id,
                "price": new_price_id,
            }],
            proration_behavior="create_prorations",
        )
        updated = stripe.Subscription.retrieve(sub_id)
        db.update_brand_stripe(
            brand["id"],
            stripe_plan=new_price_id,
            stripe_status=updated.status,
            stripe_mrr=_sub_mrr(updated),
        )
        log.info("Updated subscription %s to %s for brand %s", sub_id, new_price_id, brand["id"])
        return {"subscription_id": sub_id, "status": updated.status, "mrr": _sub_mrr(updated)}
    except stripe.StripeError as e:
        log.error("Stripe update subscription failed: %s", e)
        return None


def cancel_subscription(db, brand, at_period_end=True):
    """Cancel a brand's subscription."""
    if not _configure(db):
        return None
    sub_id = (brand.get("stripe_subscription_id") or "").strip()
    if not sub_id:
        return None
    try:
        if at_period_end:
            sub = stripe.Subscription.modify(sub_id, cancel_at_period_end=True)
        else:
            sub = stripe.Subscription.cancel(sub_id)
        db.update_brand_stripe(
            brand["id"],
            stripe_status=sub.status,
        )
        log.info("Canceled subscription %s for brand %s (at_period_end=%s)", sub_id, brand["id"], at_period_end)
        return {"status": sub.status}
    except stripe.StripeError as e:
        log.error("Stripe cancel subscription failed: %s", e)
        return None


def get_subscription_status(db, brand):
    """Fetch live subscription status from Stripe and sync to DB."""
    if not _configure(db):
        return None
    sub_id = (brand.get("stripe_subscription_id") or "").strip()
    if not sub_id:
        return None
    try:
        sub = stripe.Subscription.retrieve(sub_id)
        db.update_brand_stripe(
            brand["id"],
            stripe_status=sub.status,
            stripe_mrr=_sub_mrr(sub),
            stripe_next_invoice="" if not sub.current_period_end else str(sub.current_period_end),
        )
        return {
            "status": sub.status,
            "mrr": _sub_mrr(sub),
            "current_period_end": sub.current_period_end,
            "cancel_at_period_end": sub.cancel_at_period_end,
        }
    except stripe.StripeError as e:
        log.error("Stripe get subscription failed: %s", e)
        return None


def get_plans(db):
    """List active Stripe prices (plans) for display."""
    if not _configure(db):
        return []
    try:
        prices = stripe.Price.list(active=True, limit=20, expand=["data.product"])
        plans = []
        for p in prices.data:
            if p.recurring:
                plans.append({
                    "price_id": p.id,
                    "name": p.product.name if hasattr(p, "product") and p.product else p.id,
                    "amount": p.unit_amount / 100.0 if p.unit_amount else 0,
                    "currency": p.currency,
                    "interval": p.recurring.interval,
                })
        return sorted(plans, key=lambda x: x["amount"])
    except stripe.StripeError as e:
        log.error("Stripe list prices failed: %s", e)
        return []


def create_billing_portal_session(db, brand, return_url):
    """Create a Stripe billing portal session for the client to manage payment."""
    if not _configure(db):
        return None
    cid = (brand.get("stripe_customer_id") or "").strip()
    if not cid:
        return None
    try:
        session = stripe.billing_portal.Session.create(
            customer=cid,
            return_url=return_url,
        )
        return session.url
    except stripe.StripeError as e:
        log.error("Stripe portal session failed: %s", e)
        return None


# ── Webhook Handling ──

def handle_webhook(db, payload, sig_header):
    """Process a Stripe webhook event. Returns (success, message)."""
    secret = (db.get_setting("stripe_webhook_secret", "") or "").strip()
    if not secret:
        return False, "Webhook secret not configured"

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, secret)
    except (ValueError, stripe.SignatureVerificationError) as e:
        return False, f"Invalid signature: {e}"

    etype = event["type"]
    data_obj = event["data"]["object"]

    brand_id = None
    metadata = data_obj.get("metadata", {})
    if metadata.get("brand_id"):
        brand_id = int(metadata["brand_id"])
    elif data_obj.get("customer"):
        brand_id = _brand_id_from_customer(db, data_obj["customer"])

    db.log_stripe_event(event["id"], etype, brand_id=brand_id, data=data_obj)

    if etype == "customer.subscription.created":
        _sync_subscription(db, brand_id, data_obj)
    elif etype == "customer.subscription.updated":
        _sync_subscription(db, brand_id, data_obj)
    elif etype == "customer.subscription.deleted":
        if brand_id:
            from datetime import datetime
            db.update_brand_stripe(
                brand_id,
                stripe_status="canceled",
                stripe_mrr=0,
                churned_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            )
    elif etype == "invoice.payment_succeeded":
        if brand_id:
            db.update_brand_stripe(brand_id, stripe_status="active")
    elif etype == "invoice.payment_failed":
        if brand_id:
            db.update_brand_stripe(brand_id, stripe_status="past_due")
    elif etype == "customer.subscription.trial_will_end":
        if brand_id:
            db.add_agency_prospect_note(
                _prospect_from_brand(db, brand_id) or 0,
                f"Trial ending soon for brand {brand_id}",
                note_type="system",
                created_by="stripe",
            )

    return True, f"Processed {etype}"


# ── Helpers ──

def _sub_mrr(sub):
    """Calculate MRR from a Stripe subscription object."""
    try:
        item = sub["items"]["data"][0]
        amount = (item["price"]["unit_amount"] or 0) / 100.0
        interval = item["price"]["recurring"]["interval"]
        if interval == "year":
            return round(amount / 12, 2)
        return round(amount, 2)
    except (KeyError, IndexError, TypeError):
        return 0


def _brand_id_from_customer(db, customer_id):
    """Look up brand_id from stripe_customer_id."""
    conn = db._conn()
    row = conn.execute(
        "SELECT id FROM brands WHERE stripe_customer_id = ?", (customer_id,)
    ).fetchone()
    conn.close()
    return row["id"] if row else None


def _prospect_from_brand(db, brand_id):
    """Find agency prospect linked to a brand."""
    conn = db._conn()
    row = conn.execute(
        "SELECT id FROM agency_prospects WHERE converted_brand_id = ?", (brand_id,)
    ).fetchone()
    conn.close()
    return row["id"] if row else None


def _sync_subscription(db, brand_id, sub_data):
    """Sync subscription data from a webhook event to the DB."""
    if not brand_id:
        return
    try:
        item = sub_data["items"]["data"][0]
        price_id = item["price"]["id"]
    except (KeyError, IndexError):
        price_id = ""

    db.update_brand_stripe(
        brand_id,
        stripe_subscription_id=sub_data.get("id", ""),
        stripe_plan=price_id,
        stripe_status=sub_data.get("status", ""),
        stripe_mrr=_sub_mrr(sub_data),
        stripe_trial_end=str(sub_data.get("trial_end", "") or ""),
        stripe_next_invoice=str(sub_data.get("current_period_end", "") or ""),
    )
