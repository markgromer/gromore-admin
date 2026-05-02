"""
Email sender - sends report HTML to contacts via SMTP.
"""
import html
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, parseaddr
from pathlib import Path


def _clean_header_value(value, max_len=255):
    return str(value or "").replace("\r", " ").replace("\n", " ").strip()[:max_len]


def _valid_email(value):
    email_address = parseaddr(_clean_header_value(value))[1]
    return email_address if "@" in email_address and "." in email_address.rsplit("@", 1)[-1] else ""


def brand_email_identity(app_config, brand=None, fallback_name="GroMore", sender_name=None, reply_to=None):
    """Return the SMTP-safe From identity plus optional brand Reply-To."""
    smtp_user = app_config.get("SMTP_USER", "")
    from_email = _valid_email(app_config.get("SMTP_FROM_EMAIL", smtp_user)) or smtp_user
    brand = brand or {}
    resolved_name = (
        _clean_header_value(sender_name, 120)
        or _clean_header_value(brand.get("email_sender_name"), 120)
        or _clean_header_value(brand.get("email_from_name"), 120)
        or _clean_header_value(brand.get("display_name") or brand.get("name"), 120)
        or _clean_header_value(app_config.get("SMTP_FROM_NAME", fallback_name), 120)
        or fallback_name
    )
    resolved_reply_to = (
        _valid_email(reply_to)
        or _valid_email(brand.get("email_reply_to"))
        or _valid_email(brand.get("reply_to_email"))
    )
    return {
        "from_name": resolved_name,
        "from_email": from_email,
        "reply_to": resolved_reply_to,
    }


def apply_brand_email_identity(msg, app_config, brand=None, fallback_name="GroMore", sender_name=None, reply_to=None):
    """Apply From and Reply-To headers while keeping SMTP envelope delivery-safe."""
    identity = brand_email_identity(app_config, brand, fallback_name, sender_name, reply_to)
    msg["From"] = formataddr((identity["from_name"], identity["from_email"]))
    if identity["reply_to"] and identity["reply_to"].lower() != identity["from_email"].lower():
        msg["Reply-To"] = formataddr((identity["from_name"], identity["reply_to"]))
    return identity


def send_report_email(app_config, brand, report, recipients):
    """
    Send the client report HTML as an email to all recipients.

    app_config: Flask app.config dict with SMTP_* keys
    brand: brand dict from DB
    report: report dict from DB
    recipients: list of contact dicts with 'name' and 'email'
    """
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    identity = brand_email_identity(app_config, brand, "Agency Reports")
    from_name = identity["from_name"]
    from_email = identity["from_email"]

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured. Set SMTP_USER and SMTP_PASSWORD environment variables.")

    # Read the client report HTML
    client_path = report.get("client_path", "")
    if not client_path or not Path(client_path).exists():
        raise FileNotFoundError(f"Client report not found: {client_path}")

    with open(client_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    month = report.get("month", "")
    subject = f"{brand['display_name']} - Monthly Performance Report - {month}"

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        for contact in recipients:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            apply_brand_email_identity(msg, app_config, brand, "Agency Reports")
            msg["To"] = contact["email"]

            # Plain text fallback
            text_part = MIMEText(
                f"Hi {contact['name']},\n\n"
                f"Your monthly performance report for {brand['display_name']} ({month}) is ready.\n"
                f"Please view this email in an HTML-capable client to see the full report.\n\n"
                f"Best regards,\n{from_name}",
                "plain",
            )

            html_part = MIMEText(html_content, "html")

            msg.attach(text_part)
            msg.attach(html_part)

            server.sendmail(from_email, contact["email"], msg.as_string())


def send_beta_welcome_email(app_config, tester, onboarding_url):
    """
    Send welcome email to an approved beta tester with a link to complete
    their onboarding (GMB manager email, Facebook profile/page links, and
    developer account setup).
    """
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    from_name = app_config.get("SMTP_FROM_NAME", "W.A.R.R.E.N. by GroMore")
    from_email = app_config.get("SMTP_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured.")

    name = tester.get("name", "there")
    email = tester["email"]
    subject = "Welcome to the W.A.R.R.E.N. Beta - Next Steps"

    html = f"""
    <div style="font-family:Inter,Arial,sans-serif;max-width:600px;margin:0 auto;padding:24px;">
        <div style="display:inline-block;padding:6px 12px;border-radius:999px;background:#fffbeb;border:1px solid #fcd34d;color:#b45309;font-size:12px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;">W.A.R.R.E.N. Beta Access</div>
        <h2 style="color:#b45309;margin-top:16px;">Welcome to W.A.R.R.E.N., {name}!</h2>
        <p>You've been approved for the W.A.R.R.E.N. beta program. Before we can connect your live account data, we need a few setup details.</p>

        <div style="background:#fffbeb;border:1px solid #f59e0b;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;color:#b45309;">A note about beta setup</h3>
            <p>While we're in beta, we still have to wire account access manually. The key pieces are your Google Business Profile manager email, your Facebook developer access, and the exact Facebook profile/page links we should use.</p>
            <p>We walk through this on our <strong>group onboarding calls</strong>. If you're unable to join one of those, you can book a <strong>15-minute Zoom call</strong> with me directly and we'll get you set up:</p>
            <div style="text-align:center;margin:16px 0;">
                <a href="https://calendly.com/nopoop520" style="display:inline-block;padding:12px 28px;background:#f59e0b;color:#fff;border-radius:8px;text-decoration:none;font-weight:600;">Book a Setup Call</a>
            </div>
        </div>

        <div style="text-align:center;margin:28px 0;">
            <a href="{onboarding_url}" style="display:inline-block;padding:14px 36px;background:linear-gradient(135deg,#f59e0b,#d97706);color:#fff;border-radius:10px;text-decoration:none;font-weight:600;font-size:1rem;">Complete Your Setup</a>
        </div>

        <div style="background:#fff7ed;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;">What we need from you:</h3>
            <ol style="line-height:2;">
                <li><strong>Google Business Profile manager email</strong> - the email address used to manage your GMB listing</li>
                <li><strong>Facebook Developer account</strong> - create a free developer account at <a href="https://developers.facebook.com" style="color:#4f46e5;">developers.facebook.com</a></li>
                <li><strong>Personal Facebook profile link</strong> - the personal profile that manages your business page</li>
                <li><strong>Facebook business page link</strong> - the public business page WARREN should connect to</li>
            </ol>
        </div>

        <div style="background:#fff7ed;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;">Which Facebook links should you send?</h3>
            <ol style="line-height:1.8;font-size:.9rem;">
                <li>Open the personal Facebook profile that has admin access to the business page. Copy that profile URL.</li>
                <li>Open the public Facebook business page. Copy that page URL.</li>
                <li>Make sure both links are accessible and are for the account/page you actually want WARREN connected to.</li>
            </ol>
            <p style="font-size:.85em;color:#666;">Examples: https://www.facebook.com/your.profile and https://www.facebook.com/yourbusiness</p>
        </div>

        <div style="background:#fdf2f8;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;">Facebook Developer Account (Free)</h3>
            <p>You'll need a free Facebook Developer account so we can add you to the app during beta. This takes about 2 minutes:</p>
            <ol style="line-height:1.8;font-size:.9rem;">
                <li>Visit <a href="https://developers.facebook.com" style="color:#4f46e5;">developers.facebook.com</a></li>
                <li>Click <strong>Get Started</strong> and log in with your Facebook account</li>
                <li>Accept the terms and complete the registration</li>
            </ol>
            <p style="font-size:.85em;color:#666;">Use the same personal Facebook profile that manages your business page.</p>
        </div>

        <p>Once you've completed the setup form and we've added your accounts to our backend, we'll send you another email with your W.A.R.R.E.N. login credentials.</p>
        <p style="font-size:.9em;color:#475569;">W.A.R.R.E.N. is delivered by GroMore Media, which handles onboarding and account operations.</p>
        <p>Questions? Reply to this email and we'll help you out.</p>
        <p style="color:#666;">- W.A.R.R.E.N. by GroMore</p>
    </div>
    """

    text = (
        f"Welcome to W.A.R.R.E.N., {name}!\n\n"
        f"You've been approved for the beta program. Complete your setup here:\n"
        f"{onboarding_url}\n\n"
        f"ABOUT BETA SETUP\n"
        f"While we're in beta, we still wire account access manually. We need the exact GMB and Facebook access details below.\n\n"
        f"We walk through this on our group onboarding calls. If you're unable to join one of those, "
        f"book a 15-minute Zoom call with me and we'll get you set up:\n"
        f"https://calendly.com/nopoop520\n\n"
        f"What we need:\n"
        f"1. Google Business Profile manager email\n"
        f"2. Create a free Facebook Developer account at developers.facebook.com\n"
        f"3. Personal Facebook profile link for the profile that manages your business page\n"
        f"4. Facebook business page link\n\n"
        f"Once setup is complete, we'll send your W.A.R.R.E.N. login credentials.\n\n"
        f"W.A.R.R.E.N. is delivered by GroMore Media.\n\n"
        f"- W.A.R.R.E.N. by GroMore"
    )

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = email
        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))
        server.sendmail(from_email, email, msg.as_string())


def send_beta_activation_email(app_config, tester, temp_password, login_url):
    """
    Send activation email to a beta tester once their accounts have been
    created in WARREN. Includes current beta setup requirements for GMB,
    Facebook developer access, and Facebook profile/page links.
    """
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    from_name = app_config.get("SMTP_FROM_NAME", "W.A.R.R.E.N. by GroMore")
    from_email = app_config.get("SMTP_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured.")

    name = tester.get("name", "there")
    email = tester["email"]
    subject = "Your W.A.R.R.E.N. Beta Access Is Active"

    html = f"""
    <div style="font-family:Inter,Arial,sans-serif;max-width:600px;margin:0 auto;padding:24px;">
        <div style="display:inline-block;padding:6px 12px;border-radius:999px;background:#fffbeb;border:1px solid #fcd34d;color:#b45309;font-size:12px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;">W.A.R.R.E.N. Beta Access</div>
        <h2 style="color:#15803d;margin-top:16px;">You're in, {name}!</h2>
        <p>Your W.A.R.R.E.N. beta account is active. You can log in now, and we need the current connection details below so we can wire your Google Business Profile and Facebook assets correctly.</p>

        <div style="text-align:center;margin:28px 0;">
            <a href="{login_url}" style="display:inline-block;padding:14px 36px;background:linear-gradient(135deg,#f59e0b,#d97706);color:#fff;border-radius:10px;text-decoration:none;font-weight:600;font-size:1rem;">Open W.A.R.R.E.N.</a>
        </div>

        <div style="background:#fdf2f8;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;"><span style="font-size:1.1em;">&#9312;</span> Send These Account Details</h3>
            <p>Please reply to this email with the following. These are the details we need for the modern WARREN setup:</p>
            <ol style="line-height:1.8;font-size:.9rem;">
                <li><strong>Google Business Profile manager email</strong> - the email address used to manage your GMB listing</li>
                <li><strong>Personal Facebook profile link</strong> - the personal profile that manages the business page</li>
                <li><strong>Facebook business page link</strong> - the public page URL for the business</li>
                <li><strong>Confirmation that your Facebook Developer account is set up</strong> at <a href="https://developers.facebook.com" style="color:#4f46e5;">developers.facebook.com</a></li>
            </ol>
            <p style="font-size:.85em;color:#666;">If you already submitted these through onboarding, just reply "submitted" and include anything that has changed.</p>
        </div>

        <div style="background:#f0fdf4;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;"><span style="font-size:1.1em;">&#9313;</span> Sign In to W.A.R.R.E.N.</h3>
            <p>Head to the login page and sign in with your beta account:</p>
            <ul style="line-height:1.8;font-size:.9rem;">
                <li><strong>Email:</strong> {email}</li>
                <li><strong>Temporary password:</strong> {temp_password}</li>
            </ul>
            <p style="font-size:.85em;color:#666;">You may be prompted to change the temporary password on first login. OAuth connections are handled after the required account details above are confirmed.</p>
        </div>

        <div style="background:#fff7ed;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;"><span style="font-size:1.1em;">&#9314;</span> Facebook Developer Setup</h3>
            <p>If you have not created the free developer account yet:</p>
            <ol style="line-height:1.8;font-size:.9rem;">
                <li>Go to <a href="https://developers.facebook.com" style="color:#4f46e5;">developers.facebook.com</a></li>
                <li>Click <strong>Get Started</strong> and log in with the personal Facebook profile that manages your business page</li>
                <li>Accept the terms and complete registration</li>
            </ol>
        </div>

        <div style="background:#eff6ff;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;"><span style="font-size:1.1em;">&#9315;</span> Share Your Feedback</h3>
            <p>As a beta tester, your input directly shapes what we build. Use the <strong>Feedback</strong>
            link in your dashboard sidebar to report bugs, request features, or tell us what's working.</p>
        </div>

        <p style="font-size:.9em;color:#475569;">W.A.R.R.E.N. is delivered by GroMore Media, which manages the beta rollout and account operations.</p>
        <p>Questions or issues signing in? Reply to this email and we'll help you out.</p>
        <p style="color:#666;">- W.A.R.R.E.N. by GroMore</p>
    </div>
    """

    text = (
        f"You're In, {name}!\n\n"
        f"Your W.A.R.R.E.N. beta account is active.\n\n"
        f"STEP 1 - Reply with these account details:\n"
        f"1. Google Business Profile manager email\n"
        f"2. Personal Facebook profile link for the profile that manages your business page\n"
        f"3. Facebook business page link\n"
        f"4. Confirmation that your free Facebook Developer account is set up at developers.facebook.com\n\n"
        f"STEP 2 - Sign In to W.A.R.R.E.N.:\n"
        f"Go to {login_url}\n"
        f"Use email/password: {email} / {temp_password}\n\n"
        f"STEP 3 - Facebook Developer Setup if you have not done it yet:\n"
        f"Go to https://developers.facebook.com, click Get Started, log in with the personal Facebook profile that manages your business page, and complete registration.\n\n"
        f"STEP 4 - Share Feedback:\n"
        f"Use the Feedback link in your dashboard sidebar.\n\n"
        f"W.A.R.R.E.N. is delivered by GroMore Media.\n\n"
        f"Questions? Reply to this email.\n\n"
        f"- W.A.R.R.E.N. by GroMore"
    )

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = email
        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))
        server.sendmail(from_email, email, msg.as_string())


def send_staff_invite_email(app_config, email, name, brand_name, temp_password, role):
    """
    Send a staff invite email with temporary login credentials.
    """
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    from_name = app_config.get("SMTP_FROM_NAME", "W.A.R.R.E.N. by GroMore")
    from_email = app_config.get("SMTP_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured.")

    role_label = role.capitalize()
    subject = f"You've been invited to {brand_name} on W.A.R.R.E.N."

    html = f"""
    <div style="font-family:Inter,Arial,sans-serif;max-width:520px;margin:0 auto;padding:24px;">
        <div style="display:inline-block;padding:6px 12px;border-radius:999px;background:#fffbeb;border:1px solid #fcd34d;color:#b45309;font-size:12px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;">W.A.R.R.E.N. Access</div>
        <h2 style="color:#b45309;margin-top:16px;">Welcome to W.A.R.R.E.N.</h2>
        <p>Hi {name},</p>
        <p>You've been added as a <strong>{role_label}</strong> for <strong>{brand_name}</strong> on W.A.R.R.E.N.</p>
        <p>Here are your temporary login credentials:</p>
        <div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:10px;padding:16px;margin:20px 0;">
            <p style="margin:4px 0;"><strong>Email:</strong> {email}</p>
            <p style="margin:4px 0;"><strong>Temporary Password:</strong> {temp_password}</p>
        </div>
        <p>Please log in and change your password as soon as possible.</p>
        <p style="font-size:.9em;color:#475569;">W.A.R.R.E.N. is delivered by GroMore Media, which manages your account access.</p>
        <p style="color:#999;font-size:.8em;margin-top:24px;">- W.A.R.R.E.N. by GroMore</p>
    </div>
    """

    text = (
        f"Hi {name},\n\n"
        f"You've been added as a {role_label} for {brand_name} on W.A.R.R.E.N.\n\n"
        f"Email: {email}\nTemporary Password: {temp_password}\n\n"
        f"Please log in and change your password as soon as possible.\n\n"
        f"W.A.R.R.E.N. is delivered by GroMore Media.\n\n"
        f"- W.A.R.R.E.N. by GroMore"
    )

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = email
        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))
        server.sendmail(from_email, email, msg.as_string())


def send_password_reset_email(app_config, email, name, reset_url):
    """
    Send a password reset link to a client user.
    """
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    from_name = app_config.get("SMTP_FROM_NAME", "GroMore")
    from_email = app_config.get("SMTP_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured.")

    subject = "Reset your W.A.R.R.E.N. password"

    html = f"""
    <div style="font-family:Inter,Arial,sans-serif;max-width:520px;margin:0 auto;padding:24px;">
        <div style="display:inline-block;padding:6px 12px;border-radius:999px;background:#fffbeb;border:1px solid #fcd34d;color:#b45309;font-size:12px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;">W.A.R.R.E.N. Access</div>
        <h2 style="color:#b45309;margin-top:16px;">Reset your password</h2>
        <p>Hi {name or 'there'},</p>
        <p>We received a request to reset your W.A.R.R.E.N. password. Click the button below to choose a new one:</p>
        <div style="text-align:center;margin:28px 0;">
            <a href="{reset_url}" style="display:inline-block;padding:12px 32px;background:#d97706;color:#fff;border-radius:10px;text-decoration:none;font-weight:600;">Reset Password</a>
        </div>
        <p style="font-size:.85em;color:#666;">This link expires in 1 hour. If you didn't request this, you can safely ignore this email.</p>
        <p style="font-size:.9em;color:#475569;">W.A.R.R.E.N. is delivered by GroMore Media, which manages your client portal access.</p>
        <p style="color:#999;font-size:.8em;margin-top:24px;">- W.A.R.R.E.N. by GroMore</p>
    </div>
    """

    text = (
        f"Hi {name or 'there'},\n\n"
        f"Reset your W.A.R.R.E.N. password by visiting:\n{reset_url}\n\n"
        f"This link expires in 1 hour.\n\n"
        f"W.A.R.R.E.N. is delivered by GroMore Media.\n\n"
        f"- W.A.R.R.E.N. by GroMore"
    )

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = email
        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))
        server.sendmail(from_email, email, msg.as_string())


def send_client_login_email(app_config, email, name, temp_password, login_url, brand_name):
    """
    Send (or re-send) client portal login credentials.
    """
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    from_name = app_config.get("SMTP_FROM_NAME", "W.A.R.R.E.N. by GroMore")
    from_email = app_config.get("SMTP_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured.")

    subject = f"Your {brand_name} W.A.R.R.E.N. login"

    html = f"""
    <div style="font-family:Inter,Arial,sans-serif;max-width:520px;margin:0 auto;padding:24px;">
        <div style="display:inline-block;padding:6px 12px;border-radius:999px;background:#fffbeb;border:1px solid #fcd34d;color:#b45309;font-size:12px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;">W.A.R.R.E.N. Access</div>
        <h2 style="color:#b45309;margin-top:16px;">Your operator login</h2>
        <p>Hi {name or 'there'},</p>
        <p>Here are your login credentials for <strong>{brand_name}</strong> inside W.A.R.R.E.N.:</p>
        <div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:10px;padding:16px;margin:20px 0;">
            <p style="margin:4px 0;"><strong>Email:</strong> {email}</p>
            <p style="margin:4px 0;"><strong>Temporary Password:</strong> {temp_password}</p>
        </div>
        <div style="text-align:center;margin:28px 0;">
            <a href="{login_url}" style="display:inline-block;padding:12px 32px;background:#d97706;color:#fff;border-radius:10px;text-decoration:none;font-weight:600;">Open W.A.R.R.E.N.</a>
        </div>
        <p style="font-size:.85em;color:#666;">Please change your password after signing in. If you didn't expect this email, you can safely ignore it.</p>
        <p style="font-size:.9em;color:#475569;">W.A.R.R.E.N. is delivered by GroMore Media, which manages your portal access.</p>
        <p style="color:#999;font-size:.8em;margin-top:24px;">- W.A.R.R.E.N. by GroMore</p>
    </div>
    """

    text = (
        f"Hi {name or 'there'},\n\n"
        f"Here are your login credentials for {brand_name} inside W.A.R.R.E.N.:\n\n"
        f"Email: {email}\nTemporary Password: {temp_password}\n\n"
        f"Sign in at: {login_url}\n\n"
        f"Please change your password after signing in.\n\n"
        f"W.A.R.R.E.N. is delivered by GroMore Media.\n\n"
        f"- W.A.R.R.E.N. by GroMore"
    )

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = email
        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))
        server.sendmail(from_email, email, msg.as_string())


def send_simple_email(app_config, email, subject, text, html=None, brand=None, sender_name=None, reply_to=None):
    """Send a simple transactional email with text and optional HTML."""
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    identity = brand_email_identity(app_config, brand, "GroMore", sender_name, reply_to)
    from_email = identity["from_email"]

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured.")

    html = html or f"<pre style=\"font-family:Arial,sans-serif;white-space:pre-wrap;\">{text}</pre>"

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        apply_brand_email_identity(msg, app_config, brand, "GroMore", sender_name, reply_to)
        msg["To"] = email
        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))
        server.sendmail(from_email, email, msg.as_string())


def send_bulk_email(app_config, recipients, subject, text, html_body=None,
                    tracking_base_url=None, token_map=None, brand=None,
                    sender_name=None, reply_to=None):
    """Send one message to many recipients using the configured SMTP account.
    
    If tracking_base_url and token_map are provided, a 1x1 tracking pixel
    is appended to each email's HTML body for open tracking.
    """
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    identity = brand_email_identity(app_config, brand, "GroMore", sender_name, reply_to)
    from_email = identity["from_email"]

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured.")

    base_html = html_body or (
        "<div style=\"font-family:Arial,sans-serif;white-space:pre-wrap;line-height:1.6;\">"
        f"{html.escape(text).replace(chr(10), '<br>')}"
        "</div>"
    )

    sent_count = 0
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        for recipient in recipients or []:
            email_address = (recipient.get("email") if isinstance(recipient, dict) else recipient) or ""
            email_address = email_address.strip()
            if not email_address:
                continue

            # Inject tracking pixel if available
            recipient_html = base_html
            if tracking_base_url and token_map and email_address in token_map:
                pixel_url = f"{tracking_base_url}/t/{token_map[email_address]}.gif"
                recipient_html = base_html + f'<img src="{pixel_url}" width="1" height="1" alt="" style="display:none;">'

            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            apply_brand_email_identity(msg, app_config, brand, "GroMore", sender_name, reply_to)
            msg["To"] = email_address
            msg.attach(MIMEText(text, "plain"))
            msg.attach(MIMEText(recipient_html, "html"))
            server.sendmail(from_email, email_address, msg.as_string())
            sent_count += 1

    return sent_count
