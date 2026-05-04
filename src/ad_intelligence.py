"""
Deterministic paid-media intelligence.

This module turns Google Ads and Meta data into one normalized evidence layer.
It is intentionally pure Python so dashboards, missions, ad generation, and chat
can all use the same paid-media diagnosis without depending on Flask.
"""

from statistics import median


def _to_float(value, default=0.0):
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _round_money(value):
    return round(_to_float(value, 0.0), 2)


def _get_metric(row, *keys, default=0.0):
    metrics = row.get("metrics") if isinstance(row, dict) else None
    sources = [row]
    if isinstance(metrics, dict):
        sources.insert(0, metrics)
    for source in sources:
        if not isinstance(source, dict):
            continue
        for key in keys:
            if source.get(key) not in (None, ""):
                return _to_float(source.get(key), default)
    return _to_float(default, default)


def _clean_name(row, fallback="Unknown"):
    if not isinstance(row, dict):
        return fallback
    for key in ("name", "campaign_name", "ad_name", "term", "query", "search_term"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return fallback


def _search_term_name(row):
    if not isinstance(row, dict):
        return ""
    for key in ("search_term", "query", "term"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _brand_value(brand, *keys):
    if not isinstance(brand, dict):
        return None
    for key in keys:
        value = brand.get(key)
        if value not in (None, ""):
            return value
    return None


def _target_cpa(analysis, brand):
    target = _to_float(_brand_value(brand, "kpi_target_cpa", "target_cpa"), 0.0)
    if target > 0:
        return round(target, 2), "brand_target"
    target = _to_float(((analysis.get("kpi_status") or {}).get("targets") or {}).get("cpa"), 0.0)
    if target > 0:
        return round(target, 2), "kpi_status"

    cpas = []
    for campaign in _raw_campaigns(analysis):
        spend = _get_metric(campaign, "spend", "cost")
        results = _get_metric(campaign, "results", "conversions")
        cpa = _get_metric(campaign, "cost_per_result", "cpa", "cpr")
        if cpa <= 0 and spend > 0 and results > 0:
            cpa = spend / results
        if cpa > 0 and results > 0:
            cpas.append(cpa)
    if cpas:
        return round(median(cpas) * 1.25, 2), "inferred_from_winners"
    return None, "missing"


def _raw_campaigns(analysis):
    google = analysis.get("google_ads") or {}
    meta = analysis.get("meta_business") or {}
    return list(google.get("campaign_analysis") or []) + list(meta.get("campaign_analysis") or [])


def _normalize_campaign(row, platform):
    spend = _get_metric(row, "spend", "cost")
    results = _get_metric(row, "results", "conversions")
    cpa = _get_metric(row, "cost_per_result", "cpa", "cpr")
    if cpa <= 0 and spend > 0 and results > 0:
        cpa = spend / results
    clicks = _get_metric(row, "clicks")
    impressions = _get_metric(row, "impressions")
    ctr = _get_metric(row, "ctr")
    if ctr <= 0 and clicks > 0 and impressions > 0:
        ctr = clicks / impressions * 100
    cpc = _get_metric(row, "cpc", "average_cpc")
    if cpc <= 0 and spend > 0 and clicks > 0:
        cpc = spend / clicks

    metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else row
    return {
        "platform": platform,
        "id": str(row.get("campaign_id") or row.get("id") or ""),
        "name": _clean_name(row, "Unnamed campaign"),
        "status": str(row.get("status") or metrics.get("status") or "").lower() or "unknown",
        "channel": row.get("channel_type") or row.get("objective") or metrics.get("channel_type") or metrics.get("objective"),
        "spend": round(spend, 2),
        "results": round(results, 2),
        "cpa": round(cpa, 2) if cpa > 0 else None,
        "impressions": round(impressions, 2),
        "clicks": round(clicks, 2),
        "ctr": round(ctr, 2),
        "cpc": round(cpc, 2) if cpc > 0 else None,
        "frequency": round(_get_metric(row, "frequency"), 2) or None,
        "issue": row.get("issue"),
        "source_status": row.get("status"),
    }


def _campaigns(analysis):
    google = analysis.get("google_ads") or {}
    meta = analysis.get("meta_business") or {}
    campaigns = []
    for row in google.get("campaign_analysis") or []:
        campaigns.append(_normalize_campaign(row, "google_ads"))
    for row in meta.get("campaign_analysis") or []:
        campaigns.append(_normalize_campaign(row, "meta_ads"))
    campaigns.sort(key=lambda row: row.get("spend") or 0, reverse=True)
    return campaigns


def _finding(key, severity, platform, title, detail, evidence, recommended_action, entity=None, action_type="diagnose", confidence="medium"):
    return {
        "key": key,
        "severity": severity,
        "platform": platform,
        "title": title,
        "detail": detail,
        "evidence": [item for item in evidence if item],
        "recommended_action": recommended_action,
        "entity": entity or {},
        "action_type": action_type,
        "confidence": confidence,
    }


def _search_term_findings(analysis, target_cpa):
    google = analysis.get("google_ads") or {}
    terms = list(google.get("search_terms") or [])
    if not terms:
        return []

    waste_floor = 20.0
    if target_cpa:
        waste_floor = max(15.0, min(50.0, target_cpa * 0.35))

    waste = []
    high_cpc = []
    for term in terms:
        spend = _get_metric(term, "spend", "cost")
        results = _get_metric(term, "results", "conversions")
        clicks = _get_metric(term, "clicks")
        cpc = _get_metric(term, "cpc", "average_cpc")
        if cpc <= 0 and spend > 0 and clicks > 0:
            cpc = spend / clicks
        name = _search_term_name(term)
        if name and spend >= waste_floor and results <= 0:
            waste.append((spend, name, term))
        if name and cpc >= 12 and results <= 0:
            high_cpc.append((cpc, name, term))

    findings = []
    if waste:
        waste.sort(reverse=True, key=lambda item: item[0])
        total = sum(item[0] for item in waste)
        names = ", ".join(f'"{name}"' for _, name, _ in waste[:5])
        findings.append(_finding(
            "google_search_term_waste",
            "high" if total >= (target_cpa or 75) else "medium",
            "google_ads",
            "Stop paying for non-converting search terms",
            f"{len(waste)} search terms spent ${total:.0f} without a recorded conversion.",
            [f"{names} spent with 0 conversions.", f"Waste floor used: ${waste_floor:.0f}."],
            "Add the named terms as negatives or split them into tighter exact/phrase match groups before they spend more.",
            entity={"terms": [item[1] for item in waste[:8]], "wasted_spend": round(total, 2)},
            action_type="cut_waste",
            confidence="high",
        ))
    if high_cpc:
        high_cpc.sort(reverse=True, key=lambda item: item[0])
        names = ", ".join(f'"{name}" (${cpc:.2f} CPC)' for cpc, name, _ in high_cpc[:4])
        findings.append(_finding(
            "google_high_cpc_terms",
            "medium",
            "google_ads",
            "Inspect expensive clicks before scaling Google Ads",
            "Several search terms have high CPC and no recorded conversion.",
            [names],
            "Check match type, landing-page fit, and lead quality before increasing budget.",
            entity={"terms": [item[1] for item in high_cpc[:6]]},
            action_type="diagnose",
            confidence="medium",
        ))
    return findings


def _campaign_findings(campaigns, target_cpa):
    findings = []
    active_spenders = [c for c in campaigns if c["spend"] > 0]
    if not active_spenders:
        return findings

    zero_result = [c for c in active_spenders if c["results"] <= 0 and c["spend"] >= 25]
    if zero_result:
        zero_result.sort(key=lambda c: c["spend"], reverse=True)
        total = sum(c["spend"] for c in zero_result)
        names = ", ".join(f'{c["name"]} (${c["spend"]:.0f})' for c in zero_result[:4])
        findings.append(_finding(
            "campaign_zero_result_spend",
            "high" if total >= (target_cpa or 75) else "medium",
            "paid_media",
            "Cut spend from campaigns with no results",
            f"{len(zero_result)} campaigns spent ${total:.0f} with no recorded result.",
            [names],
            "Pause, reduce budget, or rebuild these campaigns before adding net-new budget.",
            entity={"campaigns": [c["name"] for c in zero_result[:6]], "wasted_spend": round(total, 2)},
            action_type="cut_waste",
            confidence="high",
        ))

    if target_cpa:
        expensive = [
            c for c in active_spenders
            if c["results"] > 0 and c.get("cpa") and c["cpa"] > target_cpa * 1.35 and c["spend"] >= target_cpa
        ]
        if expensive:
            expensive.sort(key=lambda c: c["cpa"] or 0, reverse=True)
            names = ", ".join(f'{c["name"]} (${c["cpa"]:.0f} CPA)' for c in expensive[:4])
            findings.append(_finding(
                "campaign_cpa_above_target",
                "high",
                "paid_media",
                "Bring high-CPA campaigns back under target",
                f"{len(expensive)} campaigns are more than 35% over the ${target_cpa:.0f} CPA target.",
                [names],
                "Lower budget on the worst CPA campaign and move the dollars to a campaign producing leads closer to target.",
                entity={"campaigns": [c["name"] for c in expensive[:6]], "target_cpa": target_cpa},
                action_type="rebalance_budget",
                confidence="high",
            ))

        scale = [
            c for c in active_spenders
            if c["results"] >= 2 and c.get("cpa") and c["cpa"] <= target_cpa * 0.85
        ]
        if scale:
            scale.sort(key=lambda c: (c["results"], -(c.get("cpa") or 0)), reverse=True)
            best = scale[0]
            findings.append(_finding(
                "campaign_scale_candidate",
                "medium",
                best["platform"],
                "Scale the campaign already beating target CPA",
                f"{best['name']} is producing results below the ${target_cpa:.0f} CPA target.",
                [f"{best['name']}: ${best['spend']:.0f} spend, {best['results']:.0f} results, ${best['cpa']:.0f} CPA."],
                "Increase budget gradually by 15-20% and monitor CPA for the next 3-5 days.",
                entity={"campaign": best["name"], "target_cpa": target_cpa},
                action_type="scale_winner",
                confidence="medium",
            ))

    low_ctr = [c for c in active_spenders if c["impressions"] >= 500 and c["ctr"] > 0 and c["ctr"] < 0.75]
    if low_ctr:
        low_ctr.sort(key=lambda c: c["ctr"])
        names = ", ".join(f'{c["name"]} ({c["ctr"]:.2f}% CTR)' for c in low_ctr[:4])
        findings.append(_finding(
            "low_ctr_paid_campaigns",
            "medium",
            "paid_media",
            "Rewrite ads that people are ignoring",
            "Low CTR means the audience is seeing ads but not finding the message compelling enough to click.",
            [names],
            "Test sharper hooks tied to the service, city, offer, and proof instead of changing budget first.",
            entity={"campaigns": [c["name"] for c in low_ctr[:6]]},
            action_type="creative_test",
            confidence="medium",
        ))

    return findings


def _creative_findings(analysis, target_cpa):
    meta = analysis.get("meta_business") or {}
    findings = []
    metrics = meta.get("metrics") or {}
    frequency = _to_float(metrics.get("frequency"), 0.0)
    spend = _to_float(metrics.get("spend"), 0.0)
    results = _to_float(metrics.get("results"), 0.0)
    cpr = _to_float(metrics.get("cost_per_result"), 0.0)
    if cpr <= 0 and spend > 0 and results > 0:
        cpr = spend / results

    fatigue = frequency >= 3.5 and spend >= 50
    expensive_fatigue = bool(target_cpa and cpr > target_cpa * 1.15)
    if fatigue:
        findings.append(_finding(
            "meta_frequency_fatigue",
            "high" if expensive_fatigue else "medium",
            "meta_ads",
            "Refresh Meta creative before the audience burns out",
            f"Meta frequency is {frequency:.1f}, which is high enough to create fatigue risk.",
            [f"Spend ${spend:.0f}, results {results:.0f}, cost per result ${cpr:.0f}." if cpr else f"Spend ${spend:.0f}, results {results:.0f}."],
            "Launch a new creative angle or rotate the winning offer into a fresh image/video before raising budget.",
            entity={"frequency": round(frequency, 2), "spend": round(spend, 2), "cpr": round(cpr, 2) if cpr else None},
            action_type="creative_refresh",
            confidence="high" if frequency >= 4.5 else "medium",
        ))

    top_ads = list(meta.get("top_ads") or [])
    weak_ads = []
    for ad in top_ads:
        ad_spend = _get_metric(ad, "spend")
        ad_results = _get_metric(ad, "results", "conversions")
        ad_ctr = _get_metric(ad, "ctr")
        if ad_spend >= 20 and ad_results <= 0:
            weak_ads.append((ad_spend, _clean_name(ad, "Unnamed ad"), ad))
        elif ad_ctr and ad_ctr < 0.7 and ad_spend >= 20:
            weak_ads.append((ad_spend, _clean_name(ad, "Unnamed ad"), ad))
    if weak_ads:
        weak_ads.sort(reverse=True, key=lambda item: item[0])
        names = ", ".join(f"{name} (${spend:.0f})" for spend, name, _ in weak_ads[:4])
        findings.append(_finding(
            "meta_weak_ad_creative",
            "medium",
            "meta_ads",
            "Replace weak Meta ad variations",
            "Some ad variations are absorbing budget without enough response.",
            [names],
            "Pause the weakest variation and build the replacement from the best-performing angle, not from scratch.",
            entity={"ads": [item[1] for item in weak_ads[:6]]},
            action_type="creative_test",
            confidence="medium",
        ))
    return findings


def _data_gaps(analysis, brand, target_cpa_source):
    gaps = []
    google = analysis.get("google_ads") or {}
    meta = analysis.get("meta_business") or {}
    if google and not google.get("search_terms"):
        gaps.append({
            "key": "google_search_terms_missing",
            "label": "Google search-term data missing",
            "why_it_matters": "WARREN cannot identify wasted queries or negative-keyword opportunities without it.",
            "fix": "Pull search_term_view rows and keep campaign/ad group IDs with cost, clicks, and conversions.",
        })
    search_terms = google.get("search_terms") or []
    if google and search_terms and not any((row.get("ad_group_id") or row.get("ad_group_name")) for row in search_terms):
        gaps.append({
            "key": "google_ad_group_context_missing",
            "label": "Google ad-group context is thin",
            "why_it_matters": "WARREN can find bad terms faster when it knows which ad group and campaign they came from.",
            "fix": "Include ad group IDs/names and keyword/match-type context in the search-term pull.",
        })
    if meta and not meta.get("top_ads"):
        gaps.append({
            "key": "meta_ad_level_missing",
            "label": "Meta ad-level data missing",
            "why_it_matters": "Campaign totals hide creative fatigue and weak ad variations.",
            "fix": "Pull ad-level insights with spend, reach, frequency, CTR, actions, and creative name.",
        })
    if meta and not any((_get_metric(row, "frequency") > 0) for row in meta.get("campaign_analysis") or []):
        gaps.append({
            "key": "meta_frequency_by_campaign_missing",
            "label": "Meta campaign frequency missing",
            "why_it_matters": "Creative fatigue should be diagnosed by campaign/ad set, not only account average.",
            "fix": "Store frequency on campaign, ad set, and ad-level insight rows.",
        })
    if target_cpa_source == "missing":
        gaps.append({
            "key": "target_cpa_missing",
            "label": "Target CPA not set",
            "why_it_matters": "WARREN can flag obvious waste, but cannot judge scale decisions precisely without the owner target.",
            "fix": "Set the brand KPI target CPA or connect closed-won revenue so target CPA can be inferred.",
        })
    if not ((analysis.get("crm_revenue") or {}).get("totals") or {}).get("revenue") and not ((analysis.get("roas") or {}).get("attributed_revenue")):
        gaps.append({
            "key": "revenue_attribution_missing",
            "label": "Closed revenue is not tied back to ads",
            "why_it_matters": "Lead volume alone can make bad leads look profitable.",
            "fix": "Connect CRM/job revenue and preserve source/campaign IDs through the lead lifecycle.",
        })
    return gaps


def _summary(analysis, campaigns, target_cpa, target_cpa_source):
    google_metrics = ((analysis.get("google_ads") or {}).get("metrics") or {})
    meta_metrics = ((analysis.get("meta_business") or {}).get("metrics") or {})
    total_spend = _to_float(google_metrics.get("spend"), 0.0) + _to_float(meta_metrics.get("spend"), 0.0)
    total_results = _to_float(google_metrics.get("results"), 0.0) + _to_float(meta_metrics.get("results"), 0.0)
    if total_spend <= 0:
        total_spend = sum(c["spend"] for c in campaigns)
    if total_results <= 0:
        total_results = sum(c["results"] for c in campaigns)
    blended_cpa = round(total_spend / total_results, 2) if total_spend > 0 and total_results > 0 else None
    return {
        "total_spend": round(total_spend, 2),
        "total_results": round(total_results, 2),
        "blended_cpa": blended_cpa,
        "target_cpa": target_cpa,
        "target_cpa_source": target_cpa_source,
        "google_spend": _round_money(google_metrics.get("spend")),
        "meta_spend": _round_money(meta_metrics.get("spend")),
        "active_campaigns": len([c for c in campaigns if c["spend"] > 0]),
        "period": analysis.get("period") or {},
    }


def _split_findings(findings):
    return {
        "waste": [f for f in findings if f.get("action_type") == "cut_waste"],
        "scale": [f for f in findings if f.get("action_type") == "scale_winner"],
        "creative": [f for f in findings if f.get("action_type") in {"creative_refresh", "creative_test"}],
    }


def build_ad_intelligence(analysis, brand=None):
    """Return normalized paid-media diagnosis for Google Ads and Meta."""
    analysis = analysis or {}
    brand = brand or analysis.get("client_config") or {}
    target_cpa, target_source = _target_cpa(analysis, brand)
    campaigns = _campaigns(analysis)

    findings = []
    findings.extend(_search_term_findings(analysis, target_cpa))
    findings.extend(_campaign_findings(campaigns, target_cpa))
    findings.extend(_creative_findings(analysis, target_cpa))

    severity_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    findings.sort(key=lambda f: (severity_rank.get(f.get("severity"), 9), -_to_float((f.get("entity") or {}).get("wasted_spend"), 0.0)))
    split = _split_findings(findings)
    gaps = _data_gaps(analysis, brand, target_source)

    return {
        "summary": _summary(analysis, campaigns, target_cpa, target_source),
        "campaigns": campaigns[:50],
        "findings": findings[:20],
        "next_best_actions": findings[:5],
        "waste": split["waste"][:10],
        "scale": split["scale"][:10],
        "creative": split["creative"][:10],
        "data_gaps": gaps[:10],
    }
