"""Production-ready starter kits for the Warren site builder."""

from textwrap import dedent


def _token(name):
    return "{{" + str(name or "").strip() + "}}"


_PAGE_META = {
    "home": {
        "label": "Home",
        "eyebrow": "Homepage system",
        "focus": "Lead with the core promise, immediate proof, and a direct path to action.",
    },
    "about": {
        "label": "About",
        "eyebrow": "Story and trust",
        "focus": "Humanize the company, establish credibility, and keep the copy grounded in real service delivery.",
    },
    "services": {
        "label": "Services",
        "eyebrow": "Service overview",
        "focus": "Make the offer easy to scan and keep the hierarchy clean for high-intent visitors.",
    },
    "service_detail": {
        "label": "Service Detail",
        "eyebrow": "Single service page",
        "focus": "Stay specific to the service, the outcome, and the next step. Keep proof close to the offer.",
    },
    "service_area": {
        "label": "Service Area",
        "eyebrow": "Local SEO page",
        "focus": "Anchor the copy in the location, show local trust, and keep the CTA visible without overstuffing keywords.",
    },
    "contact": {
        "label": "Contact",
        "eyebrow": "Contact page",
        "focus": "Reduce friction, show real contact details, and make the response expectation obvious.",
    },
    "faq": {
        "label": "FAQ",
        "eyebrow": "Objection handling",
        "focus": "Answer practical questions clearly and support the structured data output without sounding robotic.",
    },
    "testimonials": {
        "label": "Testimonials",
        "eyebrow": "Proof page",
        "focus": "Let customer outcomes do the heavy lifting. Keep trust cues and the CTA present but restrained.",
    },
    "landing_page": {
        "label": "Landing Page",
        "eyebrow": "Focused campaign page",
        "focus": "Prioritize one offer, one audience, one CTA path, and fast-scanning proof blocks.",
    },
}


def _clean(value):
    return dedent(value).strip()


def _page_type_support(page_type):
    meta = _PAGE_META[page_type]
    return {
        "label": meta["label"],
        "eyebrow": meta["eyebrow"],
        "focus": meta["focus"],
        "title": _token("page_title"),
        "subtitle": _token("page_excerpt"),
    }


def _nav_template(kit_slug, kit_name, description, modifier):
    html = _clean(
        f"""
        <header class="sb-kit-nav sb-kit-nav--{kit_slug} sb-kit-nav--{modifier}">
            <div class="sb-kit-nav__inner">
                <a class="sb-kit-nav__brand" href="/">{_token('business_name')}</a>
                <nav class="sb-kit-nav__links" aria-label="Primary">
                    <a href="/">Home</a>
                    <a href="/services">Services</a>
                    <a href="/about">About</a>
                    <a href="/contact">Contact</a>
                </nav>
                <div class="sb-kit-nav__actions">
                    <a class="sb-kit-nav__phone" href="tel:{_token('phone')}">{_token('phone')}</a>
                    <a class="sb-kit-nav__button" href="#contact">{_token('cta_text')}</a>
                </div>
            </div>
        </header>
        """
    )
    css = _clean(
        f"""
        .sb-kit-nav--{kit_slug} {{
            position: sticky;
            top: 0;
            z-index: 30;
            backdrop-filter: blur(14px);
            background: rgba(255, 255, 255, 0.92);
            border-bottom: 1px solid rgba(15, 23, 42, 0.08);
        }}
        .sb-kit-nav--{kit_slug} .sb-kit-nav__inner {{
            width: min(1180px, calc(100% - 2rem));
            margin: 0 auto;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 1rem;
            padding: 0.9rem 0;
        }}
        .sb-kit-nav--{kit_slug} .sb-kit-nav__brand {{
            font-family: var(--sb-font-heading, 'Oswald', sans-serif);
            font-size: 1.12rem;
            letter-spacing: 0.03em;
            text-decoration: none;
            color: var(--sb-text, #0f172a);
        }}
        .sb-kit-nav--{kit_slug} .sb-kit-nav__links {{
            display: flex;
            flex-wrap: wrap;
            gap: 1rem;
        }}
        .sb-kit-nav--{kit_slug} .sb-kit-nav__links a,
        .sb-kit-nav--{kit_slug} .sb-kit-nav__phone {{
            color: var(--sb-text, #0f172a);
            text-decoration: none;
            font-size: 0.92rem;
            opacity: 0.86;
        }}
        .sb-kit-nav--{kit_slug} .sb-kit-nav__actions {{
            display: flex;
            align-items: center;
            gap: 0.8rem;
        }}
        .sb-kit-nav--{kit_slug} .sb-kit-nav__button {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-height: 42px;
            padding: 0 1rem;
            border-radius: 999px;
            text-decoration: none;
            color: #fff;
            background: linear-gradient(135deg, var(--sb-primary, #1d4ed8), var(--sb-accent, #f97316));
            box-shadow: 0 14px 30px rgba(15, 23, 42, 0.12);
        }}
        @media (max-width: 860px) {{
            .sb-kit-nav--{kit_slug} .sb-kit-nav__inner {{
                flex-direction: column;
                align-items: stretch;
            }}
            .sb-kit-nav--{kit_slug} .sb-kit-nav__links,
            .sb-kit-nav--{kit_slug} .sb-kit-nav__actions {{
                justify-content: center;
            }}
        }}
        """
    )
    return {
        "name": f"{kit_name} Navigation",
        "category": "navigation",
        "page_types": "all",
        "html_content": html,
        "css_content": css,
        "description": description,
        "sort_order": 10,
        "is_active": 1,
    }


def _footer_template(kit_slug, kit_name, description, modifier):
    html = _clean(
        f"""
        <footer class="sb-kit-footer sb-kit-footer--{kit_slug} sb-kit-footer--{modifier}">
            <div class="sb-kit-footer__inner">
                <div>
                    <p class="sb-kit-footer__eyebrow">Built for local trust</p>
                    <strong>{_token('business_name')}</strong>
                    <p>{_token('service_area')}</p>
                </div>
                <div>
                    <p class="sb-kit-footer__eyebrow">Reach the team</p>
                    <p><a href="tel:{_token('phone')}">{_token('phone')}</a></p>
                    <p>{_token('address')}</p>
                </div>
                <div>
                    <p class="sb-kit-footer__eyebrow">Next step</p>
                    <p>Need help fast? Use the main CTA or call directly to get started.</p>
                    <a class="sb-kit-footer__button" href="#contact">{_token('cta_text')}</a>
                </div>
            </div>
        </footer>
        """
    )
    css = _clean(
        f"""
        .sb-kit-footer--{kit_slug} {{
            margin-top: clamp(2rem, 5vw, 4rem);
            background: linear-gradient(135deg, rgba(15, 23, 42, 0.94), rgba(30, 41, 59, 0.96));
            color: rgba(255, 255, 255, 0.82);
            border-radius: 28px 28px 0 0;
            overflow: hidden;
        }}
        .sb-kit-footer--{kit_slug} .sb-kit-footer__inner {{
            width: min(1180px, calc(100% - 2rem));
            margin: 0 auto;
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 1.25rem;
            padding: clamp(1.4rem, 3vw, 2.25rem) 0;
        }}
        .sb-kit-footer--{kit_slug} a {{
            color: #fff;
            text-decoration: none;
        }}
        .sb-kit-footer--{kit_slug} .sb-kit-footer__eyebrow {{
            text-transform: uppercase;
            letter-spacing: 0.16em;
            font-size: 0.7rem;
            color: rgba(255, 255, 255, 0.55);
            margin-bottom: 0.4rem;
        }}
        .sb-kit-footer--{kit_slug} .sb-kit-footer__button {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            margin-top: 0.4rem;
            min-height: 42px;
            padding: 0 1rem;
            border-radius: 999px;
            background: linear-gradient(135deg, var(--sb-accent, #f97316), var(--sb-primary, #1d4ed8));
        }}
        @media (max-width: 860px) {{
            .sb-kit-footer--{kit_slug} .sb-kit-footer__inner {{
                grid-template-columns: 1fr;
            }}
        }}
        """
    )
    return {
        "name": f"{kit_name} Footer",
        "category": "footer",
        "page_types": "all",
        "html_content": html,
        "css_content": css,
        "description": description,
        "sort_order": 90,
        "is_active": 1,
    }


def _lead_engine_shell(page_type, kit_slug, kit_name):
    meta = _page_type_support(page_type)
    html = _clean(
        f"""
        <div class="sb-kit-shell sb-kit-shell--{kit_slug} sb-kit-shell--lead-engine sb-kit-shell--{page_type}">
            <section class="sb-kit-shell__banner">
                <div class="sb-kit-shell__copy">
                    <p class="sb-kit-shell__eyebrow">{meta['eyebrow']}</p>
                    <h1>{meta['title']}</h1>
                    <p class="sb-kit-shell__summary">{meta['subtitle']}</p>
                    <p class="sb-kit-shell__focus">{meta['focus']}</p>
                    <div class="sb-kit-shell__actions">
                        <a href="#contact">{_token('cta_text')}</a>
                        <a class="sb-kit-shell__phone" href="tel:{_token('phone')}">{_token('phone')}</a>
                    </div>
                </div>
                <aside class="sb-kit-shell__rail">
                    <strong>Fast-response layout</strong>
                    <ul>
                        <li>Keep trust proof above the first hard ask.</li>
                        <li>Use short blocks and visible conversion points.</li>
                        <li>Anchor the offer in {_token('service_area')}.</li>
                    </ul>
                </aside>
            </section>
            <div class="sb-kit-shell__frame">{_token('page_content')}</div>
            <section class="sb-kit-shell__close">
                <strong>Need a quicker answer?</strong>
                <p>Use the primary CTA or call {_token('phone')} to move fast.</p>
            </section>
        </div>
        """
    )
    css = _clean(
        f"""
        .sb-kit-shell--{kit_slug}.sb-kit-shell--lead-engine {{
            display: grid;
            gap: 1.25rem;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--lead-engine .sb-kit-shell__banner {{
            display: grid;
            grid-template-columns: minmax(0, 1.6fr) minmax(280px, 0.9fr);
            gap: 1rem;
            padding: clamp(1.15rem, 2.6vw, 1.8rem);
            border-radius: 24px;
            background: linear-gradient(135deg, rgba(15, 76, 129, 0.1), rgba(255, 122, 24, 0.12));
            border: 1px solid rgba(15, 76, 129, 0.14);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--lead-engine .sb-kit-shell__eyebrow {{
            margin: 0 0 0.4rem;
            text-transform: uppercase;
            letter-spacing: 0.16em;
            font-size: 0.72rem;
            color: var(--sb-primary, #0f4c81);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--lead-engine h1 {{
            margin: 0;
            font-size: clamp(1.9rem, 4vw, 3.2rem);
            line-height: 1.02;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--lead-engine .sb-kit-shell__summary {{
            margin: 0.65rem 0 0;
            font-size: 1rem;
            color: rgba(15, 23, 42, 0.78);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--lead-engine .sb-kit-shell__focus {{
            margin: 0.6rem 0 0;
            color: rgba(15, 23, 42, 0.72);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--lead-engine .sb-kit-shell__actions {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.8rem;
            margin-top: 1rem;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--lead-engine .sb-kit-shell__actions a {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-height: 44px;
            padding: 0 1rem;
            border-radius: 999px;
            text-decoration: none;
            background: var(--sb-primary, #0f4c81);
            color: #fff;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--lead-engine .sb-kit-shell__phone {{
            background: #fff;
            color: var(--sb-text, #102038);
            border: 1px solid rgba(15, 23, 42, 0.12);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--lead-engine .sb-kit-shell__rail {{
            background: rgba(255, 255, 255, 0.92);
            border-radius: 20px;
            padding: 1rem;
            border: 1px solid rgba(15, 23, 42, 0.08);
            box-shadow: 0 20px 40px rgba(15, 23, 42, 0.08);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--lead-engine .sb-kit-shell__rail ul {{
            margin: 0.75rem 0 0;
            padding-left: 1.1rem;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--lead-engine .sb-kit-shell__frame {{
            background: #fff;
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 24px;
            padding: clamp(1rem, 2vw, 1.5rem);
            box-shadow: 0 18px 35px rgba(15, 23, 42, 0.06);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--lead-engine .sb-kit-shell__close {{
            padding: 1rem 1.15rem;
            border-radius: 20px;
            background: linear-gradient(135deg, rgba(15, 76, 129, 0.08), rgba(255, 122, 24, 0.1));
        }}
        @media (max-width: 860px) {{
            .sb-kit-shell--{kit_slug}.sb-kit-shell--lead-engine .sb-kit-shell__banner {{
                grid-template-columns: 1fr;
            }}
        }}
        """
    )
    return {
        "name": f"{kit_name} - {meta['label']} Shell",
        "category": "page_shell",
        "page_types": page_type,
        "html_content": html,
        "css_content": css,
        "description": f"Conversion-first page shell for the {meta['label'].lower()} experience. Uses a compact banner, proof rail, framed content area, and a strong closing CTA.",
        "sort_order": 20,
        "is_active": 1,
    }


def _premium_authority_shell(page_type, kit_slug, kit_name):
    meta = _page_type_support(page_type)
    html = _clean(
        f"""
        <div class="sb-kit-shell sb-kit-shell--{kit_slug} sb-kit-shell--premium-authority sb-kit-shell--{page_type}">
            <section class="sb-kit-shell__banner">
                <div class="sb-kit-shell__copy">
                    <p class="sb-kit-shell__eyebrow">{meta['eyebrow']}</p>
                    <h1>{meta['title']}</h1>
                    <p class="sb-kit-shell__summary">{meta['subtitle']}</p>
                </div>
                <div class="sb-kit-shell__proof-card">
                    <p class="sb-kit-shell__proof-title">Position the brand with restraint</p>
                    <p>{meta['focus']}</p>
                    <ul>
                        <li>Show credentials before urgency.</li>
                        <li>Keep proof elegant and specific.</li>
                        <li>Let {_token('business_name')} feel established, not salesy.</li>
                    </ul>
                </div>
            </section>
            <div class="sb-kit-shell__frame">{_token('page_content')}</div>
            <section class="sb-kit-shell__close">
                <span>Premium next step</span>
                <p>Invite the prospect into a confident consultation path with {_token('cta_text').strip()}.</p>
            </section>
        </div>
        """
    )
    css = _clean(
        f"""
        .sb-kit-shell--{kit_slug}.sb-kit-shell--premium-authority {{
            display: grid;
            gap: 1.35rem;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--premium-authority .sb-kit-shell__banner {{
            display: grid;
            grid-template-columns: minmax(0, 1.4fr) minmax(320px, 0.8fr);
            gap: 1.1rem;
            align-items: stretch;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--premium-authority .sb-kit-shell__copy,
        .sb-kit-shell--{kit_slug}.sb-kit-shell--premium-authority .sb-kit-shell__proof-card {{
            border-radius: 24px;
            padding: clamp(1.15rem, 2.4vw, 1.75rem);
            background: rgba(255, 255, 255, 0.86);
            border: 1px solid rgba(15, 23, 42, 0.08);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--premium-authority .sb-kit-shell__copy {{
            background: linear-gradient(180deg, rgba(255, 255, 255, 0.9), rgba(248, 242, 232, 0.9));
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--premium-authority .sb-kit-shell__eyebrow {{
            margin: 0 0 0.45rem;
            font-size: 0.72rem;
            letter-spacing: 0.18em;
            text-transform: uppercase;
            color: var(--sb-accent, #b88746);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--premium-authority h1 {{
            margin: 0;
            font-size: clamp(2rem, 4vw, 3.35rem);
            line-height: 1.08;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--premium-authority .sb-kit-shell__summary {{
            margin: 0.8rem 0 0;
            max-width: 48rem;
            color: rgba(29, 36, 48, 0.76);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--premium-authority .sb-kit-shell__proof-title {{
            margin: 0 0 0.5rem;
            font-family: var(--sb-font-heading, 'Libre Baskerville', serif);
            color: var(--sb-primary, #0f2a3d);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--premium-authority .sb-kit-shell__proof-card ul {{
            margin: 0.75rem 0 0;
            padding-left: 1.1rem;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--premium-authority .sb-kit-shell__frame {{
            border-radius: 26px;
            padding: clamp(1.15rem, 2.2vw, 1.65rem);
            background: rgba(255, 255, 255, 0.88);
            border: 1px solid rgba(15, 23, 42, 0.07);
            box-shadow: 0 24px 46px rgba(15, 23, 42, 0.05);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--premium-authority .sb-kit-shell__close {{
            display: grid;
            gap: 0.35rem;
            padding: 1.1rem 1.2rem;
            border-radius: 22px;
            background: linear-gradient(135deg, rgba(184, 135, 70, 0.12), rgba(15, 42, 61, 0.08));
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--premium-authority .sb-kit-shell__close span {{
            text-transform: uppercase;
            letter-spacing: 0.16em;
            font-size: 0.72rem;
            color: var(--sb-accent, #b88746);
        }}
        @media (max-width: 860px) {{
            .sb-kit-shell--{kit_slug}.sb-kit-shell--premium-authority .sb-kit-shell__banner {{
                grid-template-columns: 1fr;
            }}
        }}
        """
    )
    return {
        "name": f"{kit_name} - {meta['label']} Shell",
        "category": "page_shell",
        "page_types": page_type,
        "html_content": html,
        "css_content": css,
        "description": f"Refined authority shell for the {meta['label'].lower()} page. Adds a restrained premium intro, proof panel, elegant body frame, and soft closing CTA.",
        "sort_order": 20,
        "is_active": 1,
    }


def _neighborhood_trust_shell(page_type, kit_slug, kit_name):
    meta = _page_type_support(page_type)
    html = _clean(
        f"""
        <div class="sb-kit-shell sb-kit-shell--{kit_slug} sb-kit-shell--neighborhood-trust sb-kit-shell--{page_type}">
            <section class="sb-kit-shell__banner">
                <div>
                    <p class="sb-kit-shell__eyebrow">{meta['eyebrow']}</p>
                    <h1>{meta['title']}</h1>
                    <p class="sb-kit-shell__summary">{meta['subtitle']}</p>
                </div>
                <div class="sb-kit-shell__badges">
                    <span>Friendly service</span>
                    <span>Local trust</span>
                    <span>Clear follow-through</span>
                </div>
            </section>
            <aside class="sb-kit-shell__trust-card">
                <strong>Keep the tone warm and grounded</strong>
                <p>{meta['focus']}</p>
            </aside>
            <div class="sb-kit-shell__frame">{_token('page_content')}</div>
            <section class="sb-kit-shell__close">
                <p>Give visitors a comfortable next step, then make it easy to call {_token('phone')} if they want a human answer.</p>
            </section>
        </div>
        """
    )
    css = _clean(
        f"""
        .sb-kit-shell--{kit_slug}.sb-kit-shell--neighborhood-trust {{
            display: grid;
            gap: 1rem;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--neighborhood-trust .sb-kit-shell__banner {{
            display: grid;
            gap: 0.9rem;
            padding: clamp(1.15rem, 2.6vw, 1.8rem);
            border-radius: 26px;
            background: linear-gradient(135deg, rgba(47, 111, 95, 0.1), rgba(225, 169, 72, 0.14));
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--neighborhood-trust .sb-kit-shell__eyebrow {{
            margin: 0 0 0.4rem;
            text-transform: uppercase;
            letter-spacing: 0.16em;
            font-size: 0.72rem;
            color: var(--sb-primary, #2f6f5f);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--neighborhood-trust h1 {{
            margin: 0;
            font-size: clamp(1.95rem, 4vw, 3rem);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--neighborhood-trust .sb-kit-shell__summary {{
            margin: 0.7rem 0 0;
            color: rgba(36, 49, 45, 0.78);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--neighborhood-trust .sb-kit-shell__badges {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.6rem;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--neighborhood-trust .sb-kit-shell__badges span {{
            display: inline-flex;
            align-items: center;
            min-height: 38px;
            padding: 0 0.85rem;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.88);
            border: 1px solid rgba(47, 111, 95, 0.12);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--neighborhood-trust .sb-kit-shell__trust-card {{
            padding: 1rem 1.1rem;
            border-radius: 22px;
            background: rgba(255, 255, 255, 0.86);
            border: 1px solid rgba(47, 111, 95, 0.12);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--neighborhood-trust .sb-kit-shell__frame {{
            border-radius: 24px;
            padding: clamp(1rem, 2vw, 1.4rem);
            background: #fff;
            border: 1px solid rgba(36, 49, 45, 0.08);
            box-shadow: 0 18px 35px rgba(36, 49, 45, 0.05);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--neighborhood-trust .sb-kit-shell__close {{
            padding: 1rem 1.15rem;
            border-radius: 22px;
            background: rgba(225, 169, 72, 0.12);
        }}
        """
    )
    return {
        "name": f"{kit_name} - {meta['label']} Shell",
        "category": "page_shell",
        "page_types": page_type,
        "html_content": html,
        "css_content": css,
        "description": f"Warm, community-trust shell for the {meta['label'].lower()} page. Uses a friendly banner, trust badges, a soft content frame, and a lower-friction close.",
        "sort_order": 20,
        "is_active": 1,
    }


def _service_atlas_shell(page_type, kit_slug, kit_name):
    meta = _page_type_support(page_type)
    html = _clean(
        f"""
        <div class="sb-kit-shell sb-kit-shell--{kit_slug} sb-kit-shell--service-atlas sb-kit-shell--{page_type}">
            <section class="sb-kit-shell__banner">
                <div class="sb-kit-shell__copy">
                    <p class="sb-kit-shell__eyebrow">{meta['eyebrow']}</p>
                    <h1>{meta['title']}</h1>
                    <p class="sb-kit-shell__summary">{meta['subtitle']}</p>
                </div>
                <div class="sb-kit-shell__grid">
                    <div>
                        <span>Service area</span>
                        <strong>{_token('service_area')}</strong>
                    </div>
                    <div>
                        <span>Call now</span>
                        <strong>{_token('phone')}</strong>
                    </div>
                    <div>
                        <span>Brand</span>
                        <strong>{_token('business_name')}</strong>
                    </div>
                    <div>
                        <span>Content focus</span>
                        <strong>{meta['label']}</strong>
                    </div>
                </div>
            </section>
            <div class="sb-kit-shell__frame">
                <p class="sb-kit-shell__focus">{meta['focus']}</p>
                {_token('page_content')}
            </div>
            <section class="sb-kit-shell__close">
                <strong>Make the next step obvious.</strong>
                <p>Keep the CTA consistent across location and service pages so the build stays scannable at scale.</p>
            </section>
        </div>
        """
    )
    css = _clean(
        f"""
        .sb-kit-shell--{kit_slug}.sb-kit-shell--service-atlas {{
            display: grid;
            gap: 1.1rem;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--service-atlas .sb-kit-shell__banner {{
            display: grid;
            grid-template-columns: minmax(0, 1.2fr) minmax(320px, 1fr);
            gap: 1rem;
            padding: clamp(1.15rem, 2.5vw, 1.75rem);
            border-radius: 26px;
            background: linear-gradient(135deg, rgba(23, 78, 166, 0.08), rgba(24, 185, 132, 0.12));
            border: 1px solid rgba(23, 78, 166, 0.1);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--service-atlas .sb-kit-shell__eyebrow {{
            margin: 0 0 0.4rem;
            text-transform: uppercase;
            letter-spacing: 0.16em;
            font-size: 0.72rem;
            color: var(--sb-primary, #174ea6);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--service-atlas h1 {{
            margin: 0;
            font-size: clamp(1.9rem, 4vw, 3rem);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--service-atlas .sb-kit-shell__summary {{
            margin: 0.7rem 0 0;
            color: rgba(22, 48, 71, 0.76);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--service-atlas .sb-kit-shell__grid {{
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.75rem;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--service-atlas .sb-kit-shell__grid div {{
            padding: 0.9rem;
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.9);
            border: 1px solid rgba(23, 78, 166, 0.09);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--service-atlas .sb-kit-shell__grid span {{
            display: block;
            font-size: 0.72rem;
            text-transform: uppercase;
            letter-spacing: 0.14em;
            color: rgba(22, 48, 71, 0.58);
            margin-bottom: 0.25rem;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--service-atlas .sb-kit-shell__frame {{
            border-radius: 24px;
            padding: clamp(1rem, 2vw, 1.45rem);
            background: #fff;
            border: 1px solid rgba(22, 48, 71, 0.08);
            box-shadow: 0 20px 36px rgba(22, 48, 71, 0.05);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--service-atlas .sb-kit-shell__focus {{
            margin: 0 0 1rem;
            padding: 0.9rem 1rem;
            border-radius: 16px;
            background: rgba(23, 78, 166, 0.06);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--service-atlas .sb-kit-shell__close {{
            padding: 1rem 1.15rem;
            border-radius: 22px;
            background: rgba(24, 185, 132, 0.12);
        }}
        @media (max-width: 860px) {{
            .sb-kit-shell--{kit_slug}.sb-kit-shell--service-atlas .sb-kit-shell__banner,
            .sb-kit-shell--{kit_slug}.sb-kit-shell--service-atlas .sb-kit-shell__grid {{
                grid-template-columns: 1fr;
            }}
        }}
        """
    )
    return {
        "name": f"{kit_name} - {meta['label']} Shell",
        "category": "page_shell",
        "page_types": page_type,
        "html_content": html,
        "css_content": css,
        "description": f"Scannable service-and-location shell for the {meta['label'].lower()} page. Uses a structured intro grid, framed content area, and a consistent CTA close for scale.",
        "sort_order": 20,
        "is_active": 1,
    }


def _authority_local_operator_shell(page_type, kit_slug, kit_name):
    meta = _page_type_support(page_type)
    html = _clean(
        f"""
        <div class="sb-kit-shell sb-kit-shell--{kit_slug} sb-kit-shell--authority-local-operator sb-kit-shell--{page_type}">
            <section class="sb-kit-shell__hero-shell">
                <div class="sb-kit-shell__copy">
                    <p class="sb-kit-shell__eyebrow">{meta['eyebrow']}</p>
                    <h1>{meta['title']}</h1>
                    <p class="sb-kit-shell__summary">{meta['subtitle']}</p>
                    <div class="sb-kit-shell__trust-strip">
                        <span>Fast quotes</span>
                        <span>No contracts</span>
                        <span>Local reliability</span>
                    </div>
                    <div class="sb-kit-shell__actions">
                        <a href="#contact">{_token('cta_text')}</a>
                        <a class="sb-kit-shell__phone" href="tel:{_token('phone')}">{_token('phone')}</a>
                    </div>
                </div>
                <aside class="sb-kit-shell__ops-rail">
                    <p class="sb-kit-shell__rail-label">Operator notes</p>
                    <strong>Make the next step frictionless.</strong>
                    <p>{meta['focus']}</p>
                    <div class="sb-kit-shell__ops-grid">
                        <div>
                            <span>Service area</span>
                            <strong>{_token('service_area')}</strong>
                        </div>
                        <div>
                            <span>Brand posture</span>
                            <strong>Authority and speed</strong>
                        </div>
                        <div>
                            <span>Primary path</span>
                            <strong>Quote or call now</strong>
                        </div>
                        <div>
                            <span>Proof cue</span>
                            <strong>Reviews before the ask</strong>
                        </div>
                    </div>
                </aside>
            </section>
            <section class="sb-kit-shell__proof-band">
                <div>
                    <span>Built for owner-operators</span>
                    <strong>Keep the page sharp, direct, and visibly local.</strong>
                </div>
                <p>Front-load trust, move fast into the offer, and keep the quote path visible without making the page feel cheap or spammy.</p>
            </section>
            <div class="sb-kit-shell__frame">{_token('page_content')}</div>
            <section class="sb-kit-shell__close">
                <div>
                    <span>Ready to move?</span>
                    <strong>Give visitors the easiest next step on the page.</strong>
                </div>
                <p>Use the main CTA, the phone option, or the quote tool without making people hunt for the path.</p>
            </section>
        </div>
        """
    )
    css = _clean(
        f"""
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator {{
            display: grid;
            gap: 1.15rem;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__hero-shell {{
            display: grid;
            grid-template-columns: minmax(0, 1.25fr) minmax(320px, .95fr);
            gap: 1rem;
            align-items: stretch;
            padding: clamp(1.2rem, 2.8vw, 1.95rem);
            border-radius: 28px;
            background:
                radial-gradient(circle at top right, rgba(255, 183, 3, 0.18) 0%, rgba(255,255,255,0) 38%),
                linear-gradient(145deg, rgba(11, 31, 58, 0.98), rgba(17, 37, 66, 0.96));
            color: #fff;
            overflow: hidden;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__copy {{
            display: grid;
            gap: .95rem;
            align-content: center;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__eyebrow {{
            margin: 0;
            width: fit-content;
            padding: .45rem .82rem;
            border-radius: 999px;
            background: rgba(255,255,255,.08);
            text-transform: uppercase;
            letter-spacing: .16em;
            font-size: .72rem;
            color: #ffb703;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator h1 {{
            margin: 0;
            max-width: 10ch;
            font-size: clamp(2.2rem, 5vw, 4.4rem);
            line-height: .92;
            letter-spacing: -.05em;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__summary {{
            margin: 0;
            max-width: 40rem;
            color: rgba(255,255,255,.82);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__trust-strip {{
            display: flex;
            flex-wrap: wrap;
            gap: .65rem;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__trust-strip span {{
            display: inline-flex;
            align-items: center;
            min-height: 40px;
            padding: 0 .9rem;
            border-radius: 999px;
            background: rgba(255,255,255,.09);
            border: 1px solid rgba(255,255,255,.08);
            color: rgba(255,255,255,.92);
            font-size: .9rem;
            font-weight: 600;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__actions {{
            display: flex;
            flex-wrap: wrap;
            gap: .8rem;
            margin-top: .2rem;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__actions a {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-height: 46px;
            padding: 0 1.05rem;
            border-radius: 999px;
            text-decoration: none;
            background: #ffb703;
            color: #0b1f3a;
            font-weight: 800;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__phone {{
            background: rgba(255,255,255,.08);
            border: 1px solid rgba(255,255,255,.1);
            color: #fff;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__ops-rail {{
            display: grid;
            gap: .8rem;
            padding: 1rem;
            border-radius: 22px;
            background: linear-gradient(180deg, rgba(255,255,255,.94), rgba(244,248,252,.88));
            color: #102038;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__rail-label {{
            margin: 0;
            text-transform: uppercase;
            letter-spacing: .16em;
            font-size: .7rem;
            color: rgba(16, 32, 56, .58);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__ops-grid {{
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: .7rem;
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__ops-grid div {{
            padding: .85rem;
            border-radius: 18px;
            background: rgba(255,255,255,.9);
            border: 1px solid rgba(11, 31, 58, .08);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__ops-grid span {{
            display: block;
            margin-bottom: .2rem;
            text-transform: uppercase;
            letter-spacing: .14em;
            font-size: .68rem;
            color: rgba(16, 32, 56, .55);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__proof-band {{
            display: grid;
            grid-template-columns: minmax(220px, .9fr) minmax(0, 1.1fr);
            gap: 1rem;
            padding: 1rem 1.1rem;
            border-radius: 22px;
            background: linear-gradient(135deg, rgba(46, 139, 87, 0.14), rgba(255, 183, 3, 0.18));
            border: 1px solid rgba(46, 139, 87, 0.1);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__proof-band span {{
            display: block;
            margin-bottom: .3rem;
            text-transform: uppercase;
            letter-spacing: .16em;
            font-size: .7rem;
            color: rgba(11, 31, 58, .58);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__frame {{
            border-radius: 26px;
            padding: clamp(1.1rem, 2.3vw, 1.7rem);
            background: linear-gradient(180deg, rgba(255,255,255,.98), rgba(248,250,252,.9));
            border: 1px solid rgba(11, 31, 58, .08);
            box-shadow: 0 22px 44px rgba(11, 31, 58, .08);
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__close {{
            display: grid;
            grid-template-columns: minmax(240px, .85fr) minmax(0, 1.15fr);
            gap: 1rem;
            padding: 1.05rem 1.15rem;
            border-radius: 22px;
            background: linear-gradient(135deg, rgba(11, 31, 58, 0.08), rgba(46, 139, 87, 0.12));
        }}
        .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__close span {{
            display: block;
            margin-bottom: .3rem;
            text-transform: uppercase;
            letter-spacing: .16em;
            font-size: .7rem;
            color: rgba(11, 31, 58, .58);
        }}
        @media (max-width: 860px) {{
            .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__hero-shell,
            .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__proof-band,
            .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__close,
            .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator .sb-kit-shell__ops-grid {{
                grid-template-columns: 1fr;
            }}
            .sb-kit-shell--{kit_slug}.sb-kit-shell--authority-local-operator h1 {{
                max-width: none;
            }}
        }}
        """
    )
    return {
        "name": f"{kit_name} - {meta['label']} Shell",
        "category": "page_shell",
        "page_types": page_type,
        "html_content": html,
        "css_content": css,
        "description": f"Fast-response authority shell for the {meta['label'].lower()} page. Uses a high-contrast split hero, operational trust rail, proof band, and a frictionless CTA close.",
        "sort_order": 20,
        "is_active": 1,
    }


def _kit_definition(name, slug, description, prompt_notes, theme, nav_desc, footer_desc, shell_builder, modifier, sort_order, is_default=False):
    templates = [
        _nav_template(slug, name, nav_desc, modifier),
        _footer_template(slug, name, footer_desc, modifier),
    ]
    for page_type in _PAGE_META:
        templates.append(shell_builder(page_type, slug, name))

    return {
        "theme": theme,
        "templates": templates,
        "site_template": {
            "name": name,
            "slug": slug,
            "description": description,
            "prompt_notes": prompt_notes,
            "sort_order": sort_order,
            "is_default": 1 if is_default else 0,
            "is_active": 1,
        },
    }


def get_production_site_kit_definitions():
    return [
        _kit_definition(
            name="Lead Engine",
            slug="lead-engine",
            description="Bold, conversion-first local service kit with compact copy, visible CTAs, and quick proof placement.",
            prompt_notes="Keep the pacing tight. Use short sections, decisive CTA rhythm, and trust proof high on the page.",
            theme={
                "name": "Lead Engine Theme",
                "description": "A high-conversion service-business theme with strong contrast and fast-response energy.",
                "primary_color": "#0f4c81",
                "secondary_color": "#0b2447",
                "accent_color": "#ff7a18",
                "text_color": "#102038",
                "bg_color": "#f7fbff",
                "font_heading": "Oswald",
                "font_body": "Manrope",
                "button_style": "pill",
                "layout_style": "hero-driven",
                "is_default": 1,
                "is_active": 1,
            },
            nav_desc="High-clarity sticky navigation with direct phone action and a strong primary CTA.",
            footer_desc="Conversion-oriented footer with contact details, service area context, and a final CTA.",
            shell_builder=_lead_engine_shell,
            modifier="bold",
            sort_order=10,
            is_default=True,
        ),
        _kit_definition(
            name="Premium Authority",
            slug="premium-authority",
            description="Refined, trust-heavy site kit for premium service brands that need restraint, expertise, and confidence.",
            prompt_notes="Keep the tone composed and specific. Let credentials, outcomes, and polish carry the sale before urgency does.",
            theme={
                "name": "Premium Authority Theme",
                "description": "A refined premium-service theme with soft neutrals, restrained contrast, and editorial spacing.",
                "primary_color": "#0f2a3d",
                "secondary_color": "#52606d",
                "accent_color": "#b88746",
                "text_color": "#1d2430",
                "bg_color": "#f7f3ee",
                "font_heading": "Libre Baskerville",
                "font_body": "DM Sans",
                "button_style": "rounded",
                "layout_style": "modern-sections",
                "is_default": 0,
                "is_active": 1,
            },
            nav_desc="Editorial-style navigation for premium brands with restrained hierarchy and a polished CTA treatment.",
            footer_desc="High-trust footer for premium service brands with contact details and calm closing language.",
            shell_builder=_premium_authority_shell,
            modifier="refined",
            sort_order=20,
        ),
        _kit_definition(
            name="Neighborhood Trust",
            slug="neighborhood-trust",
            description="Warm, approachable site kit built for community-trust brands that win with friendliness and follow-through.",
            prompt_notes="Keep the tone grounded and human. Use warmth, neighborhood familiarity, and clear next steps without sounding casual or sloppy.",
            theme={
                "name": "Neighborhood Trust Theme",
                "description": "A warm local-brand theme centered on friendliness, clarity, and family-safe trust.",
                "primary_color": "#2f6f5f",
                "secondary_color": "#498f7b",
                "accent_color": "#e1a948",
                "text_color": "#24312d",
                "bg_color": "#fbf7ef",
                "font_heading": "Archivo",
                "font_body": "Work Sans",
                "button_style": "rounded",
                "layout_style": "classic-stacked",
                "is_default": 0,
                "is_active": 1,
            },
            nav_desc="Friendly local-service navigation with soft contrast and an approachable primary CTA.",
            footer_desc="Warm trust footer that reinforces contact details, local presence, and an easy next step.",
            shell_builder=_neighborhood_trust_shell,
            modifier="warm",
            sort_order=30,
        ),
        _kit_definition(
            name="Service Atlas",
            slug="service-atlas",
            description="Scannable service-and-location kit for service-heavy brands that need breadth, clarity, and local SEO scale.",
            prompt_notes="Prioritize scannability, consistent headings, and clean service-area structure. Make the offer easy to browse at a glance.",
            theme={
                "name": "Service Atlas Theme",
                "description": "A structured service-grid theme designed for multi-service and multi-area brands that need scalable clarity.",
                "primary_color": "#174ea6",
                "secondary_color": "#3b82f6",
                "accent_color": "#18b984",
                "text_color": "#163047",
                "bg_color": "#f4f9ff",
                "font_heading": "Plus Jakarta Sans",
                "font_body": "IBM Plex Sans",
                "button_style": "pill",
                "layout_style": "card-grid",
                "is_default": 0,
                "is_active": 1,
            },
            nav_desc="Structured navigation built for large service menus, location pages, and clean scanning on desktop and mobile.",
            footer_desc="Service-directory footer that reinforces area coverage, phone-first contact, and consistent CTA language.",
            shell_builder=_service_atlas_shell,
            modifier="grid",
            sort_order=40,
        ),
        _kit_definition(
            name="Authority Local Operator",
            slug="authority-local-operator",
            description="High-converting local-service kit built for fast quotes, strong trust, and a visibly operational local brand posture.",
            prompt_notes="Keep the tone direct, confident, and local. Front-load reviews and reliability, keep CTAs visible in the first two sections, require FAQ support on service pages, and make the quote path feel immediate without becoming spammy.",
            theme={
                "name": "Authority Core",
                "description": "High-trust, fast-conversion local service theme with sharp contrast, bold CTA rhythm, and clean operational polish.",
                "primary_color": "#0B1F3A",
                "secondary_color": "#2E8B57",
                "accent_color": "#FFB703",
                "text_color": "#1A1A1A",
                "bg_color": "#FFFFFF",
                "font_heading": "Inter",
                "font_body": "Inter",
                "button_style": "rounded",
                "layout_style": "hero-driven",
                "is_default": 0,
                "is_active": 1,
            },
            nav_desc="Sharp sticky navigation with the brand left-aligned, operational trust in the layout, and an always-visible quote-first CTA.",
            footer_desc="Three-column local-operator footer that reinforces services, service areas, phone-first contact, and one final quote action.",
            shell_builder=_authority_local_operator_shell,
            modifier="operator",
            sort_order=50,
        ),
    ]