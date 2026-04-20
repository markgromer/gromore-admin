import re
from urllib.parse import quote_plus


GOOGLE_FONT_CHOICES = [
    "Albert Sans",
    "Abril Fatface",
    "Alegreya",
    "Alegreya Sans",
    "Anton",
    "Archivo Black",
    "Archivo",
    "Archivo Narrow",
    "Asap",
    "Asap Condensed",
    "Assistant",
    "Atkinson Hyperlegible",
    "Arvo",
    "Azeret Mono",
    "Bai Jamjuree",
    "Barlow",
    "Barlow Condensed",
    "Baskervville",
    "Bebas Neue",
    "Be Vietnam Pro",
    "Bellefair",
    "Bitter",
    "Bricolage Grotesque",
    "Bowlby One",
    "Cabin",
    "Candal",
    "Catamaran",
    "Cardo",
    "Chivo",
    "Cinzel",
    "Commissioner",
    "Comfortaa",
    "Cormorant Garamond",
    "Crimson Text",
    "DM Sans",
    "DM Serif Display",
    "Didact Gothic",
    "Domine",
    "Epilogue",
    "Exo 2",
    "Faustina",
    "Figtree",
    "Fjalla One",
    "Fraunces",
    "Fredoka",
    "Fugaz One",
    "Gabarito",
    "Gloock",
    "Heebo",
    "Hanken Grotesk",
    "Hind",
    "IBM Plex Sans",
    "IBM Plex Serif",
    "Inconsolata",
    "Instrument Sans",
    "Instrument Serif",
    "Inter",
    "Josefin Sans",
    "Jost",
    "Kanit",
    "Karla",
    "Kumbh Sans",
    "Lato",
    "League Spartan",
    "Lexend",
    "Libre Baskerville",
    "Libre Franklin",
    "Literata",
    "Lora",
    "Mada",
    "Manrope",
    "Marcellus",
    "Martian Mono",
    "Maven Pro",
    "Merriweather",
    "Merriweather Sans",
    "Montserrat",
    "Monda",
    "Mukta",
    "Mulish",
    "Nanum Gothic",
    "Newsreader",
    "Onest",
    "Noto Sans",
    "Noto Serif",
    "Nunito",
    "Open Sans",
    "Oswald",
    "Outfit",
    "Overpass",
    "Oxygen",
    "Pacifico",
    "Philosopher",
    "Playfair Display",
    "Plus Jakarta Sans",
    "Poppins",
    "Prata",
    "PT Sans",
    "PT Serif",
    "Public Sans",
    "Questrial",
    "Quicksand",
    "Raleway",
    "Red Hat Display",
    "Red Hat Text",
    "Righteous",
    "Roboto",
    "Roboto Condensed",
    "Roboto Mono",
    "Roboto Slab",
    "Rubik",
    "Russo One",
    "Sarabun",
    "Schibsted Grotesk",
    "Sen",
    "Signika",
    "Sora",
    "Source Sans Pro",
    "Source Sans 3",
    "Source Serif Pro",
    "Source Serif 4",
    "Special Elite",
    "Space Grotesk",
    "Space Mono",
    "Syne",
    "Tenor Sans",
    "Teko",
    "Tinos",
    "Titillium Web",
    "Unbounded",
    "Ubuntu",
    "Urbanist",
    "Varela Round",
    "Vollkorn",
    "Wix Madefor Display",
    "Wix Madefor Text",
    "Work Sans",
    "Ysabeau",
    "Yanone Kaffeesatz",
    "Zilla Slab",
]

SERIF_FONTS = {
    "Abril Fatface",
    "Alegreya",
    "Arvo",
    "Bitter",
    "Cardo",
    "Cormorant Garamond",
    "Crimson Text",
    "DM Serif Display",
    "Domine",
    "Fraunces",
    "Gloock",
    "Instrument Serif",
    "Libre Baskerville",
    "Literata",
    "Lora",
    "Marcellus",
    "Merriweather",
    "Newsreader",
    "Noto Serif",
    "Playfair Display",
    "Prata",
    "PT Serif",
    "Roboto Slab",
    "Source Serif Pro",
    "Source Serif 4",
    "Tenor Sans",
    "Tinos",
    "Vollkorn",
    "Zilla Slab",
}

MONO_FONTS = {
    "Azeret Mono",
    "Inconsolata",
    "Martian Mono",
    "Roboto Mono",
    "Space Mono",
}

SITE_BUILDER_FONT_PAIR_CHOICES = [
    {
        "value": "inter-system",
        "label": "Inter + System",
        "description": "Minimal, fast, operational UI energy.",
        "prompt": "Inter (headings) + system sans-serif (body) - clean, fast loading",
    },
    {
        "value": "plusjakarta-inter",
        "label": "Plus Jakarta Sans + Inter",
        "description": "Premium modern service brand, crisp and controlled.",
        "prompt": "Plus Jakarta Sans (headings) + Inter (body) - crisp, premium, modern service brand",
    },
    {
        "value": "spacegrotesk-inter",
        "label": "Space Grotesk + Inter",
        "description": "Assertive headline contrast with clean body copy.",
        "prompt": "Space Grotesk (headings) + Inter (body) - assertive, modern, slightly editorial",
    },
    {
        "value": "manrope-dmsans",
        "label": "Manrope + DM Sans",
        "description": "Polished conversion stack for upscale local brands.",
        "prompt": "Manrope (headings) + DM Sans (body) - polished, contemporary, conversion-focused",
    },
    {
        "value": "archivo-worksans",
        "label": "Archivo + Work Sans",
        "description": "Operational, sturdy, practical, field-service ready.",
        "prompt": "Archivo (headings) + Work Sans (body) - sturdy, practical, operational",
    },
    {
        "value": "bebas-mulish",
        "label": "Bebas Neue + Mulish",
        "description": "Hard-hitting headlines with clean support copy.",
        "prompt": "Bebas Neue (headings) + Mulish (body) - bold headlines with clean supporting copy",
    },
    {
        "value": "raleway-lora",
        "label": "Raleway + Lora",
        "description": "Refined trust signal with a softer editorial body.",
        "prompt": "Raleway (headings) + Lora (body) - refined, trustworthy, slightly upscale",
    },
    {
        "value": "librebaskerville-source",
        "label": "Libre Baskerville + Source Sans 3",
        "description": "Classic authority with highly readable body copy.",
        "prompt": "Libre Baskerville (headings) + Source Sans 3 (body) - classic authority with readable body copy",
    },
    {
        "value": "playfair-lato",
        "label": "Playfair Display + Lato",
        "description": "Elegant trust builder for premium presentation.",
        "prompt": "Playfair Display (headings) + Lato (body) - elegant, editorial",
    },
    {
        "value": "montserrat-opensans",
        "label": "Montserrat + Open Sans",
        "description": "Versatile default for broad local-service use.",
        "prompt": "Montserrat (headings) + Open Sans (body) - modern, versatile",
    },
    {
        "value": "roboto-slab-roboto",
        "label": "Roboto Slab + Roboto",
        "description": "Technical and structured, good for utility trades.",
        "prompt": "Roboto Slab (headings) + Roboto (body) - technical, structured",
    },
    {
        "value": "poppins-nunito",
        "label": "Poppins + Nunito",
        "description": "Friendly, bright, approachable consumer tone.",
        "prompt": "Poppins (headings) + Nunito (body) - friendly, approachable",
    },
    {
        "value": "oswald-source",
        "label": "Oswald + Source Sans Pro",
        "description": "Strong industrial posture with readable content blocks.",
        "prompt": "Oswald (headings) + Source Sans Pro (body) - bold, industrial",
    },
    {
        "value": "redhat-redhat",
        "label": "Red Hat Display + Red Hat Text",
        "description": "Enterprise SaaS polish with sharper structure.",
        "prompt": "Red Hat Display (headings) + Red Hat Text (body) - enterprise-modern, clear, structured",
    },
    {
        "value": "epilogue-worksans",
        "label": "Epilogue + Work Sans",
        "description": "Contemporary brand system with strong rhythm.",
        "prompt": "Epilogue (headings) + Work Sans (body) - modern, confident, clean",
    },
    {
        "value": "instrumentserif-publicsans",
        "label": "Instrument Serif + Public Sans",
        "description": "Editorial authority without feeling old-fashioned.",
        "prompt": "Instrument Serif (headings) + Public Sans (body) - editorial authority with modern readability",
    },
    {
        "value": "schibsted-manrope",
        "label": "Schibsted Grotesk + Manrope",
        "description": "Sharp, contemporary, premium operator energy.",
        "prompt": "Schibsted Grotesk (headings) + Manrope (body) - crisp, premium, contemporary",
    },
    {
        "value": "urbanist-inter",
        "label": "Urbanist + Inter",
        "description": "Clean modern brand system with softer edges.",
        "prompt": "Urbanist (headings) + Inter (body) - clean modern UI with a softer premium feel",
    },
    {
        "value": "cinzel-librefranklin",
        "label": "Cinzel + Libre Franklin",
        "description": "Prestige-forward display paired with practical copy.",
        "prompt": "Cinzel (headings) + Libre Franklin (body) - prestige and authority balanced with practical readability",
    },
    {
        "value": "newsreader-publicsans",
        "label": "Newsreader + Public Sans",
        "description": "Warm editorial sophistication with stable body text.",
        "prompt": "Newsreader (headings) + Public Sans (body) - warm editorial sophistication with clear supporting copy",
    },
]

SITE_BUILDER_FONT_GROUPS = [
    {
        "id": "enterprise-sans",
        "label": "Enterprise Sans",
        "description": "Stable, clean, executive-grade systems for operational brands.",
        "fonts": ["Inter", "Manrope", "Public Sans", "Red Hat Text", "Wix Madefor Text", "Albert Sans"],
    },
    {
        "id": "premium-modern",
        "label": "Premium Modern",
        "description": "Crisp, elevated sans families for flagship local-service sites.",
        "fonts": ["Plus Jakarta Sans", "Schibsted Grotesk", "Space Grotesk", "Urbanist", "Epilogue", "Instrument Sans"],
    },
    {
        "id": "operator-strong",
        "label": "Operator Strong",
        "description": "Harder-working headlines for trades, teams, and direct response layouts.",
        "fonts": ["Archivo", "Archivo Black", "League Spartan", "Oswald", "Barlow Condensed", "Bebas Neue"],
    },
    {
        "id": "friendly-human",
        "label": "Friendly Human",
        "description": "Approachable families that still feel intentional and polished.",
        "fonts": ["DM Sans", "Work Sans", "Nunito", "Be Vietnam Pro", "Karla", "Figtree"],
    },
    {
        "id": "editorial-serif",
        "label": "Editorial Serif",
        "description": "Authority and sophistication without defaulting to generic brochure vibes.",
        "fonts": ["Instrument Serif", "Newsreader", "Fraunces", "Cormorant Garamond", "Playfair Display", "Libre Baskerville"],
    },
    {
        "id": "heritage-serif",
        "label": "Heritage Serif",
        "description": "More classic prestige for trust-heavy or premium brands.",
        "fonts": ["Baskervville", "Prata", "Marcellus", "Merriweather", "Literata", "Vollkorn"],
    },
    {
        "id": "creative-display",
        "label": "Creative Display",
        "description": "Distinctive display faces for stronger brand character and contrast.",
        "fonts": ["Syne", "Cinzel", "Anton", "Teko", "Righteous", "Unbounded"],
    },
    {
        "id": "utility-mono",
        "label": "Utility Mono",
        "description": "Useful accent or systems fonts for pricing, labels, and editorial contrast.",
        "fonts": ["Azeret Mono", "Martian Mono", "Roboto Mono", "Space Mono", "Inconsolata", "Monda"],
    },
]

_FONT_SANITIZE_RE = re.compile(r"[^A-Za-z0-9 '&+._-]")


def normalize_google_font_family(value):
    text = re.sub(r"\s+", " ", str(value or "").strip())
    text = _FONT_SANITIZE_RE.sub("", text)
    return text[:80].strip()


def google_font_stylesheet_href(families):
    cleaned = []
    seen = set()
    for raw in families or []:
        family = normalize_google_font_family(raw)
        key = family.lower()
        if not family or key in seen:
            continue
        seen.add(key)
        cleaned.append(family)
    if not cleaned:
        return ""
    params = "&".join(f"family={quote_plus(family)}" for family in cleaned)
    return f"https://fonts.googleapis.com/css2?{params}&display=swap"


def build_editor_font_family_options(families=None):
    options = []
    seen = set()
    for raw in families or GOOGLE_FONT_CHOICES:
        family = normalize_google_font_family(raw)
        if not family:
            continue
        key = family.lower()
        if key in seen:
            continue
        seen.add(key)
        options.append({
            "id": font_css_stack(family),
            "label": family,
        })
    return options


def build_google_font_stylesheet_chunks(families=None, chunk_size=18):
    cleaned = []
    seen = set()
    for raw in families or GOOGLE_FONT_CHOICES:
        family = normalize_google_font_family(raw)
        key = family.lower()
        if not family or key in seen:
            continue
        seen.add(key)
        cleaned.append(family)
    hrefs = []
    for index in range(0, len(cleaned), max(1, int(chunk_size or 1))):
        href = google_font_stylesheet_href(cleaned[index:index + max(1, int(chunk_size or 1))])
        if href:
            hrefs.append(href)
    return hrefs


def build_site_builder_font_preview_stylesheets(groups=None, chunk_size=18):
    if groups is None:
        return build_google_font_stylesheet_chunks(GOOGLE_FONT_CHOICES, chunk_size=chunk_size)
    families = []
    for group in groups:
        families.extend(group.get("fonts") or [])
    return build_google_font_stylesheet_chunks(families, chunk_size=chunk_size)


def font_css_stack(family):
    normalized = normalize_google_font_family(family)
    if not normalized:
        return "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"
    if normalized in MONO_FONTS:
        fallback = "monospace"
    elif normalized in SERIF_FONTS:
        fallback = "serif"
    else:
        fallback = "sans-serif"
    return f"'{normalized}', {fallback}"