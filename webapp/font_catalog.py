import re
from urllib.parse import quote_plus


GOOGLE_FONT_CHOICES = [
    "Abril Fatface",
    "Alegreya",
    "Alegreya Sans",
    "Anton",
    "Archivo",
    "Archivo Narrow",
    "Arvo",
    "Assistant",
    "Barlow",
    "Barlow Condensed",
    "Bebas Neue",
    "Bitter",
    "Cabin",
    "Cardo",
    "Chivo",
    "Comfortaa",
    "Cormorant Garamond",
    "Crimson Text",
    "DM Sans",
    "DM Serif Display",
    "Domine",
    "Exo 2",
    "Figtree",
    "Fjalla One",
    "Fraunces",
    "Fredoka",
    "Fugaz One",
    "Hind",
    "IBM Plex Sans",
    "IBM Plex Serif",
    "Inconsolata",
    "Inter",
    "Josefin Sans",
    "Jost",
    "Kanit",
    "Karla",
    "Lato",
    "Lexend",
    "Libre Baskerville",
    "Libre Franklin",
    "Lora",
    "Manrope",
    "Merriweather",
    "Montserrat",
    "Mukta",
    "Mulish",
    "Newsreader",
    "Noto Sans",
    "Noto Serif",
    "Nunito",
    "Open Sans",
    "Oswald",
    "Outfit",
    "Oxygen",
    "Pacifico",
    "Playfair Display",
    "Plus Jakarta Sans",
    "Poppins",
    "PT Sans",
    "PT Serif",
    "Public Sans",
    "Quicksand",
    "Raleway",
    "Roboto",
    "Roboto Condensed",
    "Roboto Mono",
    "Roboto Slab",
    "Rubik",
    "Russo One",
    "Sarabun",
    "Signika",
    "Sora",
    "Source Sans 3",
    "Source Serif 4",
    "Space Grotesk",
    "Space Mono",
    "Teko",
    "Titillium Web",
    "Ubuntu",
    "Urbanist",
    "Varela Round",
    "Work Sans",
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
    "Libre Baskerville",
    "Lora",
    "Merriweather",
    "Newsreader",
    "Noto Serif",
    "Playfair Display",
    "PT Serif",
    "Roboto Slab",
    "Source Serif 4",
    "Zilla Slab",
}

MONO_FONTS = {
    "Inconsolata",
    "Roboto Mono",
    "Space Mono",
}

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