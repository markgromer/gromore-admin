# Site Builder Asset Pack Spec

This is the cleanest way to hand reusable design assets to the builder.

## What to provide first

Priority order:

1. Section library
2. Theme token packs
3. Image packs
4. Full-page exemplars only as reference

Why:

- Sections are the most reusable input and map best to how the builder generates pages.
- Theme packs give deterministic colors, fonts, button styles, and layout defaults.
- Image packs let us keep visual quality high without inventing placeholders.
- Full themes or full pages are useful, but they are better as examples than as the primary generation primitive.

## Current runtime behavior

The builder now uses Site Builder Admin assets in three practical ways:

- The active default theme is snapshotted into each build intake and used as a fallback design preset.
- Active prompt overrides are snapshotted into each build intake and injected into generation prompts.
- Active shared header and footer templates are snapshotted into each build intake and wrapped around assembled page HTML, except landing pages.

Current image-library storage:

- Admin uploads live in webapp/static/uploads/sb_images
- Those images can be published into WordPress media from the admin flow

## Recommended handoff format

If you want to hand me assets in-repo instead of pasting them in chat, use one pack folder per style system.

Recommended folder convention:

- data/imports/site_builder_packs/<pack-slug>/theme.json
- data/imports/site_builder_packs/<pack-slug>/sections/*.json
- data/imports/site_builder_packs/<pack-slug>/images/*
- data/imports/site_builder_packs/<pack-slug>/images/manifest.json
- data/imports/site_builder_packs/<pack-slug>/examples/*.html

That import path is a repo convention for clean handoff. It is not an automated importer yet.

## Theme pack format

One theme pack should describe one coherent design system.

Example:

```json
{
  "name": "Warm Trades Modern",
  "description": "Clean residential service look with warm accents and strong CTA contrast.",
  "primary_color": "#16324f",
  "secondary_color": "#2d5b88",
  "accent_color": "#f97316",
  "text_color": "#17212b",
  "bg_color": "#f7f4ef",
  "font_heading": "Oswald",
  "font_body": "Source Sans Pro",
  "button_style": "pill",
  "layout_style": "modern-sections",
  "custom_css": ".trust-strip{letter-spacing:.04em;}"
}
```

Rules:

- Keep it to actual tokens, not long prose.
- Use real hex values.
- One theme should feel internally consistent.
- Do not mix three unrelated visual styles in one theme pack.

## Section pack format

Each section file should represent one reusable pattern, not a whole page.

Example:

```json
{
  "name": "Residential Hero With Phone CTA",
  "category": "hero",
  "page_types": "home,service_detail,service_area",
  "description": "Large hero with proof bar, direct headline, phone CTA, and short service bullets.",
  "sort_order": 10,
  "tokens": [
    "{{business_name}}",
    "{{service_area}}",
    "{{phone}}",
    "{{cta_text}}",
    "{{service_name}}"
  ],
  "html_content": "<section class=\"hero\"><div class=\"wrap\"><p class=\"eyebrow\">{{service_area}}</p><h2>{{business_name}}</h2><a href=\"tel:{{phone}}\">{{cta_text}}</a></div></section>",
  "css_content": ".hero{padding:72px 0;}"
}
```

Rules:

- One file per section.
- Name the pattern by intent, not by vague style language.
- page_types should be explicit when the section is specialized.
- description should explain when to use it.
- html_content should be production-shaped HTML, not screenshots or notes.
- css_content can be blank if the section relies on theme tokens.

Best categories to start with:

- header
- footer
- hero
- services
- proof
- testimonials
- faq
- cta
- contact
- offer
- before-after
- pricing
- process

## Image pack format

Images should come with metadata, not just loose files.

Example manifest:

```json
[
  {
    "file": "truck-team-01.jpg",
    "category": "hero",
    "alt": "Two plumbers standing beside a branded service van",
    "tags": ["plumbing", "team", "residential"],
    "orientation": "landscape",
    "recommended_page_types": ["home", "about"],
    "recommended_sections": ["hero", "team", "trust"],
    "focal_point": "center",
    "notes": "Good for homepage and about page hero use"
  }
]
```

Rules:

- Use final filenames, not temp exports.
- Include alt text and category.
- Tell me where each image belongs when possible.
- If you already know the exact section usage, say so in notes.

## Full-page exemplars

Use these only as references.

Good use:

- A homepage whose section order you like
- A service page whose heading rhythm is strong
- A footer style worth reusing

Bad use:

- Treating a whole page as the only source of truth
- Expecting one full page to generalize into a whole system without section breakdowns

If you provide full-page exemplars, add a short note with:

- what to copy
- what not to copy
- which sections should become reusable templates

## Minimum useful pack

For one strong vertical, the minimum useful pack is:

- 1 theme pack
- 8 to 15 section templates
- 10 to 30 labeled images
- 1 to 3 exemplar pages

That is enough to create visibly cleaner, more deterministic output.

## Best way to give assets to me right now

Fastest live path:

- Put themes, templates, prompt overrides, and images into Site Builder Admin

Cleanest repo path:

- Create one structured pack folder under data/imports/site_builder_packs
- Add HTML, CSS, and image metadata in the formats above
- Tell me which pack should become the active default

## Practical recommendation

Do not start with full themes only.

Start with:

1. 1 strong theme pack
2. 1 shared header
3. 1 shared footer
4. 6 to 10 reusable body sections
5. 1 image pack mapped to those sections

That gives the builder something deterministic to use immediately and avoids a giant fragile theme system on day one.
