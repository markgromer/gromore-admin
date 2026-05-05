# Site Builder Kit Playbook

This is the working guide for building Warren site kits the way the product actually runs today.

Use this when you want to create:

- a new production-ready full site kit
- a niche-specific visual system
- reusable page shells for a vertical
- shared navigation and footer templates
- a clean handoff package for me to wire into the builder

This is different from the asset-pack spec.

- [SITE_BUILDER_ASSET_PACK_SPEC.md](SITE_BUILDER_ASSET_PACK_SPEC.md) explains the raw assets and handoff structure.
- This playbook explains how those assets become a live Warren site kit in the database and builder flow.
- [SITE_BUILDER_KIT_BRIEF_TEMPLATE.md](SITE_BUILDER_KIT_BRIEF_TEMPLATE.md) is the blank fill-in brief you can use to define a new kit quickly.
- [WARREN_WORDPRESS_TEMPLATE_HELP.md](WARREN_WORDPRESS_TEMPLATE_HELP.md) explains how to turn approved WordPress designs into reusable WARREN templates with page-template slugs and ACF field maps.

## 1. What a kit is in the current system

In Warren, a full site kit is not one giant HTML file.

It is a bundle of three layers:

1. One theme
2. A curated template library
3. One site-template record that ties the theme and templates together

In code and in the database, that means:

- `sb_themes`: colors, fonts, global visual defaults, optional global CSS
- `sb_templates`: reusable templates such as navigation, footer, and page shells
- `sb_site_templates`: the full kit record users choose from in the client builder

The current production starter kits live in [webapp/site_builder_kits.py](../webapp/site_builder_kits.py).

The client chooser loads from [webapp/client_portal.py](../webapp/client_portal.py).

The admin CRUD and install surface lives in [webapp/app.py](../webapp/app.py) and [webapp/templates/site_builder_admin.html](../webapp/templates/site_builder_admin.html).

## 2. Current runtime flow

When a client opens the builder:

1. Warren loads active `sb_site_templates`
2. The user picks a full site template
3. Warren snapshots that kit into the build intake
4. The builder uses the selected theme, prompt notes, and templates during generation
5. Page shells shape the structure of each page type
6. Navigation and footer templates wrap assembled pages

This means a kit must be built as a system, not as a single homepage mockup.

## 3. The three authoring layers

### Layer A: Theme

A theme controls the global visual language.

Current stored fields:

- `name`
- `description`
- `primary_color`
- `secondary_color`
- `accent_color`
- `text_color`
- `bg_color`
- `font_heading`
- `font_body`
- `button_style`
- `layout_style`
- `custom_css`
- `preview_image`
- `is_default`
- `is_active`

Use the theme for:

- palette
- typography
- button feel
- baseline spacing and atmosphere
- kit-wide utility CSS

Do not use the theme for:

- page-specific structure
- page copy
- service-specific content blocks

### Layer B: Templates

Templates are reusable HTML and CSS blocks.

Current stored fields:

- `name`
- `category`
- `page_types`
- `html_content`
- `css_content`
- `preview_image`
- `description`
- `sort_order`
- `is_active`

In the current site-kit system, the most important template categories are:

- `navigation`
- `footer`
- `page_shell`

You can still use section-style templates later, but the current full-site kit system gets most of its leverage from page shells plus shared nav and footer.

### Layer C: Site template

The site template is the actual selectable kit in the client intake.

Current stored fields:

- `name`
- `slug`
- `description`
- `preview_image`
- `theme_id`
- `template_ids`
- `prompt_notes`
- `sort_order`
- `is_default`
- `is_active`

This record should describe one complete site direction a client can confidently choose.

## 4. What makes a kit enterprise-ready

An enterprise-grade kit should do four things well:

1. Give the builder a clear visual identity
2. Give the AI structural guardrails page by page
3. Still adapt cleanly to different brands inside the same niche
4. Stay maintainable when we iterate or seed updates later

That means each kit should have:

- one coherent theme, not a mixed aesthetic
- one shared navigation template
- one shared footer template
- one page shell for every important page type
- prompt notes that define tone, conversion posture, and layout discipline
- preview imagery that makes the choice understandable to the client

## 5. Minimum production kit structure

For a real production kit, use this minimum:

- 1 theme
- 1 navigation template
- 1 footer template
- 8 page shells

Recommended page shells:

- `home`
- `about`
- `services`
- `service_detail`
- `service_area`
- `contact`
- `faq`
- `testimonials`

Optional but useful:

- `landing_page`

The current production kits in [webapp/site_builder_kits.py](../webapp/site_builder_kits.py) follow this pattern.

## 6. How to think about page shells

Page shells are the most important part of the current kit architecture.

They are not full pages with finalized copy.

They are structural wrappers that tell the builder:

- how the hero should feel
- where the body copy sits
- how proof blocks are framed
- what layout rhythm the page should keep
- how each page type differs from the others

Each page shell should use Warren tokens and placeholders where needed.

Examples already used in the current system include tokens like:

- `{{business_name}}`
- `{{phone}}`
- `{{cta_text}}`
- `{{service_area}}`
- `{{page_title}}`
- `{{page_excerpt}}`

The shell should make the layout deterministic without hardcoding brand-specific facts.

## 7. Naming conventions that keep kits clean

Use names that describe purpose, not vague style adjectives.

Good:

- `Lead Engine Navigation`
- `Neighborhood Trust Footer`
- `Pet Waste Authority Home Shell`
- `Pet Waste Authority Service Area Shell`

Bad:

- `Modern Blue`
- `Clean Pro Max`
- `Homepage Template 2`

Slug rules for full site templates:

- lowercase
- hyphenated
- stable once published
- unique to the kit family

Good slugs:

- `lead-engine`
- `premium-authority`
- `pet-waste-authority`

## 8. The best workflow for building a new kit

Use this order.

1. Define the niche and the business posture
2. Define the visual strategy
3. Define page-by-page structural rules
4. Build the theme
5. Build navigation and footer
6. Build page shells for each page type
7. Write the site-template record and prompt notes
8. Seed or create the kit in admin
9. Generate 3 to 5 sample brands through it
10. Tighten weak shells instead of bloating prompt notes

Do not start by building a perfect homepage in isolation.

That produces kits that look good in screenshots but break when the builder has to produce a whole site.

## 9. What prompt notes should do

`prompt_notes` are for strategic guidance, not giant walls of design prose.

Good uses:

- define the desired tone
- define the conversion posture
- define how aggressive or restrained the copy should be
- define the kind of trust cues that matter in the niche
- define what the kit should never do

Bad uses:

- repeating raw color values
- describing every pixel of the layout
- trying to compensate for weak page-shell architecture

If you find yourself writing long prompt notes to force structure, the page shells are probably too weak.

## 10. Where preview images fit

There are two preview-image layers in the current schema:

- theme `preview_image`
- site-template `preview_image`

Use them differently.

- Theme preview: communicates the visual system
- Site-template preview: communicates the full-site direction the client is choosing

For production work, every site template should have a clear preview image.

That is what makes the chooser feel real instead of abstract.

## 11. Best way for us to build kits together

There are two good workflows.

### Workflow A: Admin-first

Use this when you want to move fast.

1. Create or edit the theme in Site Builder Admin
2. Create the navigation and footer templates
3. Create each page shell template
4. Create the full site template record and attach the theme and templates
5. Test generation with a real brand

### Workflow B: Code-first

Use this when you want a durable official starter kit.

1. Define the kit in [webapp/site_builder_kits.py](../webapp/site_builder_kits.py)
2. Seed it through the database layer in [webapp/database.py](../webapp/database.py)
3. Validate the chooser and generation flow
4. Add or update regression tests

Use code-first for official Warren kits that we want to keep versioned and idempotent.

## 12. How to package a new official kit for me

If you want me to build a new official kit efficiently, give me this exact package:

1. Kit name
2. Kit slug
3. Niche
4. Theme token set
5. Visual direction notes in 5 to 10 lines
6. Navigation concept
7. Footer concept
8. Page-shell guidance for each page type
9. Preview image references
10. Prompt notes

If you already have raw assets, put them in the pack format from [SITE_BUILDER_ASSET_PACK_SPEC.md](SITE_BUILDER_ASSET_PACK_SPEC.md).

## 13. Pet waste removal: the first niche kit brief

For the pet waste removal vertical, the kit should not feel like a generic contractor site.

It should lean into:

- recurring service simplicity
- cleanliness and relief
- trust around pets, gates, and yard access
- strong local service area clarity
- easy quote or sign-up flow
- add-on upsells like deodorizer, litter box service, or one-time cleanup

That means the kit should probably emphasize:

- recurring plan options
- service frequency choices
- service-area confidence
- safety and trust proof
- easy onboarding CTA
- CRM and quote-flow compatibility

The pet waste intake should capture at minimum:

- services offered
- recurring plans or visit frequencies
- one-time cleanup availability
- add-ons
- primary city
- secondary cities or neighborhoods
- company story
- differentiators
- CRM or quote-tool path
- preferred CTA

## 14. The biggest mistakes to avoid

Do not do these:

- build one beautiful homepage and call it a kit
- mix multiple brand aesthetics into one theme
- rely on prompt notes instead of page shells
- make every niche use the same proof language
- leave preview images blank
- create slugs or names that are too generic to manage later
- create kits that only work for one exact brand instead of the niche pattern

## 15. Enterprise kit QA checklist

Before we treat a kit as production-ready, check all of this:

- The client chooser card is visually understandable
- The theme looks coherent across all major page types
- Every required page type has a page shell
- The navigation and footer both fit the kit style
- The kit still works with long business names
- The kit still works with short and long service lists
- The kit still works with multiple service areas
- Generated pages keep CTA consistency
- The copy does not drift into generic SaaS language
- The site still feels specific to the niche
- The kit improves output quality without needing prompt babysitting

## 16. Recommended next build sequence

For the next phase, the strongest sequence is:

1. Finalize the first pet-waste-specific kit family
2. Add template preview images and default imagery guidance
3. Add niche-specific intake fields tied to that kit family
4. Validate the output against 3 different pet waste brands
5. Then repeat the same pattern for the next niche

That gives us a real vertical system instead of a generic site generator with a few style skins.
