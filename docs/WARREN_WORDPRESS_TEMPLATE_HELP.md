# WARREN WordPress Template and ACF Setup Guide

Internal/backend operator documentation. This is for WARREN admins, implementers, and WordPress builders, not for client-facing help pages.

This guide explains how to turn a WordPress design you like into a reusable WARREN Site Builder template.

The goal is not to have WARREN invent a brand-new website layout every time. The better system is:

1. Build a small set of strong WordPress templates.
2. Approve the style and layout once.
3. Give WARREN the exact WordPress template slug and ACF field map.
4. Let WARREN write the SEO-rich content and push it into those fields for each brand.

That gives us better first drafts, more consistent design quality, and a cleaner path to launching sites quickly.

## The Big Picture

A WARREN WordPress template has two parts:

- The WordPress design layer: page template file, theme styling, blocks, partials, and ACF field group.
- The WARREN mapping layer: a saved Site Builder template record that tells WARREN which WordPress template to use and which generated content belongs in which ACF fields.

When a brand chooses that template in Site Builder, WARREN generates the page strategy and copy, then publishes the page to WordPress with:

- page title
- slug
- body content
- SEO title
- SEO description
- WordPress page template slug
- mapped ACF fields

If the normal WordPress REST API rejects custom `template` or `acf` fields, WARREN falls back to the GroMore/WARREN Publisher Helper plugin. For ACF-powered templates, keep the helper plugin updated.

## What You Need Before You Start

For each WordPress site:

- WordPress admin access.
- A WordPress user with Editor or Administrator permissions.
- A WordPress Application Password for that user.
- HTTPS enabled on the site.
- REST API available, or the WARREN Publisher Helper plugin installed.
- Advanced Custom Fields installed if the template depends on ACF fields.
- The GroMore/WARREN Publisher Helper plugin version `1.4.0` or newer if direct REST publishing is blocked or ACF/template publishing fails.

For WARREN:

- WordPress Site URL saved for the brand.
- WordPress username saved for the brand.
- WordPress Application Password saved for the brand.
- Site Builder Admin access.
- A reusable Site Template record with the WordPress template slug and ACF map.

## Recommended Build Strategy

Do not start with 30 templates.

Start with 3 to 5 excellent template families:

- Local service lead-gen homepage
- Multi-service authority site
- Single-service landing site
- Premium trust-heavy brand site
- Commercial/B2B service site

Each family can serve many verticals if the field structure is flexible.

WARREN should own the copy, SEO, service-area language, FAQs, schema, calls to action, and localized proof. WordPress should own the layout, component system, and reusable design quality.

## Step 1: Build the WordPress Page Template

Create or choose a WordPress page template that represents the design you want to reuse.

Good template examples:

- `templates/warren-home.php`
- `templates/warren-service-page.php`
- `templates/warren-location-page.php`
- `templates/warren-commercial.php`

For a classic PHP theme, the template file should include a WordPress template header:

```php
<?php
/*
Template Name: WARREN Home
*/
```

The template slug you save in WARREN must match the slug WordPress expects. Common examples:

```text
templates/warren-home.php
warren-home.php
```

The right value depends on the theme structure. If WordPress shows the template in the Page Template dropdown, inspect the actual file path relative to the theme.

## Step 2: Create the ACF Field Group

In WordPress, create an ACF field group for the template.

Recommended location rule:

```text
Page Template is equal to WARREN Home
```

Use stable field names. WARREN maps into field names or field keys. Field keys are safest for first-time ACF writes, but readable field names are easier to manage.

Good field names:

```text
warren_hero_headline
warren_hero_subheadline
warren_primary_cta_text
warren_primary_cta_url
warren_services_summary
warren_service_area_summary
warren_body_html
warren_faq_items
warren_schema_json
```

Avoid vague field names:

```text
title
subtitle
text_1
box_copy
field_a
```

Those become hard to maintain once we have many templates.

## Recommended ACF Fields

For a homepage template, start with:

```text
warren_page_title
warren_hero_headline
warren_hero_subheadline
warren_primary_cta_text
warren_primary_cta_url
warren_secondary_cta_text
warren_services_summary
warren_service_area_summary
warren_trust_summary
warren_body_html
warren_faq_items
warren_seo_title
warren_seo_description
warren_schema_json
```

For service pages:

```text
warren_service_name
warren_service_headline
warren_service_intro
warren_service_benefits
warren_service_process
warren_service_faq_items
warren_service_area_summary
warren_body_html
warren_seo_title
warren_seo_description
warren_schema_json
```

For service-area pages:

```text
warren_city_name
warren_local_headline
warren_local_intro
warren_services_available
warren_local_trust_summary
warren_neighborhood_notes
warren_body_html
warren_seo_title
warren_seo_description
warren_schema_json
```

## Step 3: Build and Approve a Sample Page

Before capturing the template in WARREN, create one sample page in WordPress using the template.

Use this page to confirm:

- the hero looks strong on desktop and mobile
- headings do not overflow
- CTA buttons are clear
- fields render in the correct places
- empty optional fields do not break the design
- testimonials, reviews, services, and FAQs look good
- the page feels like a real WARREN-built site, not a generic AI draft

Once the sample looks right, copy:

- the sample page URL
- the WordPress page ID
- the WordPress template slug
- any notes about what must stay consistent

## Step 4: Capture the Template in WARREN

In WARREN admin:

1. Go to `Site Builder Admin`.
2. Open the `Site Templates` tab.
3. Use `Capture WordPress Style`.
4. Enter the template name.
5. Paste the approved WordPress sample URL.
6. Enter the source WordPress page ID if you have it.
7. Enter the WordPress template slug.
8. Add best-fit verticals.
9. Add capture notes.
10. Save the reusable WordPress template.

After saving, edit the Site Template and confirm:

- `ACF mapped` is enabled.
- the WordPress template slug is correct.
- the ACF field map JSON matches the fields in WordPress.
- the template is active.
- the description is clear enough for someone choosing it in the client builder.

## Step 5: Fill Out the ACF Field Map

The ACF field map is JSON.

The left side is the WordPress ACF field name.

The right side is the WARREN source value.

Example:

```json
{
  "default": {
    "warren_page_title": "title",
    "warren_hero_headline": "title",
    "warren_hero_subheadline": "excerpt",
    "warren_body_html": "content",
    "warren_seo_title": "seo_title",
    "warren_seo_description": "seo_description",
    "warren_primary_keyword": "primary_keyword",
    "warren_secondary_keywords": "secondary_keywords",
    "warren_faq_items": "faq_items",
    "warren_schema_json": "schemas"
  },
  "home": {
    "hero_headline": "title",
    "hero_subheadline": "excerpt",
    "primary_cta_text": "intake.cta_text",
    "service_area_summary": "intake.priority_seo_locations",
    "services_summary": "brand.primary_services",
    "seo_title": "seo_title",
    "seo_description": "seo_description"
  }
}
```

WARREN merges the maps in this order:

1. `default`
2. `pages.{page_type}` if present
3. direct page type map, such as `home`, `service_detail`, or `service_area`

That means `default` can hold shared fields, while individual page types can override or add fields.

## Supported WARREN Sources

Page sources:

```text
title
slug
label
page_type
content
content_html
full_html
excerpt
seo_title
seo_description
primary_keyword
secondary_keywords
faq_items
schemas
schema_json
schema_html
```

Brand sources:

```text
brand.name
brand.business_name
brand.website
brand.phone
brand.primary_services
brand.service_area
brand.city
brand.state
brand.brand_voice
```

Intake sources:

```text
intake.cta_text
intake.cta_phone
intake.priority_seo_locations
intake.profitable_services
intake.site_vision
intake.reference_url
```

Literal values:

```json
{
  "warren_primary_cta_text": "literal:Get a Free Estimate"
}
```

Fallback values:

```json
{
  "warren_hero_subheadline": {
    "source": "excerpt",
    "fallback": "brand.primary_services"
  }
}
```

Joined lists:

```json
{
  "warren_secondary_keywords_text": {
    "source": "secondary_keywords",
    "join": ", "
  }
}
```

JSON encoding:

```json
{
  "warren_schema_json": {
    "source": "schemas",
    "as_json": true
  }
}
```

## Page Type Examples

Use page-type-specific maps when one WordPress template supports several kinds of pages.

```json
{
  "default": {
    "warren_page_title": "title",
    "warren_body_html": "content",
    "warren_seo_title": "seo_title",
    "warren_seo_description": "seo_description"
  },
  "home": {
    "warren_hero_headline": "title",
    "warren_hero_subheadline": "excerpt",
    "warren_services_summary": "brand.primary_services",
    "warren_service_area_summary": "intake.priority_seo_locations"
  },
  "service_detail": {
    "warren_service_headline": "title",
    "warren_service_intro": "excerpt",
    "warren_service_content": "content"
  },
  "service_area": {
    "warren_local_headline": "title",
    "warren_local_intro": "excerpt",
    "warren_local_content": "content"
  }
}
```

## How WARREN Publishes the Page

When publishing a Site Builder build, WARREN:

1. Creates a draft WordPress page.
2. Applies the selected WordPress template slug.
3. Pushes the page content, excerpt, SEO meta, and mapped ACF fields.
4. Publishes the page if the requested status is `publish`.
5. Saves the WordPress page ID and URL back to the WARREN build.

If direct REST publishing works, WARREN uses it.

If direct REST publishing fails because of host security, blocked fields, or endpoint restrictions, WARREN tries the helper plugin endpoints.

For ACF templates, the helper plugin is the most reliable path because it can call WordPress functions directly, including `update_field()`.

## WordPress Helper Plugin

Use the helper plugin when:

- the host blocks REST publishing
- SiteGround or another WAF returns a challenge
- WordPress rejects custom `acf` or `template` REST parameters
- ACF fields are not updating through normal REST
- queued pull publishing is needed

In WARREN:

1. Go to the brand's Settings.
2. Open the WordPress connection panel.
3. Download the GroMore/WARREN Publisher Helper plugin.
4. Install it in WordPress under `Plugins > Add New > Upload Plugin`.
5. Activate it.
6. In WordPress, go to `Settings > GroMore Publisher`.
7. Save the WARREN app URL, brand ID, WP username, and WP Application Password if pull publishing is needed.

For this template/ACF workflow, use plugin version `1.4.0` or newer.

## Testing Checklist

Use this checklist before treating a template as production-ready.

In WordPress:

- The page template appears in the Page Template dropdown.
- The ACF field group appears only where expected.
- A manual sample page renders correctly.
- Mobile layout is clean.
- Empty optional fields do not leave broken sections.
- The page has a clear CTA above the fold.
- The page does not rely on hardcoded brand copy.

In WARREN admin:

- The Site Template is active.
- The WordPress template slug is exact.
- ACF mapping is enabled.
- ACF JSON validates.
- Field names match WordPress.
- The template has a clear description and vertical notes.

In WARREN Site Builder:

- Choose the template during intake.
- Generate a build.
- Review the pages.
- Publish to WordPress.
- Open the WordPress page.
- Confirm the template is applied.
- Confirm ACF fields are filled.
- Confirm SEO title and description are set.
- Confirm page links, CTAs, and forms are correct.

## Troubleshooting

### The page publishes but the design does not apply

Likely causes:

- wrong template slug
- template file not available in the active theme
- WordPress user lacks permission
- block theme/template hierarchy is overriding the expected template

Fix:

- Confirm the template appears in the WordPress Page Template dropdown.
- Copy the exact template path/slug.
- Re-save the WARREN Site Template.
- Republish a test page.

### The page publishes but ACF fields are empty

Likely causes:

- ACF mapping is disabled in WARREN
- field names do not match
- ACF field group location rule does not apply to the page
- direct REST path rejected the `acf` payload
- helper plugin is missing or outdated

Fix:

- Enable `ACF mapped` on the WARREN Site Template.
- Check the ACF JSON field names.
- Confirm the WordPress template slug is applied to the page.
- Update the helper plugin to version `1.4.0` or newer.
- Republish.

### The ACF field saves as raw JSON text

This can happen when ACF is not active or WARREN falls back to post meta.

Fix:

- Confirm Advanced Custom Fields is installed and active.
- Confirm the helper plugin is updated.
- Use real ACF field names or field keys.
- For complex fields, prefer ACF field keys if first-time saves are inconsistent.

### WordPress returns 401

Likely causes:

- wrong username
- wrong Application Password
- Application Password was deleted
- user does not have enough permissions

Fix:

- Create a fresh Application Password in WordPress.
- Save it in WARREN.
- Run the WordPress connection test.

### WordPress returns 403, 406, 418, 429, or a CAPTCHA page

Likely causes:

- host firewall
- security plugin
- bot protection
- blocked REST API route

Fix:

- Install or update the helper plugin.
- Use pull publishing if inbound requests are blocked.
- Whitelist the WARREN app URL or server IP if the host allows it.

### The template works for one brand but looks bad for another

Likely causes:

- the template is too niche
- fields assume a specific service type
- long service names or city names overflow
- CTA sections rely on copy that not every brand has

Fix:

- Add vertical notes to the WARREN Site Template.
- Create a separate template for that vertical.
- Make fields more generic.
- Add CSS constraints for long words and mobile wrapping.
- Test with at least three different service businesses before using it broadly.

## Naming Conventions

Use clear names that identify the template family and use case.

Good WARREN Site Template names:

```text
Local Service Premium Home
Trust-Heavy Multi-Service Site
Commercial Service Authority
Simple Quote-First Landing Site
Neighborhood SEO Service Area Kit
```

Good WordPress template slugs:

```text
templates/warren-home-premium.php
templates/warren-service-authority.php
templates/warren-commercial.php
templates/warren-location.php
```

Good ACF field names:

```text
warren_hero_headline
warren_service_area_summary
warren_primary_cta_text
warren_faq_items
warren_schema_json
```

## What Not To Do

Do not:

- build every new site from scratch
- hardcode one brand's name into a reusable template
- use vague ACF field names
- rely on one giant WYSIWYG field for the whole page
- create 30 templates before proving 3 strong ones
- skip mobile testing
- save a template in WARREN before the WordPress sample page is approved
- assume ACF fields are working without opening the published page

## Production Acceptance Standard

A template is production-ready when:

- it has a strong approved WordPress sample page
- it has a stable WordPress template slug
- it has a documented ACF field group
- the WARREN Site Template has ACF mapping enabled
- a test WARREN build can publish into WordPress
- the output looks launchable before manual polishing
- the template can support at least one clear vertical or use case
- another operator could follow the notes and understand when to use it

## Recommended First Templates To Build

Start here:

1. Home services lead-gen homepage
2. Home services service-detail page
3. Home services service-area page
4. Commercial/B2B service homepage
5. Trust-heavy premium brand homepage

Once those are strong, add vertical variants for:

- plumbing
- HVAC
- electrical
- roofing
- pest control
- pet waste removal
- cleaning
- med spa
- legal
- automotive

The system should scale by reusing field contracts and design families, not by creating one-off layouts for every client.
