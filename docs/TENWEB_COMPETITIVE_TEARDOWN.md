# 10Web Competitive Teardown

This is a product teardown focused on what matters for this repo, not a generic competitor summary.

## Core observation

10Web is not winning on AI generation alone.

They are selling a bundle:

- prompt-to-site generation
- chat-based editing
- visual editing
- managed hosting
- speed optimization
- migration and cloning
- white-label reseller and API distribution

The moat is not one feature. It is the compressed path from idea to live site to managed recurring revenue.

## What 10Web is clearly pushing

From public positioning, their headline promise is:

- production-ready website from a prompt
- live in under a minute
- editable after generation
- mobile responsive and SEO-ready
- available under your brand via reseller, API, or self-hosted WordPress flow

They also push input flexibility:

- prompt-to-site
- URL-to-site cloning
- Figma-to-site
- ecommerce generation

That matters because it reduces blank-page friction. A user can start from almost anything.

## Speed optimization stack

The speed offer is not subtle. They market a dedicated optimization layer, not just "better code".

Public claims include:

- automated 90+ PageSpeed
- improved Core Web Vitals
- Cloudflare Enterprise CDN
- full-page caching
- dynamic caching for ecommerce
- CSS, HTML, and JS minification/compression
- critical CSS
- lazyloading for images, iframes, and video
- WebP conversion
- font delivery optimization
- delay or removal of non-critical third-party JS
- WAF, SSL, DDoS, and bot protection

What this means strategically:

- They productized performance as a SKU.
- They can promise outcomes because hosting, caching, and optimization are bundled.
- The claim is less about generator quality and more about controlling the full delivery environment.

What is worth copying:

- performance presets and visible scoring inside the builder workflow
- image optimization discipline as part of the asset system
- pre-publish performance checklist
- default fast-loading section patterns instead of heavy section markup

What is not worth copying right now:

- broad 90+ score claims without controlling hosting and caching
- a separate speed product before the base site output is more deterministic

## Migration and cloning

10Web is using migration as an acquisition wedge.

Public positioning includes:

- 1-click WordPress migration
- complete transfer messaging
- no technical skills required
- URL-to-site cloning or recreation
- duplicate websites for agencies managing similar projects

This is smart because it captures users who already have site assets and do not want to start over.

What is worth copying:

- import existing sitemap, nav, footer, and service structure from a current site
- import approved copy blocks, images, and brand tokens from a current site
- offer a rebuild mode: current site in, cleaner site out
- allow section harvesting from imported pages into the reusable library

What is not worth copying right now:

- full generic site cloning as a magic promise
- fragile HTML-to-builder conversion without a clear section normalization layer

## White-label and API strategy

This is where 10Web gets more serious than a normal builder.

Public positioning shows three delivery modes:

- reseller dashboard
- Website Builder API
- self-hosted WordPress solution

API and white-label claims include:

- launch a production-ready website with one API call
- fully branded dashboard and editor
- billing, subscriptions, invoices, and revenue dashboards
- white-labeled WordPress admin
- custom pricing plans
- customer and team management
- support for custom user data integration and platform-specific flows
- dedicated onboarding, Slack support, and account management

Self-hosted WordPress flow includes:

- license plugin
- WP Toolkit or hosting-plan setup
- pre-installed plugin/theme sets
- create-site flow that lands users inside a branded WordPress AI builder
- chat editor, visual editor, and outline editor inside WordPress

What this means strategically:

- They are not just selling sites. They are selling infrastructure leverage to hosts, SaaS companies, and agencies.
- Their builder is designed to be embedded in somebody else's product and monetized there.

What is worth copying:

- build intake from existing client data automatically
- stronger outline-first page planning before content generation
- reusable widget and section library that can be versioned by vertical
- branded admin and client flows that feel owned by this platform

What is probably not worth copying yet:

- billing and reseller infrastructure
- a broad external API before the internal asset pipeline is stable
- a huge widget catalog before the top 10 patterns are excellent

## Their real UX play

10Web keeps reducing the number of hard decisions the user has to make.

Their stack appears to do this sequence:

1. Start from prompt, URL, or Figma
2. Generate a full outline and design system
3. Let the user edit with chat or visual tools
4. Handle hosting, speed, and launch
5. Keep the site in a managed environment

That is why the product feels "fast". The user is not switching tools every five minutes.

## What this repo should copy next

Priority 1:

- deterministic section library by vertical
- deterministic theme packs
- stronger outline-first builder flow
- better image library tagging and section mapping
- existing-site import that extracts nav, footer, service pages, and proof blocks

Priority 2:

- guided rewrite and section swap workflow inside the editor
- publish-time performance checks
- prebuilt conversion templates for top local-service funnels

Priority 3:

- partner or white-label surface area
- API for build requests
- seeded vertical packs distributed across multiple clients or brands

## What this repo should avoid

- trying to out-market 10Web on hosting claims without owning the same stack
- chasing a giant widget count
- vague "AI builds anything" messaging when the win here should be vertical quality and deterministic output
- generic cloning promises without import normalization

## Product direction for this codebase

The strongest move here is not to become a generic website builder.

The strongest move is:

- local-service focused
- high-conversion section packs
- theme packs that actually get used
- existing-site import and cleanup
- WordPress publish path
- brand, quote-tool, offer, and SEO context baked into generation

That is narrower than 10Web, but more defensible for this audience.

## Recommendation

Use 10Web as proof that the market wants one compressed workflow.

Copy the workflow compression, not the product sprawl.

For this repo, that means:

1. Intake to outline to sectioned build
2. Reusable section and image packs by vertical
3. Existing-site import and migration helpers
4. Shared header/footer/theme enforcement
5. Performance and publish checks

That path is realistic, product-coherent, and directly aligned with the builder work already in this codebase.
