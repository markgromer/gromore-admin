# GroMore Platform - Revision History

Complete development history from initial commit to present. Each entry references the git commit hash where applicable.

---

## Phase 1: Foundation (Initial Build)

### `3cdfca0` - Initial Commit
- Agency analytics platform with web admin dashboard
- Flask + SQLite + Jinja2 stack
- Basic brand management and report viewing

### `eb9a108` - CSV Reports + Render Deploy
- CSV-based report generation
- Month context handling
- OpenAI API key setting in admin
- Fixed Render boot issues

### `569ad89` - CSV-First Workflow
- Month context throughout reports
- CSV status tracking
- Actionable error messages

### `631ca50` - `853a645` - Auth Hardening
- Persistent Flask secret key (no more login resets on redeploy)
- Environment-based admin password reset for Render
- Bootstrap username/password recovery

### `691cd86` - AI Brief
- OpenAI-powered AI brief generation
- Embedded AI analysis in reports

### `05dbe8a` - `fb60d4d` - Brand AI Chat + Render Config
- Brand-level AI chat assistant
- Fix Render config and persistent storage

---

## Phase 2: OAuth + Integrations

### `8618cd2` - `f3e94f8` - Meta OAuth
- Meta (Facebook) OAuth integration
- Hardened callback handling

### `c352d60` - Dashboard Upgrade
- AJAX chat (no page reloads)
- Quick-action buttons, markdown rendering
- Dashboard health panel
- Auto-detect APP_URL
- Token expiry warnings

### `d116056` - `93b0eaa` - OAuth Reliability
- Fix redirect_uri_mismatch behind Render proxy (X-Forwarded-Proto)
- Read Google OAuth creds from DB in callback
- OAuth token auto-refresh
- Surface API pull errors instead of swallowing them

### `74251b8` - Persistent Storage Fix
- Auto-detect Render via RENDER env or /data mount
- Always use /data/ paths on Render
- Startup path logging

### `ef52873` - `fa9127a` - CSRF + UX
- CSRF protection across all forms
- Dashboard quick actions
- Meta auto-refresh
- Agency branding in UI
- Settings test buttons
- Report deduplication
- Exempt OAuth blueprints from CSRF

---

## Phase 3: Deep Analytics

### `94dd218` - `6872cec` - Brand Settings Expansion
- Brand voice/tone configuration
- KPI targets
- CRM integration settings
- AI context wiring

### `9769428` - API Data Fixes
- Fix API data format issues
- Report regeneration
- OAuth redirect_uri normalization

### `78bf948` - `f82c371` - Platform-Specific Analytics
- Meta reporting: campaign + ad-level insights
- GA4 depth: source and landing-page intelligence
- Google Ads spend tracking end-to-end

### `05097f6` - `d7fe208` - Revenue Intelligence
- Google Ads spend tracking
- KPI target intelligence
- Strategic reporting layer

### `80dd621` - `ad1323e` - CRM + Revenue
- True ROAS from CRM/offline revenue data
- Secure CRM revenue webhook automation
- One-click CRM webhook test

### `7cb9167` - `dbb6f5e` - Report Polish
- Webhook health status display
- 3-decimal report precision cap

### `bd508e9` - `4625111` - Competitive Intelligence
- Keyword intelligence module
- Competitor watch in reports

### `7077e2b` - Deep-Dive Insights Hub
- Deep-dive insights hub
- Stronger AI strategy context

### `3bf278f` - `30038bd` - Industry Benchmarks
- Research-backed industry benchmarks with per-niche website metrics
- Recalibrated pet waste removal benchmarks from operator feedback

---

## Phase 4: Client Portal

### `3930281` - Client Self-Service Portal
- Client login system
- Client dashboard
- Step-by-step action advisor

### `63699e4` - `4bb94a3` - Client Campaigns + OAuth
- Campaign management in client portal
- Client-facing Google and Meta OAuth connections
- Fix OAuth redirect URI for client flow

### `74f9c48` - OAuth Redirect Fix
- Fix OAuth redirect URI mismatch for client portal connections

### `6c4a90f` - `290d571` - Action Steps + Ad Builder
- AI-generated deliverables from real account data
- Ad Builder with platform-specific output
- Reframe action plan to GroMore platform tone

### `151884b` - `84c1f60` - Ad Builder Strategies
- Objective strategies (Search, Display, PMax, Video)
- Strategy-based prompting
- Strategy-specific Google formats
- Data-used rationale in output

### `3e0d777` - Client Report Overhaul
- 10x client report enhancement
- Google Ads search terms pipeline

### `0f7f39a` - `915a218` - Client UX Polish
- My Business settings page
- Async dashboard loading animation
- Nav tab click loading (progress bar + page fade)

### `96e6a28` - Help Center
- Help center page
- Client OpenAI key/model selection on settings

---

## Phase 5: Creative Center

### `b2ec20d` - Creative Center Launch
- Logo upload
- AI copy generation
- Pillow image compositing (server-side)

### `f767417` - `5bc0cb9` - Creative Stability Fixes
- Robust font loading for Linux
- Fix font.size crash on fallback fonts
- Show server errors in UI
- Wrap route in try/except for JSON errors
- Optimize gradient rendering
- Handle HTML error responses in JS

### `7d90859` - Memory Optimization
- Rewrite creative gen for low memory (Render free tier)
- RGB mode (no RGBA overlays)
- Thumbnail cap on source images
- JPEG output instead of PNG

### `cc01f5c` - CSRF Fix
- Include CSRF token in AJAX FormData and X-CSRFToken header

### `2389cc3` - Style Controls
- Overlay templates (lower third, full overlay, etc.)
- Text placement options
- Font family choices

### `eeb338c` - Creative Upgrade
- AI style prompt for creative direction
- New overlay/shape templates
- One-click Ad Builder to Creative transfer (sessionStorage bridge)

### `ff5f60b` - Fallback Template
- Automatic fallback template on render errors

### `f9fa02b` - `d149454` - Visual Controls
- Semi-transparent brand colors in overlays
- Enlarged logo (3x)
- Logo corner selector
- Bubbles/boxes/full-lower-third templates
- Gradient modes
- Phone/website footer toggles

### `d1393c7` - `5e6fb97` - Typography
- Per-element font family/weight/color controls
- More font families added
- One-click style presets (Bold Promo, Clean Minimal, Story Social, Luxury Classic)
- Simple tuning sliders
- 16px safe padding on all overlays

### `4471fb9` - `e2f32e3` - Brand Kit
- Multiple logo variants with rename/delete
- Set primary logo with fallback handling
- Custom logo X/Y placement in creatives

---

## Phase 6: AI Chat + Client Intelligence

### `ef6dc3e` - `d66f184` - Client AI System
- Client action AI chat
- Deep analysis capabilities
- Google Drive settings fields (folder_id, sheet_id)
- Global assistant with per-workflow models

### `1aa3d9b` - `d53f8e3` - Chat System Prompt
- Admin-controlled chat system prompt
- Premium client UI polish
- DEFAULT_CHAT_SYSTEM_PROMPT constant
- Comprehensive chatbot system prompt with full portal awareness

### `2fa85de` - Campaign Draft + Config
- Save as Draft for campaigns
- Config check endpoint
- Detailed error messages for missing Google/Meta config

### `095bd47` - Jarvis Personality
- Warm, witty, Iron Man-inspired voice (later replaced by Warren)

### `cee5a86` - `13883ef` - Google Ads API Upgrade
- Upgrade API v18 to v19 (v18 sunset)
- Clean error messages instead of raw HTML dumps
- Clickable link to Google Ads API Center

### `ac8dd70` - Chat Upgrade
- Markdown rendering in chat
- Animated typing effect
- 25-message memory window
- Copy button + clear chat
- Better personality integration

### `01ab71e` - `5d998c6` - Action Steps AI Overhaul
- Fallback steps when AI generation fails
- Per-item data attached to AI prompts
- WRONG/RIGHT examples for specificity
- Force specificity in generated action items

### `db068ec` - `83adb50` - OAuth Error Messages
- Better Google OAuth error messages (invalid_client, redirect_uri_mismatch)
- Fix 500 on Google/Meta OAuth callback (supply client_base.html context variables)

---

## Phase 7: Organic Facebook + Meta

### `0697652` - `a717b69` - Organic Facebook Tracking
- Page insights collection
- Top posts analytics
- AI pipeline for organic analysis
- Suggestions engine
- Dashboard cards for organic metrics
- Admin Page ID field
- Admin insights section
- Data flow fixes

### `7e43d18` - `2eb8e26` - Page Detection
- Auto-detect page ID from OAuth
- Use page access token for insights
- Flash found pages, warn if none
- Clear page ID on disconnect

### `66269eb` - Warren Replaces Jarvis
- Replace Jarvis personality with Warren
- Strategic decision engine personality

---

## Phase 8: Metric Accuracy + Debugging

### `4b569c4` - `613f02a` - Metric Fixes
- Fix broken fallback code
- Detailed logging for organic debug
- Fix deprecated metric references
- Per-metric fallback system
- Organic diagnostic route
- Fix CTR dilution
- Avg position: top 5 queries by impressions, best 3 positions

### `451fa0c` - Meta OAuth Error
- Better explain invalid app secret clearly

---

## Phase 9: Warren AI + Web Search

### `f88ebc2` - Warren Tools
- Web search capability (DuckDuckGo)
- Image generation via DALL-E 3
- OpenAI function calling framework

### `1d8bf2f` - Web Search Fix + Place ID
- Fix web search to use real DuckDuckGo results
- Place ID finder directly on heatmap page

---

## Phase 10: Local Rank Heatmap

### `717ee21` - Heatmap Launch
- Local rank heatmap with grid-based scanning
- Dismissable action items
- Google Maps API integration

### `38ce77e` - `f7c3fd7` - Heatmap Config
- CSRF token on Maps API key form
- Allow google_maps_api_key in update allowlist

### `aca316a` - AJAX CSRF
- Add X-CSRFToken header to all AJAX fetch calls in heatmap and actions

### `309433d` - `18738f5` - Geocode Fixes
- Error handling for geocode API calls
- Fix geocode save using correct DB method

### `10990a3` - Heatmap Improvements
- Google Map display
- Fix search radius scaling
- Improve business name matching

### `0b86c0e` - `4abc680` - Heatmap Debug
- Debug output for scan diagnostics
- Fix duplicate lines in heatmap.py

### `fcbacba` - Debug Persistence
- Remove auto-reload after scan so debug info stays visible

### `4307059` - Place ID Finder
- Place ID finder in Connections tab
- Fallback to legacy Places API

### `9174a31` - CSRF Fix
- Add missing CSRF token to admin login form

### `8131090` - API Error Surfacing
- Surface actual Google API errors in Place ID search
- Add Find Place fallback

### `2947fda` - Manual Place ID
- Manual Place ID entry input
- Better empty results error messaging

### `00272a7` - API Diagnostics
- Surface API diagnostics in heatmap debug panel

### `cac4b59` - Nearby Search Fallback
- Nearby Search fallback for service-area businesses

### `69e9c8c` - Place ID Verification
- Place ID verification endpoint
- Show what Place ID resolves to in debug panel

---

## Phase 11: Warren Long-Term Memory

### `824e75a` - Warren Memory System
- `warren_memories` table (category, title, content, embedding, status)
- CRUD methods: add, get, update, get_with_embeddings
- OpenAI text-embedding-3-small for vector embeddings
- Cosine similarity vector search
- Two new tools: save_memory, recall_memories
- Auto-recall relevant memories before each response
- System prompt sections: "YOUR MEMORY" + "MEMORY DISCIPLINE"
- Updated chat_with_warren() signature with db/brand_id params

---

## Phase 12: Creative + Drive + Ad Builder Upgrade (In Progress)

### UNCOMMITTED - Google Drive Integration Module
**New file: `webapp/google_drive.py`**
- Token refresh handling (uses existing OAuth refresh_token)
- `get_valid_access_token()` - auto-refresh if expired
- `ensure_folder_structure()` - creates Creatives, Ads, Images, Reports subfolders
- `upload_file()` - multipart upload to specific subfolder
- `list_files()` - list files in subfolder with metadata
- `download_file()` - download file content by ID
- `setup_brand_drive()` - one-call setup for new brands

### UNCOMMITTED - Drive Auto-Save in Creative + Ad Builder
**Modified: `webapp/client_portal.py`**
- Creative generate route: auto-saves output JPEG to Drive "Creatives" folder
- Ad Builder generate route: auto-saves ad package JSON to Drive "Ads" folder
- Updated Google Drive settings route to auto-create subfolder structure on save
- New API routes: `/api/drive/files/<subfolder>` (list), `/api/drive/upload` (upload)

### UNCOMMITTED - Creative Center Canvas Editor (Fabric.js)
**Modified: `webapp/templates/client/client_creative.html`**
- Complete rewrite from form-submit to Fabric.js drag-and-drop canvas
- Three-column layout: tools/upload (left), canvas (center), properties (right)
- Toolbar: Add Headline, Body, CTA text, Rectangle, Circle, Logo
- Image upload: drag-and-drop + file picker, multiple images supported
- AI image generation: prompt-based via Warren's DALL-E tool
- Quick overlays: Bottom Fade, Top Fade, Full Tint, Banner, Center Strip
- Layer management panel with reorder and delete
- Properties panel: edit text, font, color, size, opacity, position per element
- Canvas size selector for all 6 ad formats
- Export as PNG at full resolution with auto-save to Drive
- Server Generate button (legacy Pillow path) still available
- Drive Images panel: pull images from brand's Drive Images folder
- Ad Builder prefill via sessionStorage still works
- AI Copy generator: describe ad, get headline/body/CTA, place on canvas

### PENDING - Ad Builder SERP Preview
- Google Search preview showing how ad will appear in results
- Image integration with AI generation
- Warren inline recommendations
- Save completed ad package to Drive

---

## Architecture Notes

### Tech Stack
- **Backend**: Flask 3.x + SQLite + Jinja2
- **Hosting**: Render (free tier), auto-deploy from main branch
- **AI**: OpenAI GPT-4o/4o-mini, DALL-E 3, text-embedding-3-small
- **Google**: Analytics Data API, Search Console, Ads API v19, Drive API, Maps/Places API
- **Meta**: Marketing API, Page Insights API
- **Frontend**: Bootstrap 5.3, Bootstrap Icons, Fabric.js 5.3 (canvas editor), Marked.js (markdown)
- **Image**: Pillow (server-side), Fabric.js (client-side canvas)

### Key Files
| File | Purpose |
|------|---------|
| `webapp/app.py` | Flask app factory, route registration |
| `webapp/database.py` | SQLite schema, all CRUD operations |
| `webapp/client_portal.py` | All client-facing routes (~2300 lines) |
| `webapp/ai_assistant.py` | Warren AI: chat, tools, memory, embeddings |
| `webapp/ad_builder.py` | AI ad copy generation (Google + Facebook) |
| `webapp/google_drive.py` | Drive API: upload, folders, file management |
| `webapp/heatmap.py` | Local rank scanning, Places API |
| `webapp/report_runner.py` | Report generation pipeline |
| `webapp/api_bridge.py` | Google Ads API + GA4 data pulls |
| `webapp/campaign_manager.py` | Campaign CRUD |
| `webapp/oauth_google.py` | Google OAuth callback handler |
| `webapp/client_oauth_google.py` | Client-facing Google OAuth flow |

### Database Tables
- `brands` - Brand profiles with all settings and API keys
- `connections` - OAuth tokens per brand per platform
- `client_users` - Client portal login credentials
- `warren_memories` - Long-term AI memory with vector embeddings
- `reports`, `action_items`, `campaigns` - Core business data
