# GroMore Platform - Complete Feature Inventory
> Last updated: April 24, 2026 - Audited against live codebase and commits through `41ecc48`

---

## Recent Additions Since April 16, 2026

### Warren Automations + Reliability
- **Appointment Reminder System** - Day-ahead Sweep and Go reminder automation with enable/disable controls, send-time window, channel routing, dedupe protection, and manual run support
- **Reminder Run Reporting** - Per-brand run logs with waiting/completed/failed states, reasons, counts, and local-time summary metadata
- **Cron Auth Hardening** - Locked down appointment reminder cron endpoint auth flow and Render cron integration
- **Legacy Schedule Compatibility** - Accepts legacy reminder settings like AM/PM send times and timezone aliases (for example, Central Time labels)
- **Timezone-Aware Day-Ahead Date Selection** - Calculates tomorrow in each brand's configured local timezone before querying SNG dispatch data
- **Active Client Marketing Safety** - Prevents active clients from being enrolled into Warren marketing flows unintentionally
- **Sweep and Go Event Foundation** - Webhook/event ingestion layer for CRM-triggered Warren automations
- **CRM Event Automations** - Triggered Warren workflows for CRM lifecycle events

### Client Portal + Dashboard UX
- **Connections/Automations Split** - Broke out client connections and automations into clearer dedicated flows
- **Dashboard Simplification** - Reduced complexity and noise in dashboard layout and logic for faster client comprehension
- **Warren Snapshot Refinement** - Streamlined Warren snapshot panels and trimmed dashboard console verbosity
- **Inline Warren Console Transcript** - Added in-context transcript visibility inside dashboard workflow
- **Navigation Simplification** - Cleaner client navigation and tab flow to reduce click friction
- **Client Password Settings** - Added first-class password management inside client settings

### Facebook Post Scheduler + Social Workflow
- **Recurring Character Builder** - Persistent storytelling character controls for recurring social content
- **Storytelling Profile Controls** - Configurable narrative profile and cadence options for post generation
- **Story Builder Integration** - Warren-assisted story workflow connected to scheduler authoring
- **CSV Review Upgrades** - Editable/selectable CSV post review before scheduling
- **Authenticity Guardrails** - Stronger anti-generic content checks for Facebook outputs
- **Preview + Length Controls** - Better preview surfaces with practical length constraints
- **Drive Upload Reliability** - Improved Google Drive image URL handling, upload recovery, and error messaging
- **Post Proof + Cleanup Flow** - Added scheduler post proofing and cleanup pipeline improvements
- **Timezone-Aware Scheduling Validation** - Scheduler now correctly handles browser-local `datetime-local` inputs instead of rejecting valid near-future posts as "already passed"

### Heatmap + Local SEO Intelligence
- **Async Large Heatmap Scans** - Moved heavy scans to asynchronous execution for reliability at larger radii/keyword sets
- **Browser-Driven Maps Engine** - Rebuilt heatmap collection around browser-rendered Maps results for stronger real-world capture
- **Docker/Runtime Improvements** - Deployment/runtime updates for browser-based heatmap jobs, including Playwright availability in deploy targets
- **Fallback Resilience** - Better key/location fallbacks, restored maps connections, and improved SAB ranking language clarity
- **Name-Matching Flexibility** - Relaxed listing-name matching to reduce false negatives during competitor grid analysis

### Site Builder + Visual Editing Platform
- **Site Builder Engine Launch** - Full builder backend, admin panel, client UI, and agent-powered prompt workflows
- **5-Step Intake Wizard** - Expanded guided intake with business context, page selection, and form-aware inputs
- **Design + Brand Kit Expansion** - Richer design controls, expanded brand kit handling, quote controls, and layout flexibility
- **Reference-Site Inspiration Pipeline** - Extracted section patterns from reference sites and fed them into builder generation
- **GrapesJS Visual Editor** - Wix-like visual editor workflow with richer image/background placement and improved UX
- **AI Rewrite + Custom Pages** - In-editor AI rewrite support and custom page generation/editing
- **Template Kit Workflow** - Production-ready site kits, operator kits, and default page system modernization
- **Workspace + Review UX Improvements** - Better cloning, cleanup, admin uploads, step layout, and review experience
- **Connections Reliability + Help Coverage** - Hardened builder reliability and added hideable onboarding/help guidance

### Revenue + CRM Intelligence
- **SNG Revenue Sync Fixes** - Improved auto-sync correctness for CRM-derived revenue data
- **Revenue Payload Fallbacks** - Added support for list-shaped payloads and fallback handling in SNG revenue endpoints
- **Opportunity Insights Panel** - Repurposed SNG CRM panel for richer opportunity intelligence views

### QA + Stability Improvements
- **Feature Flag Seed Race Condition Fix** - Eliminated duplicate-seed race with insert-ignore strategy
- **Client Tab Loading Robustness** - Fixed client tab loading regressions tied to organic post count flows
- **Appointment Reminder Time Display Accuracy** - Client automations/settings now show reminder run times in configured local timezone context

---

## W.A.R.R.E.N. - AI Sales Agent

**Weighted Analysis for Revenue, Reach, Engagement & Navigation**

### Channels
- **SMS** (via Quo/OpenPhone) - Two-way text conversations with leads
- **Facebook Messenger** - Full Messenger integration with 24-hour response window enforcement
- **Meta Lead Forms** - Auto-capture from Facebook/Instagram lead gen campaigns
- **Hosted Warren Lead Forms** - Brand-specific public lead form pages with standalone link and iframe embed support
- **Email** - Billing reminder delivery channel

### Autonomous Capabilities
- **AI-Powered Auto-Reply** - GPT-4o generates contextual responses to inbound messages
- **Confidence Gating** - Only auto-sends when confidence score is >= 0.7, otherwise holds for human review
- **Configurable Reply Delay** - 0 to 300 second delay before sending, so replies feel natural
- **Information Collection** - Progressively gathers name, phone, email, address, service needed (one field per message)
- **Hosted Form Intake** - Public form submissions create Warren lead threads with structured field capture and channel tagging
- **Objection Detection** - Identifies objections (too expensive, competitor quote, not ready, timing, etc.) and logs them for context-aware follow-up
- **Quote Generation** - Three modes: simple (single price), hybrid (range with explanation), structured (line-itemized)
- **Pipeline Auto-Advance** - Moves leads through stages automatically based on conversation progress (new > engaged > quoted > qualified > booked > won)
- **Handoff Detection** - Recognizes when to escalate to a human based on configurable rules, stops auto-replying on that thread
- **Consent-Aware Auto-Texting** - Hosted form leads only receive Warren text follow-up when texting is enabled and consent rules are satisfied
- **CRM Auto-Push** - Pushes closed leads to connected CRM (GoHighLevel, HubSpot, webhook) with full contact details

### Follow-Up & Nurture Engine
- **Automated Follow-Ups by Temperature** - Hot leads (2hr wait, 3 attempts), Warm leads (24hr, 2 attempts), Cold leads (48hr, 2 attempts)
- **Spouse/Partner Check Detection** - Recognizes "let me check with my spouse" patterns, sends low-pressure follow-up
- **Soft Close Detection** - Catches leads going quiet mid-conversation, asks if they want to continue
- **Ghost Detection** - Marks leads as lost after configurable hours (default 72) with no response
- **Do Not Disturb** - Timezone-aware quiet hours (default 9pm-8am), optional weekend blocking, per-brand toggle

### Payment Reminders
- **Billing Due Detection** - Integrates with Sweep & Go CRM to find upcoming payments
- **Dual-Channel Delivery** - Email and/or SMS reminders sent X days before due date
- **Custom Templates** - Merge fields for brand name, client name, due date
- **Duplicate Prevention** - Never sends the same reminder twice

### Training & Configuration (40+ settings)
- Reply tone, service menu, pricing notes, business hours
- Quote mode, guardrails, handoff rules
- Example language, disallowed language, objection playbook
- Closing procedure, onboarding link, booking confirmation
- Service area schedule (day-by-area routing)
- Nurture timing per temperature tier
- DND timezone, hours, weekend toggle
- Hosted lead form copy, CTA, consent text, success state, and optional field controls

### SMS Compliance
- A2P opt-out footer on all messages
- STOP/START/HELP keyword detection and processing
- Opt-in/opt-out tracking per phone number per brand
- Quo webhook signature verification

---

## CLIENT PORTAL - Lead Inbox

### Thread Management
- **Multi-Channel Thread List** - All leads from SMS, Messenger, Meta Lead Forms, and hosted Warren forms in one unified inbox
- **Channel Avatars** - Visual icons distinguishing SMS, Messenger, and lead-form threads
- **Unread Indicators** - Blue dot and bold name for threads with unread messages
- **Pipeline Status Badges** - Color-coded stage labels (new, engaged, quoted, qualified, booked, won, lost)
- **Private Thread Lock** - Lock icon on threads marked private (Warren won't auto-reply)
- **Filters** - Filter by pipeline stage and channel type
- **Thread Preview** - First 60 characters of the conversation summary

### Conversation View
- **Message Bubbles** - Inbound (white, left), outbound human (blue, right), Warren (purple, right), system (amber, center)
- **Warren Draft** - One-click AI-suggested reply that auto-fills the reply box for editing before sending
- **Manual Reply** - Type and send messages directly from the inbox
- **Pipeline Stage Selector** - Click any stage to move the lead through the pipeline
- **Handoff Button** - Flag thread for human takeover, stops Warren auto-reply
- **Private Toggle** - Mute Warren on specific threads (for personal conversations on the business number)
- **Delete Lead** - Remove thread and all messages with confirmation

### Pipeline Analytics
- **Total Leads Count** - Sum of all threads
- **Active Leads Count** - Total minus won and lost
- **Close Rate** - Won / (Won + Lost) percentage
- **Average Response Time** - Minutes between first inbound and first outbound
- **Pipeline Funnel** - Horizontal stacked bars showing lead distribution by stage

### Hosted Lead Form Builder
- **Brand-Specific Public Form URL** - Each brand can publish a hosted Warren form at its own public link
- **Iframe Embed Code** - Copy/paste embed snippet for dropping the Warren form onto almost any website
- **Always-Required Contact Fields** - Full name and mobile number stay required on every hosted form
- **Selectable Extra Fields** - Owners can toggle service needed, email, company name, service address, and job details from the backend
- **Service Menu Support** - Optional service list can render as guided choices instead of free text
- **Consent + Success State Controls** - Customize SMS consent copy, CTA text, headline, intro, and success message per brand

---

## CLIENT PORTAL - Commercial Accounts

### Commercial Lead Engine
- **Commercial Target Search** - Search by location and commercial account type
- **Website + Contact Enrichment** - Pull website, phone, public email, and basic site signals
- **Brand-Scoped Import** - Import commercial targets directly into the client's WARREN pipeline
- **Website-Aware Dedupe** - Match imported accounts by website, email, or business identity
- **Commercial Strategy Brief** - Generate outreach angle, likely buyer, pain points, and next actions

### Qualification + Walkthrough
- **Proposal Qualification Form** - Capture decision-maker role, buying process, budget, goals, and timeline
- **Commercial Walkthrough Capture** - Record property label, waste station count, common areas, relief areas, access notes, gate notes, and disposal constraints
- **Add-On Capture** - Track refill, deodorizer, sanitizer, and other service requirements
- **Walkthrough Media Links** - Store walkthrough photo URLs in the commercial record
- **Proposal Readiness Tracking** - Keep qualification and walkthrough inputs tied to proposal prep

### Proposal + Nurture
- **Commercial Email Outreach** - Send manual outbound emails from the commercial thread
- **Commercial Drip Enrollment** - Enroll accounts into commercial nurture sequences
- **Thread-Aware Drip Logging** - Write drip sends and failures back into the same account timeline
- **Structured Proposal Builder** - Itemized recurring pricing for stations, common areas, relief areas, setup, and add-ons
- **Package Comparison** - Generate Basic, Standard, and Premium commercial packages from one scoped proposal model
- **Quote Status Tracking** - Track draft, sent, and approved commercial proposals

### Service Proof
- **Service Visit Log** - Store dated proof-of-service entries per commercial account
- **Station + Gate Confirmation** - Track stations serviced, bag restocks, and gate-secured confirmation
- **Issue Tracking** - Log issues and exceptions found during service visits
- **Client-Facing Notes** - Capture visit notes suitable for manager recap
- **Manager Recap Preview** - Generate an account-level summary from logged service visits inside the commercial workspace
- **Thread Timeline Integration** - Service visits also log timeline events and system messages in WARREN

---

## CLIENT PORTAL - Dashboard & Performance

### Overview
- **Overall Health Grade** - A through F letter grade with numeric score
- **W.A.R.R.E.N.'s Call** - Monthly AI-generated narrative summarizing performance
- **Channel Performance Cards** - Scorecards for website traffic, Facebook ads, Google ads, organic social, SEO
- **Performance Highlights** - AI-generated top 3 wins
- **Performance Concerns** - AI-generated top 3 issues needing attention
- **Month Selector** - View data for any reporting month
- **Live Refresh** - Pull fresh data from all connected APIs with error toast if something fails
- **Synced Timestamp** - Shows when data was last refreshed

### KPI Tracking
- **KPI Dashboard** - Track target CPA, leads, and ROAS against goals
- **Lead Pacing** - Shows if lead volume is ahead or behind the monthly target
- **Plain-English Explanations** - Every metric explained in simple terms
- **Color-Coded Status** - Visual indicators for on track, at risk, or behind

### Health Meter
- **Letter Grade** - A through F based on overall performance
- **Pace Label** - Ahead of pace / on pace / behind pace indicator
- **Progress Bar** - Visual bar showing performance relative to target

---

## CLIENT PORTAL - AI Assistant (W.A.R.R.E.N. Chat)

- **Real-Time Chat** - Conversational AI that knows the business and its data
- **Web Search** - Pull in live information from the web during conversations
- **Image Generation** - Create ad images and visuals with DALL-E
- **Long-Term Memory** - Saves and recalls important details across sessions
- **Chat History** - Full conversation history with option to clear
- **Proactive Suggestions** - Surfaces relevant tips based on current data
- **Model Selection** - Choose between GPT-4o-mini, GPT-4o, o3-mini, and others

---

## CLIENT PORTAL - Campaigns

### Campaign List & Management
- **Unified Campaign List** - View all Google Ads and Meta campaigns in one place
- **Real-Time Performance Metrics** - Impressions, clicks, CTR, CPC, conversions, cost per campaign
- **Pause/Resume Campaigns** - One-click campaign control
- **Budget Adjustment** - Change campaign budgets in real time
- **Negative Keywords** - Add negative keywords to Google Ads campaigns
- **Change Audit Log** - Full record of every campaign change made through the platform

### Campaign Creation & Launch
- **Quick-Launch Shortcut** - Answer 5 questions, get a ready-to-launch campaign
- **Strategy Selection** - Predefined strategies (Meta: Omnipresent, Lead Gen, Hyper-Local, Retargeting; Google: Search RSA, Display, PMax, Video, Lead Gen, Local Domination, Competitor Conquest)
- **AI Campaign Plan** - AI generates the full campaign plan based on strategy and business data
- **Configuration Form** - Review and customize every detail before launch
- **Image Upload** - Attach creative assets to campaigns
- **Draft Save/Load** - Save campaigns as drafts, come back later
- **Preflight Checks** - Automated validation catches issues before launch
- **One-Click Launch** - Launch directly to Google Ads or Meta from the platform

---

## CLIENT PORTAL - Ad Builder (AI-Powered)

- **Google Search RSA** - Responsive search ad headlines and descriptions
- **Google Display Ads** - Display ad copy
- **Google Performance Max** - PMax asset generation
- **Google Video Ads** - Video ad scripts and copy
- **Meta Feed Ads** - Facebook/Instagram feed ad copy
- **Meta Stories Ads** - Stories-format ad creation
- **3-5 Variations** - Multiple copy variations per ad type
- **Audience Recommendations** - AI suggests targeting based on business data
- **Industry-Specific Patterns** - Ad hooks tuned for plumbing, HVAC, dental, legal, and more
- **Ad Quality Scoring** - Each generated ad gets a quality score with explanation
- **Implementation Instructions** - Step-by-step guide for manual launch

---

## CLIENT PORTAL - Missions (Gamified Action Plan)

- **AI-Generated Missions** - Prioritized action items based on real performance data
- **Skill Level Profiles** - Beginner, Intermediate, Advanced tracks with different complexity
- **Step-by-Step Instructions** - Each action includes exactly how to implement it with platform-specific steps
- **Exact Targets From Live Data** - Missions can name real pages, queries, campaigns, ads, and other connected-data targets instead of generic placeholders
- **Delegate-Ready Handoff Notes** - Website, SEO, and creative missions can generate copy-and-send notes for a developer or designer
- **Platform-Aware Routing** - Paid-media missions use the actual connected channel context so Meta-only accounts are not sent to Google Ads, and vice versa
- **SEO Reality Checks** - Low-volume SEO opportunities can be rewritten to improve existing pages first instead of recommending unnecessary new local pages
- **Owner-Friendly Copy** - Mission notes and handoff messages are written in plainer language for small business owners
- **Difficulty Ratings** - 1 to 3 stars so users know what they can handle
- **XP & Leveling** - Progress from Rookie to Legend as actions are completed
- **Skill Categories** - Organized by Ad Optimization, SEO, Website Performance, Budget Strategy, Creative, Social, Strategy
- **Dismiss/Restore** - Mark actions as done or irrelevant, bring them back if needed
- **Ask Warren** - Chat with the AI about any specific mission

---

## CLIENT PORTAL - Your Team (AI Agent Squad)

### Agent Roster (9 Specialists)
- **Scout** - Google Ads campaign analysis and optimization
- **Penny** - Meta Ads performance review
- **Ace** - SEO keyword ranking and content analysis
- **Radar** - Website funnel analysis and conversion optimization
- **Hawk** - Competitor market analysis and counter-moves
- **Pulse** - KPI forecasting and prediction
- **Spark** - Revenue analysis and ROI tracking
- **Bridge** - CRM integration and pipeline health
- **Atlas** - Strategic overview and coordination

### Agent Operations
- **Hire/Fire Agents** - Activate or deactivate specific agents
- **Train Agents** - Provide custom instructions and context per agent
- **View Findings** - See agent recommendations filtered by month, agent, severity
- **Vote on Findings** - Thumbs up/down feedback to improve agent accuracy
- **Dismiss Findings** - Hide irrelevant recommendations
- **Run Full Team** - Trigger all agents to analyze current data on demand
- **Activity Log** - View agent run history with daily and total task counts

### Agent Intelligence
- **QA Review** - Validates agent outputs against business rules and benchmarks
- **Seasonal Forecasting** - Predicts next month metrics using historical and trend data
- **Anomaly Detection** - Flags unusual metric changes
- **Auto-Generated Tasks** - Agents create actionable tasks in the task system
- **Web Search** - Agents pull real-time market data during analysis

---

## CLIENT PORTAL - Hiring Hub

### Job Management
- **Create/Edit Jobs** - Title, department, location, salary range, description
- **AI Job Description Generator** - AI writes full job posting from title and criteria
- **AI Screening Questions** - Generates gate questions for candidate filtering
- **Job Status** - Draft, active, closed

### Candidate Pipeline
- **Application Intake** - Collect candidates via public job board URL
- **AI Candidate Scoring** - Automated resume and application evaluation
- **Candidate Status Tracking** - Applied > Screening > Interview > Offer > Hired (or Rejected)
- **Candidate Notes** - Internal notes per candidate
- **Rejection with Auto-Email** - Reject and notify in one click
- **Offer with Auto-Email** - Send offer and notify in one click

### AI Interview System
- **Schedule Interviews** - Set time and send link
- **Live AI Interview** - Structured Q&A with AI-powered question flow
- **First Question Generation** - AI generates contextual opening question
- **Step-by-Step Progression** - AI drives interview through relevant topics
- **Signal Scoring** - AI evaluates candidate responses in real time

### Public Job Board
- **Public Job Listing Page** - Shareable URL for each job
- **Branded Application Form** - Company logo and details on the application page
- **Mobile-Friendly** - Responsive design for applicants

---

## CLIENT PORTAL - Business Tools

### Google Business Profile
- **Profile Data Viewing** - See all GBP information in one place
- **Completeness Score** - 0 to 100% audit of profile completeness
- **Missing Fields Checklist** - Exactly what needs to be filled in
- **Recent Reviews Display** - Latest customer reviews
- **AI Audit** - GPT-powered recommendations for profile improvement

### Local Rank Heatmap Scanner
- **Geographic Grid Visualization** - See rankings mapped across a geographic area
- **Keyword Ranking Across Grid Points** - How a keyword ranks at different locations
- **Grid Configuration** - Customize grid size and search radius
- **Color-Coded Heatmap** - Visual ranking strength by location
- **Average Rank Calculation** - Overall ranking score across all grid points
- **Scan History** - View past scans and track changes over time
- **Location Saving** - Save searched locations for repeat scans

### Competitor Intelligence
- **Competitor Tracking** - Add and manage a list of competitors
- **Google Places Data** - Ratings, review counts, address, hours
- **Meta Ad Library Scraping** - See competitors' active Facebook/Instagram ads
- **Website Analysis** - Automated competitor website review
- **Pricing Intelligence** - Extract pricing from competitor sites (service-level detection, price type classification)
- **AI Competitor Report** - Narrative synthesis of all competitor data
- **Counter-Move Recommendations** - What to do in response
- **Auto-Refresh** - Data refreshes every 7 days, manual refresh available

### Post Scheduler
- **Social Post Scheduling** - Schedule organic posts to Facebook
- **Calendar View** - Visual calendar of all scheduled posts
- **Bulk Scheduling** - Schedule multiple posts at once
- **Image Upload** - Attach images to posts
- **URL Links** - Add link attachments
- **Status Tracking** - Pending, published, or failed with error logging

### Blog Management
- **AI Blog Generation** - Generate full blog posts using brand context
- **Blog Editor** - Create and edit posts with rich interface
- **CSV Import** - Bulk import blog post ideas and drafts
- **WordPress Auto-Publishing** - Publish directly to WordPress site
- **Blog Scheduling** - Schedule posts for future publication
- **SEO Fields** - Title, description, slug, categories, tags

### Creative Canvas / Design Studio
- **Ad Template Library** - Browse pre-built ad templates
- **Visual Canvas Editor** - Draw, add text, shapes, and images
- **AI Image Generation** - DALL-E integration for ad visuals
- **Format Selection** - Templates for Facebook feed, Instagram Stories, and other formats
- **Template Save/Load** - Save custom templates for reuse
- **Template Export** - Export finished designs

### Google Drive Integration
- **File Browser** - Browse the brand's Google Drive folder
- **Subfolder Navigation** - Navigate through organized subfolders
- **File Upload/Download** - Full file management
- **Image Preview** - Thumbnail previews for image files
- **Auto-Folder Creation** - Creates Creatives, Ads, Images, and Reports folders
- **Asset Selection** - Pick Drive images for use in the ad builder

---

## CLIENT PORTAL - Staff & Tasks

### Staff Management
- **Invite Staff** - Add team members with role assignment
- **Roles** - Owner, Manager, Staff with different permission levels
- **Activate/Deactivate** - Toggle access without deletion
- **Role-Based Access** - Managers create/update tasks, Staff sees only assigned tasks

### Task System
- **Create Tasks** - Title, description, checklist steps, priority, due date
- **Assign Tasks** - Distribute work to team members
- **Checklist Steps** - Multi-step tasks with individual completion tracking
- **Create from Agent Findings** - Auto-generate tasks from AI agent recommendations
- **Role-Based Views** - Staff see only their tasks, Managers see all

---

## CLIENT PORTAL - Settings & Configuration

### Brand Profile
- **Brand Voice** - Tone and communication style
- **Active Offers** - Current promotions and deals
- **Target Audience** - Ideal customer description
- **Competitor List** - Direct competitor tracking
- **Service Area** - Geographic coverage
- **Primary Services** - Core service offerings
- **Business Goals** - Strategic objectives
- **Reporting Notes** - Custom notes for reports

### AI Configuration
- **Model Selection** - Choose default AI model
- **Model Override Per Purpose** - Different models for chat, images, analysis, ad building
- **API Key Override** - Use a custom OpenAI key

### Connections
- **Google OAuth** - Connect Google Ads, Analytics, Search Console
- **Meta OAuth** - Connect Facebook/Instagram ads and pages
- **CRM Connection** - GoHighLevel, HubSpot, Sweep & Go, or custom webhook
- **WordPress** - Blog publishing credentials
- **Google Maps** - API key for heatmap scanner
- **Quo SMS** - API key and phone number for Warren SMS

### Warren Configuration
- **40+ Training Settings** - Full control over Warren's behavior (see Warren section above)
- **Master Enable/Disable** - Turn Warren on/off entirely
- **Reply Delay** - 0-300 second delay timer
- **Channel Toggles** - Enable/disable SMS and Meta lead forms independently
- **Hosted Lead Form Builder** - Configure the public Warren form, share link, iframe embed, and optional intake fields per brand

### Feedback
- **Bug Reports** - Submit bugs with category tagging
- **Feature Requests** - Request new capabilities
- **Star Rating** - 1-5 rating with each submission

---

## ADMIN PORTAL

### Brand Management
- **Create/Edit/Delete Brands** - Full CRUD with 50+ configurable fields
- **API Credentials** - GA4, GSC, Meta, Google Ads per brand
- **Connection Status Dashboard** - See which APIs are connected
- **Brand Usage Pulse** - Track which brands are actively using features
- **Client User Impersonation** - Log in as any client user to debug issues
- **Feature Access Control** - Enable/disable features per brand

### OAuth & API Configuration
- **Google OAuth Setup** - Client ID, Secret, Google Ads developer token
- **Meta OAuth Setup** - App ID, App Secret
- **Token Expiry Monitoring** - 14-day warnings before tokens expire

### Report Generation
- **Generate Reports** - Create reports for any brand/month
- **Batch Generation** - Generate all reports at once
- **CSV Upload** - Fallback data upload when APIs aren't connected
- **Internal + Client Reports** - Detailed tactical vs. simplified professional versions
- **Email Delivery** - Send to contact lists with batch email
- **WordPress Publishing** - Publish reports as WordPress posts

### Analytics & Data
- **Multi-Source Data** - GA4, Google Search Console, Google Ads, Meta Ads, Meta organic
- **Month-Over-Month Trending** - Track changes over time
- **Industry Benchmarks** - Compare to industry standards
- **Metric Scoring** - Excellent, good, average, below average, poor
- **AI Brief Generation** - Executive summaries (internal and client versions)
- **Suggestions Engine** - Automated recommendations with priority ranking

### AI Intelligence System
- **Ad Examples Library** - Curated good/bad examples across platforms and industries
- **Ad Best Practices Database** - Guidelines by platform, format, category
- **Niche Ad Library** - 850+ industry-specific ad examples (plumbing, HVAC, dental, legal, more)
- **Campaign Strategy Templates** - Predefined strategies with icons, colors, blueprints
- **Master Prompt Management** - Control AI prompts used by ad builders
- **Ad News Digest** - Platform update tracking

### Agency CRM
- **Pipeline Board** - Track prospects through stages (new, contacted, demo, proposal, negotiating, won, lost)
- **Prospect Management** - Full CRUD with notes, messages, and activity history
- **Import from Leads** - Pull assessment and signup leads into the CRM pipeline
- **Convert to Client** - Turn a prospect into a brand with one click

### Stripe Billing
- **Customer Management** - Create/retrieve Stripe customers per brand
- **Subscription Creation** - Create subscriptions with optional trial periods
- **Plan Changes** - Upgrade/downgrade pricing tiers
- **Cancellation** - Cancel at period end or immediately
- **Billing Portal** - Generate Stripe self-service portal for clients
- **Webhook Processing** - Handle Stripe events (subscription updates, payments, failures)
- **MRR Dashboard** - Monthly Recurring Revenue, active/trialing/churned counts

### Beta Program Management
- **Application Review** - Review beta signup submissions
- **Approval Workflow** - Approve (auto-creates brand + client user), reject, or remove
- **Activation Emails** - Send login credentials with setup instructions
- **Broadcast Email** - Send emails to beta users, all users, admins, or single recipients
- **Email Open Tracking** - Per-recipient tracking pixel with open rate dashboard
- **Broadcast History** - View all past broadcasts with recipient-level drill-down
- **Feedback Collection** - Read feedback with categories, ratings, and themes
- **Feedback Responses** - Reply to feedback
- **AI Feedback Digest** - Generate GPT summaries of recent feedback, recurring themes, and recommended priorities
- **AI Dev Rollout Briefs** - Build an internal implementation plan with likely code areas, rollout steps, and QA checks from live feedback batches
- **Draft Reply Suggestions** - Generate per-feedback draft replies with recommended status, confidence, and one-click apply/send actions
- **Feature Request Promotion** - Convert feedback into tracked upgrade considerations
- **Upgrade Tracking** - Priority, feasibility, safety risk, status per consideration

### Drip Campaigns
- **Sequence Builder** - Create multi-step email sequences
- **Step Configuration** - Delay days, subject, HTML/text body per step
- **Enrollment Management** - Auto-enroll from assessment or signup
- **Send Tracking** - View delivery status per enrollment per step
- **Auto-Completion** - Detect when sequence is finished
- **Conversion Tracking** - Mark enrolled leads as converted

### Diagnostic Tools
- **Brand Health Diagnose** - `/api/brand/<id>/diagnose` endpoint checks all connections
- **Facebook Organic API Test** - Test Meta connection and permissions
- **SMTP Test** - Send test email
- **OpenAI Test** - Verify API key and model
- **WordPress Test** - Test connection and auth

### Finance
- **Revenue Data Entry** - Log revenue and closed deals per brand per month
- **CRM Revenue Auto-Sync** - Pull from Sweep & Go, GoHighLevel, Jobber automatically
- **Finance Dashboard** - View financial data alongside ad performance

### Feature Flags
- **Per-Feature Toggles** - Enable/disable any portal feature globally
- **Access Levels** - All users, beta only, or disabled
- **Category Organization** - Group flags by functional area

### Global Settings
- **SMTP Configuration** - Email server with testing
- **OpenAI API** - Key and model selection with testing
- **WordPress** - URL and credentials with testing
- **App URL** - Auto-detection and manual override
- **Setup Wizard** - Interactive first-run configuration
- **Configuration Checklist** - Health status of all settings

---

## BACKGROUND AUTOMATION

### Scheduled Jobs
- **Report Generation** - Batch create and email reports for all brands
- **Revenue Sync** - Daily pull from connected CRMs (Sweep & Go, GoHighLevel)
- **AI Agent Runs** - Daily full analysis across all brands
- **Dashboard Refresh** - Cache update for stale dashboards
- **Warren Nurture** - Process pending follow-ups across all brands
- **Payment Reminders** - Send billing notifications for upcoming due dates
- **Drip Campaign Processing** - Send pending drip emails
- **Blog Publishing** - Auto-publish scheduled posts to WordPress

---

## INTEGRATIONS

### Google
- Google Analytics 4 - Website traffic, users, conversions
- Google Search Console - Organic search performance, keywords, rankings
- Google Ads - Campaign management, performance data, budget control
- Google Business Profile - Business listing data, reviews, completeness audit
- Google Drive - File storage, asset management
- Google Maps/Places API - Local ranking, competitor data, geocoding

### Meta
- Meta Ads (Facebook/Instagram) - Campaign management, ad performance, audience data
- Meta Organic - Organic social media metrics
- Facebook Messenger - Two-way conversation with leads
- Meta Lead Forms - Auto-capture from lead gen campaigns
- Meta Ad Library - Competitor ad intelligence

### CRM
- GoHighLevel - Lead and revenue sync
- HubSpot - Lead and revenue sync
- Sweep & Go (SNG) - Revenue, clients, billing integration
- Jobber - Invoice sync
- Generic Webhook - Custom CRM integration

### Other
- OpenAI (GPT-4o, GPT-4o-mini, o3-mini, DALL-E) - AI assistant, ad generation, agent brains, Warren
- Stripe - Subscription billing, customer management, payment processing
- Quo/OpenPhone - SMS messaging for Warren
- WordPress REST API - Report and blog publishing
- SMTP Email - Report delivery, notifications, broadcasts, password resets

---

## EMAIL TYPES

- Monthly report delivery
- Beta welcome and activation emails
- Staff invite emails
- Password reset emails
- Client portal login credentials
- Broadcast emails (with open tracking)
- Hiring emails (offers, interview scheduling, rejections)
- Billing payment reminders
- Drip campaign sequences
- Upgrade request notifications
