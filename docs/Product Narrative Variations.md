# Product Narrative Variations (Writer-Ready)

Date: 2026-03-22

Purpose: This file contains three audience-specific versions of the same product story:
- SMB client-facing narrative
- Agency/operator narrative
- Technical/architecture brief

These drafts are designed to be persuasive without over-claiming. They describe a system that connects data ingestion, reporting, planning, and (user-triggered) execution for Google Ads and Meta, with auditability and safety defaults.

## Messaging guardrails (use in any version)

Use language like:
- “Connects”, “pulls”, “generates”, “suggests”, “lets you launch”, “logs changes”, “starts campaigns paused by default”.
- “Operator-in-the-loop” or “human-triggered execution”.

Avoid language like:
- “Fully autonomous”, “AI runs your ads”, “set it and forget it”, “always optimizes automatically”.

If asked “Does it run ads automatically?” the accurate answer is:
- It can generate plans and create/modify campaigns when a user triggers an action in the portal. It is not a background bot that changes spend continuously without user intent.

---

# 1) SMB Client Version (Plain-English)

## What this is

This is an ads and reporting operating system for your business. It does four things, in one connected loop:
1. Connects to your accounts (Google and Meta) and pulls performance data.
2. Turns that data into month-over-month reporting and clear “what to do next” recommendations.
3. Helps generate campaign plans and creative from structured prompts and proven templates.
4. Lets your team launch or adjust campaigns in a controlled, auditable way.

It is not just “AI that makes ad copy”. It is the workflow that keeps strategy, execution, and reporting synced.

## What you actually get (outcomes)

- Monthly reports that are client-readable, plus deeper internal reports for the team.
- A consistent way to see what changed, what worked, and what to do next.
- Faster campaign setup through guided planning, drafts, and reusable templates.
- Safer changes: new campaigns start paused by default and actions are logged.

## How it works (high level)

1. Connect accounts: Google (GA4, Search Console, Google Ads scope) and Meta.
2. Select the properties/accounts you want associated with your brand.
3. Pull in data (API and/or CSV imports) and merge it into a single reporting dataset.
4. Generate reports: month-over-month comparisons, insights, and suggestions.
5. When you want to act: generate a plan, review it, and then launch or adjust campaigns.

## Why it is different from “AI runs ads”

Most “AI ads” tools stop at drafts. This system is built around the whole cycle:
- Data in
- Analysis and recommendations
- Plan and creative
- User-triggered execution
- Audit trail
- Next month’s reporting built on what really happened

## Safety and control

- New campaigns are created paused by default (so nothing starts spending unexpectedly).
- Budget/status changes are explicit actions, not hidden background behavior.
- Key actions are logged so you can trace what was changed and when.

## Suggested short pitch (30 seconds)

“We connect your Google and Meta data, turn it into real reporting and next-step recommendations, then let us plan and launch campaigns in a controlled way. It is one system that ties reporting to action, instead of separate tools and spreadsheets.”

---

# 2) Agency / Operator Version (Operator-first, scalable)

## Positioning

This is an agency operating system that bridges analytics, reporting, creative generation, and campaign execution with traceability. It is designed for repeatable delivery across many brands without turning the work into spreadsheet chaos.

## Core capabilities (operator view)

- Multi-brand hub: brand records, connections, contacts, reports, drafts.
- Data ingestion and normalization:
  - Supports CSV imports in a consistent folder structure.
  - Can also pull API data (GA4, Search Console, Google Ads, Meta insights), with a “prefer API when available” merge approach.
- Reporting pipeline:
  - Generates two flavors of output: internal tactical reports and client-facing reports.
  - Month-over-month comparisons and suggestion generation for common home services categories.
- Ad Intelligence and knowledge layer:
  - Curated examples and best-practice libraries by niche.
  - “Digest” and prompt rebuild utilities for keeping guidance current.
- Execution layer (human-triggered, audited):
  - Campaign plan generation.
  - Launch flows that create campaigns from plans.
  - Mutations for status and budget changes; negative keyword additions for Google.
  - Audit logging of changes.

## Why this is operationally valuable

- One workflow from reporting to action: insights do not die in a PDF.
- Safer launch posture: campaigns start paused by default so reviews can happen before spend.
- Faster iteration: structured plans and templates reduce rework and “blank page” time.
- Accountability: change logs make post-mortems and client comms easier.

## How you can sell it (agency pitch)

“Most agencies have reporting in one tool, ad ops in another, and ‘AI’ somewhere else. This unifies the loop. We connect accounts, generate reporting with consistent interpretation and suggestions, then move into planning and controlled execution with an audit trail. It increases speed without sacrificing control.”

## What not to promise

- Do not promise continuous autonomous optimization.
- Do not promise automatic budget reallocation without human review.

## Suggested scope statement (for proposals)

- “We deliver monthly reporting plus operator-grade diagnostics, and we execute changes through a controlled workflow that logs actions and defaults new launches to paused.”

---

# 3) Technical / Architecture Brief (Concise, accurate)

## Stack summary

- Backend: Python + Flask web app
- UI: Server-rendered templates plus JSON endpoints for dashboard actions
- Data store: SQLite (brands, connections, reports, drafts, audit logs, assistant memory, etc.)
- External APIs (via OAuth and token refresh):
  - Google: GA4, Search Console, Google Ads scope; client flow can include Drive and Sheets scopes
  - Meta: Ads insights and related scopes; supports long-lived token exchange
- Reporting engine: A “webapp” runner bridges into an existing “src/” analytics pipeline that parses, analyzes, suggests, and renders report outputs

## High-level components

- Admin portal
  - Brand management, connection setup, report generation, ad intelligence tooling
  - Job-style report generation and sending
- Client portal
  - Client login and dashboard
  - “Mission Control” style actions for pulling data, generating plans, launching, and managing campaigns
- API bridge
  - Centralized token refresh rules and API pulls per brand
  - Pulls GA4, GSC, Google Ads (with a fallback path), Meta ads insights, and Facebook organic metrics when page context is available
- Campaign manager
  - Implements user-triggered mutations (status, budget, negatives for Google)
  - Implements launch flows that create new campaigns from generated plans
  - Safety default: create campaigns paused by default
  - Writes audit logs for traceability
  - Google Ads: direct REST calls to googleads.googleapis.com (not the google-ads SDK); Meta: via facebook-business SDK
- AI layer
  - Used to generate briefs, plans, and creative copy in a structured workflow
  - Includes a memory/embedding-backed store for assistant context

## Data flow (typical month)

1. Data arrives via CSV imports and/or API pulls.
2. Data is normalized and merged (API preferred when present).
3. Reporting pipeline generates HTML/PDF outputs and stores paths/metadata.
4. Recommendations feed into planning workflows.
5. When the user triggers execution, campaign manager calls the relevant APIs and logs mutations.

## Operational safeguards

- Token refresh is handled centrally rather than ad-hoc per endpoint.
- New campaigns start paused by default.
- An audit trail records campaign mutations.

## Extension points (practical)

- Add new data sources by extending the API bridge and mapping into the reporting dataset.
- Add new operator workflows by adding client portal actions that call existing campaign/report primitives.
- Add new niches or playbooks by extending the ad intelligence libraries and prompt seeds.
