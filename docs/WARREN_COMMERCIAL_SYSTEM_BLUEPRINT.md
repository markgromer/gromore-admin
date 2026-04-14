# W.A.R.R.E.N. Commercial System Blueprint

Date: April 14, 2026

Purpose: This document turns the recent research into an implementation-grade blueprint for the WARREN commercial system. It is designed to help product and engineering decide what to build next without bloating the platform into full property-management software.

This blueprint is intentionally mapped to the current WARREN architecture already in the codebase:

- Commercial workspace and routes in `webapp/client_portal.py`
- Commercial discovery in `webapp/commercial_prospector.py`
- Commercial strategy logic in `webapp/commercial_strategy.py`
- Thread, event, quote, and drip persistence in `webapp/database.py`
- Commercial drip logging in `webapp/drip_engine.py`

## 1. Product Definition

WARREN's commercial system should be positioned and built as a commercial revenue and account-operations layer for service businesses.

It should not be treated as:

- just a scraper
- just an outreach sequencer
- just a quote builder
- full property-management software

The right product definition is:

WARREN helps a service business find commercial accounts, qualify them, scope the work, send a defensible proposal, prove service delivery, and retain or expand the account from one operational thread.

## 2. What Research Says Matters

Across AppFolio, Buildium, Housecall Pro, Jobber, and commercial pet-waste operators, the consistent pattern is this:

1. The best systems keep one source of truth for account data, communication history, proposals, and service proof.
2. Commercial sales gets better when walkthrough data is structured instead of gathered ad hoc.
3. Proposals convert better when scope, frequency, options, and next steps are easy to understand.
4. Commercial retention improves when managers receive visible proof of service and clear monthly reporting.
5. Good automation is workflow glue, not a replacement for operator judgment.

That means the next step for WARREN is not more prospect scraping alone. The next step is closing the loop from discovery to service proof.

## 3. Current System Baseline

WARREN already has the correct base primitives in place.

### Current Implementation Status

Implemented now:

- client-side commercial search, import, refresh, and qualification
- manual outreach and commercial nurture enrollment
- structured proposal builder with Basic, Standard, and Premium package comparison
- commercial walkthrough capture inside the account detail workspace
- structured commercial service visit logging for proof of service
- manager recap preview generated from service-visit data inside the commercial workspace

Still pending from the broader roadmap:

- monthly manager recap generation
- renewal and expansion prompts
- optional multi-contact and multi-property account modeling

### Existing Foundation

- `lead_threads` already acts as the account thread and activity spine.
- `commercial_data_json` already stores structured commercial account details.
- `lead_messages` already stores outreach and nurture communication history.
- `lead_events` already stores milestone events.
- `lead_quotes` already stores proposal and quote data.
- Drip enrollment and drip send history already connect back to commercial threads.

### Existing Commercial Workflow

The current client-side commercial implementation already supports:

- location-based commercial search
- import and dedupe into WARREN lead threads
- qualification and commercial brief generation
- refresh and merge behavior for imported records
- manual commercial email sending
- commercial drip enrollment
- structured proposal building for recurring commercial work

This is the correct backbone. The next work should extend this model, not replace it.

## 4. Product Principles

Use these as guardrails for every commercial feature:

1. One account, one thread, one timeline.
2. Every sales claim should lead to an operational record.
3. Every recurring account should be easy to review at renewal time.
4. Every field added should either improve quoting, delivery, or retention.
5. Avoid building generic back-office software when a commercial service workflow will do.

## 5. Core Modules

The commercial system should be organized into five modules.

### Module 1: Market Discovery

Purpose: Find relevant commercial targets and convert them into account threads.

Key responsibilities:

- search by location and account type
- import and dedupe accounts
- enrich website, email, phone, and site signals
- identify likely buyer role and first outreach angle

What matters most:

- clean identity matching
- confidence around website and contact data
- easy import into the brand's existing lead pipeline

### Module 2: Property Qualification

Purpose: Turn a raw commercial target into a scoped account with enough detail to quote and route correctly.

Key responsibilities:

- buyer and role qualification
- service-area validation
- site walkthrough capture
- pain point and risk capture
- proposal readiness scoring

Required walkthrough fields:

- property type
- property count or site count
- common area count
- relief area count
- waste station count
- station condition and refill need
- pet traffic estimate
- current cleanliness condition
- access and gate procedure
- disposal constraints
- current vendor status
- required add-ons
- notes by area
- walkthrough photos

### Module 3: Proposal and Close

Purpose: Convert qualification data into a clean, defensible recurring commercial proposal.

Key responsibilities:

- recurring pricing model
- setup fees and initial cleanups
- add-on packaging
- proposal status tracking
- follow-up prompts
- approval capture

Proposal requirements:

- scope summary
- service frequency
- service-day plan
- itemized recurring line items
- setup line items
- add-ons and options
- clear assumptions
- guarantee or service standard
- approval CTA
- follow-up text or next-step copy

Recommended commercial proposal pattern:

- Basic: core cleanup only
- Standard: cleanup plus station management
- Premium: cleanup plus station management plus deodorizer or sanitization and service proof extras

### Module 4: Service Proof

Purpose: Give property managers and operators visible proof that the service happened and the account is under control.

Key responsibilities:

- visit logging
- service exceptions
- gate secured confirmation
- station refill confirmation
- area completion notes
- issue escalation
- photo proof when appropriate

Required proof-of-service outputs:

- visit timestamp
- site or area summary
- station service completed yes or no
- bag refill completed yes or no
- gate secured yes or no
- issue notes
- optional photo attachments
- client-facing summary text

### Module 5: Retention and Expansion

Purpose: Keep recurring commercial accounts visible, easy to justify, and easy to expand.

Key responsibilities:

- monthly recap generation
- renewal timing and reminders
- add-on suggestions
- multi-property expansion opportunities
- dormant or at-risk account review

Signals to monitor:

- no recent service proof
- frequent issue reports
- proposal sent but stalled
- repeated deodorizer or sanitizer add-on use
- nearby properties under same manager or group

## 6. Recommended Data Model

WARREN should keep using the current lead-thread model as the commercial account spine.

### Keep As-Is

- `lead_threads`: account thread and commercial account container
- `commercial_data_json`: structured account and qualification payload
- `lead_messages`: outreach and communication log
- `lead_events`: timeline and audit trail
- `lead_quotes`: proposal storage
- drip enrollment records: nurture state

### Extend `commercial_data_json`

For the next phase, keep most commercial account data in `commercial_data_json`, but organize it into clearer sub-sections.

Recommended shape:

```json
{
  "account_profile": {
    "business_name": "",
    "account_type": "",
    "service_area": "",
    "website": "",
    "primary_email": "",
    "primary_phone": "",
    "decision_maker_name": "",
    "decision_maker_role": "",
    "current_vendor_status": "",
    "property_count": "",
    "stage": ""
  },
  "walkthrough": {
    "common_area_count": 0,
    "relief_area_count": 0,
    "waste_station_count": 0,
    "pet_traffic_estimate": "",
    "site_condition": "",
    "access_notes": "",
    "disposal_notes": "",
    "required_add_ons": [],
    "photos": []
  },
  "strategy": {
    "outreach_angle": "",
    "pain_points": [],
    "next_action": "",
    "proposal_status": "",
    "proposal_readiness": ""
  },
  "proposal_builder": {
    "service_frequency": "",
    "service_days": "",
    "monthly_management_fee": 0,
    "scope_summary": "",
    "notes": ""
  },
  "account_health": {
    "risk_level": "",
    "renewal_window": "",
    "expansion_notes": ""
  }
}
```

This keeps the MVP simple while making the payload easier to reason about and evolve.

### Add In V2: `commercial_service_visits`

This is the first table that is worth adding instead of burying everything in JSON.

Recommended fields:

- `id`
- `brand_id`
- `thread_id`
- `service_date`
- `completed_at`
- `completed_by`
- `property_label`
- `summary`
- `waste_station_count_serviced`
- `bags_restocked`
- `gate_secured`
- `issues_json`
- `photos_json`
- `client_note`
- `internal_note`
- `created_at`
- `updated_at`

Why this should be a table:

- visit history will grow quickly
- reporting by month and account becomes much easier
- proof-of-service should not require decoding historical event blobs

### Add In V3 Only If Needed

- `commercial_contacts` for multiple roles on one account
- `commercial_properties` for multi-site accounts under one management company
- `commercial_approvals` if formal approval workflows become necessary

Do not add these tables before there is real pressure from usage.

## 7. Route and UI Plan

The current commercial workspace already provides the right entry point. The next route and screen changes should stay tightly scoped.

### Existing Screens To Keep

- commercial search and import workspace
- commercial account detail workspace
- qualification form
- outreach and nurture controls
- proposal builder

### Next Screens To Add

#### A. Walkthrough Screen or Section

Goal: structured field capture for scoping.

Should include:

- property layout inputs
- waste-station and common-area counts
- condition and access notes
- add-on requirements
- photo upload slots

#### B. Proposal Send and Status Section

Goal: turn the built proposal into a trackable commercial sales artifact.

Should include:

- preview of proposal packages
- send action
- proposal status timeline
- opened or acknowledged state if available
- follow-up reminders

#### C. Service Proof Section

Goal: log completed work from the same account thread.

Should include:

- quick-add visit record
- station refill checkbox
- gate secured checkbox
- issue and exception notes
- photo attachments
- client-facing recap preview

#### D. Monthly Account Review Section

Goal: make renewal and retention easy.

Should include:

- visits this month
- last service date
- issues raised
- active add-ons
- renewal timing
- suggested upsell or expansion action

## 8. Phased Roadmap

### Phase 1: Finish the Commercial Sales Spine

Objective: Make the current system harder to break and easier to close with.

Build:

- structured walkthrough fields inside the commercial workspace
- proposal package support: basic, standard, premium
- proposal send status and follow-up state
- better stage transitions: new, qualified, proposal_ready, proposal_sent, active_account, at_risk
- event logging for proposal lifecycle milestones
- branded proposal export or clean send format

Do not build yet:

- dispatching
- tenant portals
- full scheduling engine

Success criteria:

- a user can search, qualify, quote, send, and follow up from one commercial thread

### Phase 2: Add Service Proof and Account Reporting

Objective: Give the system retention value after the sale.

Build:

- `commercial_service_visits` table
- visit logging UI
- issue and exception capture
- proof-of-service recap generation
- monthly summary generation per commercial account
- manager-facing email recap template

Do not build yet:

- complex routing or field dispatch
- large analytics dashboards with dozens of charts

Success criteria:

- a commercial account can show what was serviced, what issues were found, and what value was delivered this month

### Phase 3: Add Retention and Expansion Intelligence

Objective: Help operators retain accounts and grow within management groups.

Build:

- renewal reminders based on account timing
- at-risk scoring based on issue volume and service proof gaps
- expansion prompts for nearby or related properties
- contact-role support for regional managers, boards, and on-site staff
- optional multi-property account view

Do not build yet:

- generic enterprise workflow builders
- custom automation designer

Success criteria:

- the operator can review commercial account health and identify the next best growth action without digging through raw notes

## 9. What “Top Tier” Actually Means Here

For WARREN, parity with strong operators and service software does not mean matching every feature in AppFolio or Jobber.

It means delivering these outcomes well:

- commercial targets become clean accounts quickly
- reps gather consistent scoping data
- proposals are credible and easy to approve
- every delivered visit can be proven
- managers receive useful summaries
- renewals and upsells are prompted at the right time

If those five things work, WARREN will feel operationally stronger than many bloated systems with longer feature lists.

## 10. Implementation Priorities For This Codebase

These are the most valuable next build targets given the current code.

### Priority 1

Add structured walkthrough support to the commercial thread detail view and normalize those fields through the existing commercial payload builder.

Why:

- this improves quoting quality immediately
- this creates the inputs needed for service proof later
- this fits the current `commercial_data_json` model cleanly

### Priority 2

Add proposal package support and proposal send-state tracking using the existing `lead_quotes` record.

Why:

- this upgrades close rate without new core tables
- this uses the existing quote persistence already in production

### Priority 3

Add service proof logging as a dedicated table with a lightweight thread-level UI.

Why:

- this is the first major retention differentiator
- this creates forwardable value for managers and boards
- this provides clean data for future monthly recap generation

### Priority 4

Generate monthly commercial account recap output from service visits, events, and active proposal data.

Why:

- this is where transparency and renewal leverage show up
- it turns WARREN from sales tooling into account-management leverage

## 11. Avoid These Traps

Do not turn this into:

- a generic CRM rewrite
- a property-management platform clone
- a dispatching product before service proof exists
- a bloated analytics dashboard before commercial account summaries exist
- a feature pile of unrelated commercial widgets

The winning shape is narrower:

find the account, scope the work, send the proposal, prove the service, retain the account.

## 12. Recommended Immediate Next Build

If engineering starts immediately, the best next implementation sprint is:

1. Add a structured commercial walkthrough block to the current account detail view.
2. Expand proposal builder output to support tiered packages and better send tracking.
3. Add commercial service visit logging plus manager-facing recap scaffolding.

That sequence keeps the commercial system lightweight, uses the code already written, and adds the highest-value capabilities in the right order.