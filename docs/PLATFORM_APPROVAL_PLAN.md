# Platform Approval Plan

## Phase 1 Launch Position

- Meta is Phase 1 and includes Messenger, lead forms, and related webhook flows.
- Google should not block the Meta submission.
- Google approval should be split into:
  - Phase 1: GA4, Search Console, Google Ads reporting
  - Phase 2: Drive and Sheets sync, unless scope design is narrowed first

## Meta Submission Scope

Current requested scopes in code:

- `ads_read`
- `ads_management`
- `read_insights`
- `business_management`
- `pages_show_list`
- `pages_read_engagement`
- `pages_read_user_content`
- `pages_manage_posts`
- `pages_manage_metadata`
- `pages_messaging`
- `leads_retrieval`

## Meta Submission Surfaces

Public URLs now available on the app domain:

- Privacy Policy: `/privacy`
- Terms of Service: `/terms`
- Meta Data Deletion Instructions: `/meta/data-deletion`

## Meta Readiness Requirements Before App Review

- Confirm production domain and callback URLs are final.
- Verify Meta webhooks using the platform-level verify token.
- Reconnect at least one test brand so the new scopes are granted.
- Prepare a screencast showing:
  - business logs in with Meta
  - correct Facebook Page is connected
  - Messenger inbound message creates or updates a lead thread
  - Warren replies in-thread
  - lead form submission creates a thread
- Be ready to explain that Messenger responses are restricted to the standard response window.

## Remaining Meta Review Risks

- Messenger nurture outside the response window is now blocked in code.
- If future product plans require post-window Messenger follow-up, that needs a separate policy-compliant design.
- Human handoff and escalation behavior should be easy to demonstrate during review.

## Google Readiness Recommendation

- Keep Google approval narrow first: reporting scopes only.
- Do not lead the first Google verification with full Drive access unless it is unavoidable.
- If Drive remains required, plan for a second approval track with tighter product justification and updated consent copy.

## Manual Checklist

- Publish privacy and terms pages wherever the public website is hosted.
- Put the same URLs into Meta App Review fields.
- Add the Meta data deletion URL in the Meta app settings.
- Verify the production app domain in Meta and Google where required.
- Record app-review videos before submission.