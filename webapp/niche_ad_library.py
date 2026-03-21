"""
Niche Ad Library - Master-level ad examples for each industry vertical.

Each niche has multiple examples across Google Search, Google Display, Meta Feed,
and Meta Stories - covering good and bad patterns with detailed analysis.

Philosophy: Master of one, then the next - foundational mastery that compounds.
Each niche's examples reflect deep understanding of that industry's buyers,
objections, seasonal patterns, and conversion triggers.
"""

# ─────────────────────────────────────────────────────────────────
#  PLUMBING
# ─────────────────────────────────────────────────────────────────
PLUMBING_ADS = [
    # Google Search - Good
    {
        "platform": "google", "format": "search_rsa", "industry": "plumbing",
        "headline": "Plumber Here in 60 Min",
        "description": "Clogged drain? Leaking pipe? We arrive within the hour. Flat-rate pricing before we start. Licensed, insured, 4.9 stars.",
        "quality": "good", "score": 9,
        "analysis": "Top performer because: (1) Specific response time '60 Min' beats vague 'fast service'. "
                    "(2) Addresses two common emergencies immediately. (3) 'Flat-rate pricing before we start' removes the #1 fear (surprise bill). "
                    "(4) Trust stack: licensed, insured, 4.9 stars. Every word earns its place.",
        "principles": ["specific_time", "pain_match", "price_transparency", "trust_stack"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "plumbing",
        "headline": "$49 Drain Clearing Special",
        "description": "Most drains cleared in under an hour. No trip charge. If we can't fix it, you don't pay. Call now - same-day openings.",
        "quality": "good", "score": 9,
        "analysis": "Price in headline pre-qualifies and stops the scroll. (1) '$49' is specific and affordable. "
                    "(2) 'Most drains cleared in under an hour' sets expectations. (3) 'No trip charge' removes a known objection. "
                    "(4) 'If we can't fix it, you don't pay' is zero-risk. (5) 'Same-day openings' creates urgency without being pushy.",
        "principles": ["price_in_headline", "time_estimate", "zero_risk", "objection_removal", "soft_urgency"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "plumbing",
        "headline": "Water Heater Out? We Fix Today",
        "description": "No hot water is miserable. We stock the 5 most common water heater parts on every truck. Same-day repair or replacement.",
        "quality": "good", "score": 8,
        "analysis": "Empathy plus capability: (1) 'No hot water is miserable' validates the frustration. "
                    "(2) 'Stock the 5 most common parts on every truck' is a concrete differentiator - not 'we come prepared' but HOW. "
                    "(3) 'Same-day repair or replacement' covers both outcomes.",
        "principles": ["empathy_lead", "concrete_differentiator", "outcome_coverage"],
    },
    # Google Search - Bad
    {
        "platform": "google", "format": "search_rsa", "industry": "plumbing",
        "headline": "Quality Plumbing Service",
        "description": "We are a full-service plumbing company serving the greater area. Call us today for all your plumbing needs.",
        "quality": "bad", "score": 2,
        "analysis": "Generic to the point of invisibility: (1) 'Quality' - every plumber claims this. "
                    "(2) 'Full-service' - meaningless without specifics. (3) 'Greater area' - which area? "
                    "(4) 'All your plumbing needs' - the homeowner has ONE need right now. (5) Zero urgency, zero differentiation, zero reason to click.",
        "principles": ["vague_claims", "no_specifics", "no_urgency", "no_differentiation"],
    },
    # Meta Feed - Good
    {
        "platform": "meta", "format": "feed", "industry": "plumbing",
        "headline": "Why Your Water Bill Spiked",
        "description": "A running toilet wastes 200 gallons a day. A slow drip wastes 3,000 gallons a year. "
                       "Most leaks take 15 minutes to fix. Free leak check - we'll tell you exactly what's going on.",
        "quality": "good", "score": 9,
        "analysis": "Education-first approach that stops the scroll: (1) Specific numbers (200 gal/day, 3,000 gal/year) shock and inform. "
                    "(2) '15 minutes to fix' makes action feel easy. (3) 'Free leak check' is low-friction entry. "
                    "(4) 'Tell you exactly what's going on' positions as honest advisor, not salesperson.",
        "principles": ["education_first", "shocking_numbers", "make_it_easy", "low_friction", "advisor_positioning"],
    },
    {
        "platform": "meta", "format": "feed", "industry": "plumbing",
        "headline": "Before & After: Kitchen Remodel",
        "description": "This family's 1990s kitchen had corroded galvanized pipes, no dishwasher hookup, and low water pressure everywhere. "
                       "3 days later: new copper lines, instant hot water, and a kitchen that actually works. Full gallery in comments.",
        "quality": "good", "score": 8,
        "analysis": "Story-driven content that works on Meta: (1) 'Before & After' is inherently clickable. "
                    "(2) Specifics of the old problem (corroded galvanized, no dishwasher, low pressure) are relatable. "
                    "(3) '3 days' is a specific timeline. (4) 'Gallery in comments' drives engagement and algorithm reach.",
        "principles": ["before_after", "relatable_specifics", "timeline", "engagement_driver"],
    },
    # Meta Feed - Bad
    {
        "platform": "meta", "format": "feed", "industry": "plumbing",
        "headline": "Need a Plumber? Call Us!",
        "description": "ABC Plumbing has been serving the community for over 20 years. "
                       "We do it all - drains, water heaters, remodels, and more. Licensed and insured. Call today!",
        "quality": "bad", "score": 2,
        "analysis": "This is an ad pretending to be a social post. On Meta, users skip anything that looks like an ad. "
                    "(1) 'Need a plumber?' - yes, but this doesn't tell me why YOU. (2) 'Over 20 years' - longevity alone doesn't win. "
                    "(3) 'We do it all' - jack of all trades messaging. (4) 'Call today' - no reason given for why today.",
        "principles": ["looks_like_an_ad", "no_differentiation", "jack_of_all_trades", "no_reason_to_act"],
    },
]

# ─────────────────────────────────────────────────────────────────
#  HVAC
# ─────────────────────────────────────────────────────────────────
HVAC_ADS = [
    # Google Search - Good
    {
        "platform": "google", "format": "search_rsa", "industry": "hvac",
        "headline": "AC Died? Fixed by Tonight",
        "description": "We carry the top 20 parts on every van. 89% of AC repairs finished same visit. $75 diagnostic waived with repair. Book now.",
        "quality": "good", "score": 9,
        "analysis": "Every element reduces anxiety: (1) 'Fixed by tonight' is the promise they need on a 95-degree day. "
                    "(2) '89% finished same visit' is a specific, believable stat. (3) '$75 diagnostic waived with repair' removes the 'just to look at it' objection. "
                    "(4) 'Top 20 parts on every van' - concrete proof of preparedness.",
        "principles": ["urgency_match", "specific_stat", "diagnostic_waiver", "preparedness_proof"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "hvac",
        "headline": "New AC System - $0 Down",
        "description": "$0 down, 0% APR for 60 months on qualifying systems. Includes 10-year parts & labor warranty. Free in-home estimate.",
        "quality": "good", "score": 9,
        "analysis": "Financing in HVAC is a massive conversion driver: (1) '$0 Down' removes the biggest barrier to replacement. "
                    "(2) '0% APR for 60 months' is a clear, compelling offer. (3) '10-year parts & labor' differentiates from competitors who only offer parts. "
                    "(4) 'Free in-home estimate' is the logical next step.",
        "principles": ["financing_lead", "specific_terms", "warranty_differentiator", "clear_next_step"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "hvac",
        "headline": "Furnace Tune-Up $79",
        "description": "21-point inspection. Catch problems before they leave you without heat. Includes filter replacement. Book before Dec 1 - schedule fills fast.",
        "quality": "good", "score": 8,
        "analysis": "Seasonal maintenance done right: (1) Price in headline attracts price-conscious shoppers. "
                    "(2) '21-point inspection' sounds thorough and specific. (3) 'Before they leave you without heat' - loss aversion. "
                    "(4) 'Before Dec 1' creates season-appropriate urgency.",
        "principles": ["price_in_headline", "specific_inspection", "loss_aversion", "seasonal_urgency"],
    },
    # Google Search - Bad
    {
        "platform": "google", "format": "search_rsa", "industry": "hvac",
        "headline": "Heating & Cooling Experts",
        "description": "Trust our team of certified HVAC professionals for all your heating and cooling needs. We provide top-quality service.",
        "quality": "bad", "score": 2,
        "analysis": "Completely invisible in a competitive SERP: (1) 'Experts' - self-proclaimed, unproven. "
                    "(2) 'Certified HVAC professionals' is the bare minimum, not a differentiator. "
                    "(3) 'All your heating and cooling needs' says nothing specific. (4) 'Top-quality service' - meaningless filler.",
        "principles": ["self_proclaimed_expertise", "bare_minimum_claims", "no_specifics", "meaningless_filler"],
    },
    # Meta Feed - Good
    {
        "platform": "meta", "format": "feed", "industry": "hvac",
        "headline": "Your Energy Bill is NOT Normal",
        "description": "The average home loses 20-30% of conditioned air through duct leaks. That's $400-600/year going into your attic. "
                       "Free duct inspection this month - we'll show you exactly where your money is going.",
        "quality": "good", "score": 9,
        "analysis": "Pain-point education that creates demand: (1) Headline challenges a belief - scroll-stopping. "
                    "(2) Dollar amounts make the invisible visible ($400-600/year). (3) 'Going into your attic' makes it tangible. "
                    "(4) 'Show you exactly' positions as transparent. Time-limited offer adds urgency without being pushy.",
        "principles": ["challenge_belief", "invisible_made_visible", "tangible_loss", "transparency", "time_limit"],
    },
    {
        "platform": "meta", "format": "feed", "industry": "hvac",
        "headline": "This Unit Was 23 Years Old",
        "description": "Homeowner was spending $380/month in summer to cool a 1,800 sq ft house. Replaced with a 16 SEER2 system. "
                       "First bill after: $167. The system pays for itself. Real customer, real numbers.",
        "quality": "good", "score": 9,
        "analysis": "Case study format is Meta gold: (1) Specific age (23 years) creates relatability. "
                    "(2) Before/after numbers ($380 vs $167) are undeniable. (3) 'Pays for itself' frames cost as investment. "
                    "(4) 'Real customer, real numbers' - authenticity sells. (5) Reads like a post, not an ad.",
        "principles": ["case_study", "before_after_numbers", "investment_framing", "authenticity", "post_not_ad"],
    },
]

# ─────────────────────────────────────────────────────────────────
#  ELECTRICAL
# ─────────────────────────────────────────────────────────────────
ELECTRICAL_ADS = [
    # Google Search - Good
    {
        "platform": "google", "format": "search_rsa", "industry": "electrical",
        "headline": "Electrician - 1 Hour Away",
        "description": "Outlet sparking? Breaker keeps tripping? We diagnose and fix most electrical issues same day. Upfront pricing, no surprises.",
        "quality": "good", "score": 9,
        "analysis": "(1) '1 Hour Away' is specific and comforting. (2) Two common problems ('sparking', 'tripping') match search intent. "
                    "(3) 'Same day' response. (4) 'Upfront pricing, no surprises' addresses the electrician pricing fear head-on.",
        "principles": ["specific_time", "problem_match", "same_day", "price_transparency"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "electrical",
        "headline": "Panel Upgrade - Flat Rate",
        "description": "Still on a 100-amp panel? Most modern homes need 200 amps. We handle permits, inspection, and utility coordination. Free quote.",
        "quality": "good", "score": 8,
        "analysis": "Targets a high-value job with education: (1) 'Flat rate' removes big-ticket anxiety. "
                    "(2) '100-amp to 200-amp' educates why they need this. (3) 'Permits, inspection, utility coordination' shows they handle the hassle.",
        "principles": ["flat_rate_assurance", "education_sell", "hassle_removal"],
    },
    # Meta Feed - Good
    {
        "platform": "meta", "format": "feed", "industry": "electrical",
        "headline": "This Is What a 40-Year-Old Panel Looks Like",
        "description": "See those burn marks? That's not cosmetic - it's a fire risk. If your home was built before 1985, your panel may look like this. "
                       "Free safety inspection - takes 20 minutes, could save your home.",
        "quality": "good", "score": 9,
        "analysis": "Fear-based content that's actually responsible: (1) Visual hook ('burn marks') is perfect for image-driven Meta. "
                    "(2) 'Fire risk' creates urgency from genuine concern. (3) 'Built before 1985' targets a huge audience. "
                    "(4) '20 minutes' makes the inspection feel effortless. (5) 'Could save your home' stakes are real.",
        "principles": ["visual_hook", "legitimate_urgency", "audience_targeting", "effortless_action", "real_stakes"],
    },
    # Google Search - Bad
    {
        "platform": "google", "format": "search_rsa", "industry": "electrical",
        "headline": "Professional Electricians",
        "description": "Our team of electricians can handle any job big or small. Contact us today for a free estimate on your electrical project.",
        "quality": "bad", "score": 2,
        "analysis": "Indistinguishable from every other electrician ad: (1) 'Professional' is assumed, not a selling point. "
                    "(2) 'Any job big or small' is the opposite of specialization. (3) 'Electrical project' is cold and corporate. "
                    "Homeowners search 'my outlet is sparking' not 'electrical project'.",
        "principles": ["assumed_qualities", "anti_specialization", "corporate_language"],
    },
]

# ─────────────────────────────────────────────────────────────────
#  ROOFING
# ─────────────────────────────────────────────────────────────────
ROOFING_ADS = [
    # Google Search - Good
    {
        "platform": "google", "format": "search_rsa", "industry": "roofing",
        "headline": "Roof Leak? Emergency Tarp Today",
        "description": "Stop the damage now. We tarp today, permanent fix within 48 hours. Insurance claims handled. Free inspection and estimate.",
        "quality": "good", "score": 9,
        "analysis": "Addresses the immediate panic: (1) 'Emergency tarp today' solves the RIGHT-NOW problem. "
                    "(2) 'Permanent fix within 48 hours' gives timeline. (3) 'Insurance claims handled' removes a major headache. "
                    "(4) Clear progression: tarp now, fix soon, insurance handled.",
        "principles": ["immediate_solution", "timeline_clarity", "insurance_handling", "clear_progression"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "roofing",
        "headline": "Roof Replacement - $0 Down",
        "description": "GAF Master Elite certified - top 2% of roofers nationwide. 50-year warranty. Financing available. Free drone inspection.",
        "quality": "good", "score": 9,
        "analysis": "Premium positioning with accessible entry point: (1) '$0 Down' opens the door. "
                    "(2) 'Top 2% of roofers' is a concrete authority claim. (3) '50-year warranty' signals confidence. "
                    "(4) 'Free drone inspection' is modern and differentiating.",
        "principles": ["accessible_premium", "authority_stat", "warranty_confidence", "modern_differentiator"],
    },
    # Meta Feed - Good
    {
        "platform": "meta", "format": "feed", "industry": "roofing",
        "headline": "Hail Damage? Check Before Your Deadline",
        "description": "Insurance companies set claim deadlines. After last month's storm, homeowners have a limited window to file. "
                       "Free inspection - we document everything for your claim. Don't leave money on the table.",
        "quality": "good", "score": 9,
        "analysis": "Post-storm ad that creates real urgency: (1) 'Deadline' introduces time pressure from insurance, not the roofer. "
                    "(2) 'After last month's storm' is hyper-relevant and timely. (3) 'Document everything for your claim' positions as an ally. "
                    "(4) 'Don't leave money on the table' - loss aversion.",
        "principles": ["external_urgency", "timely_relevance", "ally_positioning", "loss_aversion"],
    },
    {
        "platform": "meta", "format": "feed", "industry": "roofing",
        "headline": "This Roof Was Installed by the 'Cheap Guy'",
        "description": "Homeowner saved $2,000 going with the lowest bid 4 years ago. Just paid us $14,000 to tear it off and redo it. "
                       "Bad installation caused leaks, rotted decking, and voided the shingle warranty. Cheap is expensive.",
        "quality": "good", "score": 9,
        "analysis": "Cautionary tale that sells premium work: (1) 'Cheap guy' - everyone knows one. "
                    "(2) $2,000 saved vs $14,000 spent - the math tells the story. (3) Specific consequences (leaks, rot, voided warranty). "
                    "(4) 'Cheap is expensive' - memorable closer. (5) Educates without directly selling.",
        "principles": ["cautionary_tale", "math_comparison", "specific_consequences", "memorable_closer", "indirect_sell"],
    },
    # Google Search - Bad
    {
        "platform": "google", "format": "search_rsa", "industry": "roofing",
        "headline": "Best Roofers in Town",
        "description": "We offer residential and commercial roofing services. Free estimates available. Call our team today!",
        "quality": "bad", "score": 2,
        "analysis": "Invisible and forgettable: (1) 'Best' is unsubstantiated. (2) 'Residential and commercial' is not a benefit. "
                    "(3) 'Free estimates available' - so does everyone. (4) Nothing addresses the stress of needing a roofer.",
        "principles": ["unsubstantiated_superlative", "features_not_benefits", "table_stakes", "no_empathy"],
    },
]

# ─────────────────────────────────────────────────────────────────
#  LANDSCAPING
# ─────────────────────────────────────────────────────────────────
LANDSCAPING_ADS = [
    # Google Search - Good
    {
        "platform": "google", "format": "search_rsa", "industry": "landscaping",
        "headline": "Weekly Mowing from $35",
        "description": "Show up on time, every time. Same crew each week. Mow, edge, blow included. No contracts - cancel anytime. Free first mow.",
        "quality": "good", "score": 9,
        "analysis": "Every line removes an objection: (1) Price sets expectations immediately. "
                    "(2) 'Show up on time, every time' - the #1 complaint about lawn services. (3) 'Same crew each week' builds trust. "
                    "(4) 'No contracts' removes commitment fear. (5) 'Free first mow' is a risk-free trial.",
        "principles": ["price_transparency", "objection_1_addressed", "consistency_promise", "no_commitment", "free_trial"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "landscaping",
        "headline": "Yard Cleanup - 1 Day Done",
        "description": "Overgrown? Embarrassing? We'll have it looking like a magazine cover by end of day. Shrubs, beds, edging, hauling - everything.",
        "quality": "good", "score": 8,
        "analysis": "(1) 'Overgrown? Embarrassing?' mirrors the internal dialogue. (2) 'Magazine cover by end of day' paints the result. "
                    "(3) 'Everything' included removes scope anxiety. (4) Conversational and judgment-free tone.",
        "principles": ["mirror_internal_dialogue", "result_painting", "scope_clarity", "judgment_free"],
    },
    # Meta Feed - Good
    {
        "platform": "meta", "format": "feed", "industry": "landscaping",
        "headline": "Curb Appeal: Before & After",
        "description": "This front yard went from 'that house' to the best-looking property on the block in 2 days. "
                       "New mulch, edging, shrub shaping, and seasonal color. Same budget you'd spend on a weekend dinner out.",
        "quality": "good", "score": 9,
        "analysis": "Visual transformation content is click candy on Meta: (1) 'That house' is relatable and slightly funny. "
                    "(2) '2 days' is manageable. (3) Itemized work shows scope. (4) Budget comparison to dinner makes it feel affordable.",
        "principles": ["transformation_visual", "relatable_humor", "manageable_timeline", "budget_reframe"],
    },
    # Google Search - Bad
    {
        "platform": "google", "format": "search_rsa", "industry": "landscaping",
        "headline": "Professional Landscaping",
        "description": "We provide lawn care and landscaping services for residential and commercial properties. Contact us for a quote.",
        "quality": "bad", "score": 2,
        "analysis": "Written by the business owner who thinks 'professional' sells: (1) No price, no promise, no personality. "
                    "(2) 'Residential and commercial' is for the website, not the ad. (3) 'Contact us for a quote' - why? There's no reason given.",
        "principles": ["no_personality", "wrong_scope", "no_reason_to_act"],
    },
]

# ─────────────────────────────────────────────────────────────────
#  PEST CONTROL
# ─────────────────────────────────────────────────────────────────
PEST_CONTROL_ADS = [
    # Google Search - Good
    {
        "platform": "google", "format": "search_rsa", "industry": "pest_control",
        "headline": "Ants Gone. Guaranteed.",
        "description": "One treatment. 90-day guarantee. If they come back, so do we - free. Safe for kids and pets. Same-day service available.",
        "quality": "good", "score": 9,
        "analysis": "Three words that say everything: (1) 'Ants Gone' is the result, not the process. (2) 'Guaranteed' removes all risk. "
                    "(3) '90-day guarantee' is specific. (4) 'Safe for kids and pets' addresses the #1 concern. (5) 'Same-day' for when they can't wait.",
        "principles": ["result_focused", "guarantee", "safety_assurance", "same_day"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "pest_control",
        "headline": "Termite Inspection - Free",
        "description": "Termites cause $5 billion in damage annually. Most homeowners don't know until it's too late. Free inspection takes 30 minutes.",
        "quality": "good", "score": 8,
        "analysis": "Fear + education + easy action: (1) '$5 billion' statistic shocks. (2) 'Don't know until it's too late' creates urgency. "
                    "(3) '30 minutes' and 'free' make the barrier almost zero.",
        "principles": ["shocking_stat", "hidden_danger", "low_barrier"],
    },
    # Meta Feed - Good
    {
        "platform": "meta", "format": "feed", "industry": "pest_control",
        "headline": "Spotted This in Your Garage?",
        "description": "Those tiny holes in your baseboards aren't from wear and tear. They're carpenter ant galleries. "
                       "If you see sawdust-like shavings nearby, you have an active colony. "
                       "Free inspection - we'll check your whole home in 30 minutes.",
        "quality": "good", "score": 9,
        "analysis": "Perfect hook for visual platform: (1) Question headline + photo of damage = immediate engagement. "
                    "(2) 'Aren't from wear and tear' - a reveal that changes perception. (3) Diagnostic markers (holes, shavings) educate. "
                    "(4) Urgency ('active colony') without fear-mongering. (5) Easy call to action.",
        "principles": ["visual_hook_question", "perception_shift", "diagnostic_education", "natural_urgency"],
    },
    {
        "platform": "meta", "format": "feed", "industry": "pest_control",
        "headline": "Why the DIY Spray Didn't Work",
        "description": "Raid kills the bugs you see. It does nothing for the nest, the eggs, or the entry points. "
                       "That's why they keep coming back. One professional treatment targets the source - 90-day guarantee.",
        "quality": "good", "score": 9,
        "analysis": "Addresses the DIY instinct head-on: (1) Acknowledges they already tried something - shows understanding. "
                    "(2) Explains WHY DIY fails without condescending. (3) 'Nest, eggs, entry points' educates. "
                    "(4) 'Targets the source' differentiates professional service. (5) Guarantee seals it.",
        "principles": ["acknowledge_diy", "explain_why", "education_differentiator", "source_targeting", "guarantee"],
    },
    # Google Search - Bad
    {
        "platform": "google", "format": "search_rsa", "industry": "pest_control",
        "headline": "Pest Control Solutions",
        "description": "We offer comprehensive pest control services for all types of pests. Residential and commercial. Call for pricing.",
        "quality": "bad", "score": 2,
        "analysis": "Nobody searches for 'pest control solutions' - they search for 'ants in my kitchen' or 'rat in my attic'. "
                    "(1) Generic to the point of being invisible. (2) 'All types of pests' is the opposite of specialization. "
                    "(3) 'Call for pricing' is a wall, not an invitation.",
        "principles": ["search_intent_mismatch", "generic_invisible", "anti_specialization", "pricing_wall"],
    },
]

# ─────────────────────────────────────────────────────────────────
#  CLEANING
# ─────────────────────────────────────────────────────────────────
CLEANING_ADS = [
    # Google Search - Good
    {
        "platform": "google", "format": "search_rsa", "industry": "cleaning",
        "headline": "House Cleaning from $120",
        "description": "Background-checked, insured cleaners. Same team every visit. Supplies included. Book online in 60 seconds. Happy home guarantee.",
        "quality": "good", "score": 9,
        "analysis": "(1) Price in headline filters and attracts. (2) 'Background-checked' addresses the #1 trust issue - strangers in your home. "
                    "(3) 'Same team' builds comfort. (4) 'Book online in 60 seconds' is modern and frictionless. (5) 'Happy home guarantee' is warm.",
        "principles": ["price_transparency", "trust_in_home", "consistency", "frictionless_booking", "warm_guarantee"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "cleaning",
        "headline": "Move-Out Clean - Done Right",
        "description": "Get your full deposit back. We clean to landlord inspection standards. Ovens, baseboards, inside cabinets - everything they check.",
        "quality": "good", "score": 8,
        "analysis": "Targets a specific, high-motivation moment: (1) 'Get your full deposit back' - THAT'S the real motivation. "
                    "(2) 'Landlord inspection standards' speaks their language. (3) Itemized extras (ovens, baseboards, cabinets) show thoroughness.",
        "principles": ["real_motivation", "audience_language", "thoroughness_proof"],
    },
    # Meta Feed - Good
    {
        "platform": "meta", "format": "feed", "industry": "cleaning",
        "headline": "Saturday Morning vs. Saturday Free",
        "description": "You can spend your Saturday scrubbing toilets and mopping floors. Or you can wake up to a clean house and go to the farmers market. "
                       "Starting at $120. Same cleaner every time. Cancel anytime.",
        "quality": "good", "score": 9,
        "analysis": "Sells the lifestyle, not the service: (1) Contrast framing is powerful. (2) 'Farmers market' paints a specific, aspirational scene. "
                    "(3) Price, consistency, and flexibility in three short lines. (4) Not selling cleaning - selling time back.",
        "principles": ["lifestyle_sell", "contrast_framing", "aspirational_scene", "selling_time_not_service"],
    },
    # Google Search - Bad
    {
        "platform": "google", "format": "search_rsa", "industry": "cleaning",
        "headline": "Professional Cleaning Services",
        "description": "We offer residential and commercial cleaning services. Our experienced team will leave your space spotless. Get a quote today.",
        "quality": "bad", "score": 2,
        "analysis": "Template ad that every cleaning company runs: (1) 'Professional' is assumed. "
                    "(2) 'Residential and commercial' splits focus. (3) 'Spotless' is a cliche. (4) Nothing about trust, pricing, or booking.",
        "principles": ["cliche_language", "split_focus", "zero_differentiation"],
    },
]

# ─────────────────────────────────────────────────────────────────
#  GENERAL CONTRACTING
# ─────────────────────────────────────────────────────────────────
GENERAL_CONTRACTING_ADS = [
    # Google Search - Good
    {
        "platform": "google", "format": "search_rsa", "industry": "general_contracting",
        "headline": "Kitchen Remodel - Fixed Price",
        "description": "No change orders, no surprise costs. Final price before demo day. 3D design included. Licensed, bonded, 12-month warranty.",
        "quality": "good", "score": 9,
        "analysis": "Addresses the #1 contractor fear: (1) 'Fixed price' and 'No change orders' tackle the budget-overrun nightmare. "
                    "(2) 'Final price before demo day' is a concrete promise. (3) '3D design' is a modern differentiator. "
                    "(4) Trust stack: licensed, bonded, warranty.",
        "principles": ["fear_addressed", "concrete_promise", "modern_differentiator", "trust_stack"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "general_contracting",
        "headline": "Bathroom Remodel: 5 Days",
        "description": "We specialize in bathrooms. Our 5-day process minimizes disruption. Tile, fixtures, vanity - turnkey. See the 3D preview before we start.",
        "quality": "good", "score": 8,
        "analysis": "Specialization signals mastery: (1) '5 days' sets clear expectations. (2) 'Minimizes disruption' addresses the renovation-life-chaos fear. "
                    "(3) 'Turnkey' means they handle everything. (4) '3D preview' reduces the unknown.",
        "principles": ["specialization_mastery", "timeline_promise", "disruption_awareness", "turnkey", "preview_confidence"],
    },
    # Meta Feed - Good
    {
        "platform": "meta", "format": "feed", "industry": "general_contracting",
        "headline": "From Dated to Modern in 10 Days",
        "description": "This 1987 kitchen had popcorn ceilings, oak cabinets, and laminate counters. "
                       "10 days later: quartz counters, shaker cabinets, and a pot filler the homeowner always dreamed about. "
                       "Full timelapse video in comments.",
        "quality": "good", "score": 9,
        "analysis": "Transformation content with engagement hook: (1) Specific year (1987) triggers 'mine looks like that'. "
                    "(2) Before items are recognizable and common (popcorn, oak, laminate). (3) After items are aspirational but achievable. "
                    "(4) 'Pot filler' is a dream detail. (5) 'Timelapse in comments' drives engagement.",
        "principles": ["transformation", "relatable_before", "aspirational_after", "dream_detail", "engagement_hook"],
    },
    # Google Search - Bad
    {
        "platform": "google", "format": "search_rsa", "industry": "general_contracting",
        "headline": "General Contractor Services",
        "description": "We handle all types of construction and remodeling projects. Residential and commercial. Licensed and insured. Free quote.",
        "quality": "bad", "score": 2,
        "analysis": "The 'we do everything' contractor ad: (1) 'All types' means mastery of none. "
                    "(2) Nothing about timelines, pricing, or process. (3) The person searching 'kitchen remodel' sees this and keeps scrolling.",
        "principles": ["jack_of_all_trades", "no_process", "no_timeline"],
    },
]

# ─────────────────────────────────────────────────────────────────
#  PAINTING
# ─────────────────────────────────────────────────────────────────
PAINTING_ADS = [
    # Google Search - Good
    {
        "platform": "google", "format": "search_rsa", "industry": "painting",
        "headline": "Interior Painting - Per Room",
        "description": "Know the cost before we start. Average room: $350-$500 including premium paint. Furniture moved by us. Done in 1 day per room.",
        "quality": "good", "score": 9,
        "analysis": "(1) 'Per room' pricing is what homeowners think in terms of. (2) Price range ($350-$500) pre-qualifies. "
                    "(3) 'Including premium paint' - no hidden material upcharge. (4) 'Furniture moved by us' removes a hassle. "
                    "(5) '1 day per room' sets clear expectations.",
        "principles": ["buyer_language_pricing", "range_prequalify", "included_materials", "hassle_removal", "timeline"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "painting",
        "headline": "Exterior Painting Experts",
        "description": "Proper prep is 80% of a great paint job. We power wash, scrape, prime, caulk, THEN paint. 5-year warranty on peeling.",
        "quality": "good", "score": 8,
        "analysis": "Process-based differentiation: (1) 'Proper prep is 80%' educates and positions. "
                    "(2) Listing the prep steps (wash, scrape, prime, caulk) proves they do the work others skip. "
                    "(3) '5-year warranty on peeling' backs up the claim with confidence.",
        "principles": ["process_education", "showing_not_telling", "warranty_confidence"],
    },
    # Meta Feed - Good
    {
        "platform": "meta", "format": "feed", "industry": "painting",
        "headline": "Picking Paint Colors? Start Here",
        "description": "We've painted 400+ homes. These are the 5 exterior color combos that get the most compliments in our area. "
                       "Free color consultation included with every estimate. No more staring at 200 swatches alone.",
        "quality": "good", "score": 9,
        "analysis": "Value-first content that solves a real problem: (1) Color choice paralysis is REAL - this addresses it. "
                    "(2) '400+ homes' and 'our area' show local expertise. (3) 'Most compliments' taps into social validation. "
                    "(4) 'Free color consultation' is a natural lead magnet. (5) '200 swatches alone' is relatable frustration.",
        "principles": ["value_first", "paralysis_solution", "local_expertise", "social_validation", "natural_lead_magnet"],
    },
    # Google Search - Bad
    {
        "platform": "google", "format": "search_rsa", "industry": "painting",
        "headline": "Painting Services Available",
        "description": "We provide interior and exterior painting services. Quality work at competitive prices. Call for a free estimate.",
        "quality": "bad", "score": 2,
        "analysis": "Says nothing a homeowner can act on: (1) 'Available' - obviously. (2) 'Quality work at competitive prices' means nothing. "
                    "(3) No timeline, no process, no price guidance. The searcher has no reason to pick this over 5 identical ads.",
        "principles": ["obvious_claims", "meaningless_value_prop", "no_actionable_info"],
    },
]

# ─────────────────────────────────────────────────────────────────
#  GARAGE DOOR
# ─────────────────────────────────────────────────────────────────
GARAGE_DOOR_ADS = [
    # Google Search - Good
    {
        "platform": "google", "format": "search_rsa", "industry": "garage_door",
        "headline": "Garage Door Stuck? 1-Hour Fix",
        "description": "Spring broke? Off-track? We carry all major springs on our truck. Most repairs done in one trip. $39 service call with repair.",
        "quality": "good", "score": 9,
        "analysis": "Matches the exact moment of frustration: (1) 'Stuck?' matches the search. (2) Two common causes show expertise. "
                    "(3) 'Carry all major springs' means no second trip. (4) '$39 service call with repair' removes the diagnostic fee fear.",
        "principles": ["situation_match", "expertise_signals", "one_trip_promise", "affordable_entry"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "garage_door",
        "headline": "New Garage Door - Installed",
        "description": "50+ styles in stock. Insulated options available. Professional install in 4 hours. Includes disposal of old door. Free estimate.",
        "quality": "good", "score": 8,
        "analysis": "Replacement made simple: (1) '50+ styles' shows selection. (2) 'Insulated options' targets energy-conscious buyers. "
                    "(3) '4 hours' for a full install is impressive and specific. (4) 'Disposal of old door' shows turnkey thinking.",
        "principles": ["selection_variety", "energy_conscious", "specific_install_time", "turnkey"],
    },
    # Meta Feed - Good
    {
        "platform": "meta", "format": "feed", "industry": "garage_door",
        "headline": "Biggest Difference for $1,200",
        "description": "A new garage door is the #1 ROI home improvement according to Remodeling Magazine. It's 40% of your home's face. "
                       "See what a single upgrade does for curb appeal. Before/after gallery below.",
        "quality": "good", "score": 9,
        "analysis": "Investment framing with authority: (1) '#1 ROI' stat from Remodeling Magazine is credible. "
                    "(2) '40% of your home's face' is a striking visualization. (3) '$1,200' makes it feel accessible. "
                    "(4) Before/after gallery is engagement gold on Meta.",
        "principles": ["roi_framing", "authoritative_source", "striking_stat", "accessible_price", "visual_content"],
    },
]

# ─────────────────────────────────────────────────────────────────
#  FOUNDATION REPAIR
# ─────────────────────────────────────────────────────────────────
FOUNDATION_REPAIR_ADS = [
    # Google Search - Good
    {
        "platform": "google", "format": "search_rsa", "industry": "foundation_repair",
        "headline": "Foundation Cracks? Free Check",
        "description": "Not all cracks are structural. We'll tell you which ones to worry about - for free. Engineered solutions. Transferable warranty.",
        "quality": "good", "score": 9,
        "analysis": "Calms the panic while building trust: (1) 'Not all cracks are structural' immediately reduces anxiety. "
                    "(2) 'Tell you which ones to worry about - for free' positions as honest advisor. "
                    "(3) 'Engineered solutions' signals technical authority. (4) 'Transferable warranty' matters for resale.",
        "principles": ["anxiety_reduction", "honest_advisor", "technical_authority", "resale_value"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "foundation_repair",
        "headline": "Selling a Home? Fix First",
        "description": "Foundation issues kill deals. We provide repair + engineer's letter for your buyer's lender. Most jobs done in 1-3 days.",
        "quality": "good", "score": 9,
        "analysis": "Targets a high-urgency moment: (1) Selling homeowners MUST fix or lose the sale. "
                    "(2) 'Engineer's letter for your buyer's lender' shows knowledge of the real-estate process. "
                    "(3) '1-3 days' won't delay closing. Every element serves the seller's timeline.",
        "principles": ["high_urgency_moment", "process_knowledge", "timeline_fit"],
    },
    # Meta Feed - Good
    {
        "platform": "meta", "format": "feed", "industry": "foundation_repair",
        "headline": "That Door That Won't Close Right?",
        "description": "Sticky doors, cracked drywall, and gaps around window frames aren't 'settling.' "
                       "They're symptoms of foundation movement. Most homeowners wait until it's $15,000+. "
                       "Catch it early: free foundation check.",
        "quality": "good", "score": 9,
        "analysis": "Symptom-to-cause education that creates demand: (1) Starts with everyday annoyances people dismiss. "
                    "(2) 'Aren't settling' is a belief correction. (3) '$15,000+' stakes make early action obvious. "
                    "(4) 'Catch it early' reframes the free check as smart, not scary.",
        "principles": ["symptom_recognition", "belief_correction", "cost_of_waiting", "smart_reframe"],
    },
]

# ─────────────────────────────────────────────────────────────────
#  WATER DAMAGE
# ─────────────────────────────────────────────────────────────────
WATER_DAMAGE_ADS = [
    # Google Search - Good
    {
        "platform": "google", "format": "search_rsa", "industry": "water_damage",
        "headline": "Water Damage? We're En Route",
        "description": "Mold starts in 24-48 hours. We respond in 60 minutes. Extract, dry, restore. Direct insurance billing - less out-of-pocket for you.",
        "quality": "good", "score": 9,
        "analysis": "Urgency backed by facts: (1) 'Mold starts in 24-48 hours' creates legitimate, educational urgency. "
                    "(2) '60 minutes' response time. (3) 'Extract, dry, restore' - clear 3-step process. "
                    "(4) 'Direct insurance billing' removes the money fear.",
        "principles": ["educational_urgency", "fast_response", "clear_process", "insurance_billing"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "water_damage",
        "headline": "Flooded Basement Recovery",
        "description": "We pump, sanitize, and dry in 24 hours. IICRC certified. Industrial equipment, not box fans. Insurance paperwork handled.",
        "quality": "good", "score": 8,
        "analysis": "(1) '24 hours' for full recovery. (2) 'IICRC certified' is the industry gold standard. "
                    "(3) 'Industrial equipment, not box fans' differentiates from DIY and amateurs. (4) Paperwork handled reduces overwhelm.",
        "principles": ["fast_resolution", "certification_authority", "equipment_differentiator", "overwhelm_reduction"],
    },
    # Meta Feed - Good
    {
        "platform": "meta", "format": "feed", "industry": "water_damage",
        "headline": "Don't Rip Out That Drywall Yet",
        "description": "Most homeowners panic and start tearing walls apart after a leak. STOP. "
                       "With proper commercial drying, we save 70% of 'damaged' drywall and cut your restoration cost in half. "
                       "Free moisture mapping assessment.",
        "quality": "good", "score": 9,
        "analysis": "Stops a costly mistake: (1) 'STOP' is a scroll-stopper. (2) Challenges the panic instinct with expertise. "
                    "(3) '70%' and 'cut in half' are specific, money-saving numbers. (4) 'Moisture mapping' sounds technical and thorough.",
        "principles": ["mistake_prevention", "expertise_challenge", "money_saving_numbers", "technical_differentiation"],
    },
]

# ─────────────────────────────────────────────────────────────────
#  PET WASTE REMOVAL
# ─────────────────────────────────────────────────────────────────
PET_WASTE_REMOVAL_ADS = [
    # Google Search - Good
    {
        "platform": "google", "format": "search_rsa", "industry": "pet_waste_removal",
        "headline": "Yard Cleaned: You Won't Lift a Finger",
        "description": "Weekly poop scooping from $15/visit. We come, we scoop, we sanitize. Gate code access - you don't even have to be home.",
        "quality": "good", "score": 9,
        "analysis": "Ultimate convenience pitch: (1) '$15/visit' is impulse-buy territory. (2) 'We come, we scoop, we sanitize' is punchy and complete. "
                    "(3) 'Gate code access' means zero effort required. (4) The whole ad says: this problem costs $15 to never think about again.",
        "principles": ["impulse_price", "punchy_process", "zero_effort", "problem_elimination"],
    },
    {
        "platform": "google", "format": "search_rsa", "industry": "pet_waste_removal",
        "headline": "Dog Poop Removal Service",
        "description": "Once a week, every week. Your yard stays clean, your kids can actually play outside. From $60/month. Cancel anytime.",
        "quality": "good", "score": 8,
        "analysis": "(1) 'Kids can actually play outside' - that's the real why. (2) Consistency promise. "
                    "(3) Monthly pricing feels manageable. (4) 'Cancel anytime' removes commitment anxiety.",
        "principles": ["real_benefit_unlocked", "consistency", "manageable_price", "no_commitment"],
    },
    # Meta Feed - Good
    {
        "platform": "meta", "format": "feed", "industry": "pet_waste_removal",
        "headline": "The Chore Nobody Fights Over",
        "description": "Nobody volunteers for poop duty. That's why 200+ families in [City] let us handle it weekly. "
                       "$15/visit. Takes us 10 minutes. Saves you an argument every Saturday. "
                       "First scoop free - see why people never go back to doing it themselves.",
        "quality": "good", "score": 9,
        "analysis": "Humor-driven relatability: (1) 'Nobody volunteers for poop duty' - every dog owner nods. "
                    "(2) '200+ families' is social proof. (3) 'Saves you an argument' is the real value prop. "
                    "(4) 'Never go back' hooks the trial. (5) Reads like a friend's recommendation, not an ad.",
        "principles": ["humor_relatability", "social_proof_local", "real_value_prop", "trial_hook", "friend_recommendation"],
    },
    # Meta Feed - Bad
    {
        "platform": "meta", "format": "feed", "industry": "pet_waste_removal",
        "headline": "Pet Waste Removal Services",
        "description": "We offer professional pet waste removal services for residential properties. "
                       "Keep your yard clean and sanitary. Contact us today for pricing and availability.",
        "quality": "bad", "score": 2,
        "analysis": "Sucks the fun out of a service that should sell itself: (1) 'Professional pet waste removal services' - nobody talks like that. "
                    "(2) 'Residential properties' - corporate language for a poop-scooping service. "
                    "(3) No price, no personality, no humor. This service is funny, lean into it.",
        "principles": ["corporate_language_mismatch", "no_personality", "missed_tone"],
    },
]


# ─────────────────────────────────────────────────────────────────
#  Aggregated library for seeding
# ─────────────────────────────────────────────────────────────────
ALL_NICHE_ADS = (
    PLUMBING_ADS
    + HVAC_ADS
    + ELECTRICAL_ADS
    + ROOFING_ADS
    + LANDSCAPING_ADS
    + PEST_CONTROL_ADS
    + CLEANING_ADS
    + GENERAL_CONTRACTING_ADS
    + PAINTING_ADS
    + GARAGE_DOOR_ADS
    + FOUNDATION_REPAIR_ADS
    + WATER_DAMAGE_ADS
    + PET_WASTE_REMOVAL_ADS
)
