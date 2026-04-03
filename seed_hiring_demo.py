"""
Seed the Hiring Hub with a demo job and 3 demo applicants at different pipeline stages.
Run once:  python seed_hiring_demo.py
"""
import sys, os, json

# Ensure the webapp package is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from webapp.database import WebDB

db = WebDB("data/database/gromore.db")
db.init()  # ensures all tables (including hiring_*) exist

# ── Find or create a brand ──
conn = db._conn()
brand = conn.execute("SELECT id, display_name FROM brands LIMIT 1").fetchone()
conn.close()

if brand:
    brand_id = brand["id"]
    brand_name = brand["display_name"]
    print(f"Using existing brand: {brand_name} (id={brand_id})")
else:
    conn = db._conn()
    conn.execute(
        """INSERT INTO brands (slug, display_name, industry, website, service_area, primary_services)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("ace-plumbing", "Ace Plumbing", "plumbing", "https://aceplumbing.example.com",
         "Phoenix Metro", "Residential plumbing, drain cleaning, water heaters"),
    )
    brand_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()
    brand_name = "Ace Plumbing"
    print(f"Created brand: {brand_name} (id={brand_id})")

# ── Create a demo job ──
gate_qs = json.dumps([
    {"id": "g1", "question": "Are you comfortable working outdoors in Arizona heat?", "options": ["Yes", "No"], "dealbreaker": "No"},
    {"id": "g2", "question": "Do you have a valid driver's license?", "options": ["Yes", "No"], "dealbreaker": "No"},
    {"id": "g3", "question": "Are you available to start within 2 weeks?", "options": ["Yes", "No", "Flexible"], "dealbreaker": None},
])

job_id = db.create_hiring_job(
    brand_id=brand_id,
    title="Service Plumber",
    department="Field Ops",
    job_type="full-time",
    location="Phoenix, AZ",
    remote="no",
    description="Looking for a reliable service plumber to handle residential calls across the Phoenix metro area. You will diagnose and repair plumbing issues, install fixtures, and maintain strong communication with homeowners.",
    requirements=json.dumps(["2+ years plumbing experience", "Valid AZ driver's license", "Own basic hand tools", "Can lift 50 lbs regularly"]),
    nice_to_haves=json.dumps(["Journeyman card", "Experience with tankless water heaters", "Bilingual English/Spanish"]),
    salary_min=55000,
    salary_max=75000,
    benefits="Health insurance, paid time off, company vehicle, tool allowance",
    screening_criteria=json.dumps({
        "must_haves": ["Reliability", "Problem-solving under pressure", "Good communication with homeowners"],
        "dealbreakers": ["No-shows to interviews", "Can't pass background check", "No driver's license"],
        "culture_notes": "Small team, family feel. We value people who show up on time and take ownership.",
        "physical_requirements": "Crawl spaces, ladders, outdoor heat"
    }),
    status="active",
)
db.update_hiring_job(job_id, gate_questions=gate_qs)
print(f"Created job: Service Plumber (id={job_id})")


# ────────────────────────────────────────────────
# CANDIDATE 1 - Marcus Rivera  (completed, strong)
# ────────────────────────────────────────────────
c1_id = db.create_hiring_candidate(
    brand_id=brand_id, job_id=job_id,
    name="Marcus Rivera", email="marcus.rivera@demo.test",
    phone="(602) 555-0142", source="website",
    resume_text="5 years residential plumbing. Journeyman card. Worked at Rooter Pro for 3 years, then independent for 2. Comfortable with re-pipes, water heater installs, drain cleaning. Own full tool set.",
)

# Update candidate to interviewed/scored state
db.update_hiring_candidate(c1_id,
    status="interviewed",
    ai_score=87,
    score_breakdown=json.dumps({
        "reliability": 18, "ownership": 17, "work_ethic": 18,
        "communication_clarity": 17, "responsiveness": 17
    }),
    ai_summary="Strong candidate with solid field experience and a no-nonsense communication style. Gave specific examples of handling callbacks and difficult homeowner situations. Shows genuine ownership of his work quality. Minor ding on communication clarity - tends to give short answers until prompted for detail.",
    ai_recommendation="interview",
    signal_reasoning=json.dumps({
        "reliability": "Mentioned he has not missed a workday in 18 months. Described his morning routine of checking the van the night before. Consistent pattern of being early to calls.",
        "ownership": "Told a story about going back to a job on his day off because the fix did not hold. Did not blame the parts or the supply house, took it on himself.",
        "work_ethic": "Currently running solo jobs and handling his own scheduling. Voluntarily mentioned he prefers staying busy over downtime. Did not hesitate on the physical requirements questions.",
        "communication_clarity": "Answers were accurate but initially brief. Opened up more in the second half. Needs a small push to elaborate but the substance is solid when he does.",
        "responsiveness": "Answered all questions within 20-30 seconds. No stalling or dodging. Came back to questions with corrections when he felt he could improve his answer."
    }),
    key_moments=json.dumps([
        "Described going back to a job on his day off because the initial repair did not hold. 'That is on me, not the customer.'",
        "When asked about dealing with an angry homeowner, gave a specific story about a flooded kitchen and how he handled it step by step.",
        "Mentioned he keeps a parts log in his truck so he never has to make a second trip to the supply house."
    ]),
    interview_questions=json.dumps([
        "Tell me about a time a repair took longer than expected. How did you keep the homeowner in the loop?",
        "You mentioned working solo for 2 years. What made you want to join a team again?",
        "Walk me through how you prep your van for the next day."
    ]),
    screening_started_at="2026-03-28 09:14:00",
    screening_completed_at="2026-03-28 09:32:00",
    response_time_avg_sec=24,
)

# Create interview record
i1_id, t1 = db.create_hiring_interview(c1_id, brand_id, job_id)
db.update_hiring_interview(i1_id,
    status="completed",
    started_at="2026-03-28 09:14:00",
    completed_at="2026-03-28 09:32:00",
    current_question=8,
    total_score=87,
    score_breakdown=json.dumps({
        "reliability": 18, "ownership": 17, "work_ethic": 18,
        "communication_clarity": 17, "responsiveness": 17
    }),
    ai_evaluation="Marcus is a strong hire. His field experience is well above the minimum bar, and his answers show real accountability. He went back to fix a job on his day off without being asked. His communication is good but not flashy - he says what needs to be said and moves on. For a service plumber role this is exactly what you want: someone reliable who takes ownership and does not waste the customer's time.",
    gate_answers=json.dumps({"g1": "Yes", "g2": "Yes", "g3": "Yes"}),
    gate_passed=1,
)

# Seed realistic conversation messages
msgs_1 = [
    ("outbound", "Hey Marcus, welcome to the screening for Service Plumber at Ace Plumbing. I'm Warren, an AI interviewer. I'll ask around 8-10 questions covering different topics. Some are scenarios, some are quick picks. Ready? Let's start with this: You arrive at a residential call and the homeowner says the previous plumber 'made it worse.' How do you handle the first 5 minutes?", 1, "scenario"),
    ("inbound", "First thing I do is listen. Let them vent. Then I ask them to show me what was done. I don't trash the other guy, I just focus on what I can see and what needs to happen next. I'd take some photos and explain the situation in plain terms.", None, None),
    ("outbound", "Good. Next one is quick - pick the answer that fits you best: When you finish a job 30 minutes early, you usually...", 2, "pick_one"),
    ("inbound", "Call dispatch to see if there is something nearby I can knock out.", None, None),
    ("outbound", "Quick fire round. First thing that comes to mind. What is the most common mistake newer plumbers make?", 3, "rapid_fire"),
    ("inbound", "Not checking their work before they leave. Turning the water back on and just walking out.", None, None),
    ("outbound", "Rank these from most to least important to you in a job: pay, schedule flexibility, team culture, learning new skills.", 4, "rank_it"),
    ("inbound", "Team culture, learning new skills, schedule flexibility, pay. Pay matters but if the environment is bad nothing else makes up for it.", None, None),
    ("outbound", "Real talk - what is the hardest part of residential plumbing that most people outside the trade do not understand?", 5, "real_talk"),
    ("inbound", "The heat. People don't get what it's like crawling under a house in July in Arizona. And the crawl spaces - you're in dirt, sometimes sewage, and you still have to smile when you come out and talk to the homeowner. It takes a specific kind of person.", None, None),
    ("outbound", "Tell me about a time you had to go back and fix something that did not hold. What happened and how did you handle it?", 6, "scenario"),
    ("inbound", "I did a faucet install and the supply line started dripping that night. The customer called the office and they called me. I went back the next morning on my day off, replaced the line, and double-checked both connections. That is on me, not the customer. If I put my name on it, it needs to be right.", None, None),
    ("outbound", "Pick one: You get a callback on a job you thought was perfect. Your first reaction is...", 7, "pick_one"),
    ("inbound", "Frustrated with myself, then I go figure out what I missed.", None, None),
    ("outbound", "Last one. If you could change one thing about how plumbing companies treat their techs, what would it be?", 8, "real_talk"),
    ("inbound", "Stop micromanaging guys who have been doing this for years. Give them the training and the tools and let them work. Most of us got into this because we like solving problems on our own.", None, None),
]
for direction, content, q_num, q_type in msgs_1:
    db.add_hiring_message(
        interview_id=i1_id, candidate_id=c1_id,
        direction=direction, channel="web_chat",
        content=content, is_question=1 if direction == "outbound" else 0,
        question_number=q_num,
        signal_scores=json.dumps({}) if direction == "outbound" else json.dumps({"overall": "positive"}),
        response_time_sec=None if direction == "outbound" else 24,
    )
print(f"  Candidate 1: Marcus Rivera (score 87, interviewed, token={t1})")


# ────────────────────────────────────────────────
# CANDIDATE 2 - Kaylee Tran  (in-progress, mid-interview)
# ────────────────────────────────────────────────
c2_id = db.create_hiring_candidate(
    brand_id=brand_id, job_id=job_id,
    name="Kaylee Tran", email="kaylee.tran@demo.test",
    phone="(480) 555-0267", source="referral",
    resume_text="1.5 years as apprentice plumber at Desert Flow Plumbing. Completed trade school program at East Valley Institute of Technology. Looking to grow into a journeyman role.",
)
db.update_hiring_candidate(c2_id,
    status="screening",
    screening_started_at="2026-04-01 14:20:00",
    response_time_avg_sec=35,
)

i2_id, t2 = db.create_hiring_interview(c2_id, brand_id, job_id)
db.update_hiring_interview(i2_id,
    status="in_progress",
    started_at="2026-04-01 14:20:00",
    current_question=4,
    gate_answers=json.dumps({"g1": "Yes", "g2": "Yes", "g3": "Flexible"}),
    gate_passed=1,
)

msgs_2 = [
    ("outbound", "Hi Kaylee, thanks for applying to the Service Plumber position at Ace Plumbing. I'm Warren, an AI screening assistant. I'll ask you around 8-10 questions. Let's jump in. Scenario: Your first solo call of the day - the homeowner says their garbage disposal is jammed and there is water backing up into the sink. Walk me through your approach.", 1, "scenario"),
    ("inbound", "I'd start by turning off the disposal and checking under the sink for any leaks. Then I'd try to clear the jam manually with an Allen wrench from the bottom. If the backup is from the disposal drain, I'd disconnect it and check for clogs in the line. If it's further down, I'd run a snake.", None, None),
    ("outbound", "Pick the answer that fits you best: When you don't know how to fix something on a call, you...", 2, "pick_one"),
    ("inbound", "Call a more experienced tech and ask for guidance before I start guessing.", None, None),
    ("outbound", "Quick fire. What is one thing you learned in trade school that turned out to be more important in the field than you expected?", 3, "rapid_fire"),
    ("inbound", "Code compliance. In school it felt like memorizing rules, but in the field it saves you from callbacks and failed inspections.", None, None),
    ("outbound", "Rank these from most to least important to you right now: getting hands-on experience, earning potential, mentorship from senior techs, schedule predictability.", 4, "rank_it"),
]
for direction, content, q_num, q_type in msgs_2:
    db.add_hiring_message(
        interview_id=i2_id, candidate_id=c2_id,
        direction=direction, channel="web_chat",
        content=content, is_question=1 if direction == "outbound" else 0,
        question_number=q_num,
        signal_scores=json.dumps({}),
        response_time_sec=None if direction == "outbound" else 35,
    )
print(f"  Candidate 2: Kaylee Tran (in-progress, mid-interview, token={t2})")


# ────────────────────────────────────────────────
# CANDIDATE 3 - Derek Solis  (applied, not started)
# ────────────────────────────────────────────────
c3_id = db.create_hiring_candidate(
    brand_id=brand_id, job_id=job_id,
    name="Derek Solis", email="derek.solis@demo.test",
    phone="(623) 555-0189", source="website",
    resume_text="3 years plumbing experience with a focus on new construction. Looking to switch to service work. Comfortable with copper, PEX, and PVC. Have worked in 110+ degree heat before.",
)

i3_id, t3 = db.create_hiring_interview(c3_id, brand_id, job_id)
# Stays as pending - candidate hasn't started yet
print(f"  Candidate 3: Derek Solis (applied, pending interview, token={t3})")


print(f"\nDone. Job ID={job_id}, Brand ID={brand_id}")
print(f"Interview links:")
print(f"  Marcus (completed):  /interview/{t1}")
print(f"  Kaylee (in-progress): /interview/{t2}")
print(f"  Derek  (pending):    /interview/{t3}")
