"""Rebuild client_team.html with animated office scene.
Agents walk around, chat, gather for briefing, work at desks."""

PATH = "webapp/templates/client/client_team.html"

PART1 = r'''{% extends "client/client_base.html" %}
{% block title %}Your Team{% endblock %}

{% block content %}
<style>
/* ── TEAM HEADER WRAP ── */
.team-wrap {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    border-radius: 20px;
    padding: 24px 28px 0;
    margin-bottom: 32px;
    box-shadow: 0 4px 24px rgba(0,0,0,.35);
    position: relative;
    overflow: hidden;
}
.team-wrap::before {
    content: '';
    position: absolute; inset: 0;
    background: radial-gradient(ellipse at 20% 50%, rgba(124,58,237,.08) 0%, transparent 60%),
                radial-gradient(ellipse at 80% 20%, rgba(59,130,246,.06) 0%, transparent 50%);
    pointer-events: none;
}
.team-header {
    display: flex; justify-content: space-between; align-items: flex-start;
    margin-bottom: 12px; position: relative; z-index: 2;
}
.team-header h2 { color: #fff; margin: 0; font-size: 1.35rem; }
.team-header p { color: rgba(255,255,255,.5); margin: 3px 0 0; font-size: 0.82rem; }
.team-stats { display: flex; gap: 24px; text-align: center; }
.team-stat .ts-val { font-size: 1.5rem; font-weight: 700; color: #fff; line-height: 1; }
.team-stat .ts-lbl { font-size: 0.62rem; text-transform: uppercase; color: rgba(255,255,255,.4); letter-spacing: 1px; margin-top: 2px; }

/* ── HERO ACTIONS ── */
.team-hero-actions {
    display: flex; align-items: center; gap: 12px; margin-bottom: 14px;
    position: relative; z-index: 2; flex-wrap: wrap;
}
.run-team-btn {
    display: inline-flex; align-items: center; gap: 8px;
    background: linear-gradient(135deg, #7c3aed, #4f46e5);
    color: #fff; border: none; padding: 10px 22px; border-radius: 10px;
    font-size: 0.88rem; font-weight: 600; cursor: pointer;
    transition: opacity 0.2s, transform 0.15s;
}
.run-team-btn:hover { opacity: 0.9; transform: translateY(-1px); }
.run-team-btn:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }
.run-team-btn .spinner-border { width: 16px; height: 16px; border-width: 2px; }
.warren-brief-toggle {
    display: none; align-items: center; gap: 6px;
    background: rgba(255,255,255,.08); border: 1px solid rgba(255,255,255,.15);
    color: rgba(255,255,255,.7); padding: 8px 16px; border-radius: 10px;
    font-size: 0.82rem; font-weight: 500; cursor: pointer; transition: all .2s;
}
.warren-brief-toggle:hover { background: rgba(255,255,255,.14); color: #fff; }
.warren-brief-toggle img { width: 22px; height: 22px; border-radius: 6px; }

/* ── WARREN BRIEF ── */
.warren-brief-panel {
    display: none; background: rgba(255,255,255,.06); border: 1px solid rgba(255,255,255,.1);
    border-radius: 12px; padding: 14px 16px; margin-bottom: 14px;
    position: relative; z-index: 2;
}
.warren-brief-panel.open { display: block; }
.warren-textarea {
    width: 100%; min-height: 70px; background: rgba(0,0,0,.25); border: 1px solid rgba(255,255,255,.12);
    border-radius: 8px; padding: 10px 12px; font-size: 0.84rem; color: #fff;
    resize: vertical; font-family: inherit;
}
.warren-textarea:focus { outline: none; border-color: rgba(124,58,237,.5); box-shadow: 0 0 0 2px rgba(124,58,237,.15); }
.warren-textarea::placeholder { color: rgba(255,255,255,.3); }
.warren-brief-hint { font-size: 0.72rem; color: rgba(255,255,255,.35); margin-top: 6px; }

/* ── OFFICE FLOOR ── */
.office-floor {
    position: relative;
    width: 100%;
    aspect-ratio: 1147 / 765;
    background: url('/static/sprites/office.png') center/cover no-repeat;
    border-radius: 12px 12px 0 0;
    overflow: hidden;
    image-rendering: auto;
}

/* ── AGENT SPRITE ── */
.office-sprite {
    position: absolute;
    width: 70px; height: 70px;
    cursor: pointer;
    transform: translate(-50%, -100%);
    transition: left 2s ease-in-out, top 2s ease-in-out;
    z-index: 10;
}
.office-sprite img {
    width: 100%; height: 100%;
    object-fit: contain;
    image-rendering: pixelated;
    filter: drop-shadow(0 3px 4px rgba(0,0,0,.35));
    transition: transform .15s;
}
.office-sprite:hover img { transform: scale(1.15); }
.office-sprite::after {
    content: '';
    position: absolute; bottom: 0; left: 50%;
    transform: translateX(-50%);
    width: 60%; height: 5px;
    background: rgba(0,0,0,.2);
    border-radius: 50%;
    filter: blur(2px);
}
.sprite-label {
    position: absolute; bottom: -16px; left: 50%;
    transform: translateX(-50%);
    background: rgba(0,0,0,.75); color: #fff;
    font-size: 0.58rem; font-weight: 600;
    padding: 2px 8px; border-radius: 6px;
    white-space: nowrap; opacity: 0;
    transition: opacity .2s; pointer-events: none;
}
.office-sprite:hover .sprite-label { opacity: 1; }

/* Walk bob */
.sprite-walking img {
    animation: spriteBob .45s ease-in-out infinite;
}
@keyframes spriteBob {
    0%, 100% { transform: translateY(0); }
    50% { transform: translateY(-5px); }
}

/* Working animation */
.sprite-working img {
    animation: spriteWork 2.5s ease-in-out infinite;
}
@keyframes spriteWork {
    0%, 100% { transform: translateY(0) rotate(0deg); }
    30% { transform: translateY(-1px) rotate(-0.5deg); }
    70% { transform: translateY(-1px) rotate(0.5deg); }
}
.work-indicator {
    position: absolute; top: -6px; right: -2px;
    width: 14px; height: 14px; border-radius: 50%;
    background: #7c3aed; border: 2px solid #1a1a2e;
    display: flex; align-items: center; justify-content: center;
    animation: indicatorPulse 1.5s ease-in-out infinite;
}
.work-indicator i { font-size: .5rem; color: #fff; }
@keyframes indicatorPulse {
    0%, 100% { transform: scale(1); }
    50% { transform: scale(1.2); }
}

/* Unhired / grey */
.sprite-unhired img {
    filter: grayscale(.85) brightness(.45) drop-shadow(0 3px 4px rgba(0,0,0,.35));
}
.sprite-unhired:hover img {
    filter: grayscale(.4) brightness(.65) drop-shadow(0 3px 4px rgba(0,0,0,.35)) scale(1.1);
}
.sprite-hire-badge {
    position: absolute; top: -10px; left: 50%;
    transform: translateX(-50%);
    background: linear-gradient(135deg, #f59e0b, #f97316);
    color: #fff; font-size: .5rem; font-weight: 800;
    letter-spacing: 1px; padding: 2px 10px; border-radius: 8px;
    box-shadow: 0 2px 8px rgba(245,158,11,.4);
    animation: hireFloat 2.5s ease-in-out infinite;
    white-space: nowrap;
}
@keyframes hireFloat {
    0%, 100% { transform: translateX(-50%) translateY(0); }
    50% { transform: translateX(-50%) translateY(-4px); }
}

/* Speech bubble */
.speech-bubble {
    position: absolute; top: -38px; left: 50%;
    transform: translateX(-50%);
    background: #fff; color: #1f2937;
    font-size: 0.65rem; font-weight: 500;
    padding: 5px 12px; border-radius: 10px;
    box-shadow: 0 3px 12px rgba(0,0,0,.2);
    white-space: nowrap; opacity: 0;
    transition: opacity .3s;
    pointer-events: none; z-index: 50;
    max-width: 200px; white-space: normal;
    text-align: center; line-height: 1.3;
}
.speech-bubble::after {
    content: '';
    position: absolute; bottom: -5px; left: 50%;
    transform: translateX(-50%);
    border-left: 5px solid transparent;
    border-right: 5px solid transparent;
    border-top: 5px solid #fff;
}
.speech-bubble.visible { opacity: 1; }

/* Warren's briefing bubble - larger */
.speech-bubble.briefing {
    font-size: 0.72rem; padding: 8px 16px;
    max-width: 260px; background: #ede9fe; color: #4c1d95;
    border: 1px solid #c4b5fd;
}
.speech-bubble.briefing::after { border-top-color: #ede9fe; }

/* Chat dots */
.chat-dots {
    position: absolute; top: -24px; left: 50%;
    transform: translateX(-50%);
    background: #fff; border-radius: 10px;
    padding: 3px 10px; box-shadow: 0 2px 8px rgba(0,0,0,.15);
    display: none; z-index: 50;
}
.chat-dots::after {
    content: '';
    position: absolute; bottom: -4px; left: 50%;
    transform: translateX(-50%);
    border-left: 4px solid transparent;
    border-right: 4px solid transparent;
    border-top: 4px solid #fff;
}
.chat-dots span {
    display: inline-block; font-weight: 700; color: #7c3aed;
    font-size: 0.8rem; line-height: 1;
    animation: dotJump 1.4s ease-in-out infinite;
}
.chat-dots span:nth-child(2) { animation-delay: .15s; }
.chat-dots span:nth-child(3) { animation-delay: .3s; }
@keyframes dotJump {
    0%, 60%, 100% { transform: translateY(0); }
    30% { transform: translateY(-4px); }
}

/* Equipment decorations */
.office-equip {
    position: absolute;
    transform: translate(-50%, -100%);
    pointer-events: none;
    z-index: 5;
    image-rendering: pixelated;
}

/* ── STATUS DOT ON SPRITE ── */
.sprite-status-dot {
    position: absolute; top: 2px; right: 2px;
    width: 10px; height: 10px; border-radius: 50%;
    border: 2px solid rgba(0,0,0,.3); z-index: 3;
}
.ssd-idle { background: #10b981; }
.ssd-working { background: #6366f1; animation: ssdPulse 1.2s infinite; }
@keyframes ssdPulse { 0%,100% { opacity:1; } 50% { opacity:.3; } }

/* ── SEQUENCE OVERLAY ── */
.sequence-banner {
    position: absolute; top: 50%; left: 50%;
    transform: translate(-50%, -50%);
    background: rgba(0,0,0,.7); color: #fff;
    padding: 10px 28px; border-radius: 12px;
    font-size: 0.9rem; font-weight: 600;
    z-index: 100; opacity: 0; transition: opacity .4s;
    pointer-events: none; text-align: center;
    backdrop-filter: blur(4px);
}
.sequence-banner.visible { opacity: 1; }

/* ── HIRE/TRAIN MODAL ── */
.hire-modal-backdrop {
    display: none; position: fixed; inset: 0;
    background: rgba(0,0,0,.6); z-index: 1050; backdrop-filter: blur(4px);
}
.hire-modal-backdrop.show { display: flex; align-items: center; justify-content: center; }
.hire-modal {
    background: #fff; border-radius: 16px; width: 460px;
    max-width: 95vw; max-height: 85vh; overflow-y: auto;
    box-shadow: 0 24px 80px rgba(0,0,0,.3); position: relative;
}
.hire-modal-head { padding: 24px 24px 0; display: flex; align-items: center; gap: 14px; }
.hire-modal-avatar {
    width: 56px; height: 56px; border-radius: 14px;
    display: flex; align-items: center; justify-content: center;
    font-size: 1.5rem; color: #fff;
}
.hire-modal-head h3 { font-size: 1.2rem; font-weight: 700; margin: 0; }
.hire-modal-head p { font-size: .85rem; color: #6b7280; margin: 2px 0 0; }
.hire-modal-close {
    position: absolute; top: 16px; right: 16px;
    background: none; border: none; font-size: 1.2rem; color: #9ca3af; cursor: pointer;
}
.hire-modal-body { padding: 20px 24px 24px; }
.hire-modal-desc { font-size: .9rem; color: #4b5563; line-height: 1.55; margin-bottom: 16px; }
.hire-modal-skills { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 20px; }
.hire-modal-skills span {
    font-size: .72rem; background: #f3f4f6; color: #6b7280;
    padding: 4px 10px; border-radius: 6px; font-weight: 500;
}
.hire-step { display: none; }
.hire-step.active { display: block; }
.train-textarea {
    width: 100%; min-height: 100px; border: 1px solid #e5e7eb;
    border-radius: 10px; padding: 12px; font-size: .88rem;
    resize: vertical; font-family: inherit;
}
.train-textarea:focus { outline: none; border-color: #6366f1; box-shadow: 0 0 0 3px rgba(99,102,241,.1); }
.train-label { font-size: .82rem; font-weight: 600; color: #374151; margin-bottom: 8px; display: block; }
.train-hint { font-size: .78rem; color: #9ca3af; margin-top: 6px; }
.train-q-list { display: flex; flex-direction: column; gap: 8px; }
.train-q-item {
    display: flex; align-items: flex-start; gap: 10px;
    font-size: .84rem; color: #374151; line-height: 1.4;
    background: #f8fafc; border-radius: 8px; padding: 8px 12px;
    border-left: 3px solid #6366f1;
}
.train-q-num {
    flex-shrink: 0; width: 22px; height: 22px; border-radius: 50%;
    background: #6366f1; color: #fff; font-size: .72rem; font-weight: 700;
    display: flex; align-items: center; justify-content: center;
}
.hire-btn {
    display: inline-flex; align-items: center; gap: 8px;
    padding: 10px 24px; border-radius: 10px; font-size: .9rem;
    font-weight: 600; border: none; cursor: pointer; transition: all .15s;
}
.hire-btn-primary { background: linear-gradient(135deg, #7c3aed, #4f46e5); color: #fff; }
.hire-btn-primary:hover { opacity: .9; }
.hire-btn-secondary { background: #f3f4f6; color: #374151; }
.hire-btn-secondary:hover { background: #e5e7eb; }
.hire-btn:disabled { opacity: .5; cursor: not-allowed; }
.hire-step-actions { display: flex; gap: 10px; margin-top: 20px; }
.train-progress { height: 6px; background: #e5e7eb; border-radius: 3px; overflow: hidden; margin-top: 16px; display: none; }
.train-progress.active { display: block; }
.train-progress-bar {
    height: 100%; background: linear-gradient(90deg, #7c3aed, #4f46e5);
    border-radius: 3px; width: 0; transition: width 1.5s ease-out;
}
.train-status { font-size: .82rem; color: #6b7280; margin-top: 8px; display: none; }
.train-status.active { display: block; }

/* ── AGENT DETAIL CARDS ── */
.agent-grid {
    display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
    gap: 20px; margin-bottom: 32px;
}
.agent-card {
    background: #fff; border: 1px solid #e5e7eb; border-radius: 14px;
    padding: 24px; transition: box-shadow 0.2s, border-color 0.2s; position: relative; cursor: pointer;
}
.agent-card:hover { box-shadow: 0 4px 20px rgba(0,0,0,0.08); border-color: #c7d2fe; }
.agent-card.working { border-color: #7c3aed; box-shadow: 0 0 0 1px #7c3aed20, 0 4px 16px rgba(124,58,237,0.12); }
.agent-card.not-hired { opacity: .55; filter: grayscale(.5); }
.agent-card.not-hired:hover { opacity: .8; filter: grayscale(.2); }
.agent-header { display: flex; align-items: center; gap: 14px; margin-bottom: 14px; }
.agent-avatar {
    width: 52px; height: 52px; border-radius: 14px;
    display: flex; align-items: center; justify-content: center;
    font-size: 1.5rem; color: #fff; flex-shrink: 0; position: relative;
}
.agent-avatar .pulse-ring {
    position: absolute; inset: -4px; border-radius: 18px;
    border: 2px solid currentColor; opacity: 0; animation: none;
}
.agent-card.working .agent-avatar .pulse-ring { animation: agentCardPulse 2s ease-in-out infinite; opacity: 1; }
@keyframes agentCardPulse {
    0% { transform: scale(1); opacity: 0.6; }
    50% { transform: scale(1.15); opacity: 0; }
    100% { transform: scale(1); opacity: 0; }
}
.agent-name { font-size: 1.1rem; font-weight: 700; color: #1f2937; margin: 0; }
.agent-role { font-size: 0.8rem; color: #6b7280; margin: 0; }
.agent-status {
    display: inline-flex; align-items: center; gap: 6px;
    font-size: 0.75rem; font-weight: 600; padding: 3px 10px;
    border-radius: 20px; text-transform: uppercase; letter-spacing: 0.3px;
}
.status-dot { width: 7px; height: 7px; border-radius: 50%; }
.status-working .status-dot { background: #7c3aed; }
.status-working { background: #ede9fe; color: #7c3aed; }
.status-idle .status-dot { background: #10b981; }
.status-idle { background: #d1fae5; color: #059669; }
.status-standby .status-dot { background: #f59e0b; }
.status-standby { background: #fef3c7; color: #d97706; }
.status-available .status-dot { background: #9ca3af; }
.status-available { background: #f3f4f6; color: #9ca3af; }
.agent-desc { font-size: 0.88rem; color: #4b5563; line-height: 1.5; margin-bottom: 14px; }
.agent-current { background: #f9fafb; border-radius: 10px; padding: 12px 14px; margin-bottom: 14px; min-height: 52px; }
.agent-current-label { font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.5px; color: #9ca3af; margin-bottom: 4px; font-weight: 600; }
.agent-current-text { font-size: 0.85rem; color: #374151; }
.agent-current-text.typing::after {
    content: ''; display: inline-block; width: 3px; height: 14px;
    background: #7c3aed; margin-left: 4px;
    animation: blink 1s step-end infinite; vertical-align: middle;
}
@keyframes blink { 50% { opacity: 0; } }
.agent-skills { display: flex; flex-wrap: wrap; gap: 6px; }
.agent-skill { font-size: 0.72rem; background: #f3f4f6; color: #6b7280; padding: 3px 8px; border-radius: 6px; font-weight: 500; }
.agent-hire-btn {
    display: inline-flex; align-items: center; gap: 6px;
    background: linear-gradient(135deg, #f59e0b, #d97706);
    color: #fff; border: none; padding: 6px 14px; border-radius: 8px;
    font-size: .78rem; font-weight: 600; cursor: pointer; transition: opacity .15s; margin-top: 12px;
}
.agent-hire-btn:hover { opacity: .9; }

/* ── FINDINGS ── */
.findings-section { margin-bottom: 32px; }
.findings-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; }
.findings-header h4 { font-size: 1rem; font-weight: 700; color: #1f2937; margin: 0; }
.findings-filters { display: flex; gap: 8px; }
.filter-pill {
    font-size: 0.75rem; font-weight: 600; padding: 4px 12px; border-radius: 20px;
    border: 1px solid #e5e7eb; background: #fff; color: #6b7280; cursor: pointer; transition: all 0.15s;
}
.filter-pill:hover { border-color: #c7d2fe; color: #4f46e5; }
.filter-pill.active { background: #4f46e5; color: #fff; border-color: #4f46e5; }
.finding-card {
    background: #fff; border: 1px solid #e5e7eb; border-radius: 12px;
    padding: 18px 20px; margin-bottom: 12px; display: flex; gap: 14px;
    align-items: flex-start; transition: border-color 0.15s;
}
.finding-card:hover { border-color: #c7d2fe; }
.finding-card.sev-critical { border-left: 4px solid #ef4444; }
.finding-card.sev-warning { border-left: 4px solid #f59e0b; }
.finding-card.sev-positive { border-left: 4px solid #10b981; }
.finding-card.sev-info { border-left: 4px solid #3b82f6; }
.finding-sev-icon {
    width: 32px; height: 32px; border-radius: 8px;
    display: flex; align-items: center; justify-content: center;
    font-size: 0.9rem; flex-shrink: 0;
}
.sev-critical .finding-sev-icon { background: #fef2f2; color: #ef4444; }
.sev-warning .finding-sev-icon { background: #fffbeb; color: #f59e0b; }
.sev-positive .finding-sev-icon { background: #ecfdf5; color: #10b981; }
.sev-info .finding-sev-icon { background: #eff6ff; color: #3b82f6; }
.finding-body { flex: 1; min-width: 0; }
.finding-title { font-size: 0.9rem; font-weight: 600; color: #1f2937; margin-bottom: 4px; }
.finding-agent { font-size: 0.72rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.4px; margin-bottom: 4px; }
.finding-detail { font-size: 0.85rem; color: #4b5563; line-height: 1.5; margin-bottom: 6px; }
.finding-action { font-size: 0.82rem; color: #7c3aed; font-weight: 500; }
.finding-action i { margin-right: 4px; }
.finding-steps { list-style: none; padding: 0; margin: 6px 0 0 0; counter-reset: step-counter; }
.finding-steps li { font-size: 0.8rem; color: #374151; padding: 3px 0 3px 24px; position: relative; line-height: 1.4; }
.finding-steps li::before {
    counter-increment: step-counter; content: counter(step-counter);
    position: absolute; left: 0; top: 3px; width: 18px; height: 18px;
    background: #ede9fe; color: #7c3aed; border-radius: 50%;
    font-size: 0.65rem; font-weight: 700; display: flex; align-items: center; justify-content: center;
}
.finding-assign-btn {
    background: none; border: 1px solid #c7d2fe; color: #4f46e5;
    font-size: 0.72rem; font-weight: 600; padding: 3px 10px;
    border-radius: 8px; cursor: pointer; white-space: nowrap;
}
.finding-assign-btn:hover { background: #4f46e5; color: #fff; }
.finding-dismiss { background: none; border: none; color: #d1d5db; cursor: pointer; font-size: 1rem; padding: 4px; flex-shrink: 0; }
.finding-dismiss:hover { color: #9ca3af; }
.findings-empty { text-align: center; padding: 40px; color: #9ca3af; background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; }
.findings-empty i { font-size: 2rem; display: block; margin-bottom: 8px; }
.finding-count-badge {
    position: absolute; top: 10px; right: 10px; background: #ef4444; color: #fff;
    font-size: 0.65rem; font-weight: 700; min-width: 20px; height: 20px; border-radius: 10px;
    display: flex; align-items: center; justify-content: center; padding: 0 5px;
}
.finding-count-badge.count-0 { display: none; }
.finding-count-badge.sev-warning { background: #f59e0b; }
.finding-count-badge.sev-positive { background: #10b981; }

/* ── ACTIVITY ── */
.activity-section { margin-top: 8px; }
.activity-section h4 { font-size: 1rem; font-weight: 700; color: #1f2937; margin-bottom: 16px; }
.activity-feed { background: #fff; border: 1px solid #e5e7eb; border-radius: 14px; overflow: hidden; }
.activity-item {
    display: flex; align-items: flex-start; gap: 12px; padding: 14px 18px;
    border-bottom: 1px solid #f3f4f6; transition: background 0.15s;
}
.activity-item:last-child { border-bottom: none; }
.activity-item:hover { background: #fafbfc; }
.activity-icon {
    width: 32px; height: 32px; border-radius: 8px;
    display: flex; align-items: center; justify-content: center;
    font-size: 0.85rem; color: #fff; flex-shrink: 0; margin-top: 2px;
}
.activity-body { flex: 1; min-width: 0; }
.activity-agent { font-weight: 600; color: #374151; font-size: 0.85rem; }
.activity-action { color: #6b7280; font-size: 0.85rem; }
.activity-detail {
    font-size: 0.82rem; color: #9ca3af; margin-top: 2px;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.activity-time { font-size: 0.72rem; color: #d1d5db; white-space: nowrap; margin-top: 3px; }
.empty-feed { padding: 40px; text-align: center; color: #9ca3af; }
.empty-feed i { font-size: 2rem; display: block; margin-bottom: 8px; }

/* ── RESPONSIVE ── */
@media (max-width: 768px) {
    .team-wrap { padding: 18px 16px 0; }
    .office-sprite { width: 50px; height: 50px; }
    .agent-grid { grid-template-columns: 1fr; }
    .team-stats { gap: 14px; }
    .team-header { flex-direction: column; gap: 10px; }
    .findings-header { flex-direction: column; gap: 10px; align-items: flex-start; }
}
@media (max-width: 480px) {
    .office-sprite { width: 40px; height: 40px; }
}
</style>

<!-- ── OFFICE SCENE ── -->
<div class="team-wrap">
    <div class="team-header">
        <div>
            <h2><i class="bi bi-building me-2"></i>{{ brand_name }} HQ</h2>
            <p>Your AI marketing team, hard at work.</p>
        </div>
        <div class="team-stats">
            <div class="team-stat"><div class="ts-val" id="stat-hired">0</div><div class="ts-lbl">Hired</div></div>
            <div class="team-stat"><div class="ts-val" id="stat-active">0</div><div class="ts-lbl">Working</div></div>
            <div class="team-stat"><div class="ts-val" id="stat-findings">-</div><div class="ts-lbl">Findings</div></div>
        </div>
    </div>
    <div class="team-hero-actions">
        <button class="run-team-btn" id="run-team-btn" onclick="runTeam()">
            <i class="bi bi-play-circle"></i> Run My Team
        </button>
        <button class="warren-brief-toggle" id="warren-brief-toggle" onclick="toggleWarrenBrief()">
            <img src="/static/sprites/agents/warren.png" alt="Warren"> Brief Warren
        </button>
        <span id="run-status" style="margin-left:8px;font-size:0.82rem;color:rgba(255,255,255,.8);"></span>
    </div>
    <div class="warren-brief-panel" id="warren-brief-panel">
        <textarea class="warren-textarea" id="warren-instructions" placeholder="Give Warren specific focus areas for this run, e.g. 'Focus on reducing CPA. Check if competitors changed their ads. Look for new keyword gaps.'"></textarea>
        <div class="warren-brief-hint">Optional - Warren will share this with the team before they get to work.</div>
    </div>
    <div class="office-floor" id="office-floor">
        <div class="sequence-banner" id="sequence-banner"></div>
    </div>
</div>

<!-- ── HIRE/TRAIN MODAL ── -->
<div class="hire-modal-backdrop" id="hire-modal-backdrop">
    <div class="hire-modal">
        <button class="hire-modal-close" onclick="closeHireModal()"><i class="bi bi-x-lg"></i></button>
        <div class="hire-modal-head" id="hire-modal-head"></div>
        <div class="hire-modal-body">
            <div class="hire-step active" id="hire-step-1">
                <div class="hire-modal-desc" id="hire-modal-desc"></div>
                <div class="hire-modal-skills" id="hire-modal-skills"></div>
                <div class="hire-step-actions">
                    <button class="hire-btn hire-btn-primary" id="hire-btn-confirm" onclick="confirmHire()">
                        <i class="bi bi-person-plus"></i> Hire This Agent
                    </button>
                    <button class="hire-btn hire-btn-secondary" onclick="closeHireModal()">Not Now</button>
                </div>
            </div>
            <div class="hire-step" id="hire-step-2">
                <p style="font-size:.88rem;color:#4b5563;margin-bottom:16px;">
                    <span id="train-guidance">Tell this agent about your business so they can do better work.</span>
                </p>
                <div id="train-questions" style="margin-bottom:14px;"></div>
                <label class="train-label" id="train-label">Training Instructions (optional)</label>
                <textarea class="train-textarea" id="train-textarea" placeholder=""></textarea>
                <div class="train-hint">You can always update this later in Settings.</div>
                <div class="train-progress" id="train-progress">
                    <div class="train-progress-bar" id="train-progress-bar"></div>
                </div>
                <div class="train-status" id="train-status"></div>
                <div class="hire-step-actions">
                    <button class="hire-btn hire-btn-primary" id="train-btn-confirm" onclick="confirmTrain()">
                        <i class="bi bi-check-lg"></i> Complete Training
                    </button>
                    <button class="hire-btn hire-btn-secondary" onclick="skipTrain()">Skip for Now</button>
                </div>
            </div>
            <div class="hire-step" id="hire-step-3">
                <div style="text-align:center;padding:20px 0;">
                    <div style="font-size:3rem;margin-bottom:12px;">&#127881;</div>
                    <h4 style="font-weight:700;margin-bottom:8px;" id="hire-done-title">Agent Hired!</h4>
                    <p style="font-size:.88rem;color:#6b7280;" id="hire-done-msg">They're ready to work.</p>
                    <button class="hire-btn hire-btn-primary" onclick="closeHireModal()" style="margin-top:16px;">
                        <i class="bi bi-arrow-right"></i> Got It
                    </button>
                </div>
            </div>
        </div>
    </div>
</div>

<!-- ── AGENT DETAIL CARDS ── -->
<div class="agent-grid" id="agent-grid"></div>

<!-- ── FINDINGS ── -->
<div class="findings-section" id="findings-section">
    <div class="findings-header">
        <h4><i class="bi bi-exclamation-diamond me-2"></i>Agent Findings</h4>
        <div class="findings-filters" id="findings-filters">
            <span class="filter-pill active" data-filter="all">All</span>
            <span class="filter-pill" data-filter="critical">Critical</span>
            <span class="filter-pill" data-filter="warning">Warning</span>
            <span class="filter-pill" data-filter="positive">Positive</span>
            <span class="filter-pill" data-filter="info">Info</span>
        </div>
    </div>
    <div id="findings-list">
        <div class="findings-empty"><i class="bi bi-robot"></i>No findings yet. Hire agents and hit "Run My Team" to start.</div>
    </div>
</div>

<!-- ── ACTIVITY ── -->
<div class="activity-section">
    <h4><i class="bi bi-activity me-2"></i>Recent Activity</h4>
    <div class="activity-feed" id="activity-feed">
        <div class="empty-feed"><i class="bi bi-clock-history"></i>Loading activity...</div>
    </div>
</div>

<script>
// ══════════════════════════════════════
//  DATA
// ══════════════════════════════════════
const AGENTS = {{ agents_json | safe }};
let hiredAgents = {{ hired_agents_json | safe }};
var csrfToken = document.querySelector('meta[name="csrf-token"]').getAttribute('content');

const AGENT_COLORS = {
    warren:'#7c3aed', scout:'#2563eb', penny:'#059669', ace:'#e11d48',
    radar:'#f59e0b', hawk:'#6366f1', pulse:'#0891b2', spark:'#d946ef',
    bridge:'#ea580c', chief:'#0f172a'
};
const AGENT_ICONS = {
    warren:'bi-stars', scout:'bi-graph-up-arrow', penny:'bi-piggy-bank',
    ace:'bi-pencil-square', radar:'bi-shield-check', hawk:'bi-binoculars',
    pulse:'bi-bar-chart-line', spark:'bi-lightbulb', bridge:'bi-diagram-3',
    chief:'bi-clipboard-check'
};
const SEV_ICONS = {
    critical:'bi-exclamation-triangle-fill', warning:'bi-exclamation-circle-fill',
    positive:'bi-check-circle-fill', info:'bi-info-circle-fill'
};

// Desk positions (% of office floor) for each agent
const DESK_POSITIONS = {
    warren:  {x:74, y:12}, scout: {x:18, y:52}, penny: {x:30, y:52},
    ace:     {x:42, y:52}, radar: {x:7,  y:68}, hawk:  {x:20, y:68},
    pulse:   {x:32, y:68}, spark: {x:46, y:20}, bridge:{x:60, y:44},
    chief:   {x:78, y:64}
};

// Where agents gather around the conference table
const GATHER_SPOTS = {
    warren: {x:46, y:36},
    scout:  {x:38, y:40}, penny: {x:54, y:40},
    ace:    {x:36, y:46}, radar: {x:56, y:46},
    hawk:   {x:38, y:52}, pulse: {x:54, y:52},
    spark:  {x:42, y:56}, bridge:{x:50, y:56},
    chief:  {x:46, y:60}
};

// Walk bounds
const BOUNDS = { minX: 5, maxX: 92, minY: 10, maxY: 82 };

// Equipment decoration positions
const EQUIP_POSITIONS = [
    {key:'plant1',      x:8,  y:12, w:40, h:50},
    {key:'plant2',      x:55, y:7,  w:40, h:50},
    {key:'plant3',      x:85, y:22, w:40, h:50},
    {key:'watercooler', x:92, y:44, w:40, h:55},
];
'''

# TRAIN_GUIDANCE is very long, split into PART2
PART2 = r'''
const TRAIN_GUIDANCE = {
    scout: {
        intro: "Scout analyzes your paid ad performance. Help them understand your ad strategy so they can spot what's working and what's wasting money.",
        questions: [
            "What platforms are you running ads on? (Google, Meta, both?)",
            "What's your most important campaign or service to advertise?",
            "What does a good lead cost for your business? Any CPA targets?",
            "Are there services or areas you do NOT want advertised?",
        ],
        placeholder: "e.g. We run Google Ads and Meta. Our bread and butter is emergency plumbing, but we're trying to grow our repipe business. A good lead costs us under $45. Don't run ads for drain cleaning, margins are too thin.",
    },
    penny: {
        intro: "Penny watches your ad spend like a hawk. Tell her about your budget goals so she can flag waste and find savings.",
        questions: [
            "What's your monthly ad budget across all platforms?",
            "Any budget split between platforms? (e.g. 70% Google, 30% Meta)",
            "What's your target cost per lead or cost per acquisition?",
            "Are there months where you scale up or pull back spending?",
        ],
        placeholder: "e.g. We spend about $3,000/mo total. Roughly $2,000 on Google, $1,000 on Meta. Target CPA is $40. We ramp up in spring and cut back Dec-Jan.",
    },
    ace: {
        intro: "Ace reviews your ad creative - headlines, descriptions, images. Tell them what resonates with your customers so they can sharpen your messaging.",
        questions: [
            "What tone works best for your customers? (professional, friendly, urgent?)",
            "What offers or hooks have worked well in the past?",
            "Any phrases, slogans, or selling points you always want included?",
            "What should your ads NEVER say?",
        ],
        placeholder: "e.g. Our customers respond to trust and speed. 'Same-day service' and 'Licensed & insured since 2003' always perform well. Never use the word 'cheap'. Our main hook is the free inspection offer.",
    },
    radar: {
        intro: "Radar monitors your online reputation - reviews, ratings, and local presence. Help them understand your review landscape.",
        questions: [
            "What's your current Google star rating, roughly?",
            "Do you actively ask customers for reviews? How?",
            "Any recurring complaints or themes in negative reviews?",
            "Are there other review sites that matter for your industry? (Yelp, BBB, Angi?)",
        ],
        placeholder: "e.g. We're at 4.6 stars with about 180 reviews. We text customers a review link after each job. Biggest complaint is scheduling delays during busy season. Yelp matters for us too.",
    },
    hawk: {
        intro: "Hawk keeps tabs on your competitors. Tell them who you're up against so they can track the right businesses.",
        questions: [
            "Who are your top 2-3 competitors in your area?",
            "What are they doing well that concerns you?",
            "Where do you think you beat them?",
            "Any competitors running aggressive ads or undercutting on price?",
        ],
        placeholder: "e.g. Main competitors are Roto-Rooter, Jones Plumbing, and AZ Pipe Pros. Roto-Rooter dominates on Google Ads spend. Jones has more reviews than us. We beat them all on response time and warranty coverage.",
    },
    pulse: {
        intro: "Pulse tracks your organic search rankings, website traffic, and SEO performance. Help them understand your online presence.",
        questions: [
            "What search terms do you most want to rank for?",
            "Do you have a blog or publish content regularly?",
            "What pages on your site generate the most leads?",
            "Any SEO work done previously? What worked or didn't?",
        ],
        placeholder: "e.g. We want to rank for 'emergency plumber Phoenix' and 'repipe specialist Scottsdale'. We have a blog but haven't posted in months. Our water heater page gets the most traffic. Had an SEO agency before but they just built spammy backlinks.",
    },
    spark: {
        intro: "Spark creates content for your brand - blog posts, social media, email. Give them your voice so everything sounds like you.",
        questions: [
            "How would you describe your brand's personality in 3 words?",
            "What topics does your audience care about most?",
            "Any content formats that work well? (how-to posts, before/after, tips?)",
            "What should Spark never write about or never say?",
        ],
        placeholder: "e.g. Our brand is honest, helpful, no-BS. Customers love our 'is it worth repairing?' posts. Before/after photos get great engagement on social. Never use scare tactics or make people feel dumb for asking questions.",
    },
    bridge: {
        intro: "Bridge connects your marketing to your sales pipeline. Help them understand how leads flow through your business.",
        questions: [
            "How do leads typically contact you? (phone, form, chat?)",
            "Do you use a CRM? Which one?",
            "What's your average close rate from lead to paying customer?",
            "What's your average job value or service ticket?",
        ],
        placeholder: "e.g. Most leads call us directly. We use ServiceTitan as our CRM. Close rate is about 60% for standard calls, 80% for emergencies. Average ticket is $350 for repairs, $8,000 for repipes.",
    },
    chief: {
        intro: "Chief is your QA manager - they review every finding before you see it. Tell them your standards so they filter out the noise.",
        questions: [
            "What kind of recommendations do you find most valuable?",
            "Anything you're tired of hearing from marketing tools? (generic advice, obvious stuff?)",
            "How specific do you need recommendations to be before they're useful?",
            "Any topics that are off the table or not relevant to your business?",
        ],
        placeholder: "e.g. I only want to see stuff I can actually act on this week. Tired of 'optimize your landing page' without specifics. If it's not backed by real data from MY account, don't show it. Don't bother me with social media stuff, we don't do that.",
    },
    warren: {
        intro: "Warren is your senior strategist and team lead. Help him understand your big-picture business goals.",
        questions: [
            "What's the #1 thing you want marketing to achieve this quarter?",
            "Are you trying to grow, maintain, or shift your business focus?",
            "What does success look like for you in 90 days?",
            "Anything important happening in your business soon? (new service, expansion, slow season?)",
        ],
        placeholder: "e.g. Main goal is hitting 50 leads/month by June. We're expanding into Scottsdale and need to build presence there. Success in 90 days means a full pipeline and booked out 2 weeks ahead. Launching a new water treatment service in April.",
    },
};

// ══════════════════════════════════════
//  ANIMATION STATE
// ══════════════════════════════════════
let animatedAgents = [];
let isInSequence = false;
let chatCooldown = {};
let allFindings = [];
let currentFilter = 'all';
let currentHireAgent = null;

// ══════════════════════════════════════
//  OFFICE INIT
// ══════════════════════════════════════
function initOffice() {
    const floor = document.getElementById('office-floor');

    // Equipment decorations
    EQUIP_POSITIONS.forEach(eq => {
        const img = document.createElement('img');
        img.className = 'office-equip';
        img.src = '/static/sprites/equipment/' + eq.key + '.png';
        img.style.left = eq.x + '%';
        img.style.top = eq.y + '%';
        img.style.width = eq.w + 'px';
        img.style.height = eq.h + 'px';
        floor.appendChild(img);
    });

    // Create agent sprites
    AGENTS.forEach(agent => {
        const isHired = !!hiredAgents[agent.key];
        const desk = DESK_POSITIONS[agent.key] || {x:50, y:50};

        const el = document.createElement('div');
        el.className = 'office-sprite';
        el.dataset.agent = agent.key;
        el.onclick = function() { openHireModal(agent.key); };

        const img = document.createElement('img');
        img.src = '/static/sprites/agents/' + agent.key + '.png';
        img.alt = agent.name;
        el.appendChild(img);

        // Name label
        const label = document.createElement('div');
        label.className = 'sprite-label';
        label.textContent = agent.name;
        el.appendChild(label);

        // Chat dots (hidden by default)
        const dots = document.createElement('div');
        dots.className = 'chat-dots';
        dots.innerHTML = '<span>.</span><span>.</span><span>.</span>';
        el.appendChild(dots);

        // Speech bubble (hidden by default)
        const speech = document.createElement('div');
        speech.className = 'speech-bubble';
        el.appendChild(speech);

        if (!isHired) {
            el.classList.add('sprite-unhired');
            el.style.left = desk.x + '%';
            el.style.top = desk.y + '%';

            const badge = document.createElement('div');
            badge.className = 'sprite-hire-badge';
            badge.textContent = 'HIRE';
            el.appendChild(badge);
        } else {
            // Random starting position for idle wandering
            const sx = BOUNDS.minX + Math.random() * (BOUNDS.maxX - BOUNDS.minX);
            const sy = BOUNDS.minY + Math.random() * (BOUNDS.maxY - BOUNDS.minY);
            el.style.left = sx + '%';
            el.style.top = sy + '%';

            // Status dot
            const sdot = document.createElement('div');
            sdot.className = 'sprite-status-dot ssd-idle';
            el.appendChild(sdot);
        }

        floor.appendChild(el);

        const agentObj = {
            key: agent.key,
            name: agent.name,
            el: el,
            x: parseFloat(el.style.left),
            y: parseFloat(el.style.top),
            hired: isHired,
            state: 'idle',
            deskX: desk.x,
            deskY: desk.y,
            moveTimeout: null,
        };
        animatedAgents.push(agentObj);

        // Set z-index based on y position
        el.style.zIndex = Math.floor(agentObj.y) + 10;
    });

    // Start idle behavior for hired agents
    animatedAgents.filter(a => a.hired).forEach(a => {
        const delay = 500 + Math.random() * 2000;
        setTimeout(() => scheduleNextMove(a), delay);
    });

    // Update Warren brief toggle visibility
    updateWarrenToggle();

    // Update hired stat
    document.getElementById('stat-hired').textContent =
        Object.keys(hiredAgents).length + '/' + AGENTS.length;
}

function updateWarrenToggle() {
    const toggle = document.getElementById('warren-brief-toggle');
    toggle.style.display = hiredAgents.warren ? 'inline-flex' : 'none';
}

function toggleWarrenBrief() {
    document.getElementById('warren-brief-panel').classList.toggle('open');
}

// ══════════════════════════════════════
//  WALKING + IDLE
// ══════════════════════════════════════
function walkTo(agent, x, y, onArrive) {
    const dx = x - agent.x;
    const dy = y - agent.y;
    const dist = Math.hypot(dx, dy);
    const duration = Math.max(1.2, dist * 0.04);

    const el = agent.el;
    el.style.transition = 'left ' + duration + 's ease-in-out, top ' + duration + 's ease-in-out';
    el.style.left = x + '%';
    el.style.top = y + '%';

    // Face direction
    const img = el.querySelector('img');
    if (dx > 2) img.style.transform = 'scaleX(1)';
    else if (dx < -2) img.style.transform = 'scaleX(-1)';

    // Walk bob
    el.classList.add('sprite-walking');
    el.classList.remove('sprite-working');

    agent.x = x;
    agent.y = y;
    agent.state = 'walking';

    // Z-index follows y position (depth sorting)
    el.style.zIndex = Math.floor(y) + 10;

    // Clear any existing movement timeout
    clearTimeout(agent.moveTimeout);

    agent.moveTimeout = setTimeout(function() {
        el.classList.remove('sprite-walking');
        agent.state = 'idle';
        if (onArrive) onArrive();
    }, duration * 1000);
}

function scheduleNextMove(agent) {
    if (isInSequence || !agent.hired) return;

    const delay = 3000 + Math.random() * 5000;
    agent.moveTimeout = setTimeout(function() {
        if (isInSequence || agent.state === 'working') return;

        // 25% chance to chat with nearby agent
        if (Math.random() < 0.25 && tryChat(agent)) return;

        // Walk to random spot
        const x = BOUNDS.minX + Math.random() * (BOUNDS.maxX - BOUNDS.minX);
        const y = BOUNDS.minY + Math.random() * (BOUNDS.maxY - BOUNDS.minY);

        walkTo(agent, x, y, function() {
            scheduleNextMove(agent);
        });
    }, delay);
}

function tryChat(agent) {
    if (chatCooldown[agent.key]) return false;

    const candidates = animatedAgents.filter(function(a) {
        return a.key !== agent.key && a.hired && a.state === 'idle'
            && !chatCooldown[a.key] && Math.hypot(a.x - agent.x, a.y - agent.y) < 22;
    });
    if (candidates.length === 0) return false;

    const partner = candidates[Math.floor(Math.random() * candidates.length)];
    const midX = (agent.x + partner.x) / 2;
    const midY = (agent.y + partner.y) / 2;

    // Walk toward each other
    walkTo(agent, midX - 3, midY);
    walkTo(partner, midX + 3, midY);

    // After arriving, face each other and show chat dots
    const arriveTime = Math.max(1200, Math.hypot(midX - agent.x, midY - agent.y) * 40);
    setTimeout(function() {
        if (isInSequence) return;

        // Face each other
        const aImg = agent.el.querySelector('img');
        const pImg = partner.el.querySelector('img');
        aImg.style.transform = partner.x > agent.x ? 'scaleX(1)' : 'scaleX(-1)';
        pImg.style.transform = agent.x > partner.x ? 'scaleX(1)' : 'scaleX(-1)';

        showChatDots(agent, 3500);
        showChatDots(partner, 3500);
    }, arriveTime + 300);

    // Set cooldown
    chatCooldown[agent.key] = true;
    chatCooldown[partner.key] = true;
    setTimeout(function() { delete chatCooldown[agent.key]; }, 10000);
    setTimeout(function() { delete chatCooldown[partner.key]; }, 10000);

    // Schedule next moves after chat
    setTimeout(function() {
        scheduleNextMove(agent);
        scheduleNextMove(partner);
    }, arriveTime + 4500);

    return true;
}

function showChatDots(agent, duration) {
    const dots = agent.el.querySelector('.chat-dots');
    if (!dots) return;
    dots.style.display = 'block';
    setTimeout(function() { dots.style.display = 'none'; }, duration);
}

function showSpeech(agent, text, duration, isBriefing) {
    const bubble = agent.el.querySelector('.speech-bubble');
    if (!bubble) return;
    bubble.textContent = text;
    bubble.className = 'speech-bubble' + (isBriefing ? ' briefing' : '') + ' visible';
    setTimeout(function() {
        bubble.classList.remove('visible');
    }, duration);
}

function showBanner(text, duration) {
    const banner = document.getElementById('sequence-banner');
    banner.textContent = text;
    banner.classList.add('visible');
    if (duration) {
        setTimeout(function() { banner.classList.remove('visible'); }, duration);
    }
}

function hideBanner() {
    document.getElementById('sequence-banner').classList.remove('visible');
}

function stopAllIdle() {
    animatedAgents.forEach(function(a) {
        clearTimeout(a.moveTimeout);
        a.el.classList.remove('sprite-walking');
        a.el.querySelector('.chat-dots').style.display = 'none';
    });
}

function setWorking(agent) {
    agent.state = 'working';
    agent.el.classList.remove('sprite-walking');
    agent.el.classList.add('sprite-working');

    // Add work indicator if not present
    if (!agent.el.querySelector('.work-indicator')) {
        const ind = document.createElement('div');
        ind.className = 'work-indicator';
        ind.innerHTML = '<i class="bi bi-gear-fill"></i>';
        agent.el.appendChild(ind);
    }

    // Update status dot
    const sdot = agent.el.querySelector('.sprite-status-dot');
    if (sdot) { sdot.className = 'sprite-status-dot ssd-working'; }
}

function clearWorking(agent) {
    agent.state = 'idle';
    agent.el.classList.remove('sprite-working');
    const ind = agent.el.querySelector('.work-indicator');
    if (ind) ind.remove();
    const sdot = agent.el.querySelector('.sprite-status-dot');
    if (sdot) { sdot.className = 'sprite-status-dot ssd-idle'; }
}

function resumeIdle() {
    isInSequence = false;
    animatedAgents.filter(function(a) { return a.hired && a.state !== 'working'; }).forEach(function(a) {
        clearWorking(a);
        const delay = 500 + Math.random() * 2000;
        setTimeout(function() { scheduleNextMove(a); }, delay);
    });
}

// ══════════════════════════════════════
//  GATHERING SEQUENCE
// ══════════════════════════════════════
function startGatheringSequence(instructions) {
    isInSequence = true;
    stopAllIdle();

    const hired = animatedAgents.filter(function(a) { return a.hired; });
    const warren = animatedAgents.find(function(a) { return a.key === 'warren' && a.hired; });
    const others = hired.filter(function(a) { return a.key !== 'warren'; });

    if (!warren) {
        // No Warren hired - just send everyone to desks
        showBanner('Sending team to their stations...', 3000);
        hired.forEach(function(a) {
            walkTo(a, a.deskX, a.deskY, function() { setWorking(a); });
        });
        setTimeout(function() { fireRunApi(); }, 3000);
        return;
    }

    // Step 1: Warren walks to table head
    var ws = GATHER_SPOTS.warren;
    walkTo(warren, ws.x, ws.y);

    // Step 2: Warren calls everyone over
    setTimeout(function() {
        showSpeech(warren, 'Team, huddle up!', 2500);
    }, 1200);

    // Step 3: Everyone walks to their gather spot
    setTimeout(function() {
        others.forEach(function(a) {
            var spot = GATHER_SPOTS[a.key] || {x: 46 + Math.random()*8 - 4, y: 50 + Math.random()*8 - 4};
            walkTo(a, spot.x, spot.y);
        });
    }, 2000);

    // Step 4: All gathered - Warren briefs
    setTimeout(function() {
        var msg = instructions
            ? (instructions.length > 90 ? instructions.substring(0, 90) + '...' : instructions)
            : "Alright, let's find opportunities and fix what's broken.";
        showSpeech(warren, msg, 4000, true);
    }, 5500);

    // Step 5: Warren says go
    setTimeout(function() {
        showSpeech(warren, "Let's get to work!", 2000);
    }, 10000);

    // Step 6: Everyone disperses to desks
    setTimeout(function() {
        showBanner('Team is at their desks...', 2500);
        hired.forEach(function(a) {
            walkTo(a, a.deskX, a.deskY, function() { setWorking(a); });
        });
    }, 12000);

    // Step 7: Fire API call
    setTimeout(function() {
        fireRunApi();
    }, 14000);
}

// ══════════════════════════════════════
//  HIRE MODAL
// ══════════════════════════════════════
function openHireModal(agentKey) {
    const agent = AGENTS.find(function(a) { return a.key === agentKey; });
    if (!agent) return;
    currentHireAgent = agentKey;

    const color = AGENT_COLORS[agentKey] || '#6b7280';
    const icon = AGENT_ICONS[agentKey] || 'bi-robot';
    const isHired = !!hiredAgents[agentKey];
    const isTrained = isHired && hiredAgents[agentKey].trained;

    document.getElementById('hire-modal-head').innerHTML =
        '<div class="hire-modal-avatar" style="background:' + color + '"><i class="bi ' + icon + '"></i></div>' +
        '<div><h3>' + agent.name + '</h3><p>' + agent.role + '</p></div>';

    document.getElementById('hire-modal-desc').textContent = agent.description;
    document.getElementById('hire-modal-skills').innerHTML =
        agent.skills.map(function(s) { return '<span>' + s + '</span>'; }).join('');

    document.querySelectorAll('.hire-step').forEach(function(s) { s.classList.remove('active'); });

    if (!isHired) {
        document.getElementById('hire-step-1').classList.add('active');
        document.getElementById('hire-btn-confirm').disabled = false;
        document.getElementById('hire-btn-confirm').innerHTML = '<i class="bi bi-person-plus"></i> Hire This Agent';
    } else if (!isTrained) {
        document.getElementById('hire-step-2').classList.add('active');
        document.getElementById('train-textarea').value = '';
        populateTrainStep(agentKey, agent.name);
    } else {
        document.getElementById('hire-step-3').classList.add('active');
        document.getElementById('hire-done-title').textContent = agent.name + ' is on your team';
        document.getElementById('hire-done-msg').textContent = 'Trained and ready. Click Run My Team to put them to work.';
    }

    document.getElementById('hire-modal-backdrop').classList.add('show');
}

function closeHireModal() {
    document.getElementById('hire-modal-backdrop').classList.remove('show');
    currentHireAgent = null;
}

function populateTrainStep(agentKey, agentName) {
    var g = TRAIN_GUIDANCE[agentKey];
    var guidanceEl = document.getElementById('train-guidance');
    var questionsEl = document.getElementById('train-questions');
    var textarea = document.getElementById('train-textarea');
    var label = document.getElementById('train-label');

    if (g) {
        guidanceEl.textContent = g.intro;
        label.textContent = 'Your answers for ' + agentName;
        textarea.placeholder = g.placeholder;
        questionsEl.innerHTML = '<div class="train-q-list">' +
            g.questions.map(function(q, i) {
                return '<div class="train-q-item"><span class="train-q-num">' + (i+1) + '</span>' + q + '</div>';
            }).join('') + '</div>';
    } else {
        guidanceEl.textContent = 'Tell ' + agentName + ' about your business.';
        label.textContent = 'Training Instructions (optional)';
        textarea.placeholder = '';
        questionsEl.innerHTML = '';
    }
}

function confirmHire() {
    if (!currentHireAgent) return;
    var btn = document.getElementById('hire-btn-confirm');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Hiring...';

    fetch('{{ url_for("client.client_team_hire") }}', {
        method: 'POST',
        headers: {'Content-Type':'application/json', 'X-CSRFToken': csrfToken},
        body: JSON.stringify({agent_key: currentHireAgent}),
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.success) {
            hiredAgents = data.hired_agents;
            // Update the sprite in the office
            var agent = animatedAgents.find(function(a) { return a.key === currentHireAgent; });
            if (agent) {
                agent.hired = true;
                agent.el.classList.remove('sprite-unhired');
                var badge = agent.el.querySelector('.sprite-hire-badge');
                if (badge) badge.remove();

                // Add status dot
                if (!agent.el.querySelector('.sprite-status-dot')) {
                    var sdot = document.createElement('div');
                    sdot.className = 'sprite-status-dot ssd-idle';
                    agent.el.appendChild(sdot);
                }

                // Start wandering from current position
                var delay = 500 + Math.random() * 1000;
                setTimeout(function() { scheduleNextMove(agent); }, delay);
            }
            updateWarrenToggle();
            document.getElementById('stat-hired').textContent =
                Object.keys(hiredAgents).length + '/' + AGENTS.length;

            // Move to training step
            document.querySelectorAll('.hire-step').forEach(function(s) { s.classList.remove('active'); });
            document.getElementById('hire-step-2').classList.add('active');
            document.getElementById('train-textarea').value = '';
            var hiredAgent = AGENTS.find(function(a) { return a.key === currentHireAgent; });
            populateTrainStep(currentHireAgent, hiredAgent ? hiredAgent.name : currentHireAgent);
        } else {
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-person-plus"></i> Hire This Agent';
        }
    })
    .catch(function() {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-person-plus"></i> Hire This Agent';
    });
}

function confirmTrain() {
    if (!currentHireAgent) return;
    var btn = document.getElementById('train-btn-confirm');
    var notes = document.getElementById('train-textarea').value.trim();
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Training...';

    var progress = document.getElementById('train-progress');
    var bar = document.getElementById('train-progress-bar');
    var statusEl = document.getElementById('train-status');
    progress.classList.add('active');
    statusEl.classList.add('active');

    var phases = [
        'Reading your business profile...',
        'Learning your industry...',
        'Calibrating analysis style...',
        'Finalizing training...',
    ];
    var phase = 0;
    statusEl.textContent = phases[0];
    bar.style.width = '10%';

    var phaseInterval = setInterval(function() {
        phase++;
        if (phase < phases.length) {
            statusEl.textContent = phases[phase];
            bar.style.width = (25 * (phase + 1)) + '%';
        }
    }, 600);

    fetch('{{ url_for("client.client_team_train") }}', {
        method: 'POST',
        headers: {'Content-Type':'application/json', 'X-CSRFToken': csrfToken},
        body: JSON.stringify({agent_key: currentHireAgent, training_notes: notes}),
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        clearInterval(phaseInterval);
        bar.style.width = '100%';
        statusEl.textContent = 'Training complete!';

        if (data.success) {
            hiredAgents = data.hired_agents;
            setTimeout(function() {
                progress.classList.remove('active');
                statusEl.classList.remove('active');
                document.querySelectorAll('.hire-step').forEach(function(s) { s.classList.remove('active'); });
                document.getElementById('hire-step-3').classList.add('active');
                var agent = AGENTS.find(function(a) { return a.key === currentHireAgent; });
                document.getElementById('hire-done-title').textContent = (agent ? agent.name : 'Agent') + ' is ready!';
                document.getElementById('hire-done-msg').textContent = 'They just joined the team. Run your team to put them to work.';
            }, 800);
        }
    })
    .catch(function() {
        clearInterval(phaseInterval);
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-check-lg"></i> Complete Training';
        progress.classList.remove('active');
        statusEl.classList.remove('active');
    });
}

function skipTrain() {
    document.getElementById('train-textarea').value = '';
    confirmTrain();
}

// ══════════════════════════════════════
//  AGENT DETAIL CARDS
// ══════════════════════════════════════
function renderAgentCard(agent, findingCounts) {
    var color = AGENT_COLORS[agent.key] || '#6b7280';
    var icon  = AGENT_ICONS[agent.key]  || 'bi-robot';
    var isHired = !!hiredAgents[agent.key];
    var isTrained = isHired && hiredAgents[agent.key].trained;
    var latest = agent.latest;
    var isWorking = latest && latest.status === 'in_progress';

    var statusClass, statusLabel;
    if (!isHired) { statusClass = 'available'; statusLabel = 'Available'; }
    else if (isWorking) { statusClass = 'working'; statusLabel = 'Working'; }
    else if (latest) { statusClass = 'idle'; statusLabel = 'Idle'; }
    else { statusClass = 'standby'; statusLabel = 'Standby'; }

    var currentText = !isHired
        ? 'Not hired yet'
        : (isWorking ? latest.action : (latest ? 'Last: ' + latest.action : 'Awaiting first task'));

    var counts = findingCounts[agent.key] || {critical:0, warning:0, positive:0, info:0, total:0};
    var badgeHtml = '';
    if (counts.critical > 0) badgeHtml = '<span class="finding-count-badge">' + counts.critical + '</span>';
    else if (counts.warning > 0) badgeHtml = '<span class="finding-count-badge sev-warning">' + counts.warning + '</span>';
    else if (counts.positive > 0) badgeHtml = '<span class="finding-count-badge sev-positive">' + counts.positive + '</span>';

    var cardClass = isWorking ? 'working' : (!isHired ? 'not-hired' : '');

    return '<div class="agent-card ' + cardClass + '" data-agent="' + agent.key + '" onclick="openHireModal(\'' + agent.key + '\')">' +
        badgeHtml +
        '<div class="agent-header">' +
            '<div class="agent-avatar" style="background:' + color + '">' +
                '<div class="pulse-ring" style="color:' + color + '"></div>' +
                '<i class="bi ' + icon + '"></i>' +
            '</div>' +
            '<div><p class="agent-name">' + agent.name + '</p><p class="agent-role">' + agent.role + '</p></div>' +
            '<div class="ms-auto"><span class="agent-status status-' + statusClass + '"><span class="status-dot"></span>' + statusLabel + '</span></div>' +
        '</div>' +
        '<div class="agent-desc">' + agent.description + '</div>' +
        '<div class="agent-current">' +
            '<div class="agent-current-label">' + (isWorking ? 'Currently doing' : 'Status') + '</div>' +
            '<div class="agent-current-text ' + (isWorking ? 'typing' : '') + '">' + currentText + '</div>' +
        '</div>' +
        '<div class="agent-skills">' + agent.skills.map(function(s) { return '<span class="agent-skill">' + s + '</span>'; }).join('') + '</div>' +
        (!isHired ? '<button class="agent-hire-btn" onclick="event.stopPropagation();openHireModal(\'' + agent.key + '\')"><i class="bi bi-person-plus"></i> Hire</button>' : '') +
    '</div>';
}

// ══════════════════════════════════════
//  FINDINGS
// ══════════════════════════════════════
function parseFindingExtra(f) {
    if (!f.extra_json) return {};
    if (typeof f.extra_json === 'object') return f.extra_json;
    try { return JSON.parse(f.extra_json); } catch(e) { return {}; }
}

function escapeHtml(text) {
    if (!text) return '';
    var div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function renderFinding(f) {
    var agentColor = AGENT_COLORS[f.agent_key] || '#6b7280';
    var agentName = (AGENTS.find(function(a) { return a.key === f.agent_key; }) || {}).name || f.agent_key;
    var sevIcon = SEV_ICONS[f.severity] || 'bi-info-circle';
    var extra = parseFindingExtra(f);
    var steps = Array.isArray(extra.steps) ? extra.steps : [];
    var stepsHtml = '';
    if (steps.length) {
        stepsHtml = '<ol class="finding-steps">' + steps.map(function(s) { return '<li>' + escapeHtml(s) + '</li>'; }).join('') + '</ol>';
    }
    return '<div class="finding-card sev-' + f.severity + '" data-severity="' + f.severity + '" data-agent="' + f.agent_key + '" data-id="' + f.id + '">' +
        '<div class="finding-sev-icon"><i class="bi ' + sevIcon + '"></i></div>' +
        '<div class="finding-body">' +
            '<div class="finding-agent" style="color:' + agentColor + '">' + agentName + '</div>' +
            '<div class="finding-title">' + escapeHtml(f.title) + '</div>' +
            '<div class="finding-detail">' + escapeHtml(f.detail) + '</div>' +
            (f.action ? '<div class="finding-action"><i class="bi bi-arrow-right-circle"></i>' + escapeHtml(f.action) + '</div>' : '') +
            stepsHtml +
        '</div>' +
        '<button class="finding-assign-btn" onclick="event.stopPropagation();createTaskFromFinding(' + f.id + ')" title="Create Task"><i class="bi bi-clipboard-plus"></i> Task</button>' +
        '<button class="finding-dismiss" onclick="event.stopPropagation();dismissFinding(' + f.id + ', this)" title="Dismiss"><i class="bi bi-x-lg"></i></button>' +
    '</div>';
}

function renderFindings(findings) {
    var list = document.getElementById('findings-list');
    var filtered = currentFilter === 'all' ? findings : findings.filter(function(f) { return f.severity === currentFilter; });
    if (!filtered.length) {
        var msg = findings.length > 0 ? 'No ' + currentFilter + ' findings.' : 'No findings yet. Hire agents and hit "Run My Team" to start.';
        list.innerHTML = '<div class="findings-empty"><i class="bi bi-robot"></i>' + msg + '</div>';
        return;
    }
    list.innerHTML = filtered.map(function(f) { return renderFinding(f); }).join('');
}

function buildFindingCounts(findings) {
    var counts = {};
    for (var i = 0; i < findings.length; i++) {
        var f = findings[i];
        if (!counts[f.agent_key]) counts[f.agent_key] = {critical:0, warning:0, positive:0, info:0, total:0};
        counts[f.agent_key][f.severity] = (counts[f.agent_key][f.severity] || 0) + 1;
        counts[f.agent_key].total++;
    }
    return counts;
}

function dismissFinding(id, btn) {
    var card = btn.closest('.finding-card');
    card.style.opacity = '0.4';
    fetch('{{ url_for("client.client_team_findings") }}'.replace('/findings', '/findings/' + id + '/dismiss'), {
        method: 'POST',
        headers: {'Content-Type':'application/json', 'X-CSRFToken': csrfToken},
    })
    .then(function(r) { return r.json(); })
    .then(function() {
        card.remove();
        allFindings = allFindings.filter(function(f) { return f.id !== id; });
        updateFindingsStat();
    })
    .catch(function() { card.style.opacity = '1'; });
}

function updateFindingsStat() {
    var critical = allFindings.filter(function(f) { return f.severity === 'critical'; }).length;
    var warning = allFindings.filter(function(f) { return f.severity === 'warning'; }).length;
    var el = document.getElementById('stat-findings');
    if (critical > 0) { el.textContent = critical; el.style.color = '#fca5a5'; }
    else if (warning > 0) { el.textContent = warning; el.style.color = '#fcd34d'; }
    else { el.textContent = allFindings.length; el.style.color = ''; }
}

function createTaskFromFinding(findingId) {
    var finding = allFindings.find(function(f) { return f.id === findingId; });
    var title = finding ? finding.title : 'Task from finding';
    var extra = finding ? parseFindingExtra(finding) : {};
    var steps = Array.isArray(extra.steps) ? extra.steps.map(function(s) { return {text: s}; }) : [];
    fetch('/client/tasks/from-finding', {
        method: 'POST',
        headers: {'Content-Type':'application/json', 'X-CSRFToken': csrfToken},
        body: JSON.stringify({finding_id: findingId, title: title, steps: steps}),
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.success) { alert('Task created! Go to Tasks to assign it.'); }
        else { alert(data.error || 'Failed to create task'); }
    })
    .catch(function() { alert('Failed to create task'); });
}

function loadFindings() {
    fetch('{{ url_for("client.client_team_findings") }}')
        .then(function(r) { return r.json(); })
        .then(function(data) {
            allFindings = data.findings || [];
            renderFindings(allFindings);
            updateFindingsStat();
        })
        .catch(function(err) { console.error('Findings load error:', err); });
}

// Filter pills
document.getElementById('findings-filters').addEventListener('click', function(e) {
    var pill = e.target.closest('.filter-pill');
    if (!pill) return;
    document.querySelectorAll('.filter-pill').forEach(function(p) { p.classList.remove('active'); });
    pill.classList.add('active');
    currentFilter = pill.dataset.filter;
    renderFindings(allFindings);
});

// ══════════════════════════════════════
//  RUN TEAM
// ══════════════════════════════════════
function runTeam() {
    var hiredCount = Object.keys(hiredAgents).length;
    if (hiredCount === 0) {
        document.getElementById('run-status').textContent = 'Hire at least one agent first!';
        setTimeout(function() { document.getElementById('run-status').textContent = ''; }, 4000);
        return;
    }

    var btn = document.getElementById('run-team-btn');
    var status = document.getElementById('run-status');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Running...';
    status.textContent = 'Assembling the team...';

    // Get Warren instructions
    var instructions = '';
    var instrField = document.getElementById('warren-instructions');
    if (instrField) instructions = instrField.value.trim();

    // Close the brief panel
    document.getElementById('warren-brief-panel').classList.remove('open');

    // Start the cinematic gathering sequence
    startGatheringSequence(instructions);
}

function fireRunApi() {
    var status = document.getElementById('run-status');
    status.textContent = 'Agents are working...';

    var body = {};
    var instrField = document.getElementById('warren-instructions');
    if (instrField && instrField.value.trim()) {
        body.instructions = instrField.value.trim();
    }

    fetch('{{ url_for("client.client_team_run") }}', {
        method: 'POST',
        headers: {'Content-Type':'application/json', 'X-CSRFToken': csrfToken},
        body: JSON.stringify(body),
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
        if (data.status === 'running' || (data.success && data.status === 'running')) {
            pollRunStatus();
        } else if (data.success === false) {
            var btn = document.getElementById('run-team-btn');
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-play-circle"></i> Run My Team';
            status.textContent = data.error || 'Something went wrong.';
            resumeIdle();
        }
    })
    .catch(function() {
        var btn = document.getElementById('run-team-btn');
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-play-circle"></i> Run My Team';
        status.textContent = 'Failed to start. Try again.';
        resumeIdle();
    });
}

function pollRunStatus() {
    var btn = document.getElementById('run-team-btn');
    var status = document.getElementById('run-status');
    var pollCount = 0;
    var maxPolls = 120;
    var phases = [
        'Agents are analyzing your data',
        'Chief is running quality checks',
        'Warren is reviewing the team output',
        'Finishing up',
    ];

    var interval = setInterval(function() {
        pollCount++;
        var phase = phases[Math.min(Math.floor(pollCount / 8), phases.length - 1)];
        var dots = '.'.repeat((pollCount % 3) + 1);
        status.textContent = phase + dots;

        fetch('{{ url_for("client.client_team_run_status") }}')
            .then(function(r) { return r.json(); })
            .then(function(data) {
                if (data.status === 'running') return;
                clearInterval(interval);
                btn.disabled = false;
                btn.innerHTML = '<i class="bi bi-play-circle"></i> Run My Team';

                if (data.status === 'done' && data.success) {
                    var qa = data.qa || {};
                    var statusMsg = 'Done! ' + data.total_findings + ' findings from ' + data.agents_ran.length + ' agents.';
                    if (qa.overall_grade && qa.overall_grade !== 'N/A') {
                        statusMsg += ' Grade: ' + qa.overall_grade + '.';
                        if (qa.killed > 0) statusMsg += ' ' + qa.killed + ' killed.';
                        if (qa.reworked > 0) statusMsg += ' ' + qa.reworked + ' reworked.';
                        statusMsg += ' ' + (qa.shipped || 0) + ' shipped.';
                    }
                    status.textContent = statusMsg;
                    hideBanner();
                    loadFindings();
                    loadTeamData();

                    // Celebration then back to idle
                    setTimeout(function() { resumeIdle(); }, 3000);
                } else {
                    status.textContent = data.error || 'Run completed with errors.';
                    resumeIdle();
                }
                setTimeout(function() { status.textContent = ''; }, 12000);
            })
            .catch(function() {});

        if (pollCount >= maxPolls) {
            clearInterval(interval);
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-play-circle"></i> Run My Team';
            status.textContent = 'Taking longer than expected. Check back in a minute.';
            resumeIdle();
            setTimeout(function() { status.textContent = ''; }, 8000);
        }
    }, 2000);
}

// ══════════════════════════════════════
//  ACTIVITY + LOAD
// ══════════════════════════════════════
function timeAgo(dateStr) {
    if (!dateStr) return '';
    var now = new Date();
    var then = new Date(dateStr + 'Z');
    var diff = Math.floor((now - then) / 1000);
    if (diff < 60) return 'just now';
    if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
    if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
    return Math.floor(diff / 86400) + 'd ago';
}

function renderActivity(items) {
    if (!items || items.length === 0) {
        return '<div class="empty-feed"><i class="bi bi-check-circle"></i>No activity yet. Run your team to start.</div>';
    }
    return items.map(function(item) {
        var agent = AGENTS.find(function(a) { return a.key === item.agent_key; }) || {};
        var color = AGENT_COLORS[item.agent_key] || '#6b7280';
        var icon  = AGENT_ICONS[item.agent_key]  || 'bi-robot';
        return '<div class="activity-item">' +
            '<div class="activity-icon" style="background:' + color + '"><i class="bi ' + icon + '"></i></div>' +
            '<div class="activity-body">' +
                '<span class="activity-agent">' + (agent.name || item.agent_key) + '</span> ' +
                '<span class="activity-action">' + item.action + '</span>' +
                (item.detail ? '<div class="activity-detail" title="' + escapeHtml(item.detail) + '">' + escapeHtml(item.detail) + '</div>' : '') +
            '</div>' +
            '<div class="activity-time">' + timeAgo(item.created_at) + '</div>' +
        '</div>';
    }).join('');
}

function loadTeamData() {
    var findingCounts = buildFindingCounts(allFindings);
    fetch('{{ url_for("client.client_team_data") }}')
        .then(function(r) { return r.json(); })
        .then(function(data) {
            if (data.hired_agents) hiredAgents = data.hired_agents;

            var agents = data.agents || AGENTS;
            var latestMap = {};
            agents.forEach(function(a) {
                if (data.latest && data.latest[a.key]) {
                    a.latest = data.latest[a.key];
                    latestMap[a.key] = data.latest[a.key];
                }
            });

            // Update animated agents based on API data
            agents.forEach(function(a) {
                var animated = animatedAgents.find(function(aa) { return aa.key === a.key; });
                if (!animated) return;

                var wasHired = animated.hired;
                animated.hired = !!hiredAgents[a.key];

                // Newly hired agent
                if (!wasHired && animated.hired) {
                    animated.el.classList.remove('sprite-unhired');
                    var badge = animated.el.querySelector('.sprite-hire-badge');
                    if (badge) badge.remove();
                    if (!animated.el.querySelector('.sprite-status-dot')) {
                        var sdot = document.createElement('div');
                        sdot.className = 'sprite-status-dot ssd-idle';
                        animated.el.appendChild(sdot);
                    }
                    scheduleNextMove(animated);
                }

                // Agent is working (from API or run status)
                if (a.latest && a.latest.status === 'in_progress' && animated.state !== 'working' && !isInSequence) {
                    clearTimeout(animated.moveTimeout);
                    walkTo(animated, animated.deskX, animated.deskY, function() { setWorking(animated); });
                }
            });

            updateWarrenToggle();
            document.getElementById('stat-hired').textContent =
                Object.keys(hiredAgents).length + '/' + AGENTS.length;

            // Render agent detail cards
            document.getElementById('agent-grid').innerHTML =
                agents.map(function(a) { return renderAgentCard(a, findingCounts); }).join('');

            var working = agents.filter(function(a) { return a.latest && a.latest.status === 'in_progress'; }).length;
            document.getElementById('stat-active').textContent = working;

            document.getElementById('activity-feed').innerHTML = renderActivity(data.activity || []);
        })
        .catch(function(err) {
            console.error('Team data error:', err);
            document.getElementById('agent-grid').innerHTML =
                AGENTS.map(function(a) { return renderAgentCard(a, {}); }).join('');
            document.getElementById('stat-active').textContent = '0';
            document.getElementById('activity-feed').innerHTML =
                '<div class="empty-feed"><i class="bi bi-check-circle"></i>No activity yet.</div>';
        });
}

// Close modal on backdrop click
document.getElementById('hire-modal-backdrop').addEventListener('click', function(e) {
    if (e.target === this) closeHireModal();
});

// ══════════════════════════════════════
//  INIT
// ══════════════════════════════════════
initOffice();
loadTeamData();
loadFindings();
setInterval(loadTeamData, 30000);
</script>
{% endblock %}
'''

with open(PATH, "w", encoding="utf-8") as f:
    f.write(PART1 + PART2)

lines = (PART1 + PART2).count('\n') + 1
print(f"Done. Wrote {lines} lines to {PATH}")
