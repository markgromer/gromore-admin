"""
Email sender - sends report HTML to contacts via SMTP.
"""
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path


def send_report_email(app_config, brand, report, recipients):
    """
    Send the client report HTML as an email to all recipients.

    app_config: Flask app.config dict with SMTP_* keys
    brand: brand dict from DB
    report: report dict from DB
    recipients: list of contact dicts with 'name' and 'email'
    """
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    from_name = app_config.get("SMTP_FROM_NAME", "Agency Reports")
    from_email = app_config.get("SMTP_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured. Set SMTP_USER and SMTP_PASSWORD environment variables.")

    # Read the client report HTML
    client_path = report.get("client_path", "")
    if not client_path or not Path(client_path).exists():
        raise FileNotFoundError(f"Client report not found: {client_path}")

    with open(client_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    month = report.get("month", "")
    subject = f"{brand['display_name']} - Monthly Performance Report - {month}"

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        for contact in recipients:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"{from_name} <{from_email}>"
            msg["To"] = contact["email"]

            # Plain text fallback
            text_part = MIMEText(
                f"Hi {contact['name']},\n\n"
                f"Your monthly performance report for {brand['display_name']} ({month}) is ready.\n"
                f"Please view this email in an HTML-capable client to see the full report.\n\n"
                f"Best regards,\n{from_name}",
                "plain",
            )

            html_part = MIMEText(html_content, "html")

            msg.attach(text_part)
            msg.attach(html_part)

            server.sendmail(from_email, contact["email"], msg.as_string())


def send_beta_welcome_email(app_config, tester, onboarding_url):
    """
    Send welcome email to an approved beta tester with a link to complete
    their onboarding (provide Facebook Page ID + Google business email).
    """
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    from_name = app_config.get("SMTP_FROM_NAME", "GroMore")
    from_email = app_config.get("SMTP_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured.")

    name = tester.get("name", "there")
    email = tester["email"]
    subject = "Welcome to the GroMore Beta - Next Steps"

    html = f"""
    <div style="font-family:Inter,Arial,sans-serif;max-width:600px;margin:0 auto;padding:24px;">
        <h2 style="color:#4f46e5;">Welcome to GroMore, {name}!</h2>
        <p>You've been approved for the GroMore beta program. Before we can activate your account, we need a few things from you.</p>

        <div style="text-align:center;margin:28px 0;">
            <a href="{onboarding_url}" style="display:inline-block;padding:14px 36px;background:linear-gradient(135deg,#4f46e5,#4338ca);color:#fff;border-radius:10px;text-decoration:none;font-weight:600;font-size:1rem;">Complete Your Setup</a>
        </div>

        <div style="background:#f0f0ff;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;">What we need from you:</h3>
            <ol style="line-height:2;">
                <li><strong>Your Meta/Facebook login email</strong> - the email you use to log into Facebook (so we can add you as a tester on our app)</li>
                <li><strong>Your Google account email</strong> - the Gmail or Google Workspace email you'll use to sign in (so we can add you as a test user)</li>
                <li><strong>Your Facebook Page ID</strong> - the numeric ID for your business page</li>
                <li><strong>Create a free Facebook Developer account</strong> at <a href="https://developers.facebook.com" style="color:#4f46e5;">developers.facebook.com</a></li>
            </ol>
        </div>

        <div style="background:#fff7ed;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;">How to find your Facebook Page ID</h3>
            <ol style="line-height:1.8;font-size:.9rem;">
                <li>Go to your Facebook Business Page</li>
                <li>Click <strong>About</strong> (on the left sidebar)</li>
                <li>Scroll down to <strong>Page transparency</strong></li>
                <li>Your Page ID is the numeric number listed there</li>
            </ol>
            <p style="font-size:.85em;color:#666;">Example: 109876543210987</p>
        </div>

        <div style="background:#fdf2f8;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;">Facebook Developer Account (Free)</h3>
            <p>You'll need a free Facebook Developer account so we can connect to your ad data. This takes about 2 minutes:</p>
            <ol style="line-height:1.8;font-size:.9rem;">
                <li>Visit <a href="https://developers.facebook.com" style="color:#4f46e5;">developers.facebook.com</a></li>
                <li>Click <strong>Get Started</strong> and log in with your Facebook account</li>
                <li>Accept the terms and complete the registration</li>
            </ol>
            <p style="font-size:.85em;color:#666;">Once you've done this, we'll add your account to our system and send you a login link.</p>
        </div>

        <p>Once you've completed the setup form and we've added your accounts to our backend, we'll send you another email with your login credentials.</p>
        <p>Questions? Reply to this email and we'll help you out.</p>
        <p style="color:#666;">- The GroMore Team</p>
    </div>
    """

    text = (
        f"Welcome to GroMore, {name}!\n\n"
        f"You've been approved for the beta program. Complete your setup here:\n"
        f"{onboarding_url}\n\n"
        f"What we need:\n"
        f"1. Your Meta/Facebook login email (so we can add you as a tester on our app)\n"
        f"2. Your Google account email (so we can add you as a test user)\n"
        f"3. Your Facebook Page ID (numeric ID from your business page)\n"
        f"4. Create a free Facebook Developer account at developers.facebook.com\n\n"
        f"Once setup is complete, we'll send your login credentials.\n\n"
        f"- The GroMore Team"
    )

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = email
        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))
        server.sendmail(from_email, email, msg.as_string())


def send_beta_activation_email(app_config, tester, temp_password, login_url):
    """
    Send activation email to a beta tester once their accounts have been
    added to the backend. Includes OAuth login instructions and FB dev acceptance.
    """
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    from_name = app_config.get("SMTP_FROM_NAME", "GroMore")
    from_email = app_config.get("SMTP_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured.")

    name = tester.get("name", "there")
    email = tester["email"]
    subject = "Your GroMore Beta Account Is Active!"

    html = f"""
    <div style="font-family:Inter,Arial,sans-serif;max-width:600px;margin:0 auto;padding:24px;">
        <h2 style="color:#10b981;">You're In, {name}!</h2>
        <p>Your GroMore beta account is now active. We've added you as a tester on both our Facebook and Google apps, so you're all set to sign in.</p>

        <div style="text-align:center;margin:28px 0;">
            <a href="{login_url}" style="display:inline-block;padding:14px 36px;background:linear-gradient(135deg,#10b981,#059669);color:#fff;border-radius:10px;text-decoration:none;font-weight:600;font-size:1rem;">Go to GroMore Login</a>
        </div>

        <div style="background:#fdf2f8;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;"><span style="font-size:1.1em;">&#9312;</span> Accept Facebook Developer Access</h3>
            <p>Before you can sign in with Facebook, you need to accept the tester invitation we sent you:</p>
            <ol style="line-height:1.8;font-size:.9rem;">
                <li>Go to <a href="https://developers.facebook.com/settings/developer/requests/" style="color:#4f46e5;">developers.facebook.com/settings/developer/requests</a></li>
                <li>Log in with the Facebook account tied to <strong>{tester.get('meta_login_email', email)}</strong></li>
                <li>You should see a pending tester invitation from <strong>GroMore</strong></li>
                <li>Click <strong>Accept</strong></li>
            </ol>
            <p style="font-size:.85em;color:#666;">If you don't see the invitation right away, give it a few minutes and refresh the page.</p>
        </div>

        <div style="background:#f0fdf4;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;"><span style="font-size:1.1em;">&#9313;</span> Sign In to GroMore</h3>
            <p>Once you've accepted the Facebook invitation, head to the login page and sign in using one of these methods:</p>
            <ul style="line-height:1.8;font-size:.9rem;">
                <li><strong>Sign in with Google</strong> using your Google account (<strong>{tester.get('google_business_email', email)}</strong>)</li>
                <li><strong>Sign in with Facebook</strong> using the Facebook account you accepted the invitation on</li>
            </ul>
            <p style="font-size:.85em;color:#666;">You can also use email/password: your email is <strong>{email}</strong> and your temporary password is <strong>{temp_password}</strong>. You'll be prompted to change it on first login.</p>
        </div>

        <div style="background:#f0f0ff;border-radius:10px;padding:20px;margin:20px 0;">
            <h3 style="margin-top:0;"><span style="font-size:1.1em;">&#9314;</span> Share Your Feedback</h3>
            <p>As a beta tester, your input directly shapes what we build. Use the <strong>Feedback</strong>
            link in your dashboard sidebar to report bugs, request features, or tell us what's working.</p>
        </div>

        <p>Questions or issues signing in? Reply to this email and we'll help you out.</p>
        <p style="color:#666;">- The GroMore Team</p>
    </div>
    """

    text = (
        f"You're In, {name}!\n\n"
        f"Your GroMore beta account is now active.\n\n"
        f"STEP 1 - Accept Facebook Developer Access:\n"
        f"Go to https://developers.facebook.com/settings/developer/requests/\n"
        f"Log in with your Facebook account ({tester.get('meta_login_email', email)})\n"
        f"Accept the tester invitation from GroMore.\n\n"
        f"STEP 2 - Sign In to GroMore:\n"
        f"Go to {login_url}\n"
        f"Sign in with Google ({tester.get('google_business_email', email)}) or Facebook.\n"
        f"Or use email/password: {email} / {temp_password}\n\n"
        f"STEP 3 - Share Feedback:\n"
        f"Use the Feedback link in your dashboard sidebar.\n\n"
        f"Questions? Reply to this email.\n\n"
        f"- The GroMore Team"
    )

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = email
        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))
        server.sendmail(from_email, email, msg.as_string())


def send_staff_invite_email(app_config, email, name, brand_name, temp_password, role):
    """
    Send a staff invite email with temporary login credentials.
    """
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    from_name = app_config.get("SMTP_FROM_NAME", "GroMore")
    from_email = app_config.get("SMTP_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured.")

    role_label = role.capitalize()
    subject = f"You've been invited to {brand_name} on GroMore"

    html = f"""
    <div style="font-family:Inter,Arial,sans-serif;max-width:520px;margin:0 auto;padding:24px;">
        <h2 style="color:#4f46e5;">Welcome to GroMore</h2>
        <p>Hi {name},</p>
        <p>You've been added as a <strong>{role_label}</strong> for <strong>{brand_name}</strong> on GroMore.</p>
        <p>Here are your temporary login credentials:</p>
        <div style="background:#f8f9fa;border-radius:10px;padding:16px;margin:20px 0;">
            <p style="margin:4px 0;"><strong>Email:</strong> {email}</p>
            <p style="margin:4px 0;"><strong>Temporary Password:</strong> {temp_password}</p>
        </div>
        <p>Please log in and change your password as soon as possible.</p>
        <p style="color:#999;font-size:.8em;margin-top:24px;">- The GroMore Team</p>
    </div>
    """

    text = (
        f"Hi {name},\n\n"
        f"You've been added as a {role_label} for {brand_name} on GroMore.\n\n"
        f"Email: {email}\nTemporary Password: {temp_password}\n\n"
        f"Please log in and change your password as soon as possible.\n\n"
        f"- The GroMore Team"
    )

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = email
        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))
        server.sendmail(from_email, email, msg.as_string())


def send_password_reset_email(app_config, email, name, reset_url):
    """
    Send a password reset link to a client user.
    """
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    from_name = app_config.get("SMTP_FROM_NAME", "GroMore")
    from_email = app_config.get("SMTP_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured.")

    subject = "Reset your GroMore password"

    html = f"""
    <div style="font-family:Inter,Arial,sans-serif;max-width:520px;margin:0 auto;padding:24px;">
        <h2 style="color:#4f46e5;">Password Reset</h2>
        <p>Hi {name or 'there'},</p>
        <p>We received a request to reset your GroMore password. Click the button below to choose a new one:</p>
        <div style="text-align:center;margin:28px 0;">
            <a href="{reset_url}" style="display:inline-block;padding:12px 32px;background:#4f46e5;color:#fff;border-radius:10px;text-decoration:none;font-weight:600;">Reset Password</a>
        </div>
        <p style="font-size:.85em;color:#666;">This link expires in 1 hour. If you didn't request this, you can safely ignore this email.</p>
        <p style="color:#999;font-size:.8em;margin-top:24px;">- The GroMore Team</p>
    </div>
    """

    text = (
        f"Hi {name or 'there'},\n\n"
        f"Reset your GroMore password by visiting:\n{reset_url}\n\n"
        f"This link expires in 1 hour.\n\n"
        f"- The GroMore Team"
    )

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = email
        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))
        server.sendmail(from_email, email, msg.as_string())


def send_client_login_email(app_config, email, name, temp_password, login_url, brand_name):
    """
    Send (or re-send) client portal login credentials.
    """
    smtp_host = app_config.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = app_config.get("SMTP_PORT", 587)
    smtp_user = app_config.get("SMTP_USER", "")
    smtp_password = app_config.get("SMTP_PASSWORD", "")
    from_name = app_config.get("SMTP_FROM_NAME", "GroMore")
    from_email = app_config.get("SMTP_FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_password:
        raise ValueError("SMTP not configured.")

    subject = f"Your {brand_name} Client Portal Login"

    html = f"""
    <div style="font-family:Inter,Arial,sans-serif;max-width:520px;margin:0 auto;padding:24px;">
        <h2 style="color:#4f46e5;">Your Client Portal Login</h2>
        <p>Hi {name or 'there'},</p>
        <p>Here are your login credentials for the <strong>{brand_name}</strong> client portal:</p>
        <div style="background:#f8f9fa;border-radius:10px;padding:16px;margin:20px 0;">
            <p style="margin:4px 0;"><strong>Email:</strong> {email}</p>
            <p style="margin:4px 0;"><strong>Temporary Password:</strong> {temp_password}</p>
        </div>
        <div style="text-align:center;margin:28px 0;">
            <a href="{login_url}" style="display:inline-block;padding:12px 32px;background:#4f46e5;color:#fff;border-radius:10px;text-decoration:none;font-weight:600;">Sign In</a>
        </div>
        <p style="font-size:.85em;color:#666;">Please change your password after signing in. If you didn't expect this email, you can safely ignore it.</p>
        <p style="color:#999;font-size:.8em;margin-top:24px;">- The GroMore Team</p>
    </div>
    """

    text = (
        f"Hi {name or 'there'},\n\n"
        f"Here are your login credentials for the {brand_name} client portal:\n\n"
        f"Email: {email}\nTemporary Password: {temp_password}\n\n"
        f"Sign in at: {login_url}\n\n"
        f"Please change your password after signing in.\n\n"
        f"- The GroMore Team"
    )

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{from_name} <{from_email}>"
        msg["To"] = email
        msg.attach(MIMEText(text, "plain"))
        msg.attach(MIMEText(html, "html"))
        server.sendmail(from_email, email, msg.as_string())
