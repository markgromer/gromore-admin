"""Spin up local server with assessment test page."""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))

from webapp.app import app
from flask import send_file, Response

@app.route("/client/assess-test")
def assess_test():
    tpl = os.path.join(os.path.dirname(__file__), "webapp", "templates", "client", "assessment_form.html")
    with open(tpl, encoding="utf-8") as f:
        widget = f.read()
    # Point widget at local server instead of Render
    widget = widget.replace("https://gromore-admin.onrender.com/client/assess", "/client/assess")
    page = (
        '<!DOCTYPE html><html><head><title>GroMore Assessment Test</title>'
        '<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">'
        '</head><body class="bg-light">'
        '<div class="container py-5" style="max-width:700px">'
        '<h2 class="mb-4">Free AI Assessment (Local Test)</h2>'
        + widget +
        '</div></body></html>'
    )
    return Response(page, content_type="text/html")

if __name__ == "__main__":
    print("Open http://127.0.0.1:5202/client/assess-test")
    app.run(port=5202, debug=False, use_reloader=False)
