# app.py
import os, glob, threading, datetime as dt, traceback, io, sys
from flask import Flask, render_template, jsonify, request, send_from_directory

# make sure relative paths in scanner.py resolve to this folder
os.chdir(os.path.dirname(os.path.abspath(__file__)))

import scanner   # your notebook code, as a module

app = Flask(__name__)

# ── in-memory job store (fine for personal use) ─────────────────────
JOBS: dict = {}           # job_id -> {status, started, finished, error, report}


def _newest_report():
    files = sorted(
        glob.glob(f"{scanner.BANGLA_DIR}/Bangla_All_In_One_*.html"),
        key=os.path.getmtime, reverse=True
    )
    return os.path.basename(files[0]) if files else None


def run_scan_job(job_id, equity, ab_mode):
    JOBS[job_id]["status"] = "running"
    # capture stdout so we can show progress (optional)
    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        scanner.canvas_batch_scan(account_equity=equity, ab_mode=ab_mode)
        JOBS[job_id]["status"]   = "done"
        JOBS[job_id]["report"]   = _newest_report()
    except Exception as e:
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["error"]  = str(e)
        traceback.print_exc(file=old)
    finally:
        sys.stdout = old
        JOBS[job_id]["log"]      = buf.getvalue()
        JOBS[job_id]["finished"] = dt.datetime.now().isoformat()


# ── ROUTES ──────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/scan", methods=["POST"])
def scan():
    equity  = float(request.form.get("equity", 1000000) or 1000000)
    ab_mode = request.form.get("ab_mode", "LONG").upper()
    job_id  = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    JOBS[job_id] = {
        "status":   "queued",
        "started":  dt.datetime.now().isoformat(),
        "equity":   equity,
        "ab_mode":  ab_mode,
    }
    threading.Thread(target=run_scan_job,
                     args=(job_id, equity, ab_mode),
                     daemon=True).start()
    return jsonify({"job_id": job_id})


@app.route("/status/<job_id>")
def status(job_id):
    return jsonify(JOBS.get(job_id, {"status": "unknown"}))


@app.route("/reports")
def list_reports():
    files = sorted(
        glob.glob(f"{scanner.BANGLA_DIR}/Bangla_All_In_One_*.html"),
        key=os.path.getmtime, reverse=True
    )
    return jsonify([os.path.basename(f) for f in files])


@app.route("/reports/<path:filename>")
def serve_report(filename):
    return send_from_directory(scanner.BANGLA_DIR, filename)


@app.route("/csv/<path:filename>")
def serve_csv(filename):
    return send_from_directory(scanner.RESULTS_DIR, filename, as_attachment=True)


if __name__ == "__main__":
    # host 0.0.0.0 so you can also open it from your phone on same Wi-Fi
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
