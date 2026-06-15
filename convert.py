#!/usr/bin/env python3

import argparse
import base64
import hashlib
import hmac
import os
import json
import secrets
import time
import tempfile
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

import psycopg2
import requests
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv


load_dotenv()


DB_HOST = os.getenv("INTERCEPT_ADSB_DB_HOST", "localhost")
DB_PORT = int(os.getenv("INTERCEPT_ADSB_DB_PORT", "5432"))
DB_NAME = os.getenv("INTERCEPT_ADSB_DB_NAME", "intercept_adsb")
DB_USER = os.getenv("INTERCEPT_ADSB_DB_USER", "intercept")
DB_PASSWORD = os.getenv("INTERCEPT_ADSB_DB_PASSWORD", "intercept")

OUTPUT_DIR = Path(os.getenv("AIRCRAFT_OUTPUT_DIR", "./adsb_exports"))
FILE_PREFIX = os.getenv("AIRCRAFT_FILE_PREFIX", "aircraft")

REFRESH_SECONDS = float(os.getenv("AIRCRAFT_REFRESH_SECONDS", "60"))
MAX_AGE_SECONDS = int(os.getenv("AIRCRAFT_MAX_AGE_SECONDS", "60"))

WRITE_LATEST = os.getenv("AIRCRAFT_WRITE_LATEST", "true").lower() in {"1", "true", "yes", "on"}
LATEST_FILE = os.getenv("AIRCRAFT_LATEST_FILE", "aircraft.json")

TIMEZONE = ZoneInfo(os.getenv("AIRCRAFT_TIMEZONE", "UTC"))

MAX_CONSECUTIVE_ERRORS = int(os.getenv("AIRCRAFT_MAX_ERRORS", "10"))

WDGWARS_API_KEY = os.getenv("WDGWARS_API_KEY", "")
WDGWARS_UPLOAD_URL = os.getenv("WDGWARS_UPLOAD_URL", "https://wdgwars.pl/api/upload/")

SESSION_MINUTES = int(os.getenv("AIRCRAFT_SESSION_MINUTES", "0"))
UPLOAD_QUEUE_MAX_AGE_HOURS = int(os.getenv("AIRCRAFT_QUEUE_MAX_AGE_HOURS", "24"))
UPLOAD_QUEUE_PATH = OUTPUT_DIR / ".upload_queue.json"

HEALTHCHECKS_URL = os.getenv("HEALTHCHECKS_URL", "")


def db_connect():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def utc_now():
    return datetime.now(timezone.utc)


def log(msg):
    ts = utc_now().astimezone(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} {msg}")


def seconds_since(dt, reference=None):
    if dt is None:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    ref = reference or utc_now()
    return max(0.0, (ref - dt).total_seconds())


def clean_callsign(value):
    if not value:
        return None
    return str(value).strip() or None


def clean_desc(value):
    if not value:
        return None
    s = str(value).strip()
    # postgres text-array literals come through as {"foo",bar,baz} — unwrap to first element
    if len(s) >= 2 and s[0] == "{" and s[-1] == "}":
        inner = s[1:-1]
        if inner.startswith('"'):
            close = inner.find('"', 1)
            return inner[1:close] or None
        return inner.split(",")[0].strip() or None
    return s or None


def session_filepath(started_at, replay=False):
    stamp = started_at.astimezone(TIMEZONE).strftime("%Y%m%d_%H%M%S%Z")
    prefix = f"{FILE_PREFIX}_replay" if replay else FILE_PREFIX
    return OUTPUT_DIR / f"{prefix}_{stamp}.json"


def build_aircraft(row, reference=None):
    snapshot = row.get("snapshot") or {}

    lat = row.get("lat")
    if lat is None:
        lat = snapshot.get("lat")

    lon = row.get("lon")
    if lon is None:
        lon = snapshot.get("lon")

    aircraft = {
        "hex": (row.get("icao") or snapshot.get("icao") or "").lower(),
        "type": "adsb_icao",
    }

    callsign = clean_callsign(row.get("callsign") or snapshot.get("callsign"))
    if callsign:
        aircraft["flight"] = callsign

    registration = row.get("registration") or snapshot.get("registration")
    if registration:
        aircraft["r"] = registration

    type_code = row.get("type_code") or snapshot.get("type_code")
    if type_code:
        aircraft["t"] = type_code

    type_desc = clean_desc(row.get("type_desc") or snapshot.get("type_desc"))
    if type_desc:
        aircraft["desc"] = type_desc

    altitude = row.get("altitude")
    if altitude is None:
        altitude = snapshot.get("altitude")
    if altitude is not None:
        aircraft["alt_baro"] = altitude

    speed = row.get("speed")
    if speed is None:
        speed = snapshot.get("speed")
    if speed is not None:
        aircraft["gs"] = speed

    heading = row.get("heading")
    if heading is None:
        heading = snapshot.get("heading")
    if heading is not None:
        aircraft["track"] = heading

    vertical_rate = row.get("vertical_rate")
    if vertical_rate is None:
        vertical_rate = snapshot.get("vertical_rate")
    if vertical_rate is not None:
        aircraft["baro_rate"] = vertical_rate

    squawk = row.get("squawk") or snapshot.get("squawk")
    if squawk:
        aircraft["squawk"] = str(squawk)

    seen = seconds_since(row.get("captured_at"), reference=reference)
    if seen is not None:
        aircraft["seen"] = round(seen, 1)

    if lat is not None and lon is not None:
        aircraft["lat"] = float(lat)
        aircraft["lon"] = float(lon)
        aircraft["seen_pos"] = round(seen if seen is not None else 0.0, 1)

    return aircraft


def fetch_current_session(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, started_at, ended_at
            FROM adsb_sessions
            WHERE started_at <= NOW()
              AND (ended_at IS NULL OR ended_at > NOW())
            ORDER BY started_at DESC
            LIMIT 1
        """)
        return cur.fetchone()


def fetch_sessions_for_date(conn, day_start, day_end):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, started_at, ended_at
            FROM adsb_sessions
            WHERE started_at < %s
              AND (ended_at IS NULL OR ended_at > %s)
            ORDER BY started_at
        """, (day_end, day_start))
        return cur.fetchall()


def fetch_aircraft(conn):
    query = """
        WITH latest_meta AS (
            SELECT DISTINCT ON (icao)
                icao,
                captured_at,
                callsign,
                registration,
                type_code,
                type_desc,
                altitude,
                speed,
                heading,
                vertical_rate,
                squawk,
                snapshot
            FROM adsb_snapshots
            WHERE captured_at >= NOW() - (%s || ' seconds')::interval
              AND icao IS NOT NULL
            ORDER BY icao, captured_at DESC
        ),
        latest_pos AS (
            SELECT DISTINCT ON (icao)
                icao,
                lat,
                lon
            FROM adsb_snapshots
            WHERE captured_at >= NOW() - (%s || ' seconds')::interval
              AND icao IS NOT NULL
              AND lat IS NOT NULL AND lon IS NOT NULL
            ORDER BY icao, captured_at DESC
        )
        SELECT m.*, p.lat, p.lon
        FROM latest_meta m
        LEFT JOIN latest_pos p USING (icao)
        ORDER BY m.captured_at DESC;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (MAX_AGE_SECONDS, MAX_AGE_SECONDS))
        rows = cur.fetchall()

    return [item for row in rows if (item := build_aircraft(row)) and item.get("hex")]


def fetch_message_count(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM adsb_messages;")
            return int(cur.fetchone()[0])
    except Exception:
        return 0


def fetch_aircraft_in_window(conn, window_start, window_end):
    query = """
        WITH latest_meta AS (
            SELECT DISTINCT ON (icao)
                icao,
                captured_at,
                callsign,
                registration,
                type_code,
                type_desc,
                altitude,
                speed,
                heading,
                vertical_rate,
                squawk,
                snapshot
            FROM adsb_snapshots
            WHERE captured_at >= %s AND captured_at < %s
              AND icao IS NOT NULL
            ORDER BY icao, captured_at DESC
        ),
        latest_pos AS (
            SELECT DISTINCT ON (icao)
                icao,
                lat,
                lon
            FROM adsb_snapshots
            WHERE captured_at >= %s AND captured_at < %s
              AND icao IS NOT NULL
              AND lat IS NOT NULL AND lon IS NOT NULL
            ORDER BY icao, captured_at DESC
        )
        SELECT m.*, p.lat, p.lon
        FROM latest_meta m
        LEFT JOIN latest_pos p USING (icao)
        ORDER BY m.captured_at DESC;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (window_start, window_end, window_start, window_end))
        rows = cur.fetchall()

    return [item for row in rows if (item := build_aircraft(row, reference=window_end)) and item.get("hex")]


def fetch_message_count_in_window(conn, window_start, window_end):
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM adsb_messages WHERE received_at >= %s AND received_at < %s;",
                (window_start, window_end),
            )
            return int(cur.fetchone()[0])
    except Exception:
        return 0


def atomic_write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, separators=(",", ":"))
            f.write("\n")

        os.replace(tmp_path, path)

    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def ping_healthcheck(success=True, message=""):
    if not HEALTHCHECKS_URL:
        return
    url = HEALTHCHECKS_URL if success else f"{HEALTHCHECKS_URL}/fail"
    try:
        requests.post(url, data=message.encode(), timeout=10)
    except Exception:
        pass


def to_upload_record(ac, now=None):
    lat = ac.get("lat")
    lon = ac.get("lon")
    if lat is None or lon is None:
        return None
    seen = ac.get("seen_pos") if ac.get("seen_pos") is not None else ac.get("seen")
    if now is not None and seen is not None:
        first_seen = datetime.fromtimestamp(now - seen, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    else:
        first_seen = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rec = {
        "icao": ac["hex"].upper(),
        "callsign": (ac.get("flight") or "").strip(),
        "lat": float(lat),
        "lon": float(lon),
        "first_seen": first_seen,
        "type": "ADSB",
    }
    if ac.get("alt_baro") is not None:
        rec["alt_ft"] = int(ac["alt_baro"])
    if ac.get("gs") is not None:
        rec["speed_kt"] = int(ac["gs"])
    if ac.get("track") is not None:
        rec["heading"] = int(ac["track"])
    return rec


# HTTP status codes generated by Cloudflare itself when the origin is
# unreachable — these are never returned by the application.
# 520-527: Cloudflare-specific origin-error range
# 530:     Cloudflare tunnel / DNS errors (carries 1xxx body codes like 1033, 1103)
# 503:     Emitted by Cloudflare when the origin is overloaded or erroring
#          before Cloudflare can attribute the fault to the origin
_CF_ORIGIN_DOWN = frozenset(range(520, 528)) | {503, 530}


def _is_cf_error(resp):
    """Return True if the response is a Cloudflare-generated error page.

    Catches tunnel errors (1033, 1103, etc.) that arrive with non-52x status
    codes such as 502 — the CF-RAY header is present on all Cloudflare-proxied
    responses, and a 5xx with that header means Cloudflare, not the origin,
    produced the error.
    """
    return resp.status_code >= 500 and "CF-RAY" in resp.headers


def _endpoint_available():
    """HEAD the upload URL with a short timeout.

    Returns False if the host is unreachable or Cloudflare reports the origin
    is down. Returns True for any other response (including 405 — the endpoint
    may not support HEAD, but a response means the host is up).
    On unexpected exceptions the check is skipped and True is returned so the
    POST can surface the real error.
    """
    try:
        resp = requests.head(WDGWARS_UPLOAD_URL, timeout=5, allow_redirects=True)
        if resp.status_code in _CF_ORIGIN_DOWN or _is_cf_error(resp):
            log(f"Upload skipped: endpoint unavailable (HTTP {resp.status_code})")
            return False
        return True
    except requests.exceptions.ConnectionError:
        log("Upload skipped: endpoint unreachable (connection refused)")
        return False
    except requests.exceptions.Timeout:
        log("Upload skipped: endpoint unreachable (timed out)")
        return False
    except Exception:
        return True


# HMAC envelope derived from gungnir by Zach B.
# https://github.com/HiroAlleyCat/gungnir — MIT License
def _build_envelope(payload_dict, api_key):
    body_json = json.dumps(payload_dict, separators=(",", ":"))
    data_b64 = base64.b64encode(body_json.encode()).decode()
    nonce = secrets.token_hex(8)
    sig = hmac.new(
        api_key.encode(),
        (nonce + data_b64).encode(),
        hashlib.sha256,
    ).hexdigest()
    return {"data": data_b64, "nonce": nonce, "sig": sig}


def upload_file(path):
    """Attempt to upload path to wdgwars.

    Returns True if the upload succeeded or failed for a non-retriable reason
    (bad data, auth rejection — retrying won't help).
    Returns False if the upload failed for a retriable reason (server down,
    CF error, rate limit, network error) — the caller should queue for retry.
    """
    if not WDGWARS_API_KEY:
        return True
    if not _endpoint_available():
        return False
    try:
        with open(path) as f:
            file_data = json.load(f)
        now_ts = file_data.get("now")
        records = [
            r for ac in file_data.get("aircraft", [])
            if (r := to_upload_record(ac, now=now_ts)) is not None
        ]
        if not records:
            log(f"Upload skipped: no GPS-bearing aircraft in {path.name}")
            return True
        payload = {"networks": [], "aircraft": records, "meshcore_nodes": []}
        envelope = _build_envelope(payload, WDGWARS_API_KEY)
        resp = requests.post(
            WDGWARS_UPLOAD_URL,
            headers={
                "Content-Type": "application/json",
                "X-API-Key": WDGWARS_API_KEY,
                "Accept": "application/json",
            },
            data=json.dumps(envelope).encode(),
            timeout=30,
        )
        if resp.status_code in _CF_ORIGIN_DOWN or _is_cf_error(resp):
            log(f"Upload skipped: origin down (HTTP {resp.status_code}): {path.name}")
            return False
        if resp.status_code == 429:
            log(f"Upload rate limited: {path.name}")
            return False
        if not resp.ok:
            msg = f"failed ({resp.status_code}): {path.name} — {resp.text}"
            log(f"Upload {msg}")
            ping_healthcheck(success=False, message=msg)
            return True  # data rejection — retrying won't help
        result = resp.json()
        log(f"Uploaded {path.name}: {result}")
        return True
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        log(f"Upload network error: {path.name}")
        return False
    except Exception as exc:
        msg = f"error uploading {path.name}: {exc}"
        log(f"Upload {msg}")
        ping_healthcheck(success=False, message=msg)
        return True


def _upload_cursor_path(session_id):
    return OUTPUT_DIR / f".upload_cursor_{session_id}.json"


def _load_last_uploaded_at(session_id):
    p = _upload_cursor_path(session_id)
    try:
        if p.exists():
            ts = json.loads(p.read_text()).get("last_uploaded_at")
            if ts:
                dt = datetime.fromisoformat(ts)
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    return None


def _save_last_uploaded_at(session_id, dt):
    try:
        atomic_write_json(_upload_cursor_path(session_id), {"last_uploaded_at": dt.isoformat()})
    except Exception as exc:
        log(f"Cursor save error: {exc}")


def _load_queue():
    try:
        if UPLOAD_QUEUE_PATH.exists():
            return json.loads(UPLOAD_QUEUE_PATH.read_text())
    except Exception:
        pass
    return []


def _save_queue(entries):
    try:
        if entries:
            atomic_write_json(UPLOAD_QUEUE_PATH, entries)
        elif UPLOAD_QUEUE_PATH.exists():
            UPLOAD_QUEUE_PATH.unlink()
    except Exception as exc:
        log(f"Queue save error: {exc}")


def _enqueue(path, started_at, window_end):
    queue = _load_queue()
    queue.append({
        "path": str(path),
        "started_at": started_at.isoformat(),
        "window_end": window_end.isoformat(),
        "queued_at": utc_now().isoformat(),
    })
    _save_queue(queue)
    log(f"Queued for retry: {path.name}")


def drain_queue():
    queue = _load_queue()
    if not queue:
        return
    if not _endpoint_available():
        return
    now = utc_now()
    remaining = []
    failed = False
    for entry in queue:
        queued_at = datetime.fromisoformat(entry["queued_at"])
        if queued_at.tzinfo is None:
            queued_at = queued_at.replace(tzinfo=timezone.utc)
        if (now - queued_at).total_seconds() > UPLOAD_QUEUE_MAX_AGE_HOURS * 3600:
            log(f"Queue entry expired, dropping: {Path(entry['path']).name}")
            continue
        if failed:
            remaining.append(entry)
            continue
        started_at = datetime.fromisoformat(entry["started_at"])
        window_end = datetime.fromisoformat(entry["window_end"])
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        if window_end.tzinfo is None:
            window_end = window_end.replace(tzinfo=timezone.utc)
        log(f"Retrying queued upload: {Path(entry['path']).name}")
        if not upload_window(Path(entry["path"]), started_at, window_end):
            failed = True
            remaining.append(entry)
        else:
            time.sleep(2)
    _save_queue(remaining)


def build_payload():
    with db_connect() as conn:
        session = fetch_current_session(conn)
        aircraft = fetch_aircraft(conn)
        messages = fetch_message_count(conn)

    payload = {
        "now": time.time(),
        "messages": messages,
        "aircraft": aircraft,
    }
    return session, payload


def upload_window(path, started_at, window_end):
    """Query [started_at, window_end), write path, and upload.

    Returns True on success or non-retriable failure, False on retriable failure.
    """
    with db_connect() as conn:
        aircraft = fetch_aircraft_in_window(conn, started_at, window_end)
        messages = fetch_message_count_in_window(conn, started_at, window_end)
    payload = {
        "now": window_end.timestamp(),
        "messages": messages,
        "aircraft": aircraft,
    }
    atomic_write_json(path, payload)
    start_str = started_at.astimezone(TIMEZONE).strftime("%H:%M:%S")
    end_str = window_end.astimezone(TIMEZONE).strftime("%H:%M:%S")
    log(f"  {path.name}: {len(aircraft)} aircraft ({start_str} → {end_str})")
    ok = upload_file(path)
    if ok:
        ping_healthcheck(success=True, message=f"{len(aircraft)} aircraft uploaded")
    return ok


def finalize_session(path, session_id, started_at):
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ended_at FROM adsb_sessions WHERE id = %s", (session_id,))
            row = cur.fetchone()
        ended_at = row[0] if row and row[0] else None
        if ended_at is not None and ended_at.tzinfo is None:
            ended_at = ended_at.replace(tzinfo=timezone.utc)
    window_end = ended_at or utc_now()
    if not upload_window(path, started_at, window_end):
        _enqueue(path, started_at, window_end)


def run_historical(target_date):
    day_start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=TIMEZONE)
    day_end = min(day_start + timedelta(days=1), utc_now())

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log(f"Historical export for {target_date} ({TIMEZONE.key})")
    log(f"DB: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    log(f"Output dir: {OUTPUT_DIR}")

    with db_connect() as conn:
        sessions = fetch_sessions_for_date(conn, day_start, day_end)

    if not sessions:
        log("No sessions found for that date.")
        return

    log(f"Found {len(sessions)} session(s)")
    written = 0

    for session in sessions:
        window_start = max(session["started_at"], day_start)
        window_end = min(session["ended_at"] or utc_now(), day_end)

        with db_connect() as conn:
            aircraft = fetch_aircraft_in_window(conn, window_start, window_end)
            messages = fetch_message_count_in_window(conn, window_start, window_end)

        if not aircraft:
            continue

        path = session_filepath(session["started_at"], replay=True)
        payload = {
            "now": window_end.timestamp(),
            "messages": messages,
            "aircraft": aircraft,
        }
        atomic_write_json(path, payload)
        upload_file(path)
        log(f"  {path.name}: {len(aircraft)} aircraft")
        written += 1

    log(f"Done: {written} session file(s) written to {OUTPUT_DIR}")


def main():
    parser = argparse.ArgumentParser(description="Intercept ADS-B aircraft.json exporter")
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Export historical data for a specific date (interpreted in AIRCRAFT_TIMEZONE) instead of running live",
    )
    args = parser.parse_args()

    if args.date:
        try:
            target_date = date.fromisoformat(args.date)
        except ValueError:
            parser.error(f"Invalid date '{args.date}' — expected YYYY-MM-DD")
        run_historical(target_date)
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    latest_path = OUTPUT_DIR / LATEST_FILE

    log("Intercept ADS-B aircraft.json exporter")
    log(f"DB: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    log(f"Output dir: {OUTPUT_DIR}")
    log(f"Refresh: every {REFRESH_SECONDS} second(s)")
    log(f"Max aircraft age: {MAX_AGE_SECONDS} second(s)")
    log(f"Session upload interval: every {SESSION_MINUTES} minute(s)" if SESSION_MINUTES else "Session upload interval: on session end only")
    log(f"Write latest file: {WRITE_LATEST}")
    if WRITE_LATEST:
        log(f"Latest file: {latest_path}")

    last_session_id = None
    last_session_file = None
    last_uploaded_at = None  # start of the next upload window; advances after each upload
    consecutive_errors = 0

    while True:
        try:
            session, payload = build_payload()

            if session is None:
                if last_session_file is not None:
                    finalize_session(last_session_file, last_session_id, last_uploaded_at)
                    last_session_file = None
                    last_session_id = None
                    last_uploaded_at = None
                log("No active session, waiting...")
                time.sleep(REFRESH_SECONDS)
                continue

            current_session_id = session["id"]
            current_file = session_filepath(session["started_at"])

            session_changed = current_session_id != last_session_id
            if session_changed:
                if last_session_file is not None:
                    finalize_session(last_session_file, last_session_id, last_uploaded_at)
                log(f"Writing session file: {current_file}")
                last_session_id = current_session_id
                last_session_file = current_file
                last_uploaded_at = _load_last_uploaded_at(current_session_id) or session["started_at"]

            atomic_write_json(current_file, payload)

            if WRITE_LATEST:
                atomic_write_json(latest_path, payload)

            timed_this_cycle = False
            if SESSION_MINUTES and last_uploaded_at is not None:
                now = utc_now()
                if (now - last_uploaded_at).total_seconds() >= SESSION_MINUTES * 60:
                    # stamp by window-end so each interval gets a unique filename
                    timed_path = session_filepath(now)
                    log(f"Timed upload: {timed_path.name}")
                    if upload_window(timed_path, last_uploaded_at, now):
                        last_uploaded_at = now
                        _save_last_uploaded_at(current_session_id, last_uploaded_at)
                        timed_this_cycle = True
                    # on failure: last_uploaded_at stays put, next interval covers wider window

            if not session_changed and not timed_this_cycle:
                drain_queue()

            consecutive_errors = 0

        except Exception as exc:
            consecutive_errors += 1
            log(f"Exporter error ({consecutive_errors}/{MAX_CONSECUTIVE_ERRORS}): {exc}")
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                log("Too many consecutive errors, exiting.")
                raise SystemExit(1)

        time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    main()
