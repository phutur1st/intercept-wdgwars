#!/usr/bin/env python3

import argparse
import os
import json
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

SESSION_MINUTES = int(os.getenv("AIRCRAFT_SESSION_MINUTES", "15"))
SESSION_SECONDS = SESSION_MINUTES * 60

WRITE_LATEST = os.getenv("AIRCRAFT_WRITE_LATEST", "true").lower() in {"1", "true", "yes", "on"}
LATEST_FILE = os.getenv("AIRCRAFT_LATEST_FILE", "aircraft.json")

TIMEZONE = ZoneInfo(os.getenv("AIRCRAFT_TIMEZONE", "UTC"))

MAX_CONSECUTIVE_ERRORS = int(os.getenv("AIRCRAFT_MAX_ERRORS", "10"))

WDGWARS_API_KEY = os.getenv("WDGWARS_API_KEY", "")
WDGWARS_UPLOAD_URL = os.getenv("WDGWARS_UPLOAD_URL", "https://wdgwars.pl/api/upload-csv")

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


def current_session_start_epoch():
    now = int(time.time())
    return now - (now % SESSION_SECONDS)


def session_filename():
    session_start = datetime.fromtimestamp(current_session_start_epoch(), tz=TIMEZONE)
    stamp = session_start.strftime("%Y%m%d_%H%M%S%Z")
    return OUTPUT_DIR / f"{FILE_PREFIX}_{stamp}.json"


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


def fetch_aircraft(conn):
    query = """
        WITH latest AS (
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
                lat,
                lon,
                squawk,
                snapshot
            FROM adsb_snapshots
            WHERE captured_at >= NOW() - (%s || ' seconds')::interval
            ORDER BY icao, captured_at DESC
        )
        SELECT *
        FROM latest
        ORDER BY captured_at DESC;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (MAX_AGE_SECONDS,))
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
        WITH latest AS (
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
                lat,
                lon,
                squawk,
                snapshot
            FROM adsb_snapshots
            WHERE captured_at >= %s AND captured_at < %s
            ORDER BY icao, captured_at DESC
        )
        SELECT *
        FROM latest
        ORDER BY captured_at DESC;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (window_start, window_end))
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


def upload_file(path):
    if not WDGWARS_API_KEY:
        return
    try:
        with open(path, "rb") as f:
            resp = requests.post(
                WDGWARS_UPLOAD_URL,
                headers={"X-API-Key": WDGWARS_API_KEY},
                files={"file": (path.name, f, "application/json")},
                timeout=30,
            )
        if resp.status_code == 429:
            msg = f"rate limited: {path.name}"
            print(f"Upload {msg}")
            ping_healthcheck(success=False, message=msg)
        elif not resp.ok:
            msg = f"failed ({resp.status_code}): {path.name} — {resp.text}"
            print(f"Upload {msg}")
            ping_healthcheck(success=False, message=msg)
        else:
            result = resp.json()
            merged = result.get("merged_samples", "?")
            msg = f"{path.name}: {merged} merged samples"
            print(f"Uploaded {msg}")
            ping_healthcheck(success=True, message=msg)
    except Exception as exc:
        msg = f"error uploading {path.name}: {exc}"
        print(f"Upload {msg}")
        ping_healthcheck(success=False, message=msg)


def build_payload():
    with db_connect() as conn:
        aircraft = fetch_aircraft(conn)
        messages = fetch_message_count(conn)

    return {
        "now": time.time(),
        "messages": messages,
        "aircraft": aircraft,
    }


def run_historical(target_date):
    day_start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=TIMEZONE)
    day_end = min(day_start + timedelta(days=1), utc_now())
    window_delta = timedelta(seconds=SESSION_SECONDS)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Historical export for {target_date} ({TIMEZONE.key})")
    print(f"DB: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"Output dir: {OUTPUT_DIR}")
    print(f"Session length: {SESSION_MINUTES} minute(s)")

    written = 0
    window_start = day_start

    while window_start < day_end:
        window_end = min(window_start + window_delta, day_end)

        with db_connect() as conn:
            aircraft = fetch_aircraft_in_window(conn, window_start, window_end)
            if aircraft:
                messages = fetch_message_count_in_window(conn, window_start, window_end)

        if aircraft:
            stamp = window_start.strftime("%Y%m%d_%H%M%S%Z")
            path = OUTPUT_DIR / f"{FILE_PREFIX}_replay_{stamp}.json"
            payload = {
                "now": window_end.timestamp(),
                "messages": messages,
                "aircraft": aircraft,
            }
            atomic_write_json(path, payload)
            upload_file(path)
            print(f"  {path.name}: {len(aircraft)} aircraft")
            written += 1

        window_start = window_end

    print(f"Done: {written} session file(s) written to {OUTPUT_DIR}")


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

    print("Intercept ADS-B aircraft.json exporter")
    print(f"DB: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"Output dir: {OUTPUT_DIR}")
    print(f"Session length: {SESSION_MINUTES} minute(s)")
    print(f"Refresh: every {REFRESH_SECONDS} second(s)")
    print(f"Max aircraft age: {MAX_AGE_SECONDS} second(s)")
    print(f"Write latest file: {WRITE_LATEST}")
    if WRITE_LATEST:
        print(f"Latest file: {latest_path}")

    last_session_file = None
    consecutive_errors = 0

    while True:
        try:
            payload = build_payload()

            current_file = session_filename()
            if current_file != last_session_file:
                print(f"Writing session file: {current_file}")
                last_session_file = current_file

            atomic_write_json(current_file, payload)
            upload_file(current_file)

            if WRITE_LATEST:
                atomic_write_json(latest_path, payload)

            consecutive_errors = 0

        except Exception as exc:
            consecutive_errors += 1
            print(f"Exporter error ({consecutive_errors}/{MAX_CONSECUTIVE_ERRORS}): {exc}")
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                print("Too many consecutive errors, exiting.")
                raise SystemExit(1)

        time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    main()
