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
    if s.startswith("{") and s.endswith("}"):
        first = s[1:-1].split(",")[0].strip().strip('"')
        return first or None
    return s or None


def current_session_start_epoch():
    """
    Returns the start epoch for the current rotation window.

    Example:
      SESSION_MINUTES=15 creates windows like:
      12:00, 12:15, 12:30, 12:45
    """
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


def fetch_aircraft():
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

    with db_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (MAX_AGE_SECONDS,))
            rows = cur.fetchall()

    aircraft = []
    for row in rows:
        item = build_aircraft(row)
        if item.get("hex"):
            aircraft.append(item)

    return aircraft


def fetch_aircraft_in_window(window_start, window_end):
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

    with db_connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (window_start, window_end))
            rows = cur.fetchall()

    aircraft = []
    for row in rows:
        item = build_aircraft(row, reference=window_end)
        if item.get("hex"):
            aircraft.append(item)

    return aircraft


def fetch_message_count():
    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM adsb_messages;")
                return int(cur.fetchone()[0])
    except Exception:
        return 0


def fetch_message_count_in_window(window_start, window_end):
    try:
        with db_connect() as conn:
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


def build_payload():
    aircraft = fetch_aircraft()

    return {
        "now": time.time(),
        "messages": fetch_message_count(),
        "aircraft": aircraft,
    }


def run_historical(target_date):
    day_start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=TIMEZONE)
    day_end = day_start + timedelta(days=1)
    window_delta = timedelta(seconds=SESSION_SECONDS)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Historical export for {target_date} ({TIMEZONE.key})")
    print(f"DB: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"Output dir: {OUTPUT_DIR}")
    print(f"Session length: {SESSION_MINUTES} minute(s)")

    written = 0
    window_start = day_start

    while window_start < day_end:
        window_end = window_start + window_delta
        aircraft = fetch_aircraft_in_window(window_start, window_end)

        if aircraft:
            stamp = window_start.strftime("%Y%m%d_%H%M%S%Z")
            path = OUTPUT_DIR / f"{FILE_PREFIX}_replay_{stamp}.json"
            payload = {
                "now": window_end.timestamp(),
                "messages": fetch_message_count_in_window(window_start, window_end),
                "aircraft": aircraft,
            }
            atomic_write_json(path, payload)
            print(f"  {path.name}: {len(aircraft)} aircraft")
            written += 1

        window_start = window_end

    print(f"Done: {written} session file(s) written to {OUTPUT_DIR}")


def main():
    parser = argparse.ArgumentParser(description="Intercept ADS-B aircraft.json exporter")
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Export historical data for a specific UTC date instead of running live",
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

    while True:
        try:
            payload = build_payload()

            current_file = session_filename()

            if current_file != last_session_file:
                print(f"Writing session file: {current_file}")
                last_session_file = current_file

            atomic_write_json(current_file, payload)

            if WRITE_LATEST:
                atomic_write_json(latest_path, payload)

        except Exception as exc:
            print(f"Exporter error: {exc}")

        time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    main()
