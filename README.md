# intercept-wdgwars

Exports live ADS-B aircraft data from an [intercept](https://github.com/smittix/intercept) PostgreSQL database into **dump1090-fa / readsb format** (`aircraft.json`), suitable for upload to [WatchDogsGo / wdgwars](https://github.com/LOCOSP/WatchDogsGo).

## How it works

The script polls `adsb_snapshots` in the intercept database on a configurable interval, builds a dump1090-fa / readsb compatible `aircraft.json` payload, and writes it atomically to disk. It maintains a rolling latest file (`aircraft.json`) and optionally rotates timestamped session files.

## Requirements

- Python 3.9+
- A running [intercept](https://github.com/smittix/intercept) instance with PostgreSQL

```
pip install -r requirements.txt
```

## Setup

```bash
cp .env.sample .env
# edit .env with your database credentials and output preferences
python convert.py
```

## Modes

### Live (default)

Polls the database continuously and writes updated files on each refresh cycle:

```bash
python convert.py
```

### Historical

Generates session files for a full UTC day from data already in the database. Useful for backfilling missed days or uploading past data to wdgwars:

```bash
python convert.py --date 2026-05-09
```

This iterates through every session window for the given date, skips windows with no aircraft, and writes the same filename format as live mode would have produced.

## Configuration

All settings are via environment variables (or `.env`):

| Variable | Default | Description |
|---|---|---|
| `INTERCEPT_ADSB_DB_HOST` | `localhost` | PostgreSQL host |
| `INTERCEPT_ADSB_DB_PORT` | `5432` | PostgreSQL port |
| `INTERCEPT_ADSB_DB_NAME` | `intercept_adsb` | Database name |
| `INTERCEPT_ADSB_DB_USER` | `intercept` | Database user |
| `INTERCEPT_ADSB_DB_PASSWORD` | `intercept` | Database password |
| `AIRCRAFT_OUTPUT_DIR` | `./adsb_exports` | Directory for output files |
| `AIRCRAFT_FILE_PREFIX` | `aircraft` | Prefix for session filenames |
| `AIRCRAFT_REFRESH_SECONDS` | `60` | How often to write (seconds) — live mode only |
| `AIRCRAFT_MAX_AGE_SECONDS` | `60` | Drop aircraft older than this — live mode only. Keep this >= `AIRCRAFT_REFRESH_SECONDS` or aircraft may be missed between writes |
| `AIRCRAFT_TIMEZONE` | `UTC` | IANA timezone for interpreting `--date` values (e.g. `America/New_York`) |
| `AIRCRAFT_SESSION_MINUTES` | `15` | Rotate to a new session file every N minutes |
| `AIRCRAFT_MAX_ERRORS` | `10` | Exit after this many consecutive DB errors — live mode only |
| `AIRCRAFT_WRITE_LATEST` | `true` | Also write a live `aircraft.json` — live mode only |
| `AIRCRAFT_LATEST_FILE` | `aircraft.json` | Name of the live file |

## Output

Live session files are written as `aircraft_YYYYMMDD_HHMMSSTZ.json` (e.g. `aircraft_20260510_140000EDT.json`). Historical (replay) session files are written as `aircraft_replay_YYYYMMDD_HHMMSSTZ.json`. The timezone abbreviation in the filename reflects `AIRCRAFT_TIMEZONE`. Both rotate on the interval set by `AIRCRAFT_SESSION_MINUTES`. These timestamped session files are what you upload to wdgwars.

The live file (`aircraft.json`) is a convenience copy of the latest payload, useful for tools that expect a fixed filename (e.g. tar1090, readsb). It is not needed for wdgwars uploads.

Session files accumulate and are not automatically pruned — set up a cron job or logrotate rule if disk space is a concern:

```
# delete session files older than 7 days
find /path/to/adsb_exports -name 'aircraft_*.json' -mtime +7 -delete
```

## Notes

- Tested against intercept schema as of May 2026
- The `snapshot` JSONB column is used as a fallback if top-level columns are null
- `messages` count in the output reflects the total row count of `adsb_messages`
- In historical mode, `messages` is scoped to each session window
