import requests
import psycopg2
from psycopg2.extras import execute_values
from google.transit import gtfs_realtime_pb2
from datetime import datetime, timezone, date, timedelta
import sys
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("NTA_API_KEY")
FEED_URL = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"

DB_CONFIG = {
    "dbname": "busconnects",
    "user": "satvik",
    "password": os.getenv("DB_PASSWORD"),
    "host": "127.0.0.1"
}

OPERATOR_MAP = {
    "7778019": "Dublin Bus",
    "7778021": "Go-Ahead",
    "7778006": "Go-Ahead",
    "7778020": "Bus Eireann"
}

# BusConnects spines
SPINE_ROUTE_MAP = {
    "5249_119701": "N", "5249_119702": "N",
    "5249_119703": "S", "5249_119704": "S", "5249_119705": "S",
    "5249_119706": "W", "5249_119707": "W", "5249_119708": "W",
    "5402_123830": "C", "5402_123831": "C", "5402_123832": "C",
    "5402_123833": "C", "5402_123834": "C", "5402_123835": "C",
    "5402_123836": "E", "5402_123837": "E",
    "5402_123841": "G", "5402_123842": "G",
    "5402_123843": "H", "5402_123844": "H", "5402_123845": "H",
    "5402_123846": "N",
    "5402_123847": "S"
}

# Go-Ahead legacy Dublin routes
LEGACY_ROUTE_MAP = {
    "5249_119681": "legacy", "5249_119682": "legacy",
    "5249_119683": "legacy", "5249_119684": "legacy",
    "5249_119685": "legacy", "5249_119686": "legacy",
    "5249_119687": "legacy", "5249_119688": "legacy",
    "5249_119689": "legacy", "5249_119690": "legacy",
    "5249_119691": "legacy", "5249_119692": "legacy",
    "5249_119693": "legacy", "5249_119694": "legacy",
    "5249_119695": "legacy", "5249_119696": "legacy",
    "5249_119697": "legacy", "5249_119698": "legacy",
    "5249_119699": "legacy", "5249_119700": "legacy",
    "5249_119709": "legacy", "5249_119710": "legacy",
    "5249_119711": "legacy", "5249_119712": "legacy",
    "5249_119713": "legacy", "5249_119714": "legacy",
    "5249_119715": "legacy", "5249_119716": "legacy",
    "5249_119717": "legacy", "5249_119718": "legacy",
    "5249_119719": "legacy", "5249_119720": "legacy",
    "5249_119721": "legacy", "5249_119722": "legacy",
    "5249_119723": "legacy", "5249_119724": "legacy",
}

# Merge both maps
ALL_ROUTE_MAP = {**SPINE_ROUTE_MAP, **LEGACY_ROUTE_MAP}

ROUTE_AGENCY_MAP = {
    # Go-Ahead spines
    "5249_119701": "7778021", "5249_119702": "7778021",
    "5249_119703": "7778021", "5249_119704": "7778021", "5249_119705": "7778021",
    "5249_119706": "7778021", "5249_119707": "7778021", "5249_119708": "7778021",
    # Dublin Bus spines
    "5402_123830": "7778019", "5402_123831": "7778019", "5402_123832": "7778019",
    "5402_123833": "7778019", "5402_123834": "7778019", "5402_123835": "7778019",
    "5402_123836": "7778019", "5402_123837": "7778019",
    "5402_123841": "7778019", "5402_123842": "7778019",
    "5402_123843": "7778019", "5402_123844": "7778019", "5402_123845": "7778019",
    "5402_123846": "7778019", "5402_123847": "7778019",
    # Go-Ahead legacy
    "5249_119681": "7778021", "5249_119682": "7778021",
    "5249_119683": "7778021", "5249_119684": "7778021",
    "5249_119685": "7778021", "5249_119686": "7778021",
    "5249_119687": "7778021", "5249_119688": "7778021",
    "5249_119689": "7778021", "5249_119690": "7778021",
    "5249_119691": "7778021", "5249_119692": "7778021",
    "5249_119693": "7778021", "5249_119694": "7778021",
    "5249_119695": "7778021", "5249_119696": "7778021",
    "5249_119697": "7778021", "5249_119698": "7778021",
    "5249_119699": "7778021", "5249_119700": "7778021",
    "5249_119709": "7778021", "5249_119710": "7778021",
    "5249_119711": "7778021", "5249_119712": "7778021",
    "5249_119713": "7778021", "5249_119714": "7778021",
    "5249_119715": "7778021", "5249_119716": "7778021",
    "5249_119717": "7778021", "5249_119718": "7778021",
    "5249_119719": "7778021", "5249_119720": "7778021",
    "5249_119721": "7778021", "5249_119722": "7778021",
    "5249_119723": "7778021", "5249_119724": "7778021",
}

def get_service_date(scheduled_arrival_secs: int) -> date:
    if not scheduled_arrival_secs:
        return datetime.now(timezone.utc).date()
    dt = datetime.fromtimestamp(scheduled_arrival_secs, tz=timezone.utc)
    if dt.hour < 4:
        return (dt - timedelta(days=1)).date()
    return dt.date()

def fetch_and_store():
    collected_at = datetime.now(timezone.utc)

    try:
        response = requests.get(
            FEED_URL,
            headers={"x-api-key": API_KEY},
            timeout=30
        )
        response.raise_for_status()
    except Exception as e:
        print(f"{collected_at} — FETCH ERROR: {e}")
        sys.exit(1)

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    rows = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        tu = entity.trip_update
        route_id = tu.trip.route_id
        trip_id = tu.trip.trip_id
        direction_id = tu.trip.direction_id if tu.trip.direction_id else None

        if route_id not in ALL_ROUTE_MAP:
            continue

        agency_id = ROUTE_AGENCY_MAP.get(route_id, "")
        operator = OPERATOR_MAP.get(agency_id, "Unknown")
        spine = ALL_ROUTE_MAP[route_id]

        for stu in tu.stop_time_update:
            scheduled_secs = stu.arrival.time if stu.arrival.time else stu.departure.time if stu.departure.time else None
            delay = stu.arrival.delay if stu.arrival.delay else stu.departure.delay if stu.departure.delay else None
            service_date = get_service_date(scheduled_secs)

            rows.append((
                trip_id, route_id, stu.stop_id, stu.stop_sequence, service_date,
                operator, direction_id, scheduled_secs,
                stu.arrival.time if stu.arrival.time else None,
                stu.departure.time if stu.departure.time else None,
                delay, spine, collected_at, collected_at
            ))

    if not rows:
        print(f"{collected_at} — No route data in feed")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO trip_observations (
            trip_id, route_id, stop_id, stop_sequence, service_date,
            operator, direction_id,
            scheduled_arrival_secs, actual_arrival_secs, actual_departure_secs,
            reported_delay_seconds, spine,
            first_seen_at, last_seen_at
        )
        VALUES %s
        ON CONFLICT (trip_id, stop_id, stop_sequence, service_date)
        DO UPDATE SET
            reported_delay_seconds  = EXCLUDED.reported_delay_seconds,
            actual_arrival_secs     = EXCLUDED.actual_arrival_secs,
            actual_departure_secs   = EXCLUDED.actual_departure_secs,
            last_seen_at            = EXCLUDED.last_seen_at
    """, rows)

    conn.commit()
    cur.close()
    conn.close()
    print(f"{collected_at} — {len(rows)} rows upserted")

if __name__ == "__main__":
    fetch_and_store()