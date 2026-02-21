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
    "host": "localhost"
}

OPERATOR_MAP = {
    "7778019": "Dublin Bus",
    "7778021": "Go-Ahead",
    "7778006": "Go-Ahead",
    "7778020": "Bus Eireann"
}

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

ROUTE_AGENCY_MAP = {
    "5249_119701": "7778021", "5249_119702": "7778021",
    "5249_119703": "7778021", "5249_119704": "7778021", "5249_119705": "7778021",
    "5249_119706": "7778021", "5249_119707": "7778021", "5249_119708": "7778021",
    "5402_123830": "7778019", "5402_123831": "7778019", "5402_123832": "7778019",
    "5402_123833": "7778019", "5402_123834": "7778019", "5402_123835": "7778019",
    "5402_123836": "7778019", "5402_123837": "7778019",
    "5402_123841": "7778019", "5402_123842": "7778019",
    "5402_123843": "7778019", "5402_123844": "7778019", "5402_123845": "7778019",
    "5402_123846": "7778019",
    "5402_123847": "7778019"
}

def get_service_date(scheduled_arrival_secs: int) -> date:
    """
    Derive the logical service date from a GTFS scheduled arrival Unix timestamp.
    GTFS trips that run past midnight (e.g. a 00:30 bus on the Friday night service)
    belong to the previous calendar date. We treat any arrival before 04:00 local time
    as belonging to the prior service day.
    """
    if not scheduled_arrival_secs:
        return datetime.now(timezone.utc).date()

    dt = datetime.fromtimestamp(scheduled_arrival_secs, tz=timezone.utc)
    # If the trip arrives between midnight and 04:00, it belongs to yesterday's service
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

        if route_id not in SPINE_ROUTE_MAP:
            continue

        agency_id = ROUTE_AGENCY_MAP.get(route_id, "")
        operator = OPERATOR_MAP.get(agency_id, "Unknown")
        spine = SPINE_ROUTE_MAP[route_id]

        for stu in tu.stop_time_update:
            scheduled_secs = stu.arrival.time if stu.arrival.time else stu.departure.time if stu.departure.time else None
            delay = stu.arrival.delay if stu.arrival.delay else stu.departure.delay if stu.departure.delay else None
            service_date = get_service_date(scheduled_secs)

            rows.append((
                trip_id,
                route_id,
                stu.stop_id,
                stu.stop_sequence,
                service_date,
                # --- columns that update on each observation ---
                operator,
                direction_id,
                scheduled_secs,
                stu.arrival.time if stu.arrival.time else None,
                stu.departure.time if stu.departure.time else None,
                delay,
                spine,
                collected_at,   # first_seen_at  (only written on INSERT)
                collected_at    # last_seen_at   (updated on every UPSERT)
            ))

    if not rows:
        print(f"{collected_at} — No spine route data in feed")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # UPSERT: natural key is (trip_id, stop_id, stop_sequence, service_date).
    # On conflict we update the delay and last_seen_at — never create a new row.
    # first_seen_at is intentionally excluded from the DO UPDATE so it is set once.
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
