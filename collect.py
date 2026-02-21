import requests
import psycopg2
from google.transit import gtfs_realtime_pb2
from datetime import datetime
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

def fetch_and_store():
    try:
        response = requests.get(
            FEED_URL,
            headers={"x-api-key": API_KEY},
            timeout=30
        )
        response.raise_for_status()
    except Exception as e:
        print(f"{datetime.utcnow()} — FETCH ERROR: {e}")
        sys.exit(1)

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    collected_at = datetime.utcnow()
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
            delay = stu.arrival.delay if stu.arrival.delay else stu.departure.delay if stu.departure.delay else None

            rows.append((
                collected_at,
                trip_id,
                route_id,
                operator,
                direction_id,
                stu.stop_id,
                stu.stop_sequence,
                stu.arrival.time if stu.arrival.time else None,
                stu.departure.time if stu.departure.time else None,
                delay,
                spine
            ))

    if not rows:
        print(f"{collected_at} — No spine route data in feed")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO trip_observations
        (collected_at, trip_id, route_id, operator, direction_id,
         stop_id, stop_sequence, scheduled_arrival_secs, actual_arrival_secs, reported_delay_seconds, spine)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f"{collected_at} — {len(rows)} rows written")

if __name__ == "__main__":
    fetch_and_store()
