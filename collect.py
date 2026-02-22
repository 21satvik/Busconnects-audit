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
TRIP_UPDATES_URL = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"
VEHICLES_URL = "https://api.nationaltransport.ie/gtfsr/v2/Vehicles"

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

SPINE_ROUTE_MAP = {
    "5249_119701": "N", "5249_119702": "N",
    "5249_119703": "S", "5249_119704": "S", "5249_119705": "S",
    "5249_119706": "W", "5249_119707": "W", "5249_119708": "W",
    "5402_123830": "C", "5402_123831": "C", "5402_123832": "C",
    "5402_123833": "C", "5402_123834": "C", "5402_123835": "C",
    "5402_123836": "E", "5402_123837": "E",
    "5402_123841": "G", "5402_123842": "G",
    "5402_123843": "H", "5402_123844": "H", "5402_123845": "H",
    "5402_123846": "N", "5402_123847": "S"
}

DB_LEGACY = {
    "5402_123775","5402_123776","5402_123777","5402_123778","5402_123779",
    "5402_123780","5402_123781","5402_123782","5402_123783","5402_123784",
    "5402_123785","5402_123786","5402_123787","5402_123788","5402_123789",
    "5402_123790","5402_123791","5402_123792","5402_123793","5402_123794",
    "5402_123795","5402_123796","5402_123797","5402_123798","5402_123799",
    "5402_123800","5402_123801","5402_123802","5402_123803","5402_123804",
    "5402_123805","5402_123806","5402_123807","5402_123808","5402_123809",
    "5402_123810","5402_123811","5402_123812","5402_123813","5402_123814",
    "5402_123815","5402_123816","5402_123817","5402_123818","5402_123819",
    "5402_123820","5402_123821","5402_123822","5402_123823","5402_123824",
    "5402_123825","5402_123826","5402_123827","5402_123828","5402_123829",
    "5402_123838","5402_123839","5402_123840",
    "5402_123848","5402_123849","5402_123850","5402_123851","5402_123852",
    "5402_123853","5402_123854","5402_123855","5402_123856","5402_123857",
    "5402_123858","5402_123859","5402_123860","5402_123861","5402_123862",
    "5402_123863","5402_123864","5402_123865","5402_123866","5402_123867",
    "5402_123868","5402_123869","5402_123870","5402_123871","5402_123872",
    "5402_123873","5402_123874","5402_123875","5402_123876","5402_123877",
    "5402_123878","5402_123879","5402_123880","5402_123881","5402_123882",
    "5402_123883","5402_123884","5402_123885","5402_123886","5402_123887",
    "5402_123888","5402_123889","5402_123890"
}

GA_LEGACY = {
    "5249_119681","5249_119682","5249_119683","5249_119684","5249_119685",
    "5249_119686","5249_119687","5249_119688","5249_119689","5249_119690",
    "5249_119691","5249_119692","5249_119693","5249_119694","5249_119695",
    "5249_119696","5249_119697","5249_119698","5249_119699","5249_119700",
    "5249_119709","5249_119710","5249_119711","5249_119712","5249_119713",
    "5249_119714","5249_119715","5249_119716","5249_119717","5249_119718",
    "5249_119719","5249_119720","5249_119721","5249_119722","5249_119723",
    "5249_119724"
}

ALL_ROUTE_MAP = {
    **SPINE_ROUTE_MAP,
    **{r: "legacy" for r in DB_LEGACY},
    **{r: "legacy" for r in GA_LEGACY}
}

ROUTE_AGENCY_MAP = {
    **{r: "7778021" for r in SPINE_ROUTE_MAP if r.startswith("5249")},
    **{r: "7778019" for r in SPINE_ROUTE_MAP if r.startswith("5402")},
    **{r: "7778019" for r in DB_LEGACY},
    **{r: "7778021" for r in GA_LEGACY}
}

def get_service_date(scheduled_arrival_secs: int) -> date:
    if not scheduled_arrival_secs:
        return datetime.now(timezone.utc).date()
    dt = datetime.fromtimestamp(scheduled_arrival_secs, tz=timezone.utc)
    if dt.hour < 4:
        return (dt - timedelta(days=1)).date()
    return dt.date()

def fetch_trip_updates(conn, collected_at):
    try:
        response = requests.get(
            TRIP_UPDATES_URL,
            headers={"x-api-key": API_KEY},
            timeout=30
        )
        response.raise_for_status()
    except Exception as e:
        print(f"{collected_at} — TRIP UPDATES FETCH ERROR: {e}")
        return 0

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
        return 0

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
    return len(rows)

def fetch_vehicle_positions(conn, collected_at):
    try:
        response = requests.get(
            VEHICLES_URL,
            headers={"x-api-key": API_KEY},
            timeout=30
        )
        response.raise_for_status()
    except Exception as e:
        print(f"{collected_at} — VEHICLES FETCH ERROR: {e}")
        return 0

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    rows = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue

        v = entity.vehicle
        route_id = v.trip.route_id

        if route_id not in ALL_ROUTE_MAP:
            continue

        agency_id = ROUTE_AGENCY_MAP.get(route_id, "")
        operator = OPERATOR_MAP.get(agency_id, "Unknown")
        spine = ALL_ROUTE_MAP[route_id]

        rows.append((
            v.vehicle.id,
            v.trip.trip_id,
            route_id,
            operator,
            spine,
            v.position.latitude,
            v.position.longitude,
            collected_at
        ))

    if not rows:
        return 0

    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO vehicle_positions (
            vehicle_id, trip_id, route_id, operator, spine,
            latitude, longitude, collected_at
        )
        VALUES %s
        ON CONFLICT (vehicle_id, collected_at) DO NOTHING
    """, rows)
    conn.commit()
    cur.close()
    return len(rows)

def main():
    collected_at = datetime.now(timezone.utc)
    conn = psycopg2.connect(**DB_CONFIG)

    trip_rows = fetch_trip_updates(conn, collected_at)
    vehicle_rows = fetch_vehicle_positions(conn, collected_at)

    conn.close()
    print(f"{collected_at} — {trip_rows} trip rows upserted, {vehicle_rows} vehicle positions written")

if __name__ == "__main__":
    main()