import psycopg2
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": "busconnects",
    "user": "satvik",
    "password": os.getenv("DB_PASSWORD"),
    "host": "127.0.0.1"
}

GHOST_THRESHOLD = 0.80  # disappeared before 80% of stops

def detect_ghosts():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO ghost_bus_candidates (
            trip_id, route_id, operator, spine,
            first_seen, last_seen,
            last_stop_sequence, expected_total_stops, confirmed_ghost
        )
        SELECT
            t.trip_id,
            t.route_id,
            MAX(t.operator),
            MAX(t.spine),
            MIN(t.first_seen_at),
            MAX(t.last_seen_at),
            MAX(t.stop_sequence),
            r.total_stops,
            TRUE
        FROM trip_observations t
        JOIN (
            SELECT trip_id, MAX(total_stops) as total_stops
            FROM route_stop_counts
            GROUP BY trip_id
        ) r ON t.trip_id = r.trip_id
        WHERE
            t.last_seen_at < NOW() - INTERVAL '15 minutes'
            AND t.service_date = CURRENT_DATE
        GROUP BY t.trip_id, t.route_id, r.total_stops
        HAVING MAX(t.stop_sequence)::float / r.total_stops < %s
        ON CONFLICT (trip_id) DO NOTHING
    """, (GHOST_THRESHOLD,))

    flagged = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"{datetime.now(timezone.utc)} — {flagged} ghost buses flagged")

if __name__ == "__main__":
    detect_ghosts()
