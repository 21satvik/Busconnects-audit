# BusConnects Audit — An Independent Data Audit of Dublin's Bus Reform

> *Three S6 buses disappeared in a row. I was standing at the UCD Village stop. Freezing. Watching them vanish from the TFI app one by one. No alert. No cancellation notice. Just… gone.*
>
> That's what started this.

BusConnects is the largest redesign of Dublin's bus network in decades. The NTA publishes quarterly KPI reports showing punctuality improvements and passenger growth. This project independently verifies those claims using real-time GTFS-R data — collected every 60 seconds, 24/7, across all 160 Dublin Bus and Go-Ahead routes.

---

## Findings

**Evening rush delays — Go-Ahead vs Dublin Bus**
Independent measurement across 3 full weekdays (Feb 23–25 2026):

| Operator | Evening Rush Avg Delay | Morning Rush Avg Delay |
|----------|----------------------|----------------------|
| Dublin Bus | 2.32 min | 2.89 min |
| Go-Ahead | 4.04 min | 2.26 min |

Go-Ahead evening rush delays run **74% worse** than Dublin Bus. The gap doesn't exist in the morning — it's specific to the evening peak.

**Ghost buses — feed visibility loss**
When a trip disappears from the live feed mid-journey, passengers lose TFI app visibility with no alert or cancellation notice. Average journey completion at point of disappearance:

| Operator | Avg Journey % at Disappearance |
|----------|-------------------------------|
| Go-Ahead (all spines) | 21–32% |
| Dublin Bus | 45–57% |

Go-Ahead legacy routes ghost at **2.2× the rate of Dublin Bus** legacy routes on a like-for-like comparison.

**NTA's own data tells a story they don't headline**
From the NTA December 2025 Progress Report (extracted into structured PostgreSQL):

- Go-Ahead W-Orbital (Phase 5a): lost km deteriorated **165%** since launch
- Go-Ahead N-Orbital (Phase 3): lost km deteriorated **37%** since launch
- All Dublin Bus phases: lost km improved over the same period
- BusConnects routes overall: **30% passenger growth** vs **6%** on non-reformed routes

The NTA leads with the punctuality headline. The reliability deterioration on Go-Ahead routes is buried in bar charts.

---

## What I Built

### Pipeline

```
NTA GTFS-R API (every 60s)
        │
        ▼
   collect.py  ──────────────────────────────────────────┐
   (cron, Oracle A1)                                      │
        │                                                 │
        ▼                                                 ▼
trip_observations table                    vehicle_positions table
(1.6M+ rows, 160 routes)                   (rolling 6hr window)
        │
        ▼
  detect_ghosts.py
  (every 30 mins)
        │
        ▼
ghost_bus_candidates table
        │
        ▼
   PostgreSQL ──► Tableau Desktop ──► Tableau Cloud (live)
```

### Stack

| Layer | Technology |
|-------|-----------|
| Infrastructure | Oracle A1 Always Free — 4GB RAM, 50GB storage, permanent free tier |
| Collection | Python + gtfs-realtime-bindings, cron every 60s |
| Storage | PostgreSQL — 5 tables, 1.6M+ rows |
| NTA KPIs | 137-row structured table extracted from 3 PDF progress reports |
| Visualisation | Tableau Desktop → Tableau Cloud (live PostgreSQL connection) |
| Schedule reference | GTFS Static — 210,440 trips loaded for delay calculations |

### Database Schema

**trip_observations** — core table. One row per trip per stop per poll.
Fields: `trip_id`, `route_id`, `stop_id`, `stop_sequence`, `service_date`, `operator`, `reported_delay_seconds`, `spine`, `first_seen_at`, `last_seen_at`

**ghost_bus_candidates** — trips that disappeared before 80% journey completion.
Detected by: last seen > 15 mins ago, first seen within 3 hours, stop sequence < 80% of expected total stops.

**nta_kpis** — 137 rows across July 2024, March 2025, December 2025 NTA reports.
Metrics: passenger boardings, punctuality %, lost km %, satisfaction score — by spine, with 2019 baseline.

---

## Scope

- **160 routes** — 116 Dublin Bus + 44 Go-Ahead Dublin city operations
- **Bus Éireann excluded** — intercounty operator, different network scope
- **Collection period** — February 22 to March 2, 2026
- **Polling frequency** — every 60 seconds via automated cron on Oracle A1

---

## A Note on Methodology

**Ghost bus percentages** require care. Go-Ahead rotates trip_ids almost entirely daily (1% reuse across days vs Dublin Bus 3.9–7.6%). Ghost rates as a percentage of unique trip_ids are inflated for Go-Ahead. Valid metrics are: absolute ghost counts per route, and average journey completion % at point of feed disappearance. Like-for-like comparisons use legacy routes only.

**Delay measurements** use `first_seen_at` timestamp in `Europe/Dublin` timezone for time-of-day bucketing. `scheduled_arrival_secs` is a Unix timestamp and 95% null in the feed — not used for delay calculations.

**NTA comparison** is descriptive only. The NTA reports phase-level aggregates — no operator-level or route-level breakdowns are published. Statistical inference is reserved for independent GTFS-R measurements where sample sizes are sufficient.

---

## Context

BusConnects is mid-rollout. Phase 7 launched October 2025. Phase 8 is planned for 2026. The first Core Bus Corridor (Ballymun/Finglas) heads to construction in 2026. No independent data audit of the programme exists — the NTA self-reports its own KPIs.

This project started after a LinkedIn post about ghost buses at UCD Village got 3,200 impressions and 45 reactions in 10 hours. Apparently other people had noticed too.

---

## Files

```
collect.py          — GTFS-R polling script (trip updates + vehicle positions)
detect_ghosts.py    — Ghost bus detection logic (runs every 30 mins)
scripts/
  nta_kpis.csv      — Structured NTA KPI data extracted from 3 progress reports
  import_kpis.py    — CSV → PostgreSQL loader
```

---

*Satvik Kumar · MSc Business Analytics, UCD Dublin · 2026*