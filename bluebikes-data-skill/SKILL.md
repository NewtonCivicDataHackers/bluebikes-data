---
name: bluebikes-data-skill
description: Query and analyze BlueBikes bikeshare trip data. Use when the user asks about bike trips, stations, ridership patterns, commuter routes, or bikeshare statistics in Boston, Cambridge, Somerville, Brookline, Newton, and surrounding Massachusetts communities.
---

# BlueBikes Data Analysis

This skill enables querying BlueBikes bikeshare data stored in Parquet files.

## Data Files

Look in `assets/` for Parquet files following this naming convention:

- `{year}-trips.parquet` — Individual trip records
- `{year}-stations.parquet` — Station-level aggregated statistics
- `{year}-station-pairs.parquet` — Station pair aggregated statistics

Example: `2025-trips.parquet`, `2025-stations.parquet`, `2025-station-pairs.parquet`

## Schemas

### trips.parquet

Individual trip records with timestamps.

| Column | Type | Description |
|--------|------|-------------|
| rideable_type | VARCHAR | `classic_bike` or `electric_bike` |
| started_at | TIMESTAMP | Trip start time |
| ended_at | TIMESTAMP | Trip end time |
| start_station_name | VARCHAR | Starting station name with municipality prefix |
| start_station_id | VARCHAR | Starting station ID (prefix indicates municipality) |
| start_municipality | VARCHAR | Starting municipality name |
| end_station_name | VARCHAR | Ending station name with municipality prefix |
| end_station_id | VARCHAR | Ending station ID |
| end_municipality | VARCHAR | Ending municipality name |
| member_casual | VARCHAR | `member` or `casual` |
| duration_minutes | INTEGER | Trip duration in minutes |

### stations.parquet

Aggregated statistics per station.

| Column | Type | Description |
|--------|------|-------------|
| station_id | VARCHAR | Station identifier |
| station_name | VARCHAR | Station name with municipality prefix |
| municipality | VARCHAR | Municipality name |
| latitude | DOUBLE | Station latitude |
| longitude | DOUBLE | Station longitude |
| trip_count_fwd | INTEGER | Departures from station |
| trip_count_rev | INTEGER | Arrivals to station |
| trip_count_bidir | INTEGER | Total trips (departures + arrivals) |
| electric_bike_percent_bidir | DOUBLE | Percentage of e-bike trips |
| duration_avg_bidir | DOUBLE | Average trip duration in minutes |

### station-pairs.parquet

Aggregated statistics for routes between station pairs.

| Column | Type | Description |
|--------|------|-------------|
| start_station | VARCHAR | Origin station ID |
| end_station | VARCHAR | Destination station ID |
| start_station_name | VARCHAR | Origin station name |
| end_station_name | VARCHAR | Destination station name |
| start_municipality | VARCHAR | Origin municipality |
| end_municipality | VARCHAR | Destination municipality |
| start_lat, start_lng | DOUBLE | Origin coordinates |
| end_lat, end_lng | DOUBLE | Destination coordinates |
| trip_count_fwd | INTEGER | Trips from start to end |
| trip_count_rev | INTEGER | Trips from end to start |
| trip_count_bidir | INTEGER | Total trips both directions |
| electric_bike_percent_bidir | DOUBLE | E-bike percentage |
| duration_avg_bidir | DOUBLE | Average duration |

## Station ID Prefixes

The first letter of station_id indicates municipality:

- `A-H` — Boston
- `K` — Brookline
- `L` — Lexington
- `M` — Cambridge
- `N` — Newton
- `R` — Revere
- `S` — Somerville
- `T` — Salem
- `V` — Medford
- `W` — Watertown

## Querying with DuckDB

DuckDB can query Parquet files directly from the command line:

```bash
duckdb -c "SELECT * FROM 'assets/2025-trips.parquet' LIMIT 10;"
```

### Common Queries

**Trip counts by municipality:**
```sql
SELECT start_municipality, COUNT(*) as trips
FROM 'assets/2025-trips.parquet'
GROUP BY start_municipality
ORDER BY trips DESC;
```

**Busiest stations:**
```sql
SELECT station_name, municipality, trip_count_bidir as trips
FROM 'assets/2025-stations.parquet'
ORDER BY trips DESC
LIMIT 20;
```

**Routes between two municipalities:**
```sql
SELECT start_station_name, end_station_name, COUNT(*) as trips
FROM 'assets/2025-trips.parquet'
WHERE start_municipality = 'Newton' AND end_municipality = 'Boston'
GROUP BY start_station_name, end_station_name
ORDER BY trips DESC
LIMIT 10;
```

**Hourly patterns:**
```sql
SELECT HOUR(started_at) as hour, COUNT(*) as trips
FROM 'assets/2025-trips.parquet'
WHERE start_station_name LIKE '%Harvard Square%'
GROUP BY hour
ORDER BY hour;
```

**Monthly trends:**
```sql
SELECT MONTHNAME(started_at) as month, COUNT(*) as trips
FROM 'assets/2025-trips.parquet'
GROUP BY MONTH(started_at), MONTHNAME(started_at)
ORDER BY MONTH(started_at);
```

**Member vs casual breakdown:**
```sql
SELECT member_casual, COUNT(*) as trips,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct
FROM 'assets/2025-trips.parquet'
GROUP BY member_casual;
```

**Round trips (same start and end station):**
```sql
SELECT start_station_name, COUNT(*) as round_trips
FROM 'assets/2025-trips.parquet'
WHERE start_station_id = end_station_id
GROUP BY start_station_name
ORDER BY round_trips DESC
LIMIT 10;
```

**Bidirectional route analysis (deduplicated pairs):**
```sql
SELECT
    LEAST(start_station_name, end_station_name) as station_a,
    GREATEST(start_station_name, end_station_name) as station_b,
    COUNT(*) as total_trips
FROM 'assets/2025-trips.parquet'
WHERE start_station_id != end_station_id
GROUP BY station_a, station_b
ORDER BY total_trips DESC
LIMIT 20;
```

## Python with DuckDB

For complex analysis, use Python with DuckDB. Use `uv run` with PEP 723 inline metadata:

```python
#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = ["duckdb"]
# ///

import duckdb

conn = duckdb.connect()
result = conn.execute("""
    SELECT start_municipality, end_municipality, COUNT(*) as trips
    FROM 'assets/2025-trips.parquet'
    GROUP BY start_municipality, end_municipality
    ORDER BY trips DESC
    LIMIT 20
""").fetchdf()

print(result)
```

Run with: `uv run script.py`

## Python with Polars

For dataframe operations, use Polars:

```python
#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = ["polars"]
# ///

import polars as pl

trips = pl.read_parquet("assets/2025-trips.parquet")

# Filter Newton trips and analyze
newton_trips = trips.filter(
    (pl.col("start_municipality") == "Newton") |
    (pl.col("end_municipality") == "Newton")
)

print(f"Newton trips: {len(newton_trips)}")
print(newton_trips.group_by("member_casual").len())
```

## Tips

- Use `LIKE '%pattern%'` for partial station name matching
- Station names include municipality prefix: `"Newton: Waban Station"`
- Use `trip_count_bidir` for direction-agnostic comparisons
- The `_fwd` suffix means departures or A→B direction
- The `_rev` suffix means arrivals or B→A direction
- E-bike trips tend to be longer distances but similar duration to classic bikes
- Round trips (same start/end) indicate recreational use
