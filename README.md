# BlueBikes Data Analysis

This repository contains tools for downloading, processing, and analyzing [BlueBikes](https://www.bluebikes.com/) bikeshare data, with a focus on Newton, Massachusetts stations. Trip data is sourced from the official [BlueBikes System Data](https://bluebikes.com/system-data) page.

This data is packaged as a Claude Code skill to allow AI Agents to search and query this data based on natural language questions. The trip data is stored in parquet files for maximum compression and efficiency. The data includes trips from 2024 and 2025.

**Repository:** https://github.com/NewtonCivicDataHackers/bluebikes-data

**Download latest skill:** https://github.com/NewtonCivicDataHackers/bluebikes-data/releases/latest/download/bluebikes-data-skill.zip

## Installing as a Claude Code Skill

To use this skill with [Claude Code](https://claude.ai/code), download and install to your personal skills directory:

```bash
curl -L -o /tmp/bluebikes-data-skill.zip https://github.com/NewtonCivicDataHackers/bluebikes-data/releases/latest/download/bluebikes-data-skill.zip
unzip -o /tmp/bluebikes-data-skill.zip -d ~/.claude/skills/
```

Or install as a project-level skill (shared with team via git):

```bash
curl -L -o /tmp/bluebikes-data-skill.zip https://github.com/NewtonCivicDataHackers/bluebikes-data/releases/latest/download/bluebikes-data-skill.zip
unzip -o /tmp/bluebikes-data-skill.zip -d .claude/skills/
```

Verify the skill is loaded by asking Claude "What skills are available?" or invoking it directly with `/bluebikes-data-skill`.

## Table of Contents

- [Installing as a Claude Code Skill](#installing-as-a-claude-code-skill)
- [Scripts](#scripts)
- [Requirements & Setup](#requirements--setup)
  - [Installing uv](#installing-uv)
- [Basic Usage](#basic-usage)
  - [Download and Process Data](#download-and-process-data)
  - [Extract Station Information](#extract-station-information)
  - [Analyze Trip Data](#analyze-trip-data)
- [Complex Analysis Pipeline](#complex-analysis-pipeline)
- [Understanding the Output](#understanding-the-output)
  - [Common Column Naming Conventions](#common-column-naming-conventions)
  - [Station Analysis Output](#station-analysis-output--stations)
  - [Station Pair Analysis Output](#station-pair-analysis-output--station-pairs)
  - [Field Relationships and Interpretations](#field-relationships-and-interpretations)
- [Data Notes](#data-notes)

## Scripts

All analysis scripts are in the `scripts/` directory:

- **download.py**: Downloads and processes BlueBikes trip data from S3 storage
- **extract_stations.py**: Extracts station information from trip data (IDs, names, coordinates)
- **filter_newton.py**: Filters trip data to only include trips involving Newton stations
- **aggregate.py**: Analyzes trip data with two modes:
  - `--stations`: Analyzes metrics for individual stations (departures/arrivals)
  - `--station-pairs`: Analyzes metrics between station pairs in both directions

## Requirements & Setup

Each script uses [PEP 723](https://peps.python.org/pep-0723/) metadata blocks to specify its requirements. We recommend using [uv](https://github.com/astral-sh/uv), a fast Python package installer and resolver.

### Installing uv

```bash
# Install uv using curl
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or using pip
pip install uv
```

The required packages will be automatically installed when running scripts with uv.

## Basic Usage

### Download and Process Data

```bash
# Download data for a specific month
uv run scripts/download.py 2024-09 > data/2024-09.csv

# Download data for the whole year
uv run scripts/download.py 2024 | gzip > data/2024.csv.gz
```

### Extract Station Information

```bash
# Extract all stations from trip data
gunzip -c data/2024.csv.gz | uv run scripts/extract_stations.py > data/stations-2024.csv
```

### Analyze Trip Data

```bash
# Analyze station-level metrics for all stations
gunzip -c data/2024.csv.gz | uv run scripts/aggregate.py --stations > data/counts-2024.csv

# Analyze station-pair metrics for all stations
gunzip -c data/2024.csv.gz | uv run scripts/aggregate.py --station-pairs > data/counts-pairs-2024.csv

# Filter and analyze only Newton stations
gunzip -c data/2024.csv.gz | uv run scripts/filter_newton.py | uv run scripts/aggregate.py --stations > data/newton-counts-2024.csv

# Filter and analyze trips between Newton station pairs
gunzip -c data/2024.csv.gz | uv run scripts/filter_newton.py | uv run scripts/aggregate.py --station-pairs > data/newton-counts-pairs-2024.csv
```

## Complex Analysis Pipeline

```bash
# Download, process, filter and analyze data for specific months
uv run scripts/download.py 2024-06 2024-12 | uv run scripts/filter_newton.py | uv run scripts/aggregate.py --stations > data/newton-counts-2024-H2.csv

# Analyze station pairs for the same data
uv run scripts/download.py 2024-06 2024-12 | uv run scripts/filter_newton.py | uv run scripts/aggregate.py --station-pairs > data/newton-counts-pairs-2024-H2.csv
```

## Understanding the Output

### Common Column Naming Conventions

The output tables use consistent naming conventions to organize metrics:

- **Direction Suffixes:**
  - `_fwd`: Forward direction (departures from a station, or A→B in station pairs)
  - `_rev`: Reverse direction (arrivals to a station, or B→A in station pairs)
  - `_bidir`: Bidirectional (combined stats for both directions)

- **Metric Types:**
  - `_avg`: Average values (like average trip duration)
  - `_percent`: Percentage values (like e-bike usage percentage)
  - No suffix: Raw counts or direct measurements

- **Human vs. Machine Fields:**
  - Columns without direction suffixes (e.g., `trip_count`) provide human-readable composite metrics
  - Individual directional columns (e.g., `trip_count_fwd`) are better for filtering and analysis

- **Most Useful Fields:**
  - The `_bidir` fields are generally most useful for cross-station comparisons
  - For station pairs, `_bidir` fields eliminate bias from fixed pair ordering

### Station Analysis Output (--stations)

The station analysis produces a table with each row representing a station and columns for various metrics.

#### Output Columns

| Column Group | Field Examples | Description |
|--------------|----------------|-------------|
| Station Info | `station_id`, `station_name`, `municipality`, `latitude`, `longitude` | Basic station information and location |
| Trip Counts | `trip_count_fwd`, `trip_count_rev`, `trip_count_bidir`, `trip_count` | Number of trips starting from, ending at, or involving the station |
| E-Bike Usage | `electric_bike_percent_fwd`, `electric_bike_percent_rev`, `electric_bike_percent_bidir` | Percentage of trips that used electric bikes |
| Duration Metrics | `duration_avg_fwd`, `duration_avg_rev`, `duration_avg_bidir` | Average trip duration in minutes |
| E-Bike Duration | `electric_bike_duration_avg_fwd`, `electric_bike_duration_avg_rev`, `electric_bike_duration_avg_bidir` | Average duration of just e-bike trips |
| Classic Bike Duration | `classic_bike_duration_avg_fwd`, `classic_bike_duration_avg_rev`, `classic_bike_duration_avg_bidir` | Average duration of just classic bike trips |

#### Example Row (Simplified)

```
station_id,station_name,municipality,trip_count_bidir,trip_count,electric_bike_percent_bidir,duration_avg_bidir
N32901,Newton: Waban Station,Newton,583,583 (F: 290 / R: 293),53.0,17.5
```

This shows station N32901 (Waban Station in Newton) had 583 total trips (290 departures, 293 arrivals), with 53% using e-bikes, and an average trip duration of 17.5 minutes.

### Station Pair Analysis Output (--station-pairs)

The station pair analysis produces a table with each row representing a station pair (A→B) with metrics for trips between them.

#### Output Columns

| Column Group | Field Examples | Description |
|--------------|----------------|-------------|
| Station Pair Info | `start_station`, `end_station`, `start_station_name`, `end_station_name`, `start_municipality`, `end_municipality` | Information about both stations in the pair |
| Coordinates | `start_lat`, `start_lng`, `end_lat`, `end_lng` | Location coordinates for mapping |
| Trip Counts | `trip_count_fwd`, `trip_count_rev`, `trip_count_bidir`, `trip_count` | Number of trips from A→B, B→A, and total in both directions |
| E-Bike Usage | `electric_bike_percent_fwd`, `electric_bike_percent_rev`, `electric_bike_percent_bidir` | Percentage of trips that used electric bikes |
| Duration Metrics | `duration_avg_fwd`, `duration_avg_rev`, `duration_avg_bidir` | Average trip duration in minutes |
| E-Bike Duration | `electric_bike_duration_avg_fwd`, `electric_bike_duration_avg_rev`, `electric_bike_duration_avg_bidir` | Average duration of just e-bike trips |
| Classic Bike Duration | `classic_bike_duration_avg_fwd`, `classic_bike_duration_avg_rev`, `classic_bike_duration_avg_bidir` | Average duration of just classic bike trips |

#### Example Row (Simplified)

```
start_station,end_station,start_station_name,end_station_name,start_municipality,end_municipality,trip_count_bidir,trip_count,electric_bike_percent_bidir
N32901,A32008,Newton: Waban Station,Boston: Harvard Square,Newton,Boston,45,45 (F: 28 / R: 17),62.0
```

This shows trips between Waban Station (Newton) and Harvard Square (Boston). There were 45 total trips between these stations (28 from Waban to Harvard, 17 from Harvard to Waban), with 62% using e-bikes.

### Field Relationships and Interpretations

#### Direction Fields

- `_fwd`: For stations, measures trips departing from the station; for station pairs, measures trips from A→B
- `_rev`: For stations, measures trips arriving at the station; for station pairs, measures trips from B→A
- `_bidir`: The combined total or weighted average of both directions
  
#### Duration Metrics Relationship

The duration fields have these relationships:
- `duration_avg_*` reflects the overall average trip duration
- `electric_bike_duration_avg_*` is the average duration for just e-bike trips
- `classic_bike_duration_avg_*` is the average duration for just classic bike trips

These duration metrics can be used to compare how trip durations differ between e-bikes and classic bikes. For example, e-bike trips are often longer in distance but might have similar or shorter durations.

#### Human-Readable vs. Analysis Fields

- Human-readable fields (e.g., `trip_count`) combine multiple metrics into a friendly format:
  ```
  "583 (F: 290 / R: 293)"
  ```
  
- Analysis fields (e.g., `trip_count_bidir`, `trip_count_fwd`, `trip_count_rev`) provide raw numbers for data processing:
  ```
  583,290,293
  ```

When working with the data:
- Use the raw fields for filtering, sorting, and calculations
- The `_bidir` fields are best for comparing across stations/pairs since they're direction-agnostic
- The formatted fields are helpful for quick human review of the data

## Data Notes

- The output includes explicit `municipality` fields for each station, making it easy to filter and analyze by location
- The BlueBikes system uses station ID prefixes to indicate municipalities 
- Station location is also available via the included latitude/longitude coordinates
- Data from BlueBikes API is available from April 2023 onwards in this format