#!/usr/bin/env python3
"""
BlueBikes Trip Data Augmenter

This script enriches BlueBikes trip data with additional fields without aggregating.
It adds municipality information and formatted station names while preserving
individual trip records and timestamps.

Usage:
    cat data/trip_data.csv | python scripts/augment.py > data/augmented_trips.csv
    cat data/trip_data.csv | python scripts/augment.py -o data/trips.parquet

Features:
    - Adds start_municipality and end_municipality columns
    - Formats station names with municipality prefix
    - Normalizes bike types (docked_bike -> classic_bike)
    - Preserves all original fields including timestamps
    - Supports CSV and Parquet output formats
"""

# /// script
# requires-python = ">=3.6"
# dependencies = [
#   "pyarrow",
# ]
# ///

import csv
import sys
import argparse


def get_municipality(station_id):
    """
    Get municipality name from station ID prefix.

    Args:
        station_id (str): Station identifier

    Returns:
        str: Municipality name or empty string if not identified
    """
    if not station_id:
        return ''

    prefix = station_id[0].upper()

    municipalities = {
        'V': 'Medford',
        'W': 'Watertown',
        'T': 'Salem',
        'K': 'Brookline',
        'L': 'Lexington',
        'M': 'Cambridge',
        'N': 'Newton',
        'R': 'Revere',
        'S': 'Somerville'
    }

    if prefix >= 'A' and prefix <= 'H':
        return 'Boston'

    return municipalities.get(prefix, '')


def format_station_name(station_id, station_name):
    """
    Format station name with municipality prefix.

    Args:
        station_id (str): Station identifier
        station_name (str): Original station name

    Returns:
        str: Formatted station name with municipality prefix
    """
    if not station_id or not station_name:
        return station_name
    municipality = get_municipality(station_id)
    if municipality:
        return f"{municipality}: {station_name}"
    return station_name


def normalize_bike_type(bike_type):
    """
    Normalize bike type names.

    Args:
        bike_type (str): Original bike type

    Returns:
        str: Normalized bike type
    """
    return 'classic_bike' if bike_type in ['docked_bike', 'classic_bike'] else bike_type


def augment_row(row):
    """
    Augment a single trip record with additional fields.

    Args:
        row (dict): Original trip record

    Returns:
        dict: Augmented trip record
    """
    augmented = dict(row)

    # Add municipalities
    start_id = row.get('start_station_id', '')
    end_id = row.get('end_station_id', '')
    augmented['start_municipality'] = get_municipality(start_id)
    augmented['end_municipality'] = get_municipality(end_id)

    # Format station names
    augmented['start_station_name'] = format_station_name(
        start_id, row.get('start_station_name', '')
    )
    augmented['end_station_name'] = format_station_name(
        end_id, row.get('end_station_name', '')
    )

    # Normalize bike type
    if 'rideable_type' in augmented:
        augmented['rideable_type'] = normalize_bike_type(augmented['rideable_type'])

    return augmented


def write_output(rows, fieldnames, output_path):
    """
    Write output data to file or stdout.

    Args:
        rows (list): List of row dictionaries
        fieldnames (list): Column names in order
        output_path (str): Output file path, or None for stdout
    """
    if output_path and output_path.endswith('.parquet'):
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Columns to exclude from parquet output (can be looked up from stations file)
        exclude_cols = {'ride_id', 'start_lat', 'start_lng', 'end_lat', 'end_lng'}
        fieldnames = [f for f in fieldnames if f not in exclude_cols]

        # Define column types for parquet
        timestamp_cols = {'started_at', 'ended_at'}
        int_cols = {'duration_minutes'}

        columns = {}
        for field in fieldnames:
            values = [row.get(field, '') for row in rows]
            if field in timestamp_cols:
                # Convert string timestamps to datetime then to pyarrow
                from datetime import datetime
                dt_values = [
                    datetime.strptime(v, '%Y-%m-%d %H:%M:%S') if v else None
                    for v in values
                ]
                columns[field] = pa.array(dt_values, type=pa.timestamp('us'))
            elif field in int_cols:
                # Convert to integers
                columns[field] = pa.array(
                    [int(v) if v else None for v in values],
                    type=pa.int32()
                )
            else:
                columns[field] = pa.array(values, type=pa.string())

        table = pa.Table.from_pydict(columns)
        pq.write_table(table, output_path, compression='zstd', compression_level=19)
    else:
        output_file = open(output_path, 'w', newline='') if output_path else sys.stdout
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        if output_path:
            output_file.close()


def main():
    parser = argparse.ArgumentParser(
        description='Augment BlueBikes trip data with municipality and formatted names.'
    )
    parser.add_argument(
        '-o', '--output',
        help='Output file path. Format detected from extension (.csv or .parquet). Defaults to stdout as CSV.'
    )

    args = parser.parse_args()

    reader = csv.DictReader(sys.stdin)

    # Build output fieldnames: original fields + new fields
    original_fields = list(reader.fieldnames)
    new_fields = ['start_municipality', 'end_municipality']
    fieldnames = []
    for field in original_fields:
        fieldnames.append(field)
        # Insert municipality fields after station_id fields
        if field == 'start_station_id':
            fieldnames.append('start_municipality')
        elif field == 'end_station_id':
            fieldnames.append('end_municipality')

    # Remove duplicates if new_fields were already added
    seen = set()
    fieldnames = [f for f in fieldnames if not (f in seen or seen.add(f))]

    # Process all rows
    output_rows = [augment_row(row) for row in reader]

    write_output(output_rows, fieldnames, args.output)


if __name__ == "__main__":
    main()
