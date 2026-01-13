#!/usr/bin/env python3
"""
Parquet to CSV Converter

Converts Parquet files to CSV format for use with tools like kepler.gl
that don't support Parquet directly.

Usage:
    uv run scripts/parquet2csv.py data.parquet > data.csv
    uv run scripts/parquet2csv.py data.parquet -o data.csv

Examples:
    uv run scripts/parquet2csv.py bluebikes-data-skill/assets/2025-stations.parquet > stations.csv
    uv run scripts/parquet2csv.py bluebikes-data-skill/assets/2025-station-pairs.parquet -o routes.csv
"""

# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "pyarrow",
# ]
# ///

import argparse
import sys
import pyarrow.parquet as pq
import pyarrow.csv as csv


def main():
    parser = argparse.ArgumentParser(
        description='Convert Parquet files to CSV format.'
    )
    parser.add_argument(
        'input',
        help='Input Parquet file path'
    )
    parser.add_argument(
        '-o', '--output',
        help='Output CSV file path (default: stdout)'
    )

    args = parser.parse_args()

    # Read parquet file
    table = pq.read_table(args.input)

    # Write to output
    if args.output:
        csv.write_csv(table, args.output)
    else:
        # Write to stdout
        csv.write_csv(table, sys.stdout.buffer)


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        pass  # Handle piping to head/less gracefully
