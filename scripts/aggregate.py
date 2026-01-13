#!/usr/bin/env python3
"""
BlueBikes Data Aggregator

This script analyzes BlueBikes bikeshare data, performing either station-level
or station-pair analysis based on command-line arguments.

Usage:
    cat data/trip_data.csv | python scripts/aggregate.py --stations > data/station_stats.csv
    cat data/trip_data.csv | python scripts/aggregate.py --station-pairs > data/station_pairs_stats.csv
    
Features:
    - Station analysis: statistics per station (arrivals/departures)
    - Station-pair analysis: statistics between station pairs
    - Command-line options to select analysis type
    - Comprehensive metrics including trip counts, e-bike percentages, and durations
"""

# /// script
# requires-python = ">=3.6"
# dependencies = [
#   "argparse",
#   "pyarrow",
# ]
# ///

import csv
import sys
import argparse
from collections import defaultdict

def normalize_bike_type(bike_type):
    """
    Convert variant bike type names to consistent format.
    
    Args:
        bike_type (str): Original bike type from data
        
    Returns:
        str: Normalized bike type ('classic_bike' or 'electric_bike')
    """
    return 'classic_bike' if bike_type in ['docked_bike', 'classic_bike'] else bike_type

def sort_id_by_n_and_alpha(station_id):
    """
    Sort key function: Newton (N) stations first, then alphabetical.
    
    This creates a custom sort order that prioritizes Newton stations
    by placing them at the top of the sorted results.
    
    Args:
        station_id (str): Station identifier
        
    Returns:
        tuple: Tuple for sorting (boolean for N-prefix, station_id)
    """
    return (not station_id.startswith('N'), station_id)

def get_municipality(station_id):
    """
    Get municipality name from station ID prefix.
    
    BlueBikes station IDs follow a pattern where the first letter
    indicates the municipality.
    
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
    
    Creates a display name with the municipality prefixed to the station name.
    
    Args:
        station_id (str): Station identifier
        station_name (str): Original station name
        
    Returns:
        str: Formatted station name with municipality prefix, or original name
    """
    if not station_id or not station_name:
        return station_name
    municipality = get_municipality(station_id)
    if municipality:
        return f"{municipality}: {station_name}"
    return station_name

def compute_station_coordinates():
    """
    First pass: compute average coordinates for each station.
    
    Many trip records have slightly different coordinate values for the same station.
    This function computes the average lat/lng for each station for more accurate
    and consistent location data.
    
    Returns:
        tuple: (avg_coords, rows)
            - avg_coords: Dictionary mapping station IDs to their average coordinates
            - rows: Original data rows for further processing
    """
    station_coords = defaultdict(lambda: {'lat_sum': 0.0, 'lng_sum': 0.0, 'count': 0})
    
    reader = csv.DictReader(sys.stdin)
    rows = list(reader)  # Store rows for second pass
    
    for row in rows:
        # Process start station
        if row.get('start_station_id') and row.get('start_lat') and row.get('start_lng'):
            station_id = row['start_station_id']
            try:
                station_coords[station_id]['lat_sum'] += float(row['start_lat'])
                station_coords[station_id]['lng_sum'] += float(row['start_lng'])
                station_coords[station_id]['count'] += 1
            except ValueError:
                pass

        # Process end station
        if row.get('end_station_id') and row.get('end_lat') and row.get('end_lng'):
            station_id = row['end_station_id']
            try:
                station_coords[station_id]['lat_sum'] += float(row['end_lat'])
                station_coords[station_id]['lng_sum'] += float(row['end_lng'])
                station_coords[station_id]['count'] += 1
            except ValueError:
                pass
    
    # Compute averages
    avg_coords = {}
    for station_id, sums in station_coords.items():
        if sums['count'] > 0:
            avg_coords[station_id] = {
                'latitude': f"{sums['lat_sum'] / sums['count']:.5f}",
                'longitude': f"{sums['lng_sum'] / sums['count']:.5f}"
            }
    
    return avg_coords, rows

def format_metric(bidir, fwd, rev, suffix="", round_digits=1):
    """
    Format a metric with its directional components.
    
    Creates a human-readable string showing bidirectional, forward, and reverse values.
    
    Args:
        bidir (float): Combined bidirectional value
        fwd (float): Forward value (departures or A to B direction)
        rev (float): Reverse value (arrivals or B to A direction)
        suffix (str): Optional suffix for values (like "%" or "min")
        round_digits (int): Number of decimal places to round to
        
    Returns:
        str: Formatted string with all three values
    """
    if suffix:
        return f"{bidir:.{round_digits}f}{suffix} (F: {fwd:.{round_digits}f}{suffix} / R: {rev:.{round_digits}f}{suffix})"
    return f"{bidir:.{round_digits}f} (F: {fwd:.{round_digits}f} / R: {rev:.{round_digits}f})"

def compute_weighted_average(total_sum, count):
    """
    Compute weighted average, handling zero division.

    Args:
        total_sum (float): Sum of values
        count (int): Number of items

    Returns:
        float: Computed average or 0.0 if count is zero
    """
    return total_sum / count if count > 0 else 0.0

def write_output(rows, fieldnames, output_path):
    """
    Write output data to file or stdout.

    Format is detected from file extension:
    - .parquet: Write as Parquet file using pyarrow
    - .csv or None: Write as CSV

    Args:
        rows (list): List of row dictionaries
        fieldnames (list): Column names in order
        output_path (str): Output file path, or None for stdout
    """
    if output_path and output_path.endswith('.parquet'):
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Define column types for parquet
        float_cols = {'latitude', 'longitude', 'start_lat', 'start_lng', 'end_lat', 'end_lng'}

        columns = {}
        for field in fieldnames:
            values = [row.get(field, '') for row in rows]
            if field in float_cols:
                # Convert coordinate strings to floats
                columns[field] = pa.array(
                    [float(v) if v else None for v in values],
                    type=pa.float64()
                )
            else:
                # Let pyarrow infer type from Python values
                columns[field] = values

        table = pa.Table.from_pydict(columns)
        pq.write_table(table, output_path, compression='zstd', compression_level=19)
    else:
        # Write CSV to file or stdout
        output_file = open(output_path, 'w', newline='') if output_path else sys.stdout
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        if output_path:
            output_file.close()

# Station stats class for station-level analysis
class StationStats:
    """
    Track station statistics with separate tallies for departures and arrivals.
    
    This class maintains counts and durations for trips starting from (forward)
    and ending at (reverse) a station, separating classic vs. electric bikes.
    """
    def __init__(self):
        # Trip counts
        self.trip_count_fwd = 0  # departures (trips starting at this station)
        self.trip_count_rev = 0  # arrivals (trips ending at this station)
        
        # Electric bike counts
        self.electric_count_fwd = 0  # electric bike departures
        self.electric_count_rev = 0  # electric bike arrivals
        
        # Duration totals
        self.total_duration_fwd = 0  # total duration of all departing trips
        self.total_duration_rev = 0  # total duration of all arriving trips
        self.electric_duration_fwd = 0  # total duration of departing e-bike trips
        self.electric_duration_rev = 0  # total duration of arriving e-bike trips
        self.classic_duration_fwd = 0  # total duration of departing classic bike trips
        self.classic_duration_rev = 0  # total duration of arriving classic bike trips
        
        # Station info
        self.station_name = ''
        self.latitude = ''
        self.longitude = ''

# Trip stats class for station-pair analysis
class TripStats:
    """
    Track trip statistics with separate tallies for each bike type.
    
    This class maintains counts and durations for trips between station pairs,
    separating statistics by bike type (classic vs. electric).
    """
    def __init__(self):
        self.trip_count = 0          # Total number of trips
        self.electric_count = 0      # Number of electric bike trips
        self.total_duration = 0      # Total duration of all trips in minutes
        self.electric_duration = 0   # Total duration of electric bike trips
        self.classic_duration = 0    # Total duration of classic bike trips

def analyze_stations(avg_coords, rows):
    """
    Main function to analyze station usage data.
    
    This function:
    1. Processes trip data to track statistics for each station
    2. Calculates metrics like e-bike percentages and average durations
    3. Outputs a comprehensive CSV of station statistics
    
    Args:
        avg_coords (dict): Dictionary of station IDs to average coordinates
        rows (list): List of trip data rows
    """
    # Initialize data structure for collecting statistics
    stations = defaultdict(StationStats)
    
    # Process rows
    for row in rows:
        start_id = row.get('start_station_id', '')
        end_id = row.get('end_station_id', '')
        
        # Skip invalid trips
        if not start_id or not end_id or start_id == end_id:
            continue
        if start_id not in avg_coords or end_id not in avg_coords:
            continue
            
        try:
            duration = float(row.get('duration_minutes', 0))
            bike_type = normalize_bike_type(row.get('rideable_type', ''))
        except ValueError:
            continue
            
        # Update departure (forward) station statistics
        start_stats = stations[start_id]
        if not start_stats.station_name:
            start_stats.station_name = format_station_name(start_id, row.get('start_station_name', ''))
            start_stats.latitude = avg_coords[start_id]['latitude']
            start_stats.longitude = avg_coords[start_id]['longitude']
        
        start_stats.trip_count_fwd += 1
        start_stats.total_duration_fwd += duration
        if bike_type == 'electric_bike':
            start_stats.electric_count_fwd += 1
            start_stats.electric_duration_fwd += duration
        else:
            start_stats.classic_duration_fwd += duration
            
        # Update arrival (reverse) station statistics
        end_stats = stations[end_id]
        if not end_stats.station_name:
            end_stats.station_name = format_station_name(end_id, row.get('end_station_name', ''))
            end_stats.latitude = avg_coords[end_id]['latitude']
            end_stats.longitude = avg_coords[end_id]['longitude']
        
        end_stats.trip_count_rev += 1
        end_stats.total_duration_rev += duration
        if bike_type == 'electric_bike':
            end_stats.electric_count_rev += 1
            end_stats.electric_duration_rev += duration
        else:
            end_stats.classic_duration_rev += duration
    
    # Define output fields
    fieldnames = [
        # Station information
        'station_id', 'station_name', 'municipality', 'latitude', 'longitude',
        # Trip counts
        'trip_count_fwd', 'trip_count_rev', 'trip_count_bidir', 'trip_count',
        # Electric bike percentages
        'electric_bike_percent_fwd', 'electric_bike_percent_rev', 
        'electric_bike_percent_bidir', 'electric_bike_percent',
        # Duration averages
        'duration_avg_fwd', 'duration_avg_rev', 'duration_avg_bidir', 'duration_avg',
        # Electric bike duration averages
        'electric_bike_duration_avg_fwd', 'electric_bike_duration_avg_rev',
        'electric_bike_duration_avg_bidir', 'electric_bike_duration_avg',
        # Classic bike duration averages
        'classic_bike_duration_avg_fwd', 'classic_bike_duration_avg_rev',
        'classic_bike_duration_avg_bidir', 'classic_bike_duration_avg'
    ]
    
    # Collect output rows
    output_rows = []

    # Sort and process data
    sorted_stations = sorted(stations.items())

    for station_id, stats in sorted_stations:
        trip_count_bidir = stats.trip_count_fwd + stats.trip_count_rev
        total_electric = stats.electric_count_fwd + stats.electric_count_rev
        
        # Skip if no trips
        if trip_count_bidir == 0:
            continue
            
        # Compute percentages and averages
        fwd_e_pct = (stats.electric_count_fwd / stats.trip_count_fwd * 100) if stats.trip_count_fwd > 0 else 0
        rev_e_pct = (stats.electric_count_rev / stats.trip_count_rev * 100) if stats.trip_count_rev > 0 else 0
        bidir_e_pct = (total_electric / trip_count_bidir * 100)
        
        # Compute duration averages
        duration_fwd = compute_weighted_average(stats.total_duration_fwd, stats.trip_count_fwd)
        duration_rev = compute_weighted_average(stats.total_duration_rev, stats.trip_count_rev)
        duration_bidir = compute_weighted_average(
            stats.total_duration_fwd + stats.total_duration_rev, 
            trip_count_bidir
        )
        
        # Compute e-bike duration averages
        e_duration_fwd = compute_weighted_average(stats.electric_duration_fwd, stats.electric_count_fwd)
        e_duration_rev = compute_weighted_average(stats.electric_duration_rev, stats.electric_count_rev)
        e_duration_bidir = compute_weighted_average(
            stats.electric_duration_fwd + stats.electric_duration_rev, 
            total_electric
        )
        
        # Compute classic bike duration averages
        c_duration_fwd = compute_weighted_average(
            stats.classic_duration_fwd, 
            stats.trip_count_fwd - stats.electric_count_fwd
        )
        c_duration_rev = compute_weighted_average(
            stats.classic_duration_rev,
            stats.trip_count_rev - stats.electric_count_rev
        )
        c_duration_bidir = compute_weighted_average(
            stats.classic_duration_fwd + stats.classic_duration_rev,
            trip_count_bidir - total_electric
        )
        
        row = {
            # Station information
            'station_id': station_id,
            'station_name': stats.station_name,
            'municipality': get_municipality(station_id),
            'latitude': stats.latitude,
            'longitude': stats.longitude,
            
            # Trip counts
            'trip_count_fwd': stats.trip_count_fwd,
            'trip_count_rev': stats.trip_count_rev,
            'trip_count_bidir': trip_count_bidir,
            'trip_count': f"{trip_count_bidir} (F: {stats.trip_count_fwd} / R: {stats.trip_count_rev})",
            
            # Electric bike percentages
            'electric_bike_percent_fwd': round(fwd_e_pct, 0),
            'electric_bike_percent_rev': round(rev_e_pct, 0),
            'electric_bike_percent_bidir': round(bidir_e_pct, 0),
            'electric_bike_percent': format_metric(bidir_e_pct, fwd_e_pct, rev_e_pct, "%", 0),
            
            # Duration averages
            'duration_avg_fwd': round(duration_fwd, 1),
            'duration_avg_rev': round(duration_rev, 1),
            'duration_avg_bidir': round(duration_bidir, 1),
            'duration_avg': format_metric(duration_bidir, duration_fwd, duration_rev),
            
            # Electric bike duration averages
            'electric_bike_duration_avg_fwd': round(e_duration_fwd, 1),
            'electric_bike_duration_avg_rev': round(e_duration_rev, 1),
            'electric_bike_duration_avg_bidir': round(e_duration_bidir, 1),
            'electric_bike_duration_avg': format_metric(e_duration_bidir, e_duration_fwd, e_duration_rev),
            
            # Classic bike duration averages
            'classic_bike_duration_avg_fwd': round(c_duration_fwd, 1),
            'classic_bike_duration_avg_rev': round(c_duration_rev, 1),
            'classic_bike_duration_avg_bidir': round(c_duration_bidir, 1),
            'classic_bike_duration_avg': format_metric(c_duration_bidir, c_duration_fwd, c_duration_rev)
        }
        output_rows.append(row)

    return fieldnames, output_rows

def analyze_station_pairs(avg_coords, rows):
    """
    Main function to analyze station pair usage data.
    
    This function:
    1. Processes trip data to track statistics for each station pair
    2. Analyzes trip patterns in both directions between station pairs
    3. Calculates metrics like e-bike percentages and average durations
    4. Outputs a comprehensive CSV of station pair statistics
    
    Args:
        avg_coords (dict): Dictionary of station IDs to average coordinates
        rows (list): List of trip data rows
    """
    # Initialize data structure for collecting statistics
    pairs = defaultdict(lambda: {
        'start_station_name': '',
        'end_station_name': '',
        'start_lat': '',
        'start_lng': '',
        'end_lat': '',
        'end_lng': '',
        'stats_fwd': TripStats(),
        'stats_rev': TripStats()
    })
    
    # Process rows
    for row in rows:
        start_id = row.get('start_station_id', '')
        end_id = row.get('end_station_id', '')
        
        # Skip invalid trips
        if not start_id or not end_id or start_id == end_id:
            continue
        if start_id not in avg_coords or end_id not in avg_coords:
            continue
            
        try:
            duration = float(row.get('duration_minutes', 0))
            bike_type = normalize_bike_type(row.get('rideable_type', ''))
        except ValueError:
            continue
            
        # Update station names and coordinates if not set
        pair_key = (start_id, end_id)
        rev_key = (end_id, start_id)
        
        if not pairs[pair_key]['start_station_name']:
            pairs[pair_key].update({
                'start_station_name': format_station_name(start_id, row.get('start_station_name', '')),
                'end_station_name': format_station_name(end_id, row.get('end_station_name', '')),
                'start_lat': avg_coords[start_id]['latitude'],
                'start_lng': avg_coords[start_id]['longitude'],
                'end_lat': avg_coords[end_id]['latitude'],
                'end_lng': avg_coords[end_id]['longitude']
            })
            
        # Update forward statistics
        stats = pairs[pair_key]['stats_fwd']
        stats.trip_count += 1
        stats.total_duration += duration
        if bike_type == 'electric_bike':
            stats.electric_count += 1
            stats.electric_duration += duration
        else:
            stats.classic_duration += duration

        # Update reverse statistics in the corresponding pair
        rev_stats = pairs[rev_key]['stats_rev']
        rev_stats.trip_count += 1
        rev_stats.total_duration += duration
        if bike_type == 'electric_bike':
            rev_stats.electric_count += 1
            rev_stats.electric_duration += duration
        else:
            rev_stats.classic_duration += duration
            
        # Update reverse pair names if not set
        if not pairs[rev_key]['start_station_name']:
            pairs[rev_key].update({
                'start_station_name': format_station_name(end_id, row.get('end_station_name', '')),
                'end_station_name': format_station_name(start_id, row.get('start_station_name', '')),
                'start_lat': avg_coords[end_id]['latitude'],
                'start_lng': avg_coords[end_id]['longitude'],
                'end_lat': avg_coords[start_id]['latitude'],
                'end_lng': avg_coords[start_id]['longitude']
            })
    
    # Define all output fields
    fieldnames = [
        # Station information
        'start_station', 'start_station_name', 'start_lat', 'start_lng',
        'end_station', 'end_station_name', 'end_lat', 'end_lng',
        'start_municipality', 'end_municipality',  # New columns
        # Trip counts
        'trip_count_fwd', 'trip_count_rev', 'trip_count_bidir', 'trip_count',
        # Electric bike percentages
        'electric_bike_percent_fwd', 'electric_bike_percent_rev', 
        'electric_bike_percent_bidir', 'electric_bike_percent',
        # Duration averages
        'duration_avg_fwd', 'duration_avg_rev', 'duration_avg_bidir', 'duration_avg',
        # Electric bike duration averages
        'electric_bike_duration_avg_fwd', 'electric_bike_duration_avg_rev',
        'electric_bike_duration_avg_bidir', 'electric_bike_duration_avg',
        # Classic bike duration averages
        'classic_bike_duration_avg_fwd', 'classic_bike_duration_avg_rev',
        'classic_bike_duration_avg_bidir', 'classic_bike_duration_avg'
    ]
    
    # Collect output rows
    output_rows = []

    # Sort and process data
    sorted_pairs = sorted(pairs.items(), key=lambda x: (
        sort_id_by_n_and_alpha(x[0][0]),
        sort_id_by_n_and_alpha(x[0][1])
    ))
    
    for (start_id, end_id), data in sorted_pairs:
        fwd_stats = data['stats_fwd']
        rev_stats = data['stats_rev']
        
        # Compute bidirectional totals
        total_trips = fwd_stats.trip_count + rev_stats.trip_count
        total_electric = fwd_stats.electric_count + rev_stats.electric_count
        
        # Skip if no trips in either direction
        if total_trips == 0:
            continue
            
        # Compute percentages and averages
        fwd_e_pct = (fwd_stats.electric_count / fwd_stats.trip_count * 100) if fwd_stats.trip_count > 0 else 0
        rev_e_pct = (rev_stats.electric_count / rev_stats.trip_count * 100) if rev_stats.trip_count > 0 else 0
        bidir_e_pct = (total_electric / total_trips * 100)
        
        # Compute duration averages
        duration_fwd = compute_weighted_average(fwd_stats.total_duration, fwd_stats.trip_count)
        duration_rev = compute_weighted_average(rev_stats.total_duration, rev_stats.trip_count)
        duration_bidir = compute_weighted_average(fwd_stats.total_duration + rev_stats.total_duration, total_trips)
        
        # Compute e-bike duration averages
        e_duration_fwd = compute_weighted_average(fwd_stats.electric_duration, fwd_stats.electric_count)
        e_duration_rev = compute_weighted_average(rev_stats.electric_duration, rev_stats.electric_count)
        e_duration_bidir = compute_weighted_average(
            fwd_stats.electric_duration + rev_stats.electric_duration, 
            total_electric
        )
        
        # Compute classic bike duration averages
        c_duration_fwd = compute_weighted_average(
            fwd_stats.classic_duration, 
            fwd_stats.trip_count - fwd_stats.electric_count
        )
        c_duration_rev = compute_weighted_average(
            rev_stats.classic_duration,
            rev_stats.trip_count - rev_stats.electric_count
        )
        c_duration_bidir = compute_weighted_average(
            fwd_stats.classic_duration + rev_stats.classic_duration,
            total_trips - total_electric
        )
        
        row = {
            # Station information
            'start_station': start_id,
            'start_station_name': data['start_station_name'],
            'start_lat': data['start_lat'],
            'start_lng': data['start_lng'],
            'end_station': end_id,
            'end_station_name': data['end_station_name'],
            'end_lat': data['end_lat'],
            'end_lng': data['end_lng'],
            'start_municipality': get_municipality(start_id),  # New field
            'end_municipality': get_municipality(end_id),      # New field
            
            # Trip counts
            'trip_count_fwd': fwd_stats.trip_count,
            'trip_count_rev': rev_stats.trip_count,
            'trip_count_bidir': total_trips,
            'trip_count': f"{total_trips} (F: {fwd_stats.trip_count} / R: {rev_stats.trip_count})",
            
            # Electric bike percentages
            'electric_bike_percent_fwd': round(fwd_e_pct, 0),
            'electric_bike_percent_rev': round(rev_e_pct, 0),
            'electric_bike_percent_bidir': round(bidir_e_pct, 0),
            'electric_bike_percent': format_metric(bidir_e_pct, fwd_e_pct, rev_e_pct, "%", 0),
            
            # Duration averages
            'duration_avg_fwd': round(duration_fwd, 1),
            'duration_avg_rev': round(duration_rev, 1),
            'duration_avg_bidir': round(duration_bidir, 1),
            'duration_avg': format_metric(duration_bidir, duration_fwd, duration_rev),
            
            # Electric bike duration averages
            'electric_bike_duration_avg_fwd': round(e_duration_fwd, 1),
            'electric_bike_duration_avg_rev': round(e_duration_rev, 1),
            'electric_bike_duration_avg_bidir': round(e_duration_bidir, 1),
            'electric_bike_duration_avg': format_metric(e_duration_bidir, e_duration_fwd, e_duration_rev),
            
            # Classic bike duration averages
            'classic_bike_duration_avg_fwd': round(c_duration_fwd, 1),
            'classic_bike_duration_avg_rev': round(c_duration_rev, 1),
            'classic_bike_duration_avg_bidir': round(c_duration_bidir, 1),
            'classic_bike_duration_avg': format_metric(c_duration_bidir, c_duration_fwd, c_duration_rev)
        }
        output_rows.append(row)

    return fieldnames, output_rows

def main():
    """
    Main function to parse arguments and run the appropriate analysis.
    """
    parser = argparse.ArgumentParser(
        description='Aggregate BlueBikes data by stations or station pairs.'
    )
    
    # Set up the mutually exclusive group for analysis type
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--stations', 
        action='store_true',
        help='Analyze data by individual station'
    )
    group.add_argument(
        '--station-pairs',
        action='store_true',
        help='Analyze data by station pairs'
    )

    parser.add_argument(
        '-o', '--output',
        help='Output file path. Format detected from extension (.csv or .parquet). Defaults to stdout as CSV.'
    )

    args = parser.parse_args()
    
    # Compute station coordinates (shared code for both analysis types)
    avg_coords, rows = compute_station_coordinates()

    # Run the appropriate analysis based on command line arguments
    if args.stations:
        fieldnames, output_rows = analyze_stations(avg_coords, rows)
    elif args.station_pairs:
        fieldnames, output_rows = analyze_station_pairs(avg_coords, rows)

    # Write output
    write_output(output_rows, fieldnames, args.output)

if __name__ == "__main__":
    main()