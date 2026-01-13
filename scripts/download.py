#!/usr/bin/env python3
"""
BlueBikes Data Downloader & Processor

This script downloads and processes Bluebikes trip data from S3 storage.
It adds duration calculations, formats coordinates with consistent precision,
and outputs cleaned CSV data.

Usage:
    python scripts/download.py YYYY[-MM] [YYYY[-MM]] > data/cleaned_data.csv
    
Examples:
    python scripts/download.py 2024 > data/bluebikes_2024.csv       # Process all data for 2024
    python scripts/download.py 2024-01 > data/bluebikes_jan2024.csv  # Process data for Jan 2024
    python scripts/download.py 2023-04 2024-11 > data/bluebikes_2023-2024.csv  # Process specific range
"""

# /// script
# requires-python = ">=3.6"
# dependencies = [
#   "python-dateutil",
# ]
# ///

from urllib.request import urlopen
import zipfile
from io import BytesIO, TextIOWrapper
import csv
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta

def clean_record(row):
    """
    Process record with reduced precision for specified fields and add duration.
    
    Args:
        row (dict): Raw trip data record
        
    Returns:
        dict: Cleaned record with standardized timestamps, duration calculation, 
              and formatted coordinates
    """
    # Create a copy of the full row
    cleaned = dict(row)
    
    # Process timestamps and calculate duration
    start_time = None
    end_time = None
    
    if 'started_at' in cleaned:
        try:
            start_time = datetime.fromisoformat(cleaned['started_at'])
            cleaned['started_at'] = start_time.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass
            
    if 'ended_at' in cleaned:
        try:
            end_time = datetime.fromisoformat(cleaned['ended_at'])
            cleaned['ended_at'] = end_time.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            pass
    
    # Calculate duration in minutes if both timestamps are valid
    if start_time and end_time:
        duration = round((end_time - start_time).total_seconds() / 60)
        cleaned['duration_minutes'] = f'{duration:d}'
    else:
        cleaned['duration_minutes'] = ''
    
    # Format lat/lng with exactly 5 decimal places
    for field in ['start_lat', 'start_lng', 'end_lat', 'end_lng']:
        if field in cleaned and cleaned[field]:
            try:
                cleaned[field] = f'{float(cleaned[field]):.5f}'
            except ValueError:
                pass
    
    return cleaned

def stream_bluebikes_data(start_date_str, end_date_str=None):
    """
    Generator function to stream Bluebikes trip data.
    
    Args:
        start_date_str (str): Start date in YYYY or YYYY-MM format
        end_date_str (str): Optional end date in YYYY or YYYY-MM format
        
    Yields:
        dict: Each trip record as a dictionary
    """
    base_url = "https://s3.amazonaws.com/hubway-data/"
    
    # Parse start date
    start_date = datetime.strptime(start_date_str, "%Y-%m" if "-" in start_date_str else "%Y")
    
    # If no end date provided, handle single month/year case
    if end_date_str is None:
        if len(start_date_str) == 4:  # Year only
            end_date = datetime(start_date.year, 12, 31)
        else:  # Month only
            end_date = start_date
    else:
        end_date = datetime.strptime(end_date_str, "%Y-%m" if "-" in end_date_str else "%Y")
        if len(end_date_str) == 4:
            end_date = datetime(end_date.year, 12, 31)
    
    first_file = True
    fieldnames = None
    
    # For each month in range
    current_date = start_date
    while current_date <= end_date:
        filename = f"{current_date.year}{current_date.month:02d}-bluebikes-tripdata.zip"
        url = base_url + filename

        try:
            try:
                response = urlopen(url)
            except Exception:
                # Fallback: some months use .csv.zip extension
                filename = f"{current_date.year}{current_date.month:02d}-bluebikes-tripdata.csv.zip"
                url = base_url + filename
                response = urlopen(url)
            zip_data = BytesIO(response.read())
            
            with zipfile.ZipFile(zip_data) as zip_file:
                csv_filename = zip_file.namelist()[0]
                
                with zip_file.open(csv_filename) as csv_file:
                    text_file = TextIOWrapper(csv_file, encoding='utf-8')
                    reader = csv.DictReader(text_file)
                    
                    # Store fieldnames from first successful file
                    if first_file:
                        fieldnames = list(reader.fieldnames)  # Convert to list to modify
                        if 'duration_minutes' not in fieldnames:  # Add new field
                            fieldnames.append('duration_minutes')
                        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
                        writer.writeheader()
                        first_file = False
                    
                    for row in reader:
                        writer.writerow(clean_record(row))
                        
        except Exception as e:
            # Skip any files that don't exist or have other issues
            pass
        
        current_date += relativedelta(months=1)

if __name__ == "__main__":
    if len(sys.argv) not in [2, 3]:
        print("Usage: python script.py YYYY[-MM] [YYYY[-MM]]", file=sys.stderr)
        print("Examples:", file=sys.stderr)
        print("  python script.py 2024", file=sys.stderr)
        print("  python script.py 2024-01", file=sys.stderr)
        print("  python script.py 2023-04 2024-11", file=sys.stderr)  # Note: Data before 2023-04 uses different format
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2] if len(sys.argv) == 3 else None
    
    stream_bluebikes_data(start_date, end_date)
