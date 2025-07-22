import argparse
import logging
import pandas as pd
import os
import re
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define timestamp formats to try
TIMESTAMP_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S.%f",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S.%f",
    "%a %b %d %H:%M:%S %Y", # Syslog format
    "%a %b %d %H:%M:%S.%f %Y"  # Extended Syslog format
]

# Define event priorities
EVENT_PRIORITIES = {
    "critical": 1,
    "high": 2,
    "medium": 3,
    "low": 4,
    "info": 5
}


def setup_argparse():
    """
    Sets up the argument parser for the command line interface.
    Returns:
        argparse.ArgumentParser: The argument parser object.
    """
    parser = argparse.ArgumentParser(description="Generates a timeline of system events from log files.")
    parser.add_argument("log_files", nargs="+", help="Path(s) to the log file(s).")
    parser.add_argument("-o", "--output", default="timeline.csv", help="Output CSV file (default: timeline.csv).")
    parser.add_argument("-tsf", "--timestamp_format",  action='append', help="Timestamp format(s) to try (e.g., '%Y-%m-%d %H:%M:%S').  Can specify multiple formats.") # Allow multiple timestamp formats to be specified
    parser.add_argument("-p", "--priority", default="info", choices=EVENT_PRIORITIES.keys(), help="Minimum event priority to include (default: info).")
    parser.add_argument("-k", "--keywords", nargs="+", help="Keywords to filter for in log entries.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output for debugging.")
    parser.add_argument("--no-header", action="store_true", help="Omit the CSV header in the output file.") # Option to disable the header for easier processing downstream

    return parser


def parse_log_entry(log_entry, timestamp_formats):
    """
    Parses a single log entry and extracts the timestamp and event description.
    Args:
        log_entry (str): The log entry string.
        timestamp_formats (list): List of timestamp formats to attempt.

    Returns:
        tuple: A tuple containing the datetime object and the event description, or (None, None) if parsing fails.
    """
    for timestamp_format in timestamp_formats:
        try:
            # Attempt to extract timestamp based on defined formats
            timestamp_str = log_entry.split(" ", 1)[0] # Basic split on the first space to isolate potential timestamp
            dt = datetime.strptime(timestamp_str, timestamp_format)
            description = log_entry[len(timestamp_str):].strip()  # Extract the rest as description
            return dt, description

        except ValueError:
            continue # Try the next format
        except IndexError:
            continue # Handle cases with malformed entries


    return None, log_entry  # If no format matches, return None and the raw log entry


def process_log_file(log_file_path, timestamp_formats, keywords=None):
    """
    Processes a single log file and extracts relevant events.
    Args:
        log_file_path (str): The path to the log file.
        timestamp_formats (list): List of timestamp formats to attempt.
        keywords (list): List of keywords to filter for.

    Returns:
        list: A list of dictionaries, where each dictionary represents an event.
    """
    events = []
    try:
        with open(log_file_path, "r", encoding="utf-8", errors='ignore') as f:  # Handle potential encoding issues
            for line in f:
                line = line.strip()
                if not line:  # Skip empty lines
                    continue

                dt, description = parse_log_entry(line, timestamp_formats)

                if dt:
                    if keywords is None or any(keyword.lower() in description.lower() for keyword in keywords): #Keyword check and case-insensitive compare
                        events.append({"timestamp": dt, "description": description, "source": log_file_path})

                else:
                    logging.warning(f"Could not parse timestamp in line: {line} from {log_file_path}")

    except FileNotFoundError:
        logging.error(f"Log file not found: {log_file_path}")
    except Exception as e:
        logging.error(f"Error processing log file {log_file_path}: {e}")
    return events


def main():
    """
    Main function to parse arguments, process log files, and generate the timeline.
    """
    parser = setup_argparse()
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Validate log file paths
    for log_file in args.log_files:
        if not os.path.isfile(log_file):
            logging.error(f"Invalid log file path: {log_file}")
            return

    # Validate timestamp formats
    if args.timestamp_format:
        timestamp_formats = args.timestamp_format
    else:
        timestamp_formats = TIMESTAMP_FORMATS # Fallback to the default list.

    # Validate priority
    min_priority_level = EVENT_PRIORITIES[args.priority]

    all_events = []
    for log_file in args.log_files:
        events = process_log_file(log_file, timestamp_formats, args.keywords)
        all_events.extend(events)

    # Convert to Pandas DataFrame for easier sorting and output
    df = pd.DataFrame(all_events)

    if not df.empty:
        df = df.sort_values(by="timestamp")
        df = df[["timestamp", "source", "description"]]  # Reorder columns
        try:
            df.to_csv(args.output, index=False, header=not args.no_header)
            logging.info(f"Timeline generated successfully and saved to: {args.output}")
        except Exception as e:
            logging.error(f"Error writing to CSV file: {e}")

    else:
        logging.warning("No events found to generate timeline.")


if __name__ == "__main__":
    main()