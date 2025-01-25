import os
import re
import csv
from datetime import datetime, timedelta

# Directory containing the text files
input_directory = "path_to_your_text_files"
# Output CSV file
output_csv = "combined_methane_data.csv"

def parse_filename(filename):
    """Extracts date and time information from the file name."""
    pattern = r"methane_v1_(\d+)_(\d+)_(\d+)_(\d+)"
    match = re.match(pattern, filename)
    if match:
        year, month, day, hour = map(int, match.groups())
        return datetime(year, month, day, hour)
    return None

def process_file(filepath, base_datetime):
    """Processes a single file and extracts methane input and output data."""
    with open(filepath, "r") as file:
        for i, line in enumerate(file):
            line = line.strip()
            # Extract methane input and output values
            match = re.match(r"Methane Input: ([-\d]+), Methane Output: ([-\d]+)", line)
            if match:
                methane_input = int(match.group(1))
                methane_output = int(match.group(2))
                # Calculate timestamp for the row
                timestamp = base_datetime + timedelta(seconds=i)
                yield timestamp.strftime("%Y-%m-%d %H:%M:%S"), methane_input, methane_output

def main():
    with open(output_csv, "w", newline="") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["Timestamp", "Methane Input", "Methane Output"])

        for filename in sorted(os.listdir(input_directory)):
            if filename.endswith(".txt"):
                filepath = os.path.join(input_directory, filename)
                base_datetime = parse_filename(filename)

                if base_datetime:
                    print(f"Processing file: {filename}")
                    for row in process_file(filepath, base_datetime):
                        csvwriter.writerow(row)

    print(f"Data combined and written to {output_csv}")

if __name__ == "__main__":
    main()
