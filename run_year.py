import os
import argparse
import pandas as pd 

def main():
    parser = argparse.ArgumentParser(
        description="Process investment data for a given year."
    )

    # process year argument
    parser.add_argument(
        "year",
        type=int,
        help="Processing year (e.g. 2025)"
    )

    # flag : is start year 
    parser.add_argument(
        "--is-start",
        action="store_true",
        help="Indicate this year is the starting year (load opening snapshots)"
    )

    args = parser.parse_args()

    year = args.year
    is_start_year = args.is_start

    print(f"Processing year: {year}")
    print(f"Is start year: {is_start_year}")

if __name__ == "__main__":
    main()