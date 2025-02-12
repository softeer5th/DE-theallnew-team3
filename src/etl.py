import sys
import os
from extract.bobae import extract_bobae
from extract.youtube import extract_youtube
from extract.clien import extract_clien


def etl(input_date, car_name):
    print("ETL started")
    extract_bobae(input_date, car_name)
    extract_youtube(input_date, car_name)
    extract_clien(input_date, car_name)

    # transform here

    print("ETL done")


if __name__ == "__main__":
    input_date = sys.argv[1] if len(sys.argv) > 1 else "2025-01"
    car_name = sys.argv[2] if len(sys.argv) > 2 else "투싼"

    etl(input_date, car_name)
