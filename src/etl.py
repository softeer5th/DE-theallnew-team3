import sys
import os
from extract.bobae import extract_bobae
from extract.youtube import extract_youtube
from extract.clien import extract_clien

from transform.transform_text import transform_text

def extract(input_date, car_name):
    print("Extract started")
    extract_youtube(input_date, car_name)
    extract_bobae(input_date, car_name)
    extract_clien(input_date, car_name)

def transform(input_date, car_name):
    print("Transform started")
    transform_text(input_date, car_name)

def etl(input_date, car_name):
    print("ETL started")
    # extract(input_date, car_name)

    # transform here
    transform_text(input_date, car_name)
    
    print("ETL done")


if __name__ == "__main__":
    input_date = sys.argv[1] if len(sys.argv) > 1 else "2025-01"
    car_name = sys.argv[2] if len(sys.argv) > 2 else "투싼"

    etl(input_date, car_name)
