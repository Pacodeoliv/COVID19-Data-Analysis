import requests
import os
from datetime import datetime, timedelta
import pandas as pd

def download_file(url, local_filename):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename

def fetch_jhu_covid_data():
    base_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"
    start_date = datetime(2020, 1, 22)  # First available date in the repository
    end_date = datetime.now()
    current_date = start_date

    # Create a directory to store the downloaded files
    os.makedirs("data/raw/daily_reports", exist_ok=True)

    while current_date <= end_date:
        date_str = current_date.strftime("%m-%d-%Y")
        file_url = f"{base_url}{date_str}.csv"
        local_filename = f"data/raw/daily_reports/{date_str}.csv"

        try:
            download_file(file_url, local_filename)
            print(f"Downloaded {date_str}.csv")
        except requests.exceptions.HTTPError:
            print(f"File not found for {date_str}")

        current_date += timedelta(days=1)

    print("Download completed.")

def combine_daily_reports():
    all_files = os.listdir("data/raw/daily_reports")
    csv_files = [file for file in all_files if file.endswith('.csv')]

    combined_df = pd.DataFrame()

    for file in csv_files:
        df = pd.read_csv(f"data/raw/daily_reports/{file}")
        df['Date'] = file[:-4]  # Extract date from filename
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    combined_df.to_csv("data/raw/combined_daily_reports.csv", index=False)
    print("Combined all daily reports into a single CSV file.")

if __name__ == "__main__":
    fetch_jhu_covid_data()
    combine_daily_reports()