import pandas as pd
import numpy as np
from datetime import datetime
import os

def load_data():
    # Path to the directory containing the CSV files
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'raw', 'daily_reports')
    
    # List all CSV files in the directory
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    
    # Read and combine all CSV files
    df_list = []
    for file in csv_files:
        file_path = os.path.join(data_dir, file)
        df = pd.read_csv(file_path)
        df['Date'] = file[:-4]  # Extract date from filename (assuming MM-DD-YYYY.csv format)
        df_list.append(df)
    
    # Combine all dataframes
    combined_df = pd.concat(df_list, ignore_index=True)
    print(f"Loaded {len(combined_df)} rows of data from {len(csv_files)} files.")
    return combined_df

def clean_data(df):
    # Rename columns for consistency
    df = df.rename(columns={
        'Country_Region': 'Country',
        'Province_State': 'State',
        'Last_Update': 'LastUpdate',
        'Lat': 'Latitude',
        'Long_': 'Longitude',
        'Case-Fatality_Ratio': 'CaseFatalityRatio'
    })

    # Convert date columns to datetime
    df['Date'] = pd.to_datetime(df['Date'], format='%m-%d-%Y')
    df['LastUpdate'] = pd.to_datetime(df['LastUpdate'], errors='coerce')  # Coerce errors for invalid dates

    # Fill NaN values
    df['State'].fillna('', inplace=True)
    for col in ['Confirmed', 'Deaths', 'Recovered', 'Active']:
        df[col] = df[col].fillna(0).astype(int)

    # Calculate missing Active cases
    df['Active'] = df['Active'].fillna(df['Confirmed'] - df['Deaths'] - df['Recovered'])

    # Calculate Case Fatality Ratio if missing
    df['CaseFatalityRatio'] = df['CaseFatalityRatio'].fillna(df['Deaths'] / df['Confirmed'] * 100)

    print("Data cleaning completed.")
    return df

def transform_data(df):
    # Create a date index
    df['DateIndex'] = df['Date'].dt.strftime('%Y%m%d').astype(int)

    # Calculate daily new cases, deaths, and recoveries
    df = df.sort_values(['Country', 'State', 'DateIndex'])
    for col in ['Confirmed', 'Deaths', 'Recovered']:
        df[f'New{col}'] = df.groupby(['Country', 'State'])[col].diff().fillna(0)

    # Calculate 7-day moving averages
    for col in ['NewConfirmed', 'NewDeaths', 'NewRecovered']:
        df[f'{col}MA7'] = df.groupby(['Country', 'State'])[col].rolling(window=7).mean().reset_index(level=[0, 1], drop=True)

    print("Data transformation completed.")
    return df

def aggregate_data(df):
    # Global daily aggregates
    global_daily = df.groupby('Date').agg({
        'Confirmed': 'sum',
        'Deaths': 'sum',
        'Recovered': 'sum',
        'Active': 'sum',
        'NewConfirmed': 'sum',
        'NewDeaths': 'sum',
        'NewRecovered': 'sum'
    }).reset_index()

    # Country daily aggregates
    country_daily = df.groupby(['Date', 'Country']).agg({
        'Confirmed': 'sum',
        'Deaths': 'sum',
        'Recovered': 'sum',
        'Active': 'sum',
        'NewConfirmed': 'sum',
        'NewDeaths': 'sum',
        'NewRecovered': 'sum'
    }).reset_index()

    print("Data aggregation completed.")
    return global_daily, country_daily

def save_processed_data(df, global_daily, country_daily):
    # Adjust the path to save processed files in the correct location
    processed_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'processed')
    os.makedirs(processed_dir, exist_ok=True)

    df.to_csv(os.path.join(processed_dir, "covid_data_cleaned.csv"), index=False)
    global_daily.to_csv(os.path.join(processed_dir, "covid_data_global.csv"), index=False)
    country_daily.to_csv(os.path.join(processed_dir, "covid_data_by_country.csv"), index=False)
    print("Processed data saved to CSV files.")

def run_etl():
    print("Starting ETL process...")
    df = load_data()
    df_cleaned = clean_data(df)
    df_transformed = transform_data(df_cleaned)
    global_daily, country_daily = aggregate_data(df_transformed)
    save_processed_data(df_transformed, global_daily, country_daily)
    print("ETL process completed successfully.")

if __name__ == "__main__":
    run_etl()
