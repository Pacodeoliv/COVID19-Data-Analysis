import pandas as pd
import numpy as np
from datetime import datetime
import os

def load_and_combine_data():
    """Load and combine COVID-19 data from the raw directory."""
    data_dir = 'data/raw/daily_reports'
    all_data = []
    
    for file in sorted(os.listdir(data_dir)):
        if not file.endswith('.csv'):
            continue
            
        df = pd.read_csv(os.path.join(data_dir, file))
        date = pd.to_datetime(file.replace('.csv', ''), format='%m-%d-%Y')
        df['Date'] = date
        all_data.append(df)
    
    return pd.concat(all_data, ignore_index=True)

def clean_data(df):
    """Clean and standardize the COVID-19 data."""
    # Standardize column names
    column_mapping = {
        'Country_Region': 'Country',
        'Province_State': 'Province',
        'Lat': 'Latitude',
        'Long_': 'Longitude',
        'Last_Update': 'LastUpdate'
    }
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    
    # Ensure essential columns exist
    essential_columns = ['Country', 'Date', 'Confirmed', 'Deaths', 'Recovered', 'Active']
    for col in essential_columns:
        if col not in df.columns:
            df[col] = 0 if col not in ['Country', 'Date'] else None
    
    # Clean numeric columns
    numeric_columns = ['Confirmed', 'Deaths', 'Recovered', 'Active']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    # Clean country names
    df['Country'] = df['Country'].replace({
        'Mainland China': 'China',
        'US': 'United States',
        'Korea, South': 'South Korea',
        'Korea, North': 'North Korea',
        'Taiwan*': 'Taiwan'
    })
    
    # Calculate Active cases where missing
    df['Active'] = df['Confirmed'] - df['Deaths'] - df['Recovered']
    df['Active'] = df['Active'].clip(lower=0)
    
    return df

def aggregate_by_country(df):
    """Aggregate data by country and date."""
    agg_cols = ['Confirmed', 'Deaths', 'Recovered', 'Active']
    
    # Group by country and date
    country_data = df.groupby(['Date', 'Country'])[agg_cols].sum().reset_index()
    
    # Calculate daily changes
    country_data = country_data.sort_values(['Country', 'Date'])
    for col in agg_cols:
        country_data[f'New{col}'] = country_data.groupby('Country')[col].diff().fillna(0)
        country_data[f'New{col}'] = country_data[f'New{col}'].clip(lower=0)
    
    return country_data

def save_processed_data(df):
    """Save processed data to CSV files."""
    output_dir = 'data/processed'
    os.makedirs(output_dir, exist_ok=True)
    
    # Save country-level data
    output_file = os.path.join(output_dir, 'covid_data_by_country.csv')
    df.to_csv(output_file, index=False)
    print(f"Saved processed data to {output_file}")

def run_etl():
    """Run the complete ETL pipeline."""
    print("Starting ETL process...")
    
    # Load and combine data
    print("Loading data...")
    raw_data = load_and_combine_data()
    
    # Clean data
    print("Cleaning data...")
    cleaned_data = clean_data(raw_data)
    
    # Aggregate by country
    print("Aggregating data...")
    country_data = aggregate_by_country(cleaned_data)
    
    # Save processed data
    print("Saving processed data...")
    save_processed_data(country_data)
    
    print("ETL process completed successfully.")
    return country_data

if __name__ == "__main__":
    run_etl()