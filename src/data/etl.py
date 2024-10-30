import pandas as pd
import numpy as np
from datetime import datetime
import os

def load_data():
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'raw', 'daily_reports')
    csv_files = sorted([f for f in os.listdir(data_dir) if f.endswith('.csv')])  # Sort files chronologically
    
    df_list = []
    for file in csv_files:
        file_path = os.path.join(data_dir, file)
        df = pd.read_csv(file_path)
        # Extract date from filename and ensure it's in correct format
        date_str = file[:-4]  # Remove .csv
        try:
            date = pd.to_datetime(date_str, format='%m-%d-%Y')
            df['Date'] = date
            df_list.append(df)
        except ValueError as e:
            print(f"Skipping file {file} due to invalid date format: {e}")
    
    combined_df = pd.concat(df_list, ignore_index=True)
    print(f"Loaded {len(combined_df)} rows of data from {len(csv_files)} files.")
    return combined_df

def clean_data(df):
    # Standardize column names
    column_mapping = {
        'Country_Region': 'Country',
        'Province_State': 'State',
        'Last_Update': 'LastUpdate',
        'Lat': 'Latitude',
        'Long_': 'Longitude',
        'Case-Fatality_Ratio': 'CaseFatalityRatio'
    }
    
    # Only rename columns that exist in the dataframe
    existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
    df = df.rename(columns=existing_columns)

    # Ensure required columns exist
    required_columns = ['Confirmed', 'Deaths', 'Recovered', 'Active']
    for col in required_columns:
        if col not in df.columns:
            print(f"Warning: Creating missing column {col}")
            df[col] = 0

    # Convert numeric columns
    numeric_columns = ['Confirmed', 'Deaths', 'Recovered', 'Active']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # Handle recovered cases
    # If Recovered is 0 but we can calculate it, do so
    df['Recovered'] = np.where(
        (df['Recovered'] == 0) & (df['Confirmed'] > 0),
        df['Confirmed'] - df['Deaths'] - df['Active'],
        df['Recovered']
    )
    
    # Ensure recovered cases don't exceed confirmed cases
    df['Recovered'] = df.apply(lambda x: min(x['Recovered'], x['Confirmed']), axis=1)
    
    # Recalculate Active cases
    df['Active'] = df['Confirmed'] - df['Deaths'] - df['Recovered']
    df['Active'] = df['Active'].clip(lower=0)  # Ensure no negative active cases

    print("Data cleaning completed.")
    return df

def transform_data(df):
    # Create a date index for sorting
    df['DateIndex'] = pd.to_datetime(df['Date']).dt.strftime('%Y%m%d').astype(int)
    
    # Sort data
    df = df.sort_values(['Country', 'State', 'DateIndex'])
    
    # Calculate daily changes
    for col in ['Confirmed', 'Deaths', 'Recovered']:
        new_col = f'New{col}'
        df[new_col] = df.groupby(['Country', 'State'])[col].diff().fillna(0)
        # Clean negative values
        df[new_col] = df[new_col].clip(lower=0)
        
        # Calculate 7-day moving averages
        df[f'{new_col}MA7'] = df.groupby(['Country', 'State'])[new_col]\
            .rolling(window=7, min_periods=1)\
            .mean()\
            .reset_index(level=[0,1], drop=True)
    
    print("Data transformation completed.")
    return df

def aggregate_data(df):
    # Aggregation columns
    agg_columns = {
        'Confirmed': 'sum',
        'Deaths': 'sum',
        'Recovered': 'sum',
        'Active': 'sum',
        'NewConfirmed': 'sum',
        'NewDeaths': 'sum',
        'NewRecovered': 'sum'
    }
    
    # Global daily aggregates
    global_daily = df.groupby('Date', as_index=False).agg(agg_columns)
    
    # Country daily aggregates
    country_daily = df.groupby(['Date', 'Country'], as_index=False).agg(agg_columns)
    
    # Sort by date
    global_daily = global_daily.sort_values('Date')
    country_daily = country_daily.sort_values(['Country', 'Date'])
    
    # Validate data
    for df_check in [global_daily, country_daily]:
        df_check['Active'] = df_check['Active'].clip(lower=0)
        df_check['Recovered'] = df_check['Recovered'].clip(lower=0)
        df_check['Recovered'] = df_check.apply(lambda x: min(x['Recovered'], x['Confirmed']), axis=1)
    
    print("Data aggregation completed.")
    return global_daily, country_daily

def save_processed_data(df, global_daily, country_daily):
    processed_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'processed')
    os.makedirs(processed_dir, exist_ok=True)
    
    # Save with date index for better sorting
    for data, filename in [
        (df, "covid_data_cleaned.csv"),
        (global_daily, "covid_data_global.csv"),
        (country_daily, "covid_data_by_country.csv")
    ]:
        # Ensure date is in consistent format
        data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%Y-%m-%d')
        data.to_csv(os.path.join(processed_dir, filename), index=False)
        
    print("Processed data saved to CSV files.")

def validate_data(df):
    """Validate data consistency"""
    issues = []
    
    # Check for negative values
    for col in ['Confirmed', 'Deaths', 'Recovered', 'Active']:
        neg_count = (df[col] < 0).sum()
        if neg_count > 0:
            issues.append(f"Found {neg_count} negative values in {col}")
    
    # Check for logical consistency
    invalid_count = (df['Confirmed'] < df['Deaths'] + df['Recovered']).sum()
    if invalid_count > 0:
        issues.append(f"Found {invalid_count} rows where Deaths + Recovered > Confirmed")
    
    if issues:
        print("Data validation issues found:")
        for issue in issues:
            print(f"- {issue}")
    else:
        print("Data validation passed.")
    
    return len(issues) == 0

def run_etl():
    print("Starting ETL process...")
    
    df = load_data()
    df_cleaned = clean_data(df)
    df_transformed = transform_data(df_cleaned)
    
    # Validate data before aggregation
    if not validate_data(df_transformed):
        print("Warning: Data validation failed. Please check the data quality.")
    
    global_daily, country_daily = aggregate_data(df_transformed)
    save_processed_data(df_transformed, global_daily, country_daily)
    print("ETL process completed successfully.")

if __name__ == "__main__":
    run_etl()