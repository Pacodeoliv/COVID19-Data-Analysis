import pandas as pd
import numpy as np
from datetime import datetime
import os

def load_and_combine_data():
    """Load and combine US COVID-19 data from the raw directory."""
    data_dir = 'data/raw/daily_reports_us'
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
    """Clean and standardize the US COVID-19 data."""
    # Ensure essential columns exist
    essential_columns = [
        'Province_State', 'Date', 'Confirmed', 'Deaths',
        'Recovered', 'Active', 'Incident_Rate', 'Total_Test_Results',
        'Hospitalization_Rate', 'Case_Fatality_Ratio'
    ]
    
    for col in essential_columns:
        if col not in df.columns:
            df[col] = 0 if col != 'Province_State' else None
    
    # Clean numeric columns
    numeric_columns = [
        'Confirmed', 'Deaths', 'Recovered', 'Active',
        'Incident_Rate', 'Total_Test_Results',
        'Hospitalization_Rate', 'Case_Fatality_Ratio'
    ]
    
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # Clean state names
    df['Province_State'] = df['Province_State'].replace({
        'Recovered': 'Unknown'  # Handle special case in the data
    })
    
    # Calculate Active cases where missing
    df['Active'] = df['Confirmed'] - df['Deaths'] - df['Recovered']
    df['Active'] = df['Active'].clip(lower=0)
    
    return df

def aggregate_by_state(df):
    """Aggregate data by state and date."""
    agg_cols = [
        'Confirmed', 'Deaths', 'Recovered', 'Active',
        'Total_Test_Results', 'Hospitalization_Rate'
    ]
    
    # Group by state and date
    state_data = df.groupby(['Date', 'Province_State'])[agg_cols].sum().reset_index()
    
    # Calculate daily changes
    state_data = state_data.sort_values(['Province_State', 'Date'])
    for col in agg_cols:
        state_data[f'New{col}'] = state_data.groupby('Province_State')[col].diff().fillna(0)
        state_data[f'New{col}'] = state_data[f'New{col}'].clip(lower=0)
    
    # Calculate rates
    state_data['Case_Fatality_Ratio'] = (state_data['Deaths'] / state_data['Confirmed'] * 100).clip(lower=0)
    state_data['Testing_Rate'] = (state_data['Total_Test_Results'] / state_data['Confirmed']).clip(lower=0)
    
    return state_data

def save_processed_data(df):
    """Save processed data to CSV files."""
    output_dir = 'data/processed'
    os.makedirs(output_dir, exist_ok=True)
    
    # Save state-level data
    output_file = os.path.join(output_dir, 'us_covid_data_by_state.csv')
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
    
    # Aggregate by state
    print("Aggregating data...")
    state_data = aggregate_by_state(cleaned_data)
    
    # Save processed data
    print("Saving processed data...")
    save_processed_data(state_data)
    
    print("ETL process completed successfully.")
    return state_data

if __name__ == "__main__":
    run_etl()