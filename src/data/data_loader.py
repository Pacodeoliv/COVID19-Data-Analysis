import pandas as pd
import requests
from datetime import datetime, timedelta
import os

class DataLoader:
    def __init__(self):
        self.base_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/"
        self.data_dir = "data"
        self.cache_file = os.path.join(self.data_dir, "covid_data_cache.csv")
        os.makedirs(self.data_dir, exist_ok=True)

    def download_data(self, start_date=None):
        """Download COVID-19 data from JHU repository"""
        if start_date is None:
            start_date = datetime(2020, 4, 12)  # First available date for US data
        
        end_date = datetime.now()
        all_data = []
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%m-%d-%Y")
            url = f"{self.base_url}{date_str}.csv"
            
            try:
                df = pd.read_csv(url)
                df['Date'] = current_date
                all_data.append(df)
                print(f"Downloaded data for {date_str}")
            except Exception as e:
                print(f"Could not download data for {date_str}: {e}")
            
            current_date += timedelta(days=1)
        
        if not all_data:
            raise ValueError("No data was downloaded")
        
        combined_df = pd.concat(all_data, ignore_index=True)
        self._save_cache(combined_df)
        return combined_df

    def _save_cache(self, df):
        """Save data to cache file"""
        df.to_csv(self.cache_file, index=False)
        print(f"Data cached to {self.cache_file}")

    def load_data(self, force_refresh=False):
        """Load data from cache or download if needed"""
        if not force_refresh and os.path.exists(self.cache_file):
            try:
                df = pd.read_csv(self.cache_file)
                df['Date'] = pd.to_datetime(df['Date'])
                return self._process_data(df)
            except Exception as e:
                print(f"Error loading cached data: {e}")
        
        df = self.download_data()
        return self._process_data(df)

    def _process_data(self, df):
        """Process and clean the data"""
        # Ensure essential columns exist
        essential_columns = [
            'Province_State', 'Confirmed', 'Deaths', 'Recovered',
            'Active', 'Incident_Rate', 'Total_Test_Results',
            'Hospitalization_Rate', 'Case_Fatality_Ratio'
        ]
        
        for col in essential_columns:
            if col not in df.columns:
                df[col] = 0 if col != 'Province_State' else ''
        
        # Convert numeric columns
        numeric_cols = [col for col in essential_columns if col != 'Province_State']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Calculate additional metrics
        df['Testing_Rate'] = (df['Total_Test_Results'] / df['Confirmed']).clip(lower=0)
        
        # Calculate daily changes
        for col in ['Confirmed', 'Deaths', 'Recovered', 'Active']:
            df[f'New_{col}'] = df.groupby('Province_State')[col].diff().fillna(0)
            df[f'New_{col}_MA7'] = df.groupby('Province_State')[f'New_{col}'].rolling(7).mean().reset_index(0, drop=True)
        
        return df