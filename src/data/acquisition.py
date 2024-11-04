import requests
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

class USCOVIDDataAcquisition:
    def __init__(self):
        self.base_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports_us/"
        self.raw_data_dir = "data/raw/daily_reports_us"
        self.processed_data_dir = "data/processed"
        
        # Ensure directories exist
        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.processed_data_dir, exist_ok=True)

    def download_file(self, url: str, local_filename: str) -> bool:
        """Download a file from URL with error handling."""
        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return True
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {url}: {str(e)}")
            return False

    def fetch_us_covid_data(self):
        """Fetch US-specific COVID-19 data from JHU repository."""
        start_date = datetime(2020, 4, 12)  # First available date for US data
        end_date = datetime.now()
        current_date = start_date
        
        downloaded_files = []
        
        while current_date <= end_date:
            date_str = current_date.strftime("%m-%d-%Y")
            file_url = f"{self.base_url}{date_str}.csv"
            local_filename = os.path.join(self.raw_data_dir, f"{date_str}.csv")
            
            if self.download_file(file_url, local_filename):
                downloaded_files.append(local_filename)
                print(f"Downloaded {date_str}.csv")
            
            current_date += timedelta(days=1)
        
        print(f"Download completed. Retrieved {len(downloaded_files)} files.")
        return downloaded_files

    def process_file(self, file_path: str) -> pd.DataFrame:
        """Process a single US CSV file."""
        date_str = os.path.basename(file_path)[:-4]  # Remove .csv extension
        
        try:
            df = pd.read_csv(file_path)
            
            # Add date column
            df['Date'] = pd.to_datetime(date_str, format='%m-%d-%Y')
            
            # Ensure all required columns exist
            required_columns = [
                'Province_State', 'Confirmed', 'Deaths', 'Recovered',
                'Active', 'Incident_Rate', 'Total_Test_Results',
                'Hospitalization_Rate', 'Case_Fatality_Ratio'
            ]
            
            for col in required_columns:
                if col not in df.columns:
                    df[col] = 0 if col != 'Province_State' else ''
            
            # Convert numeric columns
            numeric_cols = [
                'Confirmed', 'Deaths', 'Recovered', 'Active',
                'Incident_Rate', 'Total_Test_Results',
                'Hospitalization_Rate', 'Case_Fatality_Ratio'
            ]
            
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            return df
            
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
            return pd.DataFrame()

    def combine_daily_reports(self) -> pd.DataFrame:
        """Combine all US daily reports into a single DataFrame."""
        all_files = sorted([f for f in os.listdir(self.raw_data_dir) if f.endswith('.csv')])
        combined_data = []
        
        for file in all_files:
            file_path = os.path.join(self.raw_data_dir, file)
            df = self.process_file(file_path)
            if not df.empty:
                combined_data.append(df)
        
        if not combined_data:
            raise ValueError("No data was processed successfully")
        
        combined_df = pd.concat(combined_data, ignore_index=True)
        
        # Clean and validate the combined data
        combined_df = self.clean_combined_data(combined_df)
        
        return combined_df

    def clean_combined_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate the combined US dataset."""
        # Fill missing values
        df['Province_State'].fillna('Unknown', inplace=True)
        
        # Ensure numeric columns are non-negative
        numeric_cols = [
            'Confirmed', 'Deaths', 'Recovered', 'Active',
            'Incident_Rate', 'Total_Test_Results',
            'Hospitalization_Rate', 'Case_Fatality_Ratio'
        ]
        
        for col in numeric_cols:
            df[col] = df[col].clip(lower=0)
        
        # Calculate testing rate
        df['Testing_Rate'] = (df['Total_Test_Results'] / df['Confirmed']).clip(lower=0)
        
        # Sort by date and state
        df = df.sort_values(['Date', 'Province_State'])
        
        return df

    def run_acquisition(self):
        """Run the complete US data acquisition process."""
        print("Starting US COVID-19 data acquisition...")
        
        # Download fresh data
        self.fetch_us_covid_data()
        
        # Combine and process all data
        try:
            combined_df = self.combine_daily_reports()
            
            # Save processed data
            output_file = os.path.join(self.processed_data_dir, "us_covid_data_combined.csv")
            combined_df.to_csv(output_file, index=False)
            
            print(f"Data acquisition completed successfully. Output saved to {output_file}")
            print(f"Total records: {len(combined_df)}")
            print(f"Date range: {combined_df['Date'].min()} to {combined_df['Date'].max()}")
            print(f"States covered: {combined_df['Province_State'].nunique()}")
            
        except Exception as e:
            print(f"Error in data acquisition process: {str(e)}")
            raise

if __name__ == "__main__":
    covid_acquisition = USCOVIDDataAcquisition()
    covid_acquisition.run_acquisition()