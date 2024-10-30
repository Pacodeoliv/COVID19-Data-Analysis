import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Tuple, Dict

# Data loading and processing
@st.cache_data
def load_data(file_path: str = 'data/processed/covid_data_by_country.csv') -> pd.DataFrame:
    """Load and preprocess the COVID-19 data."""
    try:
        df = pd.read_csv(file_path)
        df['Date'] = pd.to_datetime(df['Date'])
        return df
    except FileNotFoundError:
        st.error(f"Error: Could not find the data file at {file_path}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()

def calculate_statistics(data: pd.DataFrame) -> Dict:
    """Calculate key statistics from the data."""
    return {
        'total_cases': data['Confirmed'].iloc[-1],
        'total_deaths': data['Deaths'].iloc[-1],
        'total_recovered': data['Recovered'].iloc[-1],
        'active_cases': data['Active'].iloc[-1],
        'mortality_rate': (data['Deaths'].iloc[-1] / data['Confirmed'].iloc[-1] * 100) 
            if data['Confirmed'].iloc[-1] > 0 else 0
    }

def create_time_series_plot(data: pd.DataFrame, country: str) -> go.Figure:
    """Create an interactive time series plot."""
    fig = go.Figure()
    
    metrics = {
        'Confirmed': '#1f77b4',
        'Deaths': '#d62728',
        'Recovered': '#2ca02c',
        'Active': '#ff7f0e'
    }
    
    for metric, color in metrics.items():
        fig.add_trace(go.Scatter(
            x=data['Date'],
            y=data[metric],
            name=metric,
            line=dict(color=color),
            hovertemplate=f"{metric}: %{{y:,.0f}}<br>Date: %{{x|%Y-%m-%d}}<extra></extra>"
        ))
    
    fig.update_layout(
        title=f'COVID-19 Statistics for {country}',
        xaxis_title='Date',
        yaxis_title='Count',
        hovermode='x unified',
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        ),
        template='plotly_white'
    )
    
    return fig

def create_daily_changes_plot(data: pd.DataFrame, country: str) -> go.Figure:
    """Create a plot showing daily changes."""
    fig = go.Figure()
    
    metrics = {
        'NewConfirmed': '#1f77b4',
        'NewDeaths': '#d62728',
        'NewRecovered': '#2ca02c'
    }
    
    for metric, color in metrics.items():
        fig.add_trace(go.Bar(
            x=data['Date'],
            y=data[metric],
            name=metric.replace('New', 'New '),
            marker_color=color,
            hovertemplate=f"%{{y:,.0f}}<br>Date: %{{x|%Y-%m-%d}}<extra></extra>"
        ))
    
    fig.update_layout(
        title=f'Daily Changes in {country}',
        xaxis_title='Date',
        yaxis_title='Count',
        barmode='group',
        template='plotly_white'
    )
    
    return fig

def display_metrics(stats: Dict):
    """Display key metrics in a formatted way."""
    cols = st.columns(5)
    metrics = [
        ("Total Cases", stats['total_cases'], "🦠"),
        ("Deaths", stats['total_deaths'], "💀"),
        ("Recovered", stats['total_recovered'], "💪"),
        ("Active Cases", stats['active_cases'], "🏥"),
        ("Mortality Rate", f"{stats['mortality_rate']:.2f}%", "📊")
    ]
    
    for col, (label, value, emoji) in zip(cols, metrics):
        col.metric(
            label=f"{emoji} {label}",
            value=f"{value:,.0f}" if isinstance(value, (int, float)) else value
        )

def main():
    # Page configuration
    st.set_page_config(
        page_title="COVID-19 Dashboard",
        page_icon="🦠",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Title and description
    st.title("🦠 COVID-19 Dashboard")
    st.markdown("Interactive dashboard showing COVID-19 statistics worldwide")
    
    # Load data
    df = load_data()
    if df.empty:
        st.stop()
    
    # Sidebar controls
    st.sidebar.header("📊 Controls")
    countries = sorted(df['Country'].unique())
    country = st.sidebar.selectbox("Select a country", countries)
    
    # Filter data for selected country
    country_data = df[df['Country'] == country]
    
    # Calculate and display key statistics
    stats = calculate_statistics(country_data)
    display_metrics(stats)
    
    # Create and display main visualizations
    st.subheader("📈 Time Series Analysis")
    time_series_fig = create_time_series_plot(country_data, country)
    st.plotly_chart(time_series_fig, use_container_width=True)
    
    st.subheader("📊 Daily Changes")
    daily_changes_fig = create_daily_changes_plot(country_data, country)
    st.plotly_chart(daily_changes_fig, use_container_width=True)
    
    # Optional raw data display
    with st.expander("🔍 View Raw Data"):
        st.dataframe(
            country_data.style.format({
                col: '{:,.0f}' for col in country_data.select_dtypes('number').columns
            })
        )

if __name__ == "__main__":
    main()