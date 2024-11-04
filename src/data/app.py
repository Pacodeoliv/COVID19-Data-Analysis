import streamlit as st
import pandas as pd
from data_loader import DataLoader
from visualizations import create_us_map, create_timeline_chart

def format_number(num):
    """Format large numbers with K/M/B suffixes"""
    if num >= 1e9:
        return f"{num/1e9:.1f}B"
    if num >= 1e6:
        return f"{num/1e6:.1f}M"
    if num >= 1e3:
        return f"{num/1e3:.1f}K"
    return f"{num:.0f}"

def create_metrics(state_data):
    """Create metrics display for the dashboard"""
    latest = state_data.iloc[-1]
    prev = state_data.iloc[-8]  # Compare with week ago
    
    metrics = {
        "Total Cases": (latest['Confirmed'], latest['Confirmed'] - prev['Confirmed']),
        "Deaths": (latest['Deaths'], latest['Deaths'] - prev['Deaths']),
        "Tests": (latest['Total_Test_Results'], latest['Total_Test_Results'] - prev['Total_Test_Results']),
        "Hospitalization Rate": (latest['Hospitalization_Rate'], latest['Hospitalization_Rate'] - prev['Hospitalization_Rate'])
    }
    
    cols = st.columns(len(metrics))
    for col, (label, (total, weekly_change)) in zip(cols, metrics.items()):
        if label == "Hospitalization Rate":
            value = f"{total:.1f}%"
            delta = f"{weekly_change:+.1f}%"
        else:
            value = format_number(total)
            delta = format_number(weekly_change) + " (7d)"
        
        col.metric(
            label=label,
            value=value,
            delta=delta,
            delta_color="inverse" if label == "Deaths" else "normal"
        )

def main():
    st.set_page_config(
        page_title="US COVID-19 Dashboard",
        page_icon="🦠",
        layout="wide"
    )
    
    st.title("🦠 US COVID-19 Dashboard")
    st.markdown("""
    This dashboard shows state-level COVID-19 statistics across the United States. 
    Data is sourced from the Johns Hopkins University CSSE COVID-19 Data Repository.
    """)
    
    # Initialize data loader
    loader = DataLoader()
    
    # Add data refresh button in sidebar
    st.sidebar.title("Controls")
    if st.sidebar.button("Refresh Data"):
        df = loader.load_data(force_refresh=True)
    else:
        df = loader.load_data()
    
    if df is None:
        st.error("Failed to load data. Please try refreshing.")
        return
    
    # State selection
    states = sorted(df['Province_State'].unique())
    state = st.sidebar.selectbox(
        "Select State",
        states,
        index=states.index('New York') if 'New York' in states else 0
    )
    
    # Metric selection for map
    map_metric = st.sidebar.selectbox(
        "Select Map Metric",
        ['Confirmed', 'Deaths', 'Incident_Rate', 'Case_Fatality_Ratio'],
        index=0
    )
    
    # Date selection
    selected_date = st.sidebar.date_input(
        "Select Date",
        value=df['Date'].max().date(),
        min_value=df['Date'].min().date(),
        max_value=df['Date'].max().date()
    )
    
    # Create US map
    st.subheader("US COVID-19 Map")
    map_fig = create_us_map(df, metric=map_metric, date=pd.to_datetime(selected_date))
    st.plotly_chart(map_fig, use_container_width=True)
    
    # Filter data for selected state
    state_data = df[df['Province_State'] == state].copy()
    
    # Display metrics for selected state
    st.subheader(f"COVID-19 Statistics for {state}")
    create_metrics(state_data)
    
    # Display timeline charts
    timeline_fig = create_timeline_chart(state_data)
    st.plotly_chart(timeline_fig, use_container_width=True)
    
    # Data table
    with st.expander("Show Raw Data"):
        st.dataframe(
            state_data.sort_values('Date', ascending=False),
            use_container_width=True
        )

if __name__ == "__main__":
    main()