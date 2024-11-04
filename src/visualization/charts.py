import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import json

def load_data():
    """Load processed US COVID-19 data."""
    try:
        df = pd.read_csv('data/processed/us_covid_data_combined.csv')
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Calculate rolling averages for smoother curves
        metrics = ['Confirmed', 'Deaths', 'Recovered', 'Active']
        for metric in metrics:
            df[f'New_{metric}'] = df.groupby('Province_State')[metric].diff().fillna(0)
            df[f'New_{metric}_MA7'] = df.groupby('Province_State')[f'New_{metric}'].rolling(7).mean().reset_index(0, drop=True)
        
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None

def format_large_number(num):
    """Format large numbers with K/M/B suffixes."""
    if num >= 1e9:
        return f"{num/1e9:.1f}B"
    if num >= 1e6:
        return f"{num/1e6:.1f}M"
    if num >= 1e3:
        return f"{num/1e3:.1f}K"
    return f"{num:.0f}"

def create_metrics(state_data):
    """Create metrics display for the dashboard."""
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
            value = format_large_number(total)
            delta = format_large_number(weekly_change) + " (7d)"
        
        col.metric(
            label=label,
            value=value,
            delta=delta,
            delta_color="inverse" if label == "Deaths" else "normal"
        )

def create_us_map(df, metric='Confirmed', date=None):
    """Create a choropleth map of the US showing COVID-19 metrics by state."""
    if date is None:
        date = df['Date'].max()
    
    # Get the latest data for each state
    latest_data = df[df['Date'] == date].copy()
    
    # Create the choropleth map
    fig = go.Figure(data=go.Choropleth(
        locations=latest_data['Province_State'],
        locationmode='USA-states',
        z=latest_data[metric],
        text=latest_data['Province_State'],
        colorscale='Reds',
        colorbar_title=metric,
        hovertemplate=(
            "<b>%{text}</b><br>" +
            f"{metric}: " + "%{z:,.0f}<br>" +
            "<extra></extra>"
        )
    ))
    
    fig.update_layout(
        title=f'US COVID-19 {metric} by State',
        geo_scope='usa',
        height=400,
        margin=dict(l=0, r=0, t=30, b=0)
    )
    
    return fig

def create_timeline_chart(state_data):
    """Create main timeline chart for state-level data."""
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            'Cumulative Cases',
            'Daily New Cases (7-day avg)',
            'Testing and Hospitalization',
            'Case Fatality Rate'
        ),
        vertical_spacing=0.15,
        horizontal_spacing=0.1
    )
    
    # Cumulative cases
    fig.add_trace(
        go.Scatter(
            x=state_data['Date'],
            y=state_data['Confirmed'],
            name="Confirmed",
            line=dict(color='#3498db', width=2)
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=state_data['Date'],
            y=state_data['Deaths'],
            name="Deaths",
            line=dict(color='#e74c3c', width=2)
        ),
        row=1, col=1
    )
    
    # Daily new cases
    fig.add_trace(
        go.Bar(
            x=state_data['Date'],
            y=state_data['New_Confirmed'],
            name="New Cases",
            marker_color='rgba(52, 152, 219, 0.3)'
        ),
        row=1, col=2
    )
    
    fig.add_trace(
        go.Scatter(
            x=state_data['Date'],
            y=state_data['New_Confirmed_MA7'],
            name="7-day avg",
            line=dict(color='#3498db', width=2)
        ),
        row=1, col=2
    )
    
    # Testing and hospitalization
    fig.add_trace(
        go.Scatter(
            x=state_data['Date'],
            y=state_data['Total_Test_Results'],
            name="Total Tests",
            line=dict(color='#9b59b6', width=2)
        ),
        row=2, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=state_data['Date'],
            y=state_data['Hospitalization_Rate'],
            name="Hosp. Rate",
            line=dict(color='#f1c40f', width=2),
            yaxis="y4"
        ),
        row=2, col=1
    )
    
    # Case fatality rate
    fig.add_trace(
        go.Scatter(
            x=state_data['Date'],
            y=state_data['Case_Fatality_Ratio'],
            name="Fatality Rate",
            line=dict(color='#e74c3c', width=2)
        ),
        row=2, col=2
    )
    
    # Update layout
    fig.update_layout(
        height=800,
        showlegend=True,
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        hovermode='x unified',
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )
    
    # Update axes
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(128,128,128,0.2)')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgba(128,128,128,0.2)')
    
    return fig

def main():
    # Page config
    st.set_page_config(
        page_title="US COVID-19 Dashboard",
        page_icon="🦠",
        layout="wide"
    )
    
    # Title and description
    st.title("🦠 US COVID-19 Dashboard")
    st.markdown("""
    This dashboard shows state-level COVID-19 statistics across the United States. 
    Data is sourced from the Johns Hopkins University CSSE COVID-19 Data Repository.
    """)
    
    # Load data
    df = load_data()
    if df is None:
        st.stop()
    
    # Sidebar controls
    st.sidebar.title("Controls")
    
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