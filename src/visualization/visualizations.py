import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

def create_us_map(df, metric='Confirmed', date=None):
    """Create a choropleth map of the US showing COVID-19 metrics by state"""
    if date is None:
        date = df['Date'].max()
    
    latest_data = df[df['Date'] == date].copy()
    
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
    """Create timeline charts for state-level data"""
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
        go.Scatter(x=state_data['Date'], y=state_data['Confirmed'],
                  name="Confirmed", line=dict(color='#3498db', width=2)),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(x=state_data['Date'], y=state_data['Deaths'],
                  name="Deaths", line=dict(color='#e74c3c', width=2)),
        row=1, col=1
    )
    
    # Daily new cases
    fig.add_trace(
        go.Bar(x=state_data['Date'], y=state_data['New_Confirmed'],
               name="New Cases", marker_color='rgba(52, 152, 219, 0.3)'),
        row=1, col=2
    )
    
    fig.add_trace(
        go.Scatter(x=state_data['Date'], y=state_data['New_Confirmed_MA7'],
                  name="7-day avg", line=dict(color='#3498db', width=2)),
        row=1, col=2
    )
    
    # Testing and hospitalization
    fig.add_trace(
        go.Scatter(x=state_data['Date'], y=state_data['Total_Test_Results'],
                  name="Total Tests", line=dict(color='#9b59b6', width=2)),
        row=2, col=1
    )
    
    fig.add_trace(
        go.Scatter(x=state_data['Date'], y=state_data['Hospitalization_Rate'],
                  name="Hosp. Rate", line=dict(color='#f1c40f', width=2)),
        row=2, col=1
    )
    
    # Case fatality rate
    fig.add_trace(
        go.Scatter(x=state_data['Date'], y=state_data['Case_Fatality_Ratio'],
                  name="Fatality Rate", line=dict(color='#e74c3c', width=2)),
        row=2, col=2
    )
    
    fig.update_layout(
        height=800,
        showlegend=True,
        template='plotly_white',
        hovermode='x unified'
    )
    
    return fig