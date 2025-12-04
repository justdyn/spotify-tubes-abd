"""
European Top 5 Football Leagues - Interactive Dashboard
========================================================
A comprehensive data visualization application for analyzing European football data
from Premier League, La Liga, Bundesliga, Serie A, and Ligue 1.

Author: Senior Data Scientist & Streamlit Developer
Database: PostgreSQL (laliga_europe)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from psycopg2.extras import RealDictCursor
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Country coordinates for mapping
COUNTRY_COORDS = {
    'England': {'lat': 52.3555, 'lon': -1.1743, 'code': 'GBR'},
    'Spain': {'lat': 40.4637, 'lon': -3.7492, 'code': 'ESP'},
    'Germany': {'lat': 51.1657, 'lon': 10.4515, 'code': 'DEU'},
    'Italy': {'lat': 41.8719, 'lon': 12.5674, 'code': 'ITA'},
    'France': {'lat': 46.2276, 'lon': 2.2137, 'code': 'FRA'},
}

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="European Top 5 Leagues Dashboard",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# CUSTOM CSS FOR BETTER UI/UX
# ============================================================================

st.markdown("""
<style>
    /* Main container styling */
    .main {
        padding: 0rem 1rem;
    }
    
    /* Metric cards */
    div[data-testid="stMetricValue"] {
        font-size: 28px;
        font-weight: 600;
    }
    
    div[data-testid="stMetricLabel"] {
        font-size: 16px;
        font-weight: 500;
    }
    
    /* Headers */
    h1 {
        color: #1f77b4;
        padding-bottom: 10px;
        border-bottom: 3px solid #1f77b4;
    }
    
    h2 {
        color: #2c3e50;
        margin-top: 20px;
    }
    
    h3 {
        color: #34495e;
    }
    
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #f8f9fa;
    }
    
    /* Tables */
    .dataframe {
        font-size: 14px;
    }
    
    /* Info boxes */
    .stAlert {
        border-radius: 10px;
    }
    
    /* Buttons */
    .stButton>button {
        border-radius: 5px;
        font-weight: 600;
    }
    
    /* Cards */
    .css-1r6slb0 {
        background-color: #ffffff;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

@st.cache_resource
def get_database_connection():
    """
    Establish connection to PostgreSQL database with connection pooling.
    Uses st.cache_resource to maintain connection across reruns.
    """
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="laliga_europe",
            user="postgres",
            password="1",
            port="5432"
        )
        return conn
    except Exception as e:
        st.error(f"❌ Database Connection Error: {str(e)}")
        st.info("Please ensure PostgreSQL is running and the database 'laliga_europe' exists.")
        st.stop()

@st.cache_data(ttl=300)  # Cache for 5 minutes
def execute_query(query, params=None):
    """
    Execute SQL query and return results as DataFrame.
    Implements caching for better performance.
    """
    conn = get_database_connection()
    try:
        df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Query Error: {str(e)}")
        return pd.DataFrame()

# ============================================================================
# DATA FETCHING FUNCTIONS
# ============================================================================

def get_leagues():
    """Fetch all leagues"""
    query = """
    SELECT league_id, name, country 
    FROM leagues 
    WHERE is_active = true
    ORDER BY name
    """
    return execute_query(query)

def get_seasons():
    """Fetch available seasons"""
    query = """
    SELECT DISTINCT season 
    FROM games 
    ORDER BY season DESC
    """
    return execute_query(query)

def get_database_overview():
    """Get comprehensive database statistics"""
    query = """
    SELECT 
        (SELECT COUNT(*) FROM leagues WHERE is_active = true) as total_leagues,
        (SELECT COUNT(*) FROM teams WHERE is_active = true) as total_teams,
        (SELECT COUNT(*) FROM players WHERE is_active = true) as total_players,
        (SELECT COUNT(*) FROM games WHERE status = 'completed') as total_games,
        (SELECT COUNT(*) FROM shots) as total_shots,
        (SELECT COUNT(*) FROM appearances) as total_appearances,
        (SELECT MIN(season) FROM games) as first_season,
        (SELECT MAX(season) FROM games) as last_season,
        (SELECT SUM(home_goals + away_goals) FROM games) as total_goals
    """
    return execute_query(query)

def get_league_standings(league_id, season):
    """Get league standings for specific season"""
    query = """
    SELECT 
        team_name,
        matches_played,
        wins,
        draws,
        losses,
        goals_for,
        goals_against,
        goal_difference,
        points,
        ROUND(avg_possession, 1) as avg_possession
    FROM mv_league_standings
    WHERE league_id = %s AND season = %s
    ORDER BY points DESC, goal_difference DESC, goals_for DESC
    """
    return execute_query(query, (league_id, season))

def get_top_scorers(league_id, season, limit=20):
    """Get top scorers for specific league and season"""
    query = """
    SELECT 
        player_name,
        nationality,
        games_played,
        total_goals,
        total_assists,
        ROUND(goals_per_90, 2) as goals_per_90,
        ROUND(avg_x_goals, 2) as avg_xg
    FROM mv_top_scorers
    WHERE league_id = %s AND season = %s
    ORDER BY total_goals DESC, goals_per_90 DESC
    LIMIT %s
    """
    return execute_query(query, (league_id, season, limit))

def get_team_performance_comparison(league_id, season):
    """Compare team performances"""
    query = """
    SELECT 
        t.name as team_name,
        COUNT(DISTINCT ts.game_id) as games_played,
        SUM(ts.goals) as goals_scored,
        ROUND(AVG(ts.x_goals)::numeric, 2) as avg_xg,
        SUM(ts.shots) as total_shots,
        SUM(ts.shots_on_target) as shots_on_target,
        ROUND((SUM(ts.shots_on_target)::numeric / NULLIF(SUM(ts.shots), 0) * 100), 1) as shot_accuracy,
        ROUND(AVG(ts.possession_percentage)::numeric, 1) as avg_possession,
        SUM(CASE WHEN ts.result = 'win' THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN ts.result = 'draw' THEN 1 ELSE 0 END) as draws,
        SUM(CASE WHEN ts.result = 'loss' THEN 1 ELSE 0 END) as losses
    FROM team_stats ts
    JOIN games g ON ts.game_id = g.game_id
    JOIN teams t ON ts.team_id = t.team_id
    WHERE g.league_id = %s AND g.season = %s AND g.status = 'completed'
    GROUP BY t.name
    ORDER BY wins DESC, goals_scored DESC
    """
    return execute_query(query, (league_id, season))

def get_goals_timeline(league_id, season):
    """Get goals scored over time"""
    query = """
    SELECT 
        DATE_TRUNC('month', g.date) as month,
        SUM(g.home_goals + g.away_goals) as total_goals,
        COUNT(*) as matches_played,
        ROUND(AVG(g.home_goals + g.away_goals)::numeric, 2) as avg_goals_per_match
    FROM games g
    WHERE g.league_id = %s AND g.season = %s AND g.status = 'completed'
    GROUP BY DATE_TRUNC('month', g.date)
    ORDER BY month
    """
    return execute_query(query, (league_id, season))

def get_shot_analysis(league_id, season):
    """Analyze shot patterns"""
    query = """
    SELECT 
        shot_result,
        COUNT(*) as shot_count,
        ROUND(AVG(x_goal)::numeric, 3) as avg_xg,
        COUNT(CASE WHEN shot_result = 'Goal' THEN 1 END) as goals
    FROM shots s
    JOIN games g ON s.game_id = g.game_id
    WHERE g.league_id = %s AND g.season = %s 
    AND shot_result IS NOT NULL
    GROUP BY shot_result
    ORDER BY shot_count DESC
    """
    return execute_query(query, (league_id, season))

def get_home_away_analysis(league_id, season):
    """Analyze home vs away performance"""
    query = """
    SELECT 
        location,
        COUNT(*) as matches,
        SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END) as draws,
        SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) as losses,
        ROUND(AVG(goals)::numeric, 2) as avg_goals,
        ROUND(AVG(x_goals)::numeric, 2) as avg_xg,
        ROUND(AVG(possession_percentage)::numeric, 1) as avg_possession
    FROM team_stats ts
    JOIN games g ON ts.game_id = g.game_id
    WHERE g.league_id = %s AND g.season = %s
    GROUP BY location
    """
    return execute_query(query, (league_id, season))

def get_player_positions_analysis(league_id, season):
    """Analyze player performance by position"""
    query = """
    SELECT 
        COALESCE(a.position, 'Unknown') as position,
        COUNT(DISTINCT a.player_id) as players_count,
        SUM(a.goals) as total_goals,
        SUM(a.assists) as total_assists,
        ROUND(AVG(a.time_played)::numeric, 1) as avg_minutes
    FROM appearances a
    JOIN games g ON a.game_id = g.game_id
    WHERE g.league_id = %s AND g.season = %s
    AND a.position IS NOT NULL
    GROUP BY a.position
    ORDER BY total_goals DESC
    """
    return execute_query(query, (league_id, season))

def get_league_comparison_stats():
    """Compare all leagues across latest season"""
    query = """
    WITH latest_season AS (
        SELECT MAX(season) as season FROM games
    )
    SELECT 
        l.name as league,
        l.country,
        COUNT(DISTINCT g.game_id) as total_matches,
        SUM(g.home_goals + g.away_goals) as total_goals,
        ROUND(AVG(g.home_goals + g.away_goals)::numeric, 2) as avg_goals_per_match,
        COUNT(DISTINCT ts.team_id) as teams_count,
        SUM(ts.shots) as total_shots,
        ROUND(AVG(ts.possession_percentage)::numeric, 1) as avg_possession
    FROM leagues l
    JOIN games g ON l.league_id = g.league_id
    JOIN team_stats ts ON g.game_id = ts.game_id
    JOIN latest_season ls ON g.season = ls.season
    WHERE g.status = 'completed'
    GROUP BY l.name, l.country
    ORDER BY total_goals DESC
    """
    return execute_query(query)

def get_teams_by_country():
    """Get teams grouped by country"""
    query = """
    SELECT 
        l.country,
        COUNT(DISTINCT t.team_id) as team_count,
        COUNT(DISTINCT g.game_id) as total_matches,
        SUM(g.home_goals + g.away_goals) as total_goals
    FROM leagues l
    JOIN teams t ON l.league_id = t.league_id
    LEFT JOIN games g ON (g.home_team_id = t.team_id OR g.away_team_id = t.team_id)
    WHERE t.is_active = true AND g.status = 'completed'
    GROUP BY l.country
    ORDER BY team_count DESC
    """
    return execute_query(query)

def get_player_nationalities():
    """Get player distribution by nationality"""
    query = """
    SELECT 
        COALESCE(p.nationality, 'Unknown') as nationality,
        COUNT(DISTINCT p.player_id) as player_count,
        SUM(a.goals) as total_goals,
        SUM(a.assists) as total_assists,
        COUNT(DISTINCT a.game_id) as total_appearances
    FROM players p
    LEFT JOIN appearances a ON p.player_id = a.player_id
    WHERE p.is_active = true
    GROUP BY p.nationality
    HAVING COUNT(DISTINCT p.player_id) >= 5
    ORDER BY player_count DESC
    LIMIT 30
    """
    return execute_query(query)

def get_league_teams_map():
    """Get teams with their league countries for mapping"""
    query = """
    SELECT 
        t.name as team_name,
        l.name as league_name,
        l.country,
        COUNT(DISTINCT g.game_id) as matches_played,
        SUM(CASE WHEN ts.result = 'win' THEN 1 ELSE 0 END) as wins,
        SUM(ts.goals) as total_goals
    FROM teams t
    JOIN leagues l ON t.league_id = l.league_id
    LEFT JOIN games g ON (g.home_team_id = t.team_id OR g.away_team_id = t.team_id)
    LEFT JOIN team_stats ts ON ts.game_id = g.game_id AND ts.team_id = t.team_id
    WHERE t.is_active = true AND g.status = 'completed'
    GROUP BY t.name, l.name, l.country
    ORDER BY total_goals DESC
    """
    return execute_query(query)

# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================

def create_standings_table(df):
    """Create formatted standings table"""
    if df.empty:
        return None
    
    # Add ranking
    df.insert(0, 'Rank', range(1, len(df) + 1))
    
    # Format column names
    df.columns = ['Rank', 'Team', 'MP', 'W', 'D', 'L', 'GF', 'GA', 'GD', 'Pts', 'Poss%']
    
    return df

def plot_goals_timeline(df):
    """Create interactive line chart for goals over time"""
    if df.empty:
        return None
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['month'],
        y=df['total_goals'],
        mode='lines+markers',
        name='Total Goals',
        line=dict(color='#1f77b4', width=3),
        marker=dict(size=8),
        hovertemplate='<b>%{x|%B %Y}</b><br>Goals: %{y}<extra></extra>'
    ))
    
    fig.add_trace(go.Scatter(
        x=df['month'],
        y=df['avg_goals_per_match'],
        mode='lines+markers',
        name='Avg Goals/Match',
        line=dict(color='#ff7f0e', width=2, dash='dash'),
        marker=dict(size=6),
        yaxis='y2',
        hovertemplate='<b>%{x|%B %Y}</b><br>Avg: %{y:.2f}<extra></extra>'
    ))
    
    fig.update_layout(
        title='Goals Scored Over Season',
        xaxis_title='Month',
        yaxis_title='Total Goals',
        yaxis2=dict(
            title='Avg Goals per Match',
            overlaying='y',
            side='right'
        ),
        hovermode='x unified',
        template='plotly_white',
        height=400
    )
    
    return fig

def plot_team_comparison(df):
    """Create bar chart comparing team performances"""
    if df.empty:
        return None
    
    # Sort by wins
    df = df.sort_values('wins', ascending=True)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=df['team_name'],
        x=df['wins'],
        name='Wins',
        orientation='h',
        marker_color='#2ecc71',
        hovertemplate='<b>%{y}</b><br>Wins: %{x}<extra></extra>'
    ))
    
    fig.add_trace(go.Bar(
        y=df['team_name'],
        x=df['draws'],
        name='Draws',
        orientation='h',
        marker_color='#f39c12',
        hovertemplate='<b>%{y}</b><br>Draws: %{x}<extra></extra>'
    ))
    
    fig.add_trace(go.Bar(
        y=df['team_name'],
        x=df['losses'],
        name='Losses',
        orientation='h',
        marker_color='#e74c3c',
        hovertemplate='<b>%{y}</b><br>Losses: %{x}<extra></extra>'
    ))
    
    fig.update_layout(
        title='Team Performance Comparison',
        xaxis_title='Number of Matches',
        yaxis_title='',
        barmode='stack',
        template='plotly_white',
        height=max(400, len(df) * 25),
        showlegend=True,
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )
    
    return fig

def plot_shot_analysis(df):
    """Create pie chart for shot analysis"""
    if df.empty:
        return None
    
    colors = ['#2ecc71', '#3498db', '#e74c3c', '#f39c12', '#9b59b6', '#95a5a6']
    
    fig = go.Figure(data=[go.Pie(
        labels=df['shot_result'],
        values=df['shot_count'],
        hole=0.4,
        marker_colors=colors,
        hovertemplate='<b>%{label}</b><br>Shots: %{value}<br>Percentage: %{percent}<extra></extra>'
    )])
    
    fig.update_layout(
        title='Shot Outcome Distribution',
        template='plotly_white',
        height=400
    )
    
    return fig

def plot_home_away_comparison(df):
    """Create grouped bar chart for home vs away comparison"""
    if df.empty:
        return None
    
    metrics = ['wins', 'draws', 'losses']
    
    fig = go.Figure()
    
    for metric in metrics:
        fig.add_trace(go.Bar(
            name=metric.capitalize(),
            x=df['location'],
            y=df[metric],
            text=df[metric],
            textposition='auto',
            hovertemplate=f'<b>%{{x}}</b><br>{metric.capitalize()}: %{{y}}<extra></extra>'
        ))
    
    fig.update_layout(
        title='Home vs Away Performance',
        xaxis_title='Location',
        yaxis_title='Number of Matches',
        barmode='group',
        template='plotly_white',
        height=400
    )
    
    return fig

def plot_top_scorers(df, top_n=10):
    """Create horizontal bar chart for top scorers"""
    if df.empty:
        return None
    
    df_top = df.head(top_n).sort_values('total_goals', ascending=True)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=df_top['player_name'],
        x=df_top['total_goals'],
        orientation='h',
        marker_color='#e74c3c',
        text=df_top['total_goals'],
        textposition='auto',
        hovertemplate='<b>%{y}</b><br>Goals: %{x}<br>Assists: %{customdata[0]}<extra></extra>',
        customdata=df_top[['total_assists']]
    ))
    
    fig.update_layout(
        title=f'Top {top_n} Goal Scorers',
        xaxis_title='Goals',
        yaxis_title='',
        template='plotly_white',
        height=max(400, top_n * 40)
    )
    
    return fig

def plot_league_comparison(df):
    """Create radar chart comparing leagues"""
    if df.empty:
        return None
    
    fig = go.Figure()
    
    for idx, row in df.iterrows():
        fig.add_trace(go.Scatterpolar(
            r=[row['total_matches'], row['total_goals'], row['avg_goals_per_match']*100, 
               row['total_shots']/100, row['avg_possession']],
            theta=['Total Matches', 'Total Goals', 'Avg Goals/Match (x100)', 
                   'Total Shots (/100)', 'Avg Possession'],
            fill='toself',
            name=row['league']
        ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, None])
        ),
        showlegend=True,
        title='League Comparison (Normalized Metrics)',
        height=500
    )
    
    return fig

def plot_possession_vs_goals(df):
    """Create scatter plot for possession vs goals"""
    if df.empty:
        return None
    
    fig = px.scatter(
        df,
        x='avg_possession',
        y='goals_scored',
        size='shot_accuracy',
        color='wins',
        hover_name='team_name',
        labels={
            'avg_possession': 'Average Possession (%)',
            'goals_scored': 'Goals Scored',
            'shot_accuracy': 'Shot Accuracy (%)',
            'wins': 'Wins'
        },
        title='Possession vs Goals Scored (Size: Shot Accuracy)',
        color_continuous_scale='Viridis'
    )
    
    fig.update_layout(
        template='plotly_white',
        height=500
    )
    
    return fig

def plot_teams_map(df):
    """Create interactive map showing teams by country"""
    if df.empty:
        return None
    
    # Add coordinates to dataframe
    df['lat'] = df['country'].map(lambda x: COUNTRY_COORDS.get(x, {}).get('lat', 0))
    df['lon'] = df['country'].map(lambda x: COUNTRY_COORDS.get(x, {}).get('lon', 0))
    df['country_code'] = df['country'].map(lambda x: COUNTRY_COORDS.get(x, {}).get('code', 'UNK'))
    
    # Filter out countries without coordinates
    df = df[df['lat'] != 0]
    
    fig = px.scatter_geo(
        df,
        lat='lat',
        lon='lon',
        size='total_goals',
        color='wins',
        hover_name='team_name',
        hover_data={
            'league_name': True,
            'country': True,
            'matches_played': True,
            'wins': True,
            'total_goals': True,
            'lat': False,
            'lon': False,
            'country_code': False
        },
        title='Teams Distribution Across Europe',
        color_continuous_scale='Viridis',
        size_max=30,
        projection='natural earth'
    )
    
    fig.update_geos(
        scope='europe',
        showland=True,
        landcolor='rgb(243, 243, 243)',
        coastlinecolor='rgb(204, 204, 204)',
        showlakes=True,
        lakecolor='rgb(230, 245, 255)',
        showcountries=True,
        countrycolor='rgb(204, 204, 204)',
        bgcolor='rgba(0,0,0,0)'
    )
    
    fig.update_layout(
        template='plotly_white',
        height=600,
        margin=dict(l=0, r=0, t=40, b=0),
        font=dict(size=12)
    )
    
    return fig

def plot_leagues_choropleth():
    """Create choropleth map of European leagues"""
    # Create data for the 5 countries
    data = []
    for country, coords in COUNTRY_COORDS.items():
        data.append({
            'country': country,
            'code': coords['code'],
            'lat': coords['lat'],
            'lon': coords['lon']
        })
    
    df = pd.DataFrame(data)
    
    fig = go.Figure(data=go.Choropleth(
        locations=df['code'],
        z=[1, 1, 1, 1, 1],  # All countries have equal weight
        text=df['country'],
        colorscale=[[0, '#1f77b4'], [1, '#ff7f0e']],
        autocolorscale=False,
        showscale=False,
        geo='geo',
        marker_line_color='white',
        marker_line_width=2,
        hovertemplate='<b>%{text}</b><br>Top 5 League Country<extra></extra>'
    ))
    
    fig.update_geos(
        scope='europe',
        showland=True,
        landcolor='rgb(243, 243, 243)',
        coastlinecolor='rgb(204, 204, 204)',
        showlakes=True,
        lakecolor='rgb(230, 245, 255)',
        showcountries=True,
        countrycolor='rgb(204, 204, 204)',
        bgcolor='rgba(0,0,0,0)',
        projection_type='natural earth'
    )
    
    fig.update_layout(
        title='European Top 5 Leagues - Geographic Distribution',
        template='plotly_white',
        height=500,
        margin=dict(l=0, r=0, t=40, b=0)
    )
    
    return fig

def plot_player_nationalities_map(df):
    """Create map showing player nationalities distribution"""
    if df.empty:
        return None
    
    # Map common nationalities to coordinates (approximate)
    nationality_coords = {
        'England': {'lat': 52.3555, 'lon': -1.1743},
        'Spain': {'lat': 40.4637, 'lon': -3.7492},
        'Germany': {'lat': 51.1657, 'lon': 10.4515},
        'Italy': {'lat': 41.8719, 'lon': 12.5674},
        'France': {'lat': 46.2276, 'lon': 2.2137},
        'Brazil': {'lat': -14.2350, 'lon': -51.9253},
        'Argentina': {'lat': -38.4161, 'lon': -63.6167},
        'Portugal': {'lat': 39.3999, 'lon': -8.2245},
        'Netherlands': {'lat': 52.1326, 'lon': 5.2913},
        'Belgium': {'lat': 50.5039, 'lon': 4.4699},
        'Croatia': {'lat': 45.1, 'lon': 15.2},
        'Serbia': {'lat': 44.0165, 'lon': 21.0059},
        'Poland': {'lat': 51.9194, 'lon': 19.1451},
        'Uruguay': {'lat': -32.5228, 'lon': -55.7658},
        'Colombia': {'lat': 4.5709, 'lon': -74.2973},
        'Senegal': {'lat': 14.4974, 'lon': -14.4524},
        'Nigeria': {'lat': 9.0820, 'lon': 8.6753},
        'Ghana': {'lat': 7.9465, 'lon': -1.0232},
        'Ivory Coast': {'lat': 7.5400, 'lon': -5.5471},
        'Morocco': {'lat': 31.7917, 'lon': -7.0926},
        'Algeria': {'lat': 28.0339, 'lon': 1.6596},
        'Japan': {'lat': 36.2048, 'lon': 138.2529},
        'South Korea': {'lat': 35.9078, 'lon': 127.7669},
        'Mexico': {'lat': 23.6345, 'lon': -102.5528},
        'Chile': {'lat': -35.6751, 'lon': -71.5430},
        'Denmark': {'lat': 56.2639, 'lon': 9.5018},
        'Sweden': {'lat': 60.1282, 'lon': 18.6435},
        'Norway': {'lat': 60.4720, 'lon': 8.4689},
        'Austria': {'lat': 47.5162, 'lon': 14.5501},
        'Switzerland': {'lat': 46.8182, 'lon': 8.2275},
    }
    
    # Add coordinates
    df['lat'] = df['nationality'].map(lambda x: nationality_coords.get(x, {}).get('lat', None))
    df['lon'] = df['nationality'].map(lambda x: nationality_coords.get(x, {}).get('lon', None))
    
    # Filter out unknown coordinates
    df_mapped = df[df['lat'].notna()].copy()
    
    if df_mapped.empty:
        return None
    
    fig = px.scatter_geo(
        df_mapped,
        lat='lat',
        lon='lon',
        size='player_count',
        color='total_goals',
        hover_name='nationality',
        hover_data={
            'player_count': True,
            'total_goals': True,
            'total_assists': True,
            'total_appearances': True,
            'lat': False,
            'lon': False
        },
        title='Player Nationalities Distribution',
        color_continuous_scale='Plasma',
        size_max=40
    )
    
    fig.update_geos(
        projection_type='natural earth',
        showland=True,
        landcolor='rgb(243, 243, 243)',
        coastlinecolor='rgb(204, 204, 204)',
        showlakes=True,
        lakecolor='rgb(230, 245, 255)',
        showcountries=True,
        countrycolor='rgb(204, 204, 204)',
        bgcolor='rgba(0,0,0,0)'
    )
    
    fig.update_layout(
        template='plotly_white',
        height=600,
        margin=dict(l=0, r=0, t=40, b=0)
    )
    
    return fig

# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    """Main application function"""
    
    # Header
    st.title("⚽ European Top 5 Football Leagues Dashboard")
    st.markdown("*Interactive analysis of Premier League, La Liga, Bundesliga, Serie A, and Ligue 1*")
    st.markdown("---")
    
    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/color/96/000000/football2--v1.png", width=80)
        st.title("🎯 Navigation")
        
        page = st.radio(
            "Select View",
            ["📊 Overview", "🏆 League Analysis", "👤 Player Statistics", 
             "📈 Advanced Analytics", "🗺️ Geographic Analysis", "🔍 Database Explorer"],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        st.info("💡 **Tip**: Hover over charts for detailed information!")
        
        # Database status
        st.markdown("---")
        st.subheader("🔌 Database Status")
        try:
            conn = get_database_connection()
            st.success("✅ Connected")
        except:
            st.error("❌ Disconnected")
    
    # ========================================================================
    # PAGE: OVERVIEW
    # ========================================================================
    
    if page == "📊 Overview":
        st.header("📊 Database Overview")
        
        # Get overview stats
        overview = get_database_overview()
        
        if not overview.empty:
            row = overview.iloc[0]
            
            # Key metrics
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("🏆 Leagues", f"{row['total_leagues']}")
            with col2:
                st.metric("⚽ Teams", f"{row['total_teams']}")
            with col3:
                st.metric("👥 Players", f"{row['total_players']:,}")
            with col4:
                st.metric("🎮 Matches", f"{row['total_games']:,}")
            with col5:
                st.metric("⚽ Goals", f"{row['total_goals']:,}")
            
            st.markdown("---")
            
            # Additional info
            col1, col2 = st.columns(2)
            
            with col1:
                st.info(f"📅 **Season Range**: {row['first_season']} - {row['last_season']}")
                st.info(f"🎯 **Total Shots**: {row['total_shots']:,}")
            
            with col2:
                st.info(f"📊 **Player Appearances**: {row['total_appearances']:,}")
                avg_goals = row['total_goals'] / row['total_games'] if row['total_games'] > 0 else 0
                st.info(f"📈 **Avg Goals/Match**: {avg_goals:.2f}")
        
        st.markdown("---")
        
        # League comparison
        st.subheader("🏆 League Comparison (Latest Season)")
        league_comp = get_league_comparison_stats()
        
        if not league_comp.empty:
            # Display table
            # Handle None values before formatting
            league_comp_display = league_comp.fillna(0)
            st.dataframe(
                league_comp_display.style.format({
                    'total_matches': '{:,}',
                    'total_goals': '{:,}',
                    'avg_goals_per_match': '{:.2f}',
                    'total_shots': '{:,}',
                    'avg_possession': '{:.1f}'
                }),
                width='stretch',
                hide_index=True
            )
            
            # Visualizations
            col1, col2 = st.columns(2)
            
            with col1:
                # Bar chart for total goals
                fig = px.bar(
                    league_comp,
                    x='league',
                    y='total_goals',
                    color='country',
                    title='Total Goals by League',
                    labels={'total_goals': 'Total Goals', 'league': 'League'},
                    text='total_goals'
                )
                fig.update_traces(textposition='outside')
                fig.update_layout(template='plotly_white', height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Bar chart for avg goals per match
                fig = px.bar(
                    league_comp,
                    x='league',
                    y='avg_goals_per_match',
                    color='country',
                    title='Average Goals per Match',
                    labels={'avg_goals_per_match': 'Avg Goals/Match', 'league': 'League'},
                    text='avg_goals_per_match'
                )
                fig.update_traces(textposition='outside', texttemplate='%{text:.2f}')
                fig.update_layout(template='plotly_white', height=400)
                st.plotly_chart(fig, use_container_width=True)
    
    # ========================================================================
    # PAGE: LEAGUE ANALYSIS
    # ========================================================================
    
    elif page == "🏆 League Analysis":
        st.header("🏆 League Analysis")
        
        # Filters
        col1, col2 = st.columns(2)
        
        with col1:
            leagues = get_leagues()
            league_options = dict(zip(leagues['name'], leagues['league_id']))
            selected_league_name = st.selectbox("Select League", list(league_options.keys()))
            selected_league_id = league_options[selected_league_name]
        
        with col2:
            seasons = get_seasons()
            selected_season = st.selectbox("Select Season", seasons['season'].tolist())
        
        st.markdown("---")
        
        # League Standings
        st.subheader(f"📋 {selected_league_name} Standings - Season {selected_season}")
        standings = get_league_standings(selected_league_id, selected_season)
        
        if not standings.empty:
            standings_table = create_standings_table(standings)
            
            # Highlight top 4 and bottom 3
            def highlight_rows(row):
                if row['Rank'] <= 4:
                    return ['background-color: #d4edda'] * len(row)
                elif row['Rank'] >= len(standings_table) - 2:
                    return ['background-color: #f8d7da'] * len(row)
                else:
                    return [''] * len(row)
            
            st.dataframe(
                standings_table.style.apply(highlight_rows, axis=1),
                width='stretch',
                hide_index=True,
                height=600
            )
            
            st.caption("🟢 Top 4: Champions League qualification | 🔴 Bottom 3: Relegation zone")
        else:
            st.warning("No standings data available for this selection.")
        
        st.markdown("---")
        
        # Team Performance Comparison
        st.subheader("📊 Team Performance Metrics")
        team_perf = get_team_performance_comparison(selected_league_id, selected_season)
        
        if not team_perf.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = plot_team_comparison(team_perf)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = plot_possession_vs_goals(team_perf)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        
        # Goals Timeline
        st.subheader("⚽ Goals Timeline")
        goals_timeline = get_goals_timeline(selected_league_id, selected_season)
        
        if not goals_timeline.empty:
            fig = plot_goals_timeline(goals_timeline)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        
        # Home vs Away Analysis
        st.subheader("🏠 Home vs Away Performance")
        home_away = get_home_away_analysis(selected_league_id, selected_season)
        
        if not home_away.empty:
            col1, col2 = st.columns([1, 1])
            
            with col1:
                fig = plot_home_away_comparison(home_away)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Display metrics
                home_data = home_away[home_away['location'] == 'home'].iloc[0] if len(home_away) > 0 else None
                away_data = home_away[home_away['location'] == 'away'].iloc[0] if len(home_away) > 1 else None
                
                if home_data is not None and away_data is not None:
                    st.markdown("#### 🏠 Home Statistics")
                    st.metric("Win Rate", f"{(home_data['wins']/home_data['matches']*100):.1f}%")
                    st.metric("Avg Goals", f"{home_data['avg_goals']:.2f}")
                    home_poss = home_data['avg_possession'] if home_data['avg_possession'] is not None else 0
                    st.metric("Avg Possession", f"{home_poss:.1f}%")
                    
                    st.markdown("#### ✈️ Away Statistics")
                    st.metric("Win Rate", f"{(away_data['wins']/away_data['matches']*100):.1f}%")
                    st.metric("Avg Goals", f"{away_data['avg_goals']:.2f}")
                    away_poss = away_data['avg_possession'] if away_data['avg_possession'] is not None else 0
                    st.metric("Avg Possession", f"{away_poss:.1f}%")
    
    # ========================================================================
    # PAGE: PLAYER STATISTICS
    # ========================================================================
    
    elif page == "👤 Player Statistics":
        st.header("👤 Player Statistics")
        
        # Filters
        col1, col2 = st.columns(2)
        
        with col1:
            leagues = get_leagues()
            league_options = dict(zip(leagues['name'], leagues['league_id']))
            selected_league_name = st.selectbox("Select League", list(league_options.keys()))
            selected_league_id = league_options[selected_league_name]
        
        with col2:
            seasons = get_seasons()
            selected_season = st.selectbox("Select Season", seasons['season'].tolist())
        
        st.markdown("---")
        
        # Top Scorers
        st.subheader("🎯 Top Goal Scorers")
        
        top_n = st.slider("Number of players to display", 5, 30, 15)
        top_scorers = get_top_scorers(selected_league_id, selected_season, limit=top_n)
        
        if not top_scorers.empty:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                fig = plot_top_scorers(top_scorers, top_n)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("#### 🏆 Top 5 Players")
                for idx, row in top_scorers.head(5).iterrows():
                    with st.container():
                        st.markdown(f"**{idx+1}. {row['player_name']}**")
                        st.caption(f"⚽ Goals: {row['total_goals']} | 🎯 Assists: {row['total_assists']}")
                        st.caption(f"📊 Goals/90: {row['goals_per_90']} | 🎮 Games: {row['games_played']}")
                        st.markdown("---")
            
            # Full table
            st.markdown("#### 📊 Detailed Statistics")
            st.dataframe(
                top_scorers.style.format({
                    'games_played': '{:,}',
                    'total_goals': '{:,}',
                    'total_assists': '{:,}',
                    'goals_per_90': '{:.2f}',
                    'avg_xg': '{:.2f}'
                }),
                width='stretch',
                hide_index=True
            )
        else:
            st.warning("No player data available for this selection.")
        
        st.markdown("---")
        
        # Position Analysis
        st.subheader("📍 Performance by Position")
        position_stats = get_player_positions_analysis(selected_league_id, selected_season)
        
        if not position_stats.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    position_stats,
                    x='position',
                    y='total_goals',
                    color='position',
                    title='Goals by Position',
                    labels={'total_goals': 'Total Goals', 'position': 'Position'},
                    text='total_goals'
                )
                fig.update_traces(textposition='outside')
                fig.update_layout(template='plotly_white', height=400, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.bar(
                    position_stats,
                    x='position',
                    y='total_assists',
                    color='position',
                    title='Assists by Position',
                    labels={'total_assists': 'Total Assists', 'position': 'Position'},
                    text='total_assists'
                )
                fig.update_traces(textposition='outside')
                fig.update_layout(template='plotly_white', height=400, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
    
    # ========================================================================
    # PAGE: ADVANCED ANALYTICS
    # ========================================================================
    
    elif page == "📈 Advanced Analytics":
        st.header("📈 Advanced Analytics")
        
        # Filters
        col1, col2 = st.columns(2)
        
        with col1:
            leagues = get_leagues()
            league_options = dict(zip(leagues['name'], leagues['league_id']))
            selected_league_name = st.selectbox("Select League", list(league_options.keys()))
            selected_league_id = league_options[selected_league_name]
        
        with col2:
            seasons = get_seasons()
            selected_season = st.selectbox("Select Season", seasons['season'].tolist())
        
        st.markdown("---")
        
        # Shot Analysis
        st.subheader("🎯 Shot Analysis")
        shot_data = get_shot_analysis(selected_league_id, selected_season)
        
        if not shot_data.empty:
            col1, col2 = st.columns([1, 1])
            
            with col1:
                fig = plot_shot_analysis(shot_data)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("#### 📊 Shot Statistics")
                total_shots = shot_data['shot_count'].sum()
                goals = shot_data[shot_data['shot_result'] == 'Goal']['shot_count'].sum() if 'Goal' in shot_data['shot_result'].values else 0
                conversion_rate = (goals / total_shots * 100) if total_shots > 0 else 0

                st.metric("Total Shots", f"{total_shots:,}")
                st.metric("Goals", f"{goals:,}")
                st.metric("Conversion Rate", f"{conversion_rate:.2f}%")

                st.markdown("---")
                st.dataframe(
                    shot_data.style.format({
                        'shot_count': '{:,}',
                        'avg_xg': '{:.3f}',
                        'goals': '{:,}'
                    }),
                    width='stretch',
                    hide_index=True
                )
        
        st.markdown("---")
        
        # Expected Goals (xG) Analysis
        st.subheader("📊 Expected Goals (xG) Analysis")
        team_perf = get_team_performance_comparison(selected_league_id, selected_season)
        
        if not team_perf.empty:
            # xG vs Actual Goals
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=team_perf['team_name'],
                y=team_perf['goals_scored'],
                mode='markers',
                name='Actual Goals',
                marker=dict(size=12, color='#e74c3c'),
                hovertemplate='<b>%{x}</b><br>Actual Goals: %{y}<extra></extra>'
            ))
            
            fig.add_trace(go.Scatter(
                x=team_perf['team_name'],
                y=team_perf['avg_xg'] * team_perf['games_played'],
                mode='markers',
                name='Expected Goals (xG)',
                marker=dict(size=12, color='#3498db', symbol='diamond'),
                hovertemplate='<b>%{x}</b><br>Expected Goals: %{y:.1f}<extra></extra>'
            ))
            
            fig.update_layout(
                title='Actual Goals vs Expected Goals (xG)',
                xaxis_title='Team',
                yaxis_title='Goals',
                template='plotly_white',
                height=500,
                xaxis_tickangle=-45
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Performance difference
            team_perf['xg_total'] = team_perf['avg_xg'] * team_perf['games_played']
            team_perf['xg_diff'] = team_perf['goals_scored'] - team_perf['xg_total']
            team_perf_sorted = team_perf.sort_values('xg_diff', ascending=True)
            
            fig = go.Figure()
            
            colors = ['#2ecc71' if x > 0 else '#e74c3c' for x in team_perf_sorted['xg_diff']]
            
            fig.add_trace(go.Bar(
                y=team_perf_sorted['team_name'],
                x=team_perf_sorted['xg_diff'],
                orientation='h',
                marker_color=colors,
                text=team_perf_sorted['xg_diff'].round(1),
                textposition='auto',
                hovertemplate='<b>%{y}</b><br>Difference: %{x:.1f}<extra></extra>'
            ))
            
            fig.update_layout(
                title='Goals vs xG Difference (Overperformance/Underperformance)',
                xaxis_title='Goals - xG',
                yaxis_title='',
                template='plotly_white',
                height=max(400, len(team_perf) * 25)
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            st.caption("🟢 Positive values indicate overperformance | 🔴 Negative values indicate underperformance")
    
    # ========================================================================
    # PAGE: GEOGRAPHIC ANALYSIS
    # ========================================================================
    
    elif page == "🗺️ Geographic Analysis":
        st.header("🗺️ Geographic Analysis")
        st.markdown("*Explore the geographic distribution of teams and players across Europe*")
        
        st.markdown("---")
        
        # European Leagues Map
        st.subheader("🌍 European Top 5 Leagues Distribution")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            fig = plot_leagues_choropleth()
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("#### 📍 League Countries")
            leagues = get_leagues()
            if not leagues.empty:
                for idx, row in leagues.iterrows():
                    st.markdown(f"**{row['name']}**")
                    st.caption(f"🌍 {row['country']}")
                    st.markdown("---")
        
        st.markdown("---")
        
        # Teams Distribution Map
        st.subheader("⚽ Teams Geographic Distribution")
        
        teams_map_data = get_league_teams_map()
        
        if not teams_map_data.empty:
            # Show map
            fig = plot_teams_map(teams_map_data)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            
            st.caption("💡 Bubble size represents total goals scored, color intensity shows number of wins")
            
            # Statistics by country
            st.markdown("---")
            st.subheader("📊 Statistics by Country")
            
            country_stats = get_teams_by_country()
            
            if not country_stats.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    # Bar chart for teams per country
                    fig = px.bar(
                        country_stats,
                        x='country',
                        y='team_count',
                        title='Number of Teams per Country',
                        labels={'team_count': 'Number of Teams', 'country': 'Country'},
                        text='team_count',
                        color='team_count',
                        color_continuous_scale='Blues'
                    )
                    fig.update_traces(textposition='outside')
                    fig.update_layout(
                        template='plotly_white',
                        height=400,
                        showlegend=False
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Bar chart for goals per country
                    fig = px.bar(
                        country_stats,
                        x='country',
                        y='total_goals',
                        title='Total Goals by Country',
                        labels={'total_goals': 'Total Goals', 'country': 'Country'},
                        text='total_goals',
                        color='total_goals',
                        color_continuous_scale='Reds'
                    )
                    fig.update_traces(textposition='outside')
                    fig.update_layout(
                        template='plotly_white',
                        height=400,
                        showlegend=False
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                # Data table
                st.markdown("#### 📋 Detailed Country Statistics")
                st.dataframe(
                    country_stats.style.format({
                        'team_count': '{:,}',
                        'total_matches': '{:,}',
                        'total_goals': '{:,}'
                    }),
                    width='stretch',
                    hide_index=True
                )
        
        st.markdown("---")
        
        # Player Nationalities Map
        st.subheader("👥 Player Nationalities Distribution")
        
        player_nationalities = get_player_nationalities()
        
        if not player_nationalities.empty:
            # Show map
            fig = plot_player_nationalities_map(player_nationalities)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            
            st.caption("💡 Bubble size represents number of players, color intensity shows total goals scored")
            
            # Top nationalities
            st.markdown("---")
            st.subheader("🌟 Top Player Nationalities")
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Bar chart for top nationalities
                top_nationalities = player_nationalities.head(15)
                fig = px.bar(
                    top_nationalities,
                    x='nationality',
                    y='player_count',
                    title='Top 15 Player Nationalities',
                    labels={'player_count': 'Number of Players', 'nationality': 'Nationality'},
                    text='player_count',
                    color='player_count',
                    color_continuous_scale='Viridis'
                )
                fig.update_traces(textposition='outside')
                fig.update_layout(
                    template='plotly_white',
                    height=400,
                    showlegend=False,
                    xaxis_tickangle=-45
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("#### 🏆 Top 5 Nationalities")
                for idx, row in player_nationalities.head(5).iterrows():
                    st.markdown(f"**{idx+1}. {row['nationality']}**")
                    st.caption(f"👥 Players: {row['player_count']}")
                    st.caption(f"⚽ Goals: {row['total_goals']:,}")
                    st.caption(f"🎯 Assists: {row['total_assists']:,}")
                    st.markdown("---")
            
            # Full table
            st.markdown("#### 📊 Complete Nationality Statistics")
            st.dataframe(
                player_nationalities.style.format({
                    'player_count': '{:,}',
                    'total_goals': '{:,}',
                    'total_assists': '{:,}',
                    'total_appearances': '{:,}'
                }),
                width='stretch',
                hide_index=True,
                height=400
            )
        
        st.markdown("---")
        
        # Interactive filters for detailed exploration
        st.subheader("🔍 Detailed Geographic Exploration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            selected_country = st.selectbox(
                "Select Country",
                ['All'] + list(COUNTRY_COORDS.keys())
            )
        
        with col2:
            view_type = st.selectbox(
                "View Type",
                ["Teams Overview", "Player Distribution"]
            )
        
        if selected_country != 'All':
            if view_type == "Teams Overview":
                # Show teams from selected country
                filtered_teams = teams_map_data[teams_map_data['country'] == selected_country]
                
                if not filtered_teams.empty:
                    st.markdown(f"#### ⚽ Teams in {selected_country}")
                    st.dataframe(
                        filtered_teams[['team_name', 'league_name', 'matches_played', 'wins', 'total_goals']].style.format({
                            'matches_played': '{:,}',
                            'wins': '{:,}',
                            'total_goals': '{:,}'
                        }),
                        width='stretch',
                        hide_index=True
                    )
                    
                    # Summary metrics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Teams", len(filtered_teams))
                    with col2:
                        st.metric("Total Matches", f"{filtered_teams['matches_played'].sum():,}")
                    with col3:
                        st.metric("Total Wins", f"{filtered_teams['wins'].sum():,}")
                    with col4:
                        st.metric("Total Goals", f"{filtered_teams['total_goals'].sum():,}")
            
            else:  # Player Distribution
                # Show players from selected country
                filtered_players = player_nationalities[player_nationalities['nationality'] == selected_country]
                
                if not filtered_players.empty:
                    st.markdown(f"#### 👥 Players from {selected_country}")
                    
                    row = filtered_players.iloc[0]
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Players", f"{row['player_count']:,}")
                    with col2:
                        st.metric("Total Goals", f"{row['total_goals']:,}")
                    with col3:
                        st.metric("Total Assists", f"{row['total_assists']:,}")
                    with col4:
                        st.metric("Appearances", f"{row['total_appearances']:,}")
                else:
                    st.info(f"No player data available for {selected_country}")
    
    # ========================================================================
    # PAGE: DATABASE EXPLORER
    # ========================================================================
    
    elif page == "🔍 Database Explorer":
        st.header("🔍 Database Explorer")
        st.markdown("Execute custom SQL queries to explore the database.")
        
        # Sample queries
        st.subheader("📝 Sample Queries")
        
        sample_queries = {
            "Top 10 Teams by Total Goals": """
                SELECT t.name, SUM(ts.goals) as total_goals
                FROM team_stats ts
                JOIN teams t ON ts.team_id = t.team_id
                GROUP BY t.name
                ORDER BY total_goals DESC
                LIMIT 10;
            """,
            "Players with Most Assists": """
                SELECT p.name, SUM(a.assists) as total_assists, COUNT(DISTINCT a.game_id) as games
                FROM appearances a
                JOIN players p ON a.player_id = p.player_id
                GROUP BY p.name
                HAVING SUM(a.assists) > 0
                ORDER BY total_assists DESC
                LIMIT 15;
            """,
            "League Statistics Summary": """
                SELECT l.name, l.country, 
                       COUNT(DISTINCT g.game_id) as matches,
                       SUM(g.home_goals + g.away_goals) as total_goals
                FROM leagues l
                JOIN games g ON l.league_id = g.league_id
                GROUP BY l.name, l.country
                ORDER BY total_goals DESC;
            """,
            "Teams with Best Shot Accuracy": """
                SELECT t.name, 
                       SUM(ts.shots_on_target) as sot,
                       SUM(ts.shots) as total_shots,
                       ROUND((SUM(ts.shots_on_target)::numeric / NULLIF(SUM(ts.shots), 0) * 100), 2) as accuracy
                FROM team_stats ts
                JOIN teams t ON ts.team_id = t.team_id
                GROUP BY t.name
                HAVING SUM(ts.shots) > 100
                ORDER BY accuracy DESC
                LIMIT 10;
            """
        }
        
        selected_sample = st.selectbox("Select a sample query", ["Custom Query"] + list(sample_queries.keys()))
        
        if selected_sample != "Custom Query":
            default_query = sample_queries[selected_sample]
        else:
            default_query = "SELECT * FROM leagues LIMIT 10;"
        
        # Query input
        query = st.text_area("SQL Query", value=default_query, height=200)
        
        col1, col2 = st.columns([1, 5])
        with col1:
            execute_button = st.button("▶️ Execute", type="primary")
        with col2:
            st.caption("⚠️ Only SELECT queries are recommended for safety")
        
        if execute_button:
            if query.strip().upper().startswith('SELECT'):
                with st.spinner("Executing query..."):
                    try:
                        result = execute_query(query)
                        
                        if not result.empty:
                            st.success(f"✅ Query executed successfully! ({len(result)} rows returned)")
                            
                            # Display results
                            st.dataframe(result, use_container_width=True, height=400)
                            
                            # Download option
                            csv = result.to_csv(index=False)
                            st.download_button(
                                label="📥 Download as CSV",
                                data=csv,
                                file_name="query_results.csv",
                                mime="text/csv"
                            )
                        else:
                            st.warning("Query returned no results.")
                    except Exception as e:
                        st.error(f"❌ Error executing query: {str(e)}")
            else:
                st.error("⚠️ Only SELECT queries are allowed for safety reasons.")
        
        st.markdown("---")
        
        # Database Schema
        st.subheader("📚 Database Schema")
        
        with st.expander("View Tables and Columns"):
            schema_query = """
                SELECT 
                    table_name,
                    column_name,
                    data_type,
                    is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position;
            """
            schema = execute_query(schema_query)
            
            if not schema.empty:
                tables = schema['table_name'].unique()
                
                for table in tables:
                    st.markdown(f"**📋 {table}**")
                    table_cols = schema[schema['table_name'] == table][['column_name', 'data_type', 'is_nullable']]
                    st.dataframe(table_cols, use_container_width=True, hide_index=True)
                    st.markdown("---")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #7f8c8d; padding: 20px;'>
        <p>⚽ European Top 5 Leagues Dashboard | Built with Streamlit & PostgreSQL</p>
        <p>Data Source: laliga_europe Database | © 2024</p>
    </div>
    """, unsafe_allow_html=True)

# ============================================================================
# RUN APPLICATION
# ============================================================================

if __name__ == "__main__":
    main()
