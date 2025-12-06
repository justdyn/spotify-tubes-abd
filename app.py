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
    
    /* Sidebar - Modern, premium, non-white background + refined typography */
    @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@300;400;600;700;800&display=swap');

    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #071233 0%, #0b2f56 100%);
        color: #e6f0fb;
        font-family: 'Manrope', Inter, system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', Arial;
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
        letter-spacing: 0.2px;
        padding: 14px 12px 18px 12px;
        border-radius: 12px;
        box-shadow: 0 8px 30px rgba(9, 18, 35, 0.6);
        transition: width .36s cubic-bezier(.2,.9,.3,1), padding .28s ease, transform .28s ease;
        min-width: 260px;
    }

    /* Collapsed variant will be injected conditionally from Python (min-width override) */

    /* Sidebar header */
    section[data-testid="stSidebar"] .sidebar-header {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 6px 4px 12px 4px;
    }

    section[data-testid="stSidebar"] .sidebar-brand {
        font-size: 18px;
        font-weight: 700;
        color: #eaf6ff;
        margin: 0;
        letter-spacing: 0.6px;
    }

    /* Nav button styling (limited to sidebar area) */
    section[data-testid="stSidebar"] .stButton>button {
        width: 100%;
        display: flex;
        align-items: center;
        gap: 12px;
        background: transparent;
        border: none;
        color: inherit;
        padding: 10px 12px;
        font-weight: 600;
        font-size: 15px;
        line-height: 1;
        border-radius: 10px;
        justify-content: flex-start;
        transition: background .18s ease, transform .08s ease, color .15s ease;
        letter-spacing: 0.4px;
    }

    section[data-testid="stSidebar"] .stButton>button:hover {
        background: rgba(255,255,255,0.04);
        transform: translateX(4px);
    }

    section[data-testid="stSidebar"] .nav-icon {
        width: 28px;
        height: 28px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        color: #cfe8ff;
    }

    /* Ensure inline svgs use currentColor so icons are monochrome and elegant */
    section[data-testid="stSidebar"] .nav-icon svg{stroke:currentColor; fill:none}

    section[data-testid="stSidebar"] .muted {
        color: #b7d6f8;
        font-weight: 500;
        font-size: 13px;
        letter-spacing: 0.3px;
    }

    /* Small helper to visually separate groups */
    section[data-testid="stSidebar"] .group-sep { margin: 10px 0; height:1px; background: rgba(255,255,255,0.03); border-radius:2px }

    /* Footer area in sidebar */
    section[data-testid="stSidebar"] .sidebar-foot { margin-top: 12px; color: #9fbfe6; font-size:12px }

    
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
            password="postgres",
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
    WITH team_goals AS (
        SELECT
            ts.team_id,
            SUM(ts.goals) as goals_for,
            SUM(CASE WHEN ts.location = 'home' THEN g.away_goals ELSE g.home_goals END) as goals_against
        FROM team_stats ts
        JOIN games g ON ts.game_id = g.game_id
        WHERE g.league_id = %s AND g.season = %s AND g.status = 'completed'
        GROUP BY ts.team_id
    ),
    team_results AS (
        SELECT
            ts.team_id,
            t.name as team_name,
            COUNT(ts.game_id) as matches_played,
            COUNT(CASE WHEN ts.result = 'win' THEN 1 END) as wins,
            COUNT(CASE WHEN ts.result = 'draw' THEN 1 END) as draws,
            COUNT(CASE WHEN ts.result = 'loss' THEN 1 END) as losses
        FROM team_stats ts
        JOIN games g ON ts.game_id = g.game_id
        JOIN teams t ON ts.team_id = t.team_id
        WHERE g.league_id = %s AND g.season = %s AND g.status = 'completed'
        GROUP BY ts.team_id, t.name
    )
    SELECT
        tr.team_name,
        tr.matches_played,
        tr.wins,
        tr.draws,
        tr.losses,
        tg.goals_for,
        tg.goals_against,
        (tg.goals_for - tg.goals_against) as goal_difference,
        (tr.wins * 3 + tr.draws) as points
    FROM team_results tr
    JOIN team_goals tg ON tr.team_id = tg.team_id
    ORDER BY points DESC, goal_difference DESC, goals_for DESC
    """
    return execute_query(query, (league_id, season, league_id, season))

def get_top_scorers(league_id, season, limit=20):
    """Get top scorers for specific league and season"""
    query = """
    SELECT
        p.name as player_name,
        COUNT(DISTINCT a.game_id) as games_played,
        SUM(a.goals) as total_goals,
        SUM(a.assists) as total_assists,
        ROUND((SUM(a.goals)::numeric / NULLIF(SUM(a.time_played), 0) * 90), 2) as goals_per_90,
        ROUND(AVG(a.x_goals)::numeric, 2) as avg_xg
    FROM appearances a
    JOIN games g ON a.game_id = g.game_id
    JOIN players p ON a.player_id = p.player_id
    WHERE g.league_id = %s AND g.season = %s AND g.status = 'completed'
    GROUP BY p.player_id, p.name
    HAVING SUM(a.goals) > 0
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
        ROUND(AVG(x_goals)::numeric, 2) as avg_xg
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
        SUM(ts.shots) as total_shots
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
    df.columns = ['Rank', 'Team', 'MP', 'W', 'D', 'L', 'GF', 'GA', 'GD', 'Pts']
    
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
               row['total_shots']/100],
            theta=['Total Matches', 'Total Goals', 'Avg Goals/Match (x100)', 
                   'Total Shots (/100)'],
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
    """Create scatter plot for goals vs shot accuracy"""
    if df.empty:
        return None
    
    fig = px.scatter(
        df,
        x='shot_accuracy',
        y='goals_scored',
        size='total_shots',
        color='wins',
        hover_name='team_name',
        labels={
            'shot_accuracy': 'Shot Accuracy (%)',
            'goals_scored': 'Goals Scored',
            'total_shots': 'Total Shots',
            'wins': 'Wins'
        },
        title='Goals Scored vs Shot Accuracy (Size: Total Shots)',
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

def create_interactive_erd_html():
    """Create interactive HTML-based ERD that can be zoomed and panned"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            .erd-container {
                width: 100%;
                height: 600px;
                border: 2px solid #e1e5e9;
                border-radius: 8px;
                overflow: hidden;
                background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
                position: relative;
            }

            .erd-canvas {
                width: 100%;
                height: 100%;
                transform-origin: center;
                transition: transform 0.1s ease-out;
                cursor: grab;
                user-select: none;
            }

            .erd-canvas:active {
                cursor: grabbing;
            }

            .table-node {
                position: absolute;
                background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
                border-radius: 12px;
                border: 3px solid;
                box-shadow: 0 8px 25px rgba(0,0,0,0.15);
                min-width: 140px;
                text-align: center;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                transform: translate(-50%, -50%);
                transition: all 0.3s ease;
            }

            .table-node:hover {
                transform: translate(-50%, -50%) scale(1.05);
                box-shadow: 0 12px 35px rgba(0,0,0,0.2);
                z-index: 100;
            }

            .table-master { border-color: #1f77b4; background: linear-gradient(135deg, #e8f4fd 0%, #b3d9ff 100%); }
            .table-fact { border-color: #ff7f0e; background: linear-gradient(135deg, #fff2e6 0%, #ffcc99 100%); }
            .table-stats { border-color: #2ca02c; background: linear-gradient(135deg, #e8f5e8 0%, #b3ffb3 100%); }

            .table-icon {
                font-size: 24px;
                margin-bottom: 5px;
            }

            .table-name {
                font-weight: bold;
                font-size: 14px;
                color: #2c3e50;
                margin-bottom: 3px;
            }

            .table-type {
                font-size: 10px;
                font-weight: 600;
                color: #7f8c8d;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }

            .relationship-line {
                position: absolute;
                height: 3px;
                background: linear-gradient(90deg, #34495e 0%, #7f8c8d 50%, #34495e 100%);
                transform-origin: center;
                border-radius: 2px;
                z-index: 10;
            }

            .relationship-label {
                position: absolute;
                background: rgba(255, 255, 255, 0.95);
                border: 2px solid #34495e;
                border-radius: 15px;
                padding: 4px 8px;
                font-size: 11px;
                font-weight: bold;
                color: #34495e;
                transform: translate(-50%, -50%);
                white-space: nowrap;
                z-index: 20;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                pointer-events: none;
            }

            .zoom-controls {
                position: absolute;
                top: 10px;
                right: 10px;
                display: flex;
                flex-direction: column;
                gap: 5px;
                z-index: 30;
            }

            .zoom-btn {
                width: 35px;
                height: 35px;
                background: rgba(255, 255, 255, 0.9);
                border: 2px solid #34495e;
                border-radius: 50%;
                cursor: pointer;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 18px;
                font-weight: bold;
                color: #34495e;
                transition: all 0.2s ease;
                box-shadow: 0 2px 8px rgba(0,0,0,0.15);
            }

            .zoom-btn:hover {
                background: #34495e;
                color: white;
                transform: scale(1.1);
            }

            .reset-btn {
                margin-top: 10px;
                width: 60px;
                height: 35px;
                background: #e74c3c;
                border: none;
                border-radius: 17px;
                color: white;
                font-size: 12px;
                font-weight: bold;
                cursor: pointer;
                transition: all 0.2s ease;
                box-shadow: 0 2px 8px rgba(0,0,0,0.15);
            }

            .reset-btn:hover {
                background: #c0392b;
                transform: scale(1.05);
            }

            .legend {
                position: absolute;
                bottom: 10px;
                left: 10px;
                background: rgba(255, 255, 255, 0.95);
                border: 2px solid #34495e;
                border-radius: 8px;
                padding: 10px;
                font-size: 12px;
                z-index: 30;
                box-shadow: 0 2px 8px rgba(0,0,0,0.15);
            }

            .legend-item {
                display: flex;
                align-items: center;
                margin-bottom: 5px;
            }

            .legend-color {
                width: 16px;
                height: 16px;
                border-radius: 3px;
                margin-right: 8px;
                border: 1px solid #666;
            }
        </style>
    </head>
    <body>
        <div class="erd-container">
            <div class="zoom-controls">
                <div class="zoom-btn" onclick="zoomIn()">+</div>
                <div class="zoom-btn" onclick="zoomOut()">−</div>
                <div class="reset-btn" onclick="resetView()">Reset</div>
            </div>

            <div class="legend">
                <div class="legend-item">
                    <div class="legend-color" style="background: linear-gradient(135deg, #e8f4fd 0%, #b3d9ff 100%); border-color: #1f77b4;"></div>
                    <span><strong>Master Tables</strong></span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: linear-gradient(135deg, #fff2e6 0%, #ffcc99 100%); border-color: #ff7f0e;"></div>
                    <span><strong>Transaction Tables</strong></span>
                </div>
                <div class="legend-item">
                    <div class="legend-color" style="background: linear-gradient(135deg, #e8f5e8 0%, #b3ffb3 100%); border-color: #2ca02c;"></div>
                    <span><strong>Statistics Tables</strong></span>
                </div>
            </div>

            <div class="erd-canvas" id="erdCanvas">
                <!-- Relationship Lines -->
                <div class="relationship-line" style="left: 350px; top: 250px; width: 100px; transform: rotate(-30deg);"></div>
                <div class="relationship-line" style="left: 280px; top: 200px; width: 100px; transform: rotate(30deg);"></div>
                <div class="relationship-line" style="left: 420px; top: 200px; width: 100px; transform: rotate(-30deg);"></div>
                <div class="relationship-line" style="left: 500px; top: 200px; width: 50px; transform: rotate(90deg);"></div>
                <div class="relationship-line" style="left: 287px; top: 387px; width: 100px; transform: rotate(-30deg);"></div>
                <div class="relationship-line" style="left: 425px; top: 387px; width: 100px; transform: rotate(30deg);"></div>
                <div class="relationship-line" style="left: 350px; top: 450px; width: 100px; transform: rotate(0deg);"></div>

                <!-- Relationship Labels -->
                <div class="relationship-label" style="left: 380px; top: 240px;">1:N</div>
                <div class="relationship-label" style="left: 310px; top: 190px;">1:N (home/away)</div>
                <div class="relationship-label" style="left: 450px; top: 190px;">1:N</div>
                <div class="relationship-label" style="left: 505px; top: 175px;">1:N</div>
                <div class="relationship-label" style="left: 320px; top: 380px;">1:N</div>
                <div class="relationship-label" style="left: 457px; top: 380px;">1:N</div>
                <div class="relationship-label" style="left: 380px; top: 440px;">1:N</div>

                <!-- Table Nodes -->
                <div class="table-node table-master" style="left: 350px; top: 150px;">
                    <div class="table-icon">🏆</div>
                    <div class="table-name">leagues</div>
                    <div class="table-type">Master</div>
                </div>

                <div class="table-node table-master" style="left: 250px; top: 300px;">
                    <div class="table-icon">⚽</div>
                    <div class="table-name">teams</div>
                    <div class="table-type">Master</div>
                </div>

                <div class="table-node table-master" style="left: 450px; top: 300px;">
                    <div class="table-icon">👤</div>
                    <div class="table-name">players</div>
                    <div class="table-type">Master</div>
                </div>

                <div class="table-node table-fact" style="left: 350px; top: 350px;">
                    <div class="table-icon">🎮</div>
                    <div class="table-name">games</div>
                    <div class="table-type">Fact</div>
                </div>

                <div class="table-node table-stats" style="left: 250px; top: 500px;">
                    <div class="table-icon">📈</div>
                    <div class="table-name">team_stats</div>
                    <div class="table-type">Stats</div>
                </div>

                <div class="table-node table-stats" style="left: 450px; top: 500px;">
                    <div class="table-icon">🏃</div>
                    <div class="table-name">appearances</div>
                    <div class="table-type">Stats</div>
                </div>

                <div class="table-node table-stats" style="left: 350px; top: 600px;">
                    <div class="table-icon">🎯</div>
                    <div class="table-name">shots</div>
                    <div class="table-type">Stats</div>
                </div>
            </div>
        </div>

        <script>
            let canvas = document.getElementById('erdCanvas');
            let isDragging = false;
            let startX, startY, initialX, initialY;
            let scale = 1;
            let translateX = 0;
            let translateY = 0;

            function updateTransform() {
                canvas.style.transform = `translate(${translateX}px, ${translateY}px) scale(${scale})`;
            }

            function zoomIn() {
                scale = Math.min(scale + 0.2, 3);
                updateTransform();
            }

            function zoomOut() {
                scale = Math.max(scale - 0.2, 0.5);
                updateTransform();
            }

            function resetView() {
                scale = 1;
                translateX = 0;
                translateY = 0;
                updateTransform();
            }

            // Mouse wheel zoom
            canvas.addEventListener('wheel', function(e) {
                e.preventDefault();
                if (e.deltaY < 0) {
                    zoomIn();
                } else {
                    zoomOut();
                }
            });

            // Mouse drag pan
            canvas.addEventListener('mousedown', function(e) {
                isDragging = true;
                startX = e.clientX - translateX;
                startY = e.clientY - translateY;
                canvas.style.cursor = 'grabbing';
            });

            document.addEventListener('mousemove', function(e) {
                if (isDragging) {
                    translateX = e.clientX - startX;
                    translateY = e.clientY - startY;
                    updateTransform();
                }
            });

            document.addEventListener('mouseup', function() {
                isDragging = false;
                canvas.style.cursor = 'grab';
            });

            // Initialize
            updateTransform();
        </script>
    </body>
    </html>
    """

    return html_content

# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    """Main application function"""
    
    # Header
    st.title("European Top 5 Football Leagues Dashboard")
    st.markdown("*Interactive analysis of Premier League, La Liga, Bundesliga, Serie A, and Ligue 1*")
    st.markdown("---")
    
    # Sidebar
    with st.sidebar:
        st.image("DAF.png", width=150)
        st.title("Navigation")
        
        page = st.radio(
            "Select View",
            ["Overview", "League Analysis", "Player Statistics", 
             "Advanced Analytics", "Database Explorer"],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        st.info("**Tip**: Hover over charts for detailed information!")
        
        # Database status
        st.markdown("---")
        st.subheader("Database Status")
        try:
            conn = get_database_connection()
            st.success("✅ Connected")
        except:
            st.error("❌ Disconnected")
    
    # ========================================================================
    # PAGE: OVERVIEW
    # ========================================================================
    
    if page == "Overview":
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
                    'total_shots': '{:,}'
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
                st.plotly_chart(fig, width='stretch')
            
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
                st.plotly_chart(fig, width='stretch')
    
    # ========================================================================
    # PAGE: LEAGUE ANALYSIS
    # ========================================================================
    
    elif page == "League Analysis":
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
            
            # Highlight top 4 and bottom 3 with high contrast colors
            def highlight_rows(row):
                if row['Rank'] <= 4:
                    return ['background-color: #28a745; color: white; font-weight: bold'] * len(row)
                elif row['Rank'] >= len(standings_table) - 2:
                    return ['background-color: #dc3545; color: white; font-weight: bold'] * len(row)
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
                    st.metric("Avg xG", f"{home_data['avg_xg']:.2f}")
                    
                    st.markdown("#### ✈️ Away Statistics")
                    st.metric("Win Rate", f"{(away_data['wins']/away_data['matches']*100):.1f}%")
                    st.metric("Avg Goals", f"{away_data['avg_goals']:.2f}")
                    st.metric("Avg xG", f"{away_data['avg_xg']:.2f}")
    
    # ========================================================================
    # PAGE: PLAYER STATISTICS
    # ========================================================================
    
    elif page == "Player Statistics":
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
    
    elif page == "Advanced Analytics":
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
    # PAGE: DATABASE EXPLORER
    # ========================================================================

    elif page == "Database Explorer":
        st.header("🔍 Database Explorer")
        st.markdown("Explore and query the European football database with comprehensive visualizations and documentation.")



        # ERD Diagram
        st.subheader("🔗 Database ERD Schema")

        try:
            st.image("erd-schema.png", caption="Database ERD Diagram", use_container_width=True)

            st.markdown("""
            **📖 Database Structure Guide:**
            - 🔵 **Master Tables** (leagues, teams, players) → Core reference data
            - 🟢 **Bridge Tables** (team_players) → Transfer history tracking
            - 🟡 **Transaction Tables** (games, team_stats, appearances, shots) → Match analytics data
            - 🔗 **Lines/arrows** → Foreign key relationships between tables
            """)
        except:
            st.warning("⚠️ ERD diagram image (erd.png) not found in current directory.")
            st.info("� Please ensure 'erd.png' file is placed in the same directory as the application.")

        st.markdown("---")

        # Teams Table
        col1, col2 = st.columns([1, 4])
        with col1:
            st.markdown("⚽ **teams**")
        with col2:
            st.markdown("*Comprehensive team registry across all 5 leagues*")

        teams_data = {
            "Attribute": ["team_id", "league_id", "name", "is_active", "created_at", "updated_at"],
            "Type": ["SERIAL", "INTEGER", "VARCHAR(100)", "BOOLEAN", "TIMESTAMP", "TIMESTAMP"],
            "Constraints": ["PRIMARY KEY", "NOT NULL, FOREIGN KEY → leagues", "NOT NULL", "NOT NULL, DEFAULT true", "NOT NULL, DEFAULT CURRENT_TIMESTAMP", "NOT NULL, DEFAULT CURRENT_TIMESTAMP"],
            "Description": ["Auto-generated primary key", "League affiliation", "Official team name", "Active competition flag", "Creation timestamp", "Last modification timestamp"]
        }
        st.dataframe(pd.DataFrame(teams_data), use_container_width=True, hide_index=True)

        st.markdown("**Relationships:** References `leagues.league_id`, Referenced by `games.home_team_id/away_team_id`, `team_stats.team_id`")
        st.markdown("**Data Volume:** ~150+ active teams")
        st.markdown("---")

        # Players Table
        col1, col2 = st.columns([1, 4])
        with col1:
            st.markdown("👤 **players**")
        with col2:
            st.markdown("*Player master data with demographics and positions*")

        players_data = {
            "Attribute": ["player_id", "name", "is_active", "created_at", "updated_at"],
            "Type": ["SERIAL", "VARCHAR(150)", "BOOLEAN", "TIMESTAMP", "TIMESTAMP"],
            "Constraints": ["PRIMARY KEY", "NOT NULL", "NOT NULL, DEFAULT true", "NOT NULL, DEFAULT CURRENT_TIMESTAMP", "NOT NULL, DEFAULT CURRENT_TIMESTAMP"],
            "Description": ["Auto-generated primary key", "Full player name", "Active status flag", "Creation timestamp", "Last modification timestamp"]
        }
        st.dataframe(pd.DataFrame(players_data), use_container_width=True, hide_index=True)

        st.markdown("**Relationships:** Referenced by `appearances.player_id`, `shots.shooter_id/assister_id`")
        st.markdown("**Data Volume:** ~5000+ active players")
        st.markdown("---")

        # Bridge Tables
        st.markdown("### 🔗 **Bridge Tables**")

        # Team Players Table
        col1, col2 = st.columns([1, 4])
        with col1:
            st.markdown("🤝 **team_players**")
        with col2:
            st.markdown("*Bridge table tracking player-team relationships and transfer history*")

        team_players_data = {
            "Attribute": ["team_player_id", "team_id", "player_id", "season_start", "season_end", "is_current", "created_at", "updated_at"],
            "Type": ["SERIAL", "INTEGER", "INTEGER", "SMALLINT", "SMALLINT", "BOOLEAN", "TIMESTAMP", "TIMESTAMP"],
            "Constraints": ["PRIMARY KEY", "FOREIGN KEY → teams", "FOREIGN KEY → players", "NOT NULL, ≥ 2014", "NULL, ≥ season_start", "NOT NULL, DEFAULT true", "DEFAULT CURRENT_TIMESTAMP", "DEFAULT CURRENT_TIMESTAMP"],
            "Description": ["Auto-generated primary key", "Team reference", "Player reference", "Season player joined", "Season player left (NULL if current)", "Current team flag", "Creation timestamp", "Last modification timestamp"]
        }
        st.dataframe(pd.DataFrame(team_players_data), use_container_width=True, hide_index=True)

        st.markdown("**Relationships:** References `teams.team_id` and `players.player_id`, enables player transfer analytics")
        st.markdown("**Purpose:** Track player history across multiple teams and seasons")
        st.markdown("---")

        # Transaction Tables
        st.markdown("### 🎯 **Transaction Tables**")

        # Games Table
        col1, col2 = st.columns([1, 4])
        with col1:
            st.markdown("🎮 **games**")
        with col2:
            st.markdown("*Central fact table containing complete match information (~10,000+ matches)*")

        games_data = {
            "Attribute": ["game_id", "league_id", "season", "game_week", "date", "home_team_id", "away_team_id", "home_goals", "away_goals", "home_probability", "draw_probability", "away_probability", "home_goals_half_time", "away_goals_half_time", "status", "created_at", "updated_at"],
            "Type": ["SERIAL", "INTEGER", "SMALLINT", "SMALLINT", "TIMESTAMP", "INTEGER", "INTEGER", "SMALLINT", "SMALLINT", "DECIMAL(5,4)", "DECIMAL(5,4)", "DECIMAL(5,4)", "SMALLINT", "SMALLINT", "VARCHAR(20)", "TIMESTAMP", "TIMESTAMP"],
            "Constraints": ["PRIMARY KEY", "NOT NULL, FOREIGN KEY → leagues", "NOT NULL, 2014-2100", "NULL, 1-50", "NOT NULL", "NOT NULL, FOREIGN KEY → teams", "NOT NULL, FOREIGN KEY → teams", "NOT NULL, DEFAULT 0, ≥ 0", "NOT NULL, DEFAULT 0, ≥ 0", "NULL, 0.0000-1.0000", "NULL, 0.0000-1.0000", "NULL, 0.0000-1.0000", "NULL, ≥ 0", "NULL, ≥ 0", "NOT NULL, DEFAULT 'completed'", "NOT NULL, DEFAULT CURRENT_TIMESTAMP", "NOT NULL, DEFAULT CURRENT_TIMESTAMP"],
            "Description": ["Auto-generated primary key", "League reference", "Starting season year", "Match week/round number", "Match date/time", "Home team reference", "Away team reference", "Home goals scored (full time)", "Away goals scored (full time)", "Pre-match win probability", "Pre-match draw probability", "Pre-match away probability", "Home goals at half time", "Away goals at half time", "Match status", "Creation timestamp", "Last modification timestamp"]
        }
        st.dataframe(pd.DataFrame(games_data), use_container_width=True, hide_index=True)

        st.markdown("**Relationships:** Central hub, referenced by all other transaction tables")
        st.markdown("**Analytics:** Core for match results, seasonal trends, prediction models")
        st.markdown("---")

        # Show expandable sections for remaining tables
        with st.expander("📈 View team_stats Table Documentation"):
            col1, col2 = st.columns([1, 4])
            with col1:
                st.markdown("📈 **team_stats**")
            with col2:
                st.markdown("*Granular team performance data for each match appearance*")

            team_stats_data = {
                "Attribute": ["game_id, team_id", "location", "goals", "x_goals", "shots", "shots_on_target", "deep_passes", "ppda", "fouls", "corners", "yellow_cards", "red_cards", "result", "created_at", "updated_at"],
                "Type": ["INTEGER, INTEGER", "location_type ENUM", "SMALLINT", "DECIMAL(8,6)", "SMALLINT", "SMALLINT", "INTEGER", "DECIMAL(8,4)", "SMALLINT", "SMALLINT", "SMALLINT", "SMALLINT", "result_type ENUM", "TIMESTAMP", "TIMESTAMP"],
                "Description": ["Composite PRIMARY KEY", "home or away", "Goals scored in match", "Expected goals metric", "Total shots attempted", "Shots on target", "Completed deep passes", "Passes per defensive action", "Fouls committed", "Corners won", "Yellow cards received", "Red cards received", "win, draw, or loss", "Creation timestamp", "Last modification timestamp"]
            }
            st.dataframe(pd.DataFrame(team_stats_data), use_container_width=True, hide_index=True)
            st.markdown("**Relationships:** References `games.game_id`, `teams.team_id`")

        with st.expander("🏃 View appearances Table Documentation"):
            col1, col2 = st.columns([1, 4])
            with col1:
                st.markdown("🏃 **appearances**")
            with col2:
                st.markdown("*Individual player statistics for each match appearance*")

            appearances_data = {
                "Attribute": ["game_id, player_id", "team_id", "goals", "own_goals", "shots", "x_goals", "x_goals_chain", "x_goals_buildup", "assists", "key_passes", "x_assists", "position", "position_order", "yellow_card", "red_card", "time_played", "substitute_in", "substitute_out", "created_at", "updated_at"],
                "Type": ["INTEGER, INTEGER", "INTEGER", "SMALLINT", "SMALLINT", "SMALLINT", "DECIMAL(8,6)", "DECIMAL(8,6)", "DECIMAL(8,6)", "SMALLINT", "SMALLINT", "DECIMAL(8,6)", "VARCHAR(10)", "SMALLINT", "BOOLEAN", "BOOLEAN", "SMALLINT", "VARCHAR(20)", "VARCHAR(20)", "TIMESTAMP", "TIMESTAMP"],
                "Description": ["Composite PRIMARY KEY", "Team player represented", "Goals scored", "Own goals", "Shots attempted", "Expected goals", "xG in possession chains", "xG in buildup plays", "Goal assists", "Key passes", "Expected assists", "Playing position", "Field position order", "Yellow card received", "Red card received", "Minutes played", "Substitute in time", "Substitute out time", "Creation timestamp", "Last modification timestamp"]
            }
            st.dataframe(pd.DataFrame(appearances_data), use_container_width=True, hide_index=True)
            st.markdown("**Relationships:** References `games.game_id`, `players.player_id`")

        with st.expander("🎯 View shots Table Documentation"):
            col1, col2 = st.columns([1, 4])
            with col1:
                st.markdown("🎯 **shots**")
            with col2:
                st.markdown("*Granular shot data with positioning and expected goal probabilities*")

            shots_data = {
                "Attribute": ["shot_id", "game_id", "team_id", "shooter_id", "assister_id", "minute", "situation", "last_action", "shot_type", "shot_result", "x_goal", "position_x", "position_y", "created_at"],
                "Type": ["BIGSERIAL", "INTEGER", "INTEGER", "INTEGER", "INTEGER", "SMALLINT", "shot_situation_type ENUM", "VARCHAR(50)", "VARCHAR(50)", "shot_result_type ENUM", "DECIMAL(8,6)", "DECIMAL(10,8)", "DECIMAL(10,8)", "TIMESTAMP"],
                "Description": ["Auto-generated PRIMARY KEY", "Game reference", "Team that took shot", "Player who shot", "Assist provider", "Match minute", "Shot situation", "Last action before shot", "Shot type (Right Foot, Header, etc.)", "Shot result (Goal, Saved, etc.)", "Expected goal probability", "Pitch X coordinate (0-1)", "Pitch Y coordinate (0-1)", "Creation timestamp"]
            }
            st.dataframe(pd.DataFrame(shots_data), use_container_width=True, hide_index=True)
            st.markdown("**Relationships:** References `games.game_id`, `players.shooter_id/assister_id`")



        st.markdown("---")

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
