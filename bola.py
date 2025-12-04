"""
European Football Leagues Analytics Dashboard
A comprehensive Streamlit application for analyzing football data
from the top 5 European leagues (Premier League, La Liga, Serie A, Bundesliga, Ligue 1)

Author: Senior Data Analyst & Streamlit Developer (20+ years experience)
Version: 1.0
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
import streamlit as st

# Optional import for enhanced menu
try:
    from streamlit_option_menu import option_menu
    HAS_OPTION_MENU = True
except ImportError:
    HAS_OPTION_MENU = False

# ============================================================================
# CONFIGURATION
# ============================================================================

# Database connection parameters
# Default password is '1' - can be overridden with DB_PASSWORD environment variable
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'laliga_europe'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '1')
}

# Page configuration
st.set_page_config(
    page_title="European Football Analytics Dashboard",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

@st.cache_resource
def get_db_connection():
    """Create and cache database connection."""
    try:
        # Explicitly set connection parameters to ensure password is used
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        return conn
    except psycopg2.OperationalError as e:
        error_msg = str(e)
        if "password authentication failed" in error_msg:
            st.error(f"❌ Database authentication failed for user '{DB_CONFIG['user']}'")
            st.info(f"💡 Current password setting: {'*' * len(DB_CONFIG['password'])} (length: {len(DB_CONFIG['password'])})")
            st.info("💡 To change password, set the DB_PASSWORD environment variable or modify bola.py")
        else:
            st.error(f"❌ Database connection error: {e}")
        st.info("🔧 Please ensure:")
        st.info("   • PostgreSQL is running")
        st.info(f"   • Database '{DB_CONFIG['database']}' exists")
        st.info(f"   • User '{DB_CONFIG['user']}' has correct password")
        return None
    except Exception as e:
        st.error(f"❌ Unexpected database error: {e}")
        st.info("Please check your database configuration.")
        return None

# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_league_standings(league_id: Optional[int] = None, season: Optional[int] = None):
    """Load league standings data."""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    SELECT 
        league_id, league_name, season, team_id, team_name,
        matches_played, points, wins, draws, losses,
        goals_for, goals_against, goal_difference, avg_possession
    FROM mv_league_standings
    WHERE 1=1
    """
    
    params = []
    if league_id:
        query += " AND league_id = %s"
        params.append(league_id)
    if season:
        query += " AND season = %s"
        params.append(season)
    
    query += " ORDER BY league_id, season, points DESC, goal_difference DESC"
    
    try:
        df = pd.read_sql_query(query, conn, params=params if params else None)
        return df
    except Exception as e:
        st.error(f"Error loading league standings: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_top_scorers(league_id: Optional[int] = None, season: Optional[int] = None, limit: int = 20):
    """Load top scorers data."""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    SELECT 
        season, league_id, league_name, player_id, player_name, nationality,
        games_played, total_goals, total_assists, total_minutes,
        goals_per_90, avg_x_goals
    FROM mv_top_scorers
    WHERE 1=1
    """
    
    params = []
    if league_id:
        query += " AND league_id = %s"
        params.append(league_id)
    if season:
        query += " AND season = %s"
        params.append(season)
    
    query += f" ORDER BY total_goals DESC LIMIT {limit}"
    
    try:
        df = pd.read_sql_query(query, conn, params=params if params else None)
        return df
    except Exception as e:
        st.error(f"Error loading top scorers: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_team_performance(team_id: Optional[int] = None, league_id: Optional[int] = None):
    """Load team performance summary."""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    SELECT 
        team_id, team_name, league_name,
        games_played, wins, draws, losses,
        goals_scored, avg_x_goals, total_shots, total_shots_on_target,
        shot_accuracy_pct, avg_possession_pct
    FROM v_team_performance
    WHERE 1=1
    """
    
    params = []
    if team_id:
        query += " AND team_id = %s"
        params.append(team_id)
    if league_id:
        query += " AND team_id IN (SELECT team_id FROM teams WHERE league_id = %s)"
        params.append(league_id)
    
    query += " ORDER BY goals_scored DESC"
    
    try:
        df = pd.read_sql_query(query, conn, params=params if params else None)
        return df
    except Exception as e:
        st.error(f"Error loading team performance: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_player_stats(league_id: Optional[int] = None, season: Optional[int] = None):
    """Load player statistics summary."""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    SELECT 
        p.player_id, p.name AS player_name, p.nationality,
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, p.date_of_birth))::INTEGER AS age,
        COUNT(DISTINCT a.game_id) AS games_played,
        SUM(a.goals) AS total_goals,
        SUM(a.assists) AS total_assists,
        SUM(a.time_played) AS total_minutes,
        ROUND((SUM(a.goals)::numeric / NULLIF(SUM(a.time_played), 0) * 90), 3) AS goals_per_90,
        ROUND((SUM(a.assists)::numeric / NULLIF(SUM(a.time_played), 0) * 90), 3) AS assists_per_90,
        ROUND(AVG(a.x_goals)::numeric, 3) AS avg_x_goals
    FROM players p
    JOIN appearances a ON p.player_id = a.player_id
    JOIN games g ON a.game_id = g.game_id
    WHERE 1=1
    """
    
    params = []
    if league_id:
        query += " AND g.league_id = %s"
        params.append(league_id)
    if season:
        query += " AND g.season = %s"
        params.append(season)
    
    query += """
    GROUP BY p.player_id, p.name, p.nationality, p.date_of_birth
    HAVING COUNT(DISTINCT a.game_id) > 0
    ORDER BY total_goals DESC, total_assists DESC
    LIMIT 100
    """
    
    try:
        df = pd.read_sql_query(query, conn, params=params if params else None)
        return df
    except Exception as e:
        st.error(f"Error loading player stats: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_games_data(league_id: Optional[int] = None, season: Optional[int] = None, limit: int = 100):
    """Load games data."""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    SELECT 
        g.game_id, g.season, g.date, g.status,
        l.name AS league_name,
        ht.name AS home_team, at.name AS away_team,
        g.home_goals, g.away_goals,
        CASE 
            WHEN g.home_goals > g.away_goals THEN ht.name
            WHEN g.away_goals > g.home_goals THEN at.name
            ELSE 'Draw'
        END AS winner
    FROM games g
    JOIN leagues l ON g.league_id = l.league_id
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    WHERE g.status = 'completed'
    """
    
    params = []
    if league_id:
        query += " AND g.league_id = %s"
        params.append(league_id)
    if season:
        query += " AND g.season = %s"
        params.append(season)
    
    query += f" ORDER BY g.date DESC LIMIT {limit}"
    
    try:
        df = pd.read_sql_query(query, conn, params=params if params else None)
        return df
    except Exception as e:
        st.error(f"Error loading games data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_shot_analysis(league_id: Optional[int] = None, season: Optional[int] = None):
    """Load shot analysis data."""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    SELECT 
        s.shot_result,
        s.situation,
        COUNT(*) AS shot_count,
        SUM(CASE WHEN s.shot_result = 'Goal' THEN 1 ELSE 0 END) AS goals,
        ROUND(AVG(s.x_goal)::numeric, 3) AS avg_x_goal,
        ROUND(SUM(CASE WHEN s.shot_result = 'Goal' THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 2) AS conversion_rate
    FROM shots s
    JOIN games g ON s.game_id = g.game_id
    WHERE s.shot_result IS NOT NULL
    """
    
    params = []
    if league_id:
        query += " AND g.league_id = %s"
        params.append(league_id)
    if season:
        query += " AND g.season = %s"
        params.append(season)
    
    query += """
    GROUP BY s.shot_result, s.situation
    ORDER BY shot_count DESC
    """
    
    try:
        df = pd.read_sql_query(query, conn, params=params if params else None)
        return df
    except Exception as e:
        st.error(f"Error loading shot analysis: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_leagues():
    """Load all leagues."""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    query = "SELECT league_id, name, country FROM leagues WHERE is_active = true ORDER BY name"
    
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"Error loading leagues: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_teams(league_id: Optional[int] = None):
    """Load teams."""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    SELECT t.team_id, t.name, l.name AS league_name, l.league_id
    FROM teams t
    JOIN leagues l ON t.league_id = l.league_id
    WHERE t.is_active = true
    """
    
    params = []
    if league_id:
        query += " AND l.league_id = %s"
        params.append(league_id)
    
    query += " ORDER BY l.name, t.name"
    
    try:
        df = pd.read_sql_query(query, conn, params=params if params else None)
        return df
    except Exception as e:
        st.error(f"Error loading teams: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_seasons():
    """Get available seasons."""
    conn = get_db_connection()
    if not conn:
        return []
    
    query = "SELECT DISTINCT season FROM games ORDER BY season DESC"
    
    try:
        df = pd.read_sql_query(query, conn)
        return sorted(df['season'].unique().tolist(), reverse=True)
    except Exception as e:
        st.error(f"Error loading seasons: {e}")
        return []

# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================

def plot_league_standings(df: pd.DataFrame):
    """Plot league standings as a table with highlighting."""
    if df.empty:
        st.info("No data available for league standings.")
        return
    
    # Create a styled dataframe
    styled_df = df.copy()
    styled_df = styled_df[['team_name', 'matches_played', 'wins', 'draws', 'losses', 
                          'goals_for', 'goals_against', 'goal_difference', 'points']]
    styled_df.columns = ['Team', 'MP', 'W', 'D', 'L', 'GF', 'GA', 'GD', 'Pts']
    
    # Add position column
    styled_df.insert(0, 'Pos', range(1, len(styled_df) + 1))
    
    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True
    )

def plot_top_scorers_chart(df: pd.DataFrame):
    """Plot top scorers bar chart."""
    if df.empty:
        st.info("No data available for top scorers.")
        return
    
    fig = px.bar(
        df.head(20),
        x='total_goals',
        y='player_name',
        orientation='h',
        color='total_goals',
        color_continuous_scale='Blues',
        text='total_goals',
        title='Top Goal Scorers',
        labels={'total_goals': 'Goals', 'player_name': 'Player'}
    )
    fig.update_traces(textposition='outside')
    fig.update_layout(
        yaxis={'categoryorder': 'total ascending'},
        height=600,
        showlegend=False
    )
    st.plotly_chart(fig, use_container_width=True)

def plot_team_performance_radar(df: pd.DataFrame, team_name: str):
    """Plot team performance radar chart."""
    if df.empty or team_name not in df['team_name'].values:
        st.info("No data available for team performance.")
        return
    
    team_data = df[df['team_name'] == team_name].iloc[0]
    
    categories = ['Goals Scored', 'Shots on Target', 'Shot Accuracy %', 'Avg Possession %']
    values = [
        team_data['goals_scored'] / df['goals_scored'].max() * 100,
        team_data['total_shots_on_target'] / df['total_shots_on_target'].max() * 100,
        team_data['shot_accuracy_pct'],
        team_data['avg_possession_pct']
    ]
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatterpolar(
        r=values,
        theta=categories,
        fill='toself',
        name=team_name
    ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100]
            )),
        showlegend=True,
        title=f"Performance Radar: {team_name}"
    )
    
    st.plotly_chart(fig, use_container_width=True)

def plot_goals_timeline(df: pd.DataFrame):
    """Plot goals timeline over seasons."""
    if df.empty:
        st.info("No data available for goals timeline.")
        return
    
    goals_by_season = df.groupby('season').agg({
        'home_goals': 'sum',
        'away_goals': 'sum'
    }).reset_index()
    
    goals_by_season['total_goals'] = goals_by_season['home_goals'] + goals_by_season['away_goals']
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=goals_by_season['season'],
        y=goals_by_season['total_goals'],
        mode='lines+markers',
        name='Total Goals',
        line=dict(width=3, color='#1f77b4')
    ))
    
    fig.add_trace(go.Scatter(
        x=goals_by_season['season'],
        y=goals_by_season['home_goals'],
        mode='lines+markers',
        name='Home Goals',
        line=dict(width=2, color='#2ca02c')
    ))
    
    fig.add_trace(go.Scatter(
        x=goals_by_season['season'],
        y=goals_by_season['away_goals'],
        mode='lines+markers',
        name='Away Goals',
        line=dict(width=2, color='#ff7f0e')
    ))
    
    fig.update_layout(
        title='Goals Timeline by Season',
        xaxis_title='Season',
        yaxis_title='Number of Goals',
        hovermode='x unified',
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)

def plot_shot_analysis(df: pd.DataFrame):
    """Plot shot analysis charts."""
    if df.empty:
        st.info("No data available for shot analysis.")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Shot result distribution
        shot_result_df = df.groupby('shot_result')['shot_count'].sum().reset_index()
        fig1 = px.pie(
            shot_result_df,
            values='shot_count',
            names='shot_result',
            title='Shot Results Distribution',
            hole=0.4
        )
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # Conversion rate by situation
        situation_df = df.groupby('situation').agg({
            'shot_count': 'sum',
            'goals': 'sum'
        }).reset_index()
        situation_df['conversion_rate'] = (situation_df['goals'] / situation_df['shot_count'] * 100).round(2)
        
        fig2 = px.bar(
            situation_df,
            x='situation',
            y='conversion_rate',
            title='Conversion Rate by Situation',
            labels={'conversion_rate': 'Conversion Rate (%)', 'situation': 'Situation'},
            color='conversion_rate',
            color_continuous_scale='Greens'
        )
        fig2.update_layout(showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

def plot_player_comparison(df: pd.DataFrame, player_names: list):
    """Plot player comparison chart."""
    if df.empty or not player_names:
        st.info("No data available for player comparison.")
        return
    
    comparison_df = df[df['player_name'].isin(player_names)].copy()
    
    if comparison_df.empty:
        st.warning("Selected players not found in data.")
        return
    
    fig = go.Figure()
    
    metrics = ['total_goals', 'total_assists', 'goals_per_90', 'assists_per_90']
    metric_labels = ['Total Goals', 'Total Assists', 'Goals per 90', 'Assists per 90']
    
    for i, (metric, label) in enumerate(zip(metrics, metric_labels)):
        fig.add_trace(go.Bar(
            name=label,
            x=comparison_df['player_name'],
            y=comparison_df[metric],
            offsetgroup=i
        ))
    
    fig.update_layout(
        title='Player Performance Comparison',
        xaxis_title='Player',
        yaxis_title='Value',
        barmode='group',
        height=500
    )
    
    st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# PAGE FUNCTIONS
# ============================================================================

def show_overview():
    """Overview/Dashboard page."""
    st.header("📊 Dashboard Overview")
    
    # Load data
    leagues_df = load_leagues()
    seasons = get_seasons()
    
    if leagues_df.empty:
        st.error("Unable to load leagues. Please check database connection.")
        return
    
    # Filters
    col1, col2 = st.columns(2)
    with col1:
        selected_league = st.selectbox(
            "Select League",
            options=['All'] + leagues_df['name'].tolist(),
            index=0
        )
    with col2:
        selected_season = st.selectbox(
            "Select Season",
            options=['All'] + [str(s) for s in seasons],
            index=0
        )
    
    league_id = None if selected_league == 'All' else leagues_df[leagues_df['name'] == selected_league]['league_id'].iloc[0]
    season = None if selected_season == 'All' else int(selected_season)
    
    # Load summary data
    games_df = load_games_data(league_id, season, limit=1000)
    standings_df = load_league_standings(league_id, season)
    top_scorers_df = load_top_scorers(league_id, season, limit=10)
    
    if games_df.empty:
        st.warning("No data available for selected filters.")
        return
    
    # KPI Metrics
    st.subheader("Key Performance Indicators")
    col1, col2, col3, col4 = st.columns(4)
    
    total_games = len(games_df)
    total_goals = games_df['home_goals'].sum() + games_df['away_goals'].sum()
    avg_goals_per_game = (total_goals / total_games) if total_games > 0 else 0
    home_win_rate = (games_df['winner'] == games_df['home_team']).sum() / total_games * 100 if total_games > 0 else 0
    
    col1.metric("Total Games", f"{total_games:,}")
    col2.metric("Total Goals", f"{total_goals:,}")
    col3.metric("Avg Goals/Game", f"{avg_goals_per_game:.2f}")
    col4.metric("Home Win Rate", f"{home_win_rate:.1f}%")
    
    # Charts
    st.subheader("Goals Timeline")
    plot_goals_timeline(games_df)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 10 Scorers")
        if not top_scorers_df.empty:
            st.dataframe(
                top_scorers_df[['player_name', 'total_goals', 'total_assists', 'goals_per_90']].head(10),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No scorer data available.")
    
    with col2:
        st.subheader("Recent Games")
        recent_games = games_df.head(10)[['date', 'home_team', 'away_team', 'home_goals', 'away_goals']]
        recent_games.columns = ['Date', 'Home', 'Away', 'HG', 'AG']
        st.dataframe(recent_games, use_container_width=True, hide_index=True)

def show_league_standings():
    """League Standings page."""
    st.header("🏆 League Standings")
    
    leagues_df = load_leagues()
    seasons = get_seasons()
    
    if leagues_df.empty:
        st.error("Unable to load leagues.")
        return
    
    # Filters
    col1, col2 = st.columns(2)
    with col1:
        selected_league = st.selectbox(
            "Select League",
            options=leagues_df['name'].tolist(),
            index=0
        )
    with col2:
        selected_season = st.selectbox(
            "Select Season",
            options=[str(s) for s in seasons],
            index=0
        )
    
    league_id = leagues_df[leagues_df['name'] == selected_league]['league_id'].iloc[0]
    season = int(selected_season)
    
    # Load standings
    standings_df = load_league_standings(league_id, season)
    
    if standings_df.empty:
        st.warning("No standings data available for selected league and season.")
        return
    
    st.subheader(f"{selected_league} - {selected_season}/{selected_season+1} Season")
    plot_league_standings(standings_df)
    
    # Additional insights
    st.subheader("Standings Insights")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Champion", standings_df.iloc[0]['team_name'] if not standings_df.empty else "N/A")
    with col2:
        most_goals = standings_df.loc[standings_df['goals_for'].idxmax(), 'team_name'] if not standings_df.empty else "N/A"
        st.metric("Most Goals", most_goals)
    with col3:
        best_defense = standings_df.loc[standings_df['goals_against'].idxmin(), 'team_name'] if not standings_df.empty else "N/A"
        st.metric("Best Defense", best_defense)

def show_players():
    """Players Analytics page."""
    st.header("👥 Players Analytics")
    
    leagues_df = load_leagues()
    seasons = get_seasons()
    
    # Filters
    col1, col2 = st.columns(2)
    with col1:
        selected_league = st.selectbox(
            "Select League",
            options=['All'] + leagues_df['name'].tolist(),
            index=0,
            key='players_league'
        )
    with col2:
        selected_season = st.selectbox(
            "Select Season",
            options=['All'] + [str(s) for s in seasons],
            index=0,
            key='players_season'
        )
    
    league_id = None if selected_league == 'All' else leagues_df[leagues_df['name'] == selected_league]['league_id'].iloc[0]
    season = None if selected_season == 'All' else int(selected_season)
    
    # Load player data
    players_df = load_player_stats(league_id, season)
    top_scorers_df = load_top_scorers(league_id, season, limit=50)
    
    if players_df.empty:
        st.warning("No player data available.")
        return
    
    # Top Scorers Chart
    st.subheader("Top Goal Scorers")
    plot_top_scorers_chart(top_scorers_df)
    
    # Player Comparison
    st.subheader("Player Comparison")
    selected_players = st.multiselect(
        "Select players to compare",
        options=players_df['player_name'].tolist(),
        default=players_df['player_name'].head(3).tolist()
    )
    
    if selected_players:
        plot_player_comparison(players_df, selected_players)
    
    # Player Statistics Table
    st.subheader("Player Statistics")
    st.dataframe(
        players_df[['player_name', 'nationality', 'games_played', 'total_goals', 
                  'total_assists', 'goals_per_90', 'assists_per_90', 'avg_x_goals']].head(50),
        use_container_width=True,
        hide_index=True
    )

def show_teams():
    """Teams Analytics page."""
    st.header("🏟️ Teams Analytics")
    
    leagues_df = load_leagues()
    
    # Filters
    selected_league = st.selectbox(
        "Select League",
        options=['All'] + leagues_df['name'].tolist(),
        index=0,
        key='teams_league'
    )
    
    league_id = None if selected_league == 'All' else leagues_df[leagues_df['name'] == selected_league]['league_id'].iloc[0]
    
    # Load team data
    teams_df = load_team_performance(league_id=league_id)
    
    if teams_df.empty:
        st.warning("No team data available.")
        return
    
    # Team selection
    selected_team = st.selectbox(
        "Select Team for Detailed Analysis",
        options=teams_df['team_name'].tolist(),
        index=0
    )
    
    # Team Performance Radar
    st.subheader(f"Performance Analysis: {selected_team}")
    plot_team_performance_radar(teams_df, selected_team)
    
    # Team Statistics Table
    st.subheader("Team Statistics")
    display_teams_df = teams_df[['team_name', 'league_name', 'games_played', 'wins', 'draws', 'losses',
                                'goals_scored', 'avg_x_goals', 'shot_accuracy_pct', 'avg_possession_pct']]
    display_teams_df.columns = ['Team', 'League', 'GP', 'W', 'D', 'L', 'Goals', 'Avg xG', 'Shot Acc %', 'Possession %']
    st.dataframe(display_teams_df, use_container_width=True, hide_index=True)

def show_shots():
    """Shot Analysis page."""
    st.header("🎯 Shot Analysis")
    
    leagues_df = load_leagues()
    seasons = get_seasons()
    
    # Filters
    col1, col2 = st.columns(2)
    with col1:
        selected_league = st.selectbox(
            "Select League",
            options=['All'] + leagues_df['name'].tolist(),
            index=0,
            key='shots_league'
        )
    with col2:
        selected_season = st.selectbox(
            "Select Season",
            options=['All'] + [str(s) for s in seasons],
            index=0,
            key='shots_season'
        )
    
    league_id = None if selected_league == 'All' else leagues_df[leagues_df['name'] == selected_league]['league_id'].iloc[0]
    season = None if selected_season == 'All' else int(selected_season)
    
    # Load shot data
    shots_df = load_shot_analysis(league_id, season)
    
    if shots_df.empty:
        st.warning("No shot data available.")
        return
    
    # Shot Analysis Charts
    plot_shot_analysis(shots_df)
    
    # Shot Statistics Table
    st.subheader("Shot Statistics by Result and Situation")
    st.dataframe(
        shots_df,
        use_container_width=True,
        hide_index=True
    )

# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    """Main application function."""
    # Sidebar navigation
    with st.sidebar:
        st.title("⚽ European Football Analytics")
        
        if HAS_OPTION_MENU:
            selected = option_menu(
                menu_title=None,
                options=["Overview", "League Standings", "Players", "Teams", "Shot Analysis"],
                icons=["house", "trophy", "people", "building", "target"],
                menu_icon="cast",
                default_index=0,
                styles={
                    "container": {"padding": "5!important", "background-color": "#fafafa"},
                    "icon": {"color": "#1f77b4", "font-size": "25px"},
                    "nav-link": {"font-size": "16px", "text-align": "left", "margin": "0px"},
                    "nav-link-selected": {"background-color": "#1f77b4"},
                }
            )
        else:
            # Fallback to simple selectbox
            selected = st.selectbox(
                "Navigation",
                options=["Overview", "League Standings", "Players", "Teams", "Shot Analysis"],
                index=0
            )
        
        st.markdown("---")
        st.markdown("### About")
        st.info(
            "This dashboard provides comprehensive analytics for the top 5 European football leagues: "
            "Premier League, La Liga, Serie A, Bundesliga, and Ligue 1."
        )
        
        # Database connection status
        st.markdown("---")
        st.markdown("### Database Status")
        conn = get_db_connection()
        if conn:
            try:
                # Test the connection with a simple query
                cursor = conn.cursor()
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                cursor.close()
                st.success("✅ Database Connected")
                st.caption(f"PostgreSQL: {version.split(',')[0]}")
            except Exception as e:
                st.error(f"❌ Connection Test Failed: {e}")
                if st.button("🔄 Clear Cache & Retry"):
                    get_db_connection.clear()
                    st.rerun()
        else:
            st.error("❌ Database Disconnected")
            st.caption("Check connection settings")
            
            # Debug info (expandable)
            with st.expander("🔍 Connection Debug Info"):
                st.code(f"""
Host: {DB_CONFIG['host']}
Port: {DB_CONFIG['port']}
Database: {DB_CONFIG['database']}
User: {DB_CONFIG['user']}
Password: {'*' * len(DB_CONFIG['password'])} (length: {len(DB_CONFIG['password'])})
                """)
                st.caption("💡 If password is wrong, set DB_PASSWORD environment variable or modify bola.py line 41")
            
            if st.button("🔄 Retry Connection"):
                get_db_connection.clear()
                st.rerun()
    
    # Route to selected page
    if selected == "Overview":
        show_overview()
    elif selected == "League Standings":
        show_league_standings()
    elif selected == "Players":
        show_players()
    elif selected == "Teams":
        show_teams()
    elif selected == "Shot Analysis":
        show_shots()

if __name__ == "__main__":
    main()

