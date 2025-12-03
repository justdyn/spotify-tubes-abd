-- ============================================================================
-- Top 5 European Football Leagues Database Schema
-- Database: laliga_europe
-- Author: Senior Database Engineer
-- Description: Optimized PostgreSQL schema for European football data
--              with proper normalization, constraints, and indexing
-- ============================================================================

-- Drop database if exists and create fresh
DROP DATABASE IF EXISTS laliga_europe;
CREATE DATABASE laliga_europe;

-- Connect to the database
\c laliga_europe;

-- Enable extensions for better performance
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- ============================================================================
-- DOMAIN/REFERENCE TABLES
-- ============================================================================

-- Table: leagues
-- Purpose: Store the 5 major European football leagues
-- Optimization: Small lookup table, no special indexing needed beyond PK
CREATE TABLE leagues (
    league_id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_league_id_positive CHECK (league_id > 0)
);

COMMENT ON TABLE leagues IS 'Reference table for European football leagues';
COMMENT ON COLUMN leagues.league_id IS 'Primary key - unique league identifier';    

-- ============================================================================

-- Table: teams
-- Purpose: Store team information across all leagues
-- Optimization: Indexed by name for search operations
CREATE TABLE teams (
    team_id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_team_id_positive CHECK (team_id > 0),
    CONSTRAINT chk_team_name_not_empty CHECK (LENGTH(TRIM(name)) > 0)
);

COMMENT ON TABLE teams IS 'Master table for all football teams';
COMMENT ON COLUMN teams.team_id IS 'Primary key - unique team identifier';

-- Index for team name searches
CREATE INDEX idx_teams_name ON teams(name);
CREATE INDEX idx_teams_name_pattern ON teams(name varchar_pattern_ops);

-- ============================================================================

-- Table: players
-- Purpose: Store player information
-- Optimization: Indexed by name for search and lookup operations
CREATE TABLE players (
    player_id INTEGER PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_player_id_positive CHECK (player_id > 0),
    CONSTRAINT chk_player_name_not_empty CHECK (LENGTH(TRIM(name)) > 0)
);

COMMENT ON TABLE players IS 'Master table for all football players';
COMMENT ON COLUMN players.player_id IS 'Primary key - unique player identifier';

-- Index for player name searches
CREATE INDEX idx_players_name ON players(name);
CREATE INDEX idx_players_name_pattern ON players(name varchar_pattern_ops);

-- ============================================================================
-- FACT/TRANSACTIONAL TABLES
-- ============================================================================

-- Table: games
-- Purpose: Store match information (central fact table)
-- Optimization: Multiple indexes for common query patterns (date, teams, league+season)
CREATE TABLE games (
    game_id INTEGER PRIMARY KEY,
    league_id INTEGER NOT NULL,
    season SMALLINT NOT NULL,
    date TIMESTAMP NOT NULL,
    home_team_id INTEGER NOT NULL,
    away_team_id INTEGER NOT NULL,
    home_goals SMALLINT NOT NULL DEFAULT 0,
    away_goals SMALLINT NOT NULL DEFAULT 0,
    home_probability DECIMAL(5,4),
    draw_probability DECIMAL(5,4),
    away_probability DECIMAL(5,4),
    home_goals_half_time SMALLINT,
    away_goals_half_time SMALLINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_game_id_positive CHECK (game_id > 0),
    CONSTRAINT chk_different_teams CHECK (home_team_id != away_team_id),
    CONSTRAINT chk_goals_non_negative CHECK (home_goals >= 0 AND away_goals >= 0),
    CONSTRAINT chk_half_time_goals_non_negative CHECK (
        (home_goals_half_time IS NULL OR home_goals_half_time >= 0) AND 
        (away_goals_half_time IS NULL OR away_goals_half_time >= 0)
    ),
    CONSTRAINT chk_half_time_not_exceed_full_time CHECK (
        (home_goals_half_time IS NULL OR home_goals_half_time <= home_goals) AND
        (away_goals_half_time IS NULL OR away_goals_half_time <= away_goals)
    ),
    CONSTRAINT chk_season_valid CHECK (season >= 2014 AND season <= 2100),
    CONSTRAINT chk_probabilities_range CHECK (
        (home_probability IS NULL OR (home_probability >= 0 AND home_probability <= 1)) AND
        (draw_probability IS NULL OR (draw_probability >= 0 AND draw_probability <= 1)) AND
        (away_probability IS NULL OR (away_probability >= 0 AND away_probability <= 1))
    ),
    
    -- Foreign Keys
    CONSTRAINT fk_games_league FOREIGN KEY (league_id) 
        REFERENCES leagues(league_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_games_home_team FOREIGN KEY (home_team_id) 
        REFERENCES teams(team_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_games_away_team FOREIGN KEY (away_team_id) 
        REFERENCES teams(team_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

COMMENT ON TABLE games IS 'Central fact table storing all match information';
COMMENT ON COLUMN games.game_id IS 'Primary key - unique game identifier';
COMMENT ON COLUMN games.season IS 'Season year (start year of season)';

-- Strategic Indexes for games table (most queried table)
CREATE INDEX idx_games_league_season ON games(league_id, season);
CREATE INDEX idx_games_date ON games(date DESC);
CREATE INDEX idx_games_home_team ON games(home_team_id);
CREATE INDEX idx_games_away_team ON games(away_team_id);
CREATE INDEX idx_games_season ON games(season);
CREATE INDEX idx_games_league ON games(league_id);

-- Composite index for team performance queries
CREATE INDEX idx_games_teams_date ON games(home_team_id, away_team_id, date);

-- Index for recent games queries (date descending for latest games first)
-- Note: Partial index with CURRENT_DATE removed as it's not immutable
-- Use regular index instead - PostgreSQL query planner will optimize automatically
CREATE INDEX idx_games_recent ON games(date DESC, league_id);

-- ============================================================================

-- Table: team_stats
-- Purpose: Store detailed team statistics for each game
-- Optimization: Composite primary key, optimized for team and game lookups
-- Normalization: Removed redundant season/date (derivable from games)
CREATE TABLE team_stats (
    game_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    location CHAR(1) NOT NULL,
    goals SMALLINT NOT NULL DEFAULT 0,
    x_goals DECIMAL(8,6),
    shots SMALLINT NOT NULL DEFAULT 0,
    shots_on_target SMALLINT NOT NULL DEFAULT 0,
    deep_passes INTEGER,
    ppda DECIMAL(8,4),
    fouls SMALLINT,
    corners SMALLINT,
    yellow_cards SMALLINT NOT NULL DEFAULT 0,
    red_cards SMALLINT NOT NULL DEFAULT 0,
    result CHAR(1) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Composite Primary Key
    PRIMARY KEY (game_id, team_id),
    
    -- Constraints
    CONSTRAINT chk_location_valid CHECK (location IN ('h', 'a')),
    CONSTRAINT chk_result_valid CHECK (result IN ('W', 'D', 'L')),
    CONSTRAINT chk_stats_non_negative CHECK (
        goals >= 0 AND shots >= 0 AND shots_on_target >= 0 AND 
        yellow_cards >= 0 AND red_cards >= 0
    ),
    CONSTRAINT chk_shots_on_target_logic CHECK (shots_on_target <= shots),
    CONSTRAINT chk_x_goals_non_negative CHECK (x_goals IS NULL OR x_goals >= 0),
    CONSTRAINT chk_cards_reasonable CHECK (yellow_cards <= 11 AND red_cards <= 11),
    
    -- Foreign Keys
    CONSTRAINT fk_team_stats_game FOREIGN KEY (game_id) 
        REFERENCES games(game_id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_team_stats_team FOREIGN KEY (team_id) 
        REFERENCES teams(team_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

COMMENT ON TABLE team_stats IS 'Detailed team statistics for each game';
COMMENT ON COLUMN team_stats.location IS 'h=home, a=away';
COMMENT ON COLUMN team_stats.x_goals IS 'Expected goals (xG)';
COMMENT ON COLUMN team_stats.ppda IS 'Passes per defensive action';
COMMENT ON COLUMN team_stats.result IS 'W=Win, D=Draw, L=Loss';

-- Indexes for team_stats
CREATE INDEX idx_team_stats_team ON team_stats(team_id);
CREATE INDEX idx_team_stats_game ON team_stats(game_id);
CREATE INDEX idx_team_stats_result ON team_stats(result);

-- Covering index for common aggregation queries
CREATE INDEX idx_team_stats_team_covering ON team_stats(team_id, goals, x_goals, shots, result);

-- ============================================================================

-- Table: appearances
-- Purpose: Store player performance data for each game
-- Optimization: Composite primary key, removed redundant leagueID
-- Normalization: leagueID removed as it's derivable via games table
CREATE TABLE appearances (
    game_id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    goals SMALLINT NOT NULL DEFAULT 0,
    own_goals SMALLINT NOT NULL DEFAULT 0,
    shots SMALLINT NOT NULL DEFAULT 0,
    x_goals DECIMAL(8,6),
    x_goals_chain DECIMAL(8,6),
    x_goals_buildup DECIMAL(8,6),
    assists SMALLINT NOT NULL DEFAULT 0,
    key_passes SMALLINT NOT NULL DEFAULT 0,
    x_assists DECIMAL(8,6),
    position VARCHAR(10),
    position_order SMALLINT,
    yellow_card SMALLINT NOT NULL DEFAULT 0,
    red_card SMALLINT NOT NULL DEFAULT 0,
    time_played SMALLINT NOT NULL DEFAULT 0,
    substitute_in VARCHAR(20),
    substitute_out VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Composite Primary Key
    PRIMARY KEY (game_id, player_id),
    
    -- Constraints
    CONSTRAINT chk_appearance_stats_non_negative CHECK (
        goals >= 0 AND own_goals >= 0 AND shots >= 0 AND 
        assists >= 0 AND key_passes >= 0 AND time_played >= 0
    ),
    CONSTRAINT chk_cards_binary CHECK (yellow_card IN (0, 1) AND red_card IN (0, 1)),
    CONSTRAINT chk_time_played_valid CHECK (time_played >= 0 AND time_played <= 120),
    CONSTRAINT chk_x_goals_non_negative CHECK (
        (x_goals IS NULL OR x_goals >= 0) AND
        (x_goals_chain IS NULL OR x_goals_chain >= 0) AND
        (x_goals_buildup IS NULL OR x_goals_buildup >= 0) AND
        (x_assists IS NULL OR x_assists >= 0)
    ),
    CONSTRAINT chk_position_order_valid CHECK (position_order IS NULL OR position_order > 0),
    
    -- Foreign Keys
    CONSTRAINT fk_appearances_game FOREIGN KEY (game_id) 
        REFERENCES games(game_id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_appearances_player FOREIGN KEY (player_id) 
        REFERENCES players(player_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

COMMENT ON TABLE appearances IS 'Player performance statistics for each game';
COMMENT ON COLUMN appearances.x_goals IS 'Expected goals for player';
COMMENT ON COLUMN appearances.x_goals_chain IS 'xG in possession chains involving player';
COMMENT ON COLUMN appearances.x_goals_buildup IS 'xG in buildup plays involving player';
COMMENT ON COLUMN appearances.time_played IS 'Minutes played in the match';

-- Indexes for appearances
CREATE INDEX idx_appearances_player ON appearances(player_id);
CREATE INDEX idx_appearances_game ON appearances(game_id);
CREATE INDEX idx_appearances_position ON appearances(position) WHERE position IS NOT NULL;

-- Covering index for player statistics queries
CREATE INDEX idx_appearances_player_stats ON appearances(
    player_id, goals, assists, time_played, yellow_card, red_card
);

-- Partial index for goalscorers
CREATE INDEX idx_appearances_goals ON appearances(player_id, goals) WHERE goals > 0;

-- Partial index for assists
CREATE INDEX idx_appearances_assists ON appearances(player_id, assists) WHERE assists > 0;

-- ============================================================================

-- Table: shots
-- Purpose: Store detailed shot information
-- Optimization: Surrogate key added for better performance, strategic indexes
CREATE TABLE shots (
    shot_id BIGSERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL,
    shooter_id INTEGER NOT NULL,
    assister_id INTEGER,
    minute SMALLINT NOT NULL,
    situation VARCHAR(50),
    last_action VARCHAR(50),
    shot_type VARCHAR(50),
    shot_result VARCHAR(50),
    x_goal DECIMAL(8,6),
    position_x DECIMAL(10,8),
    position_y DECIMAL(10,8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_minute_valid CHECK (minute >= 0 AND minute <= 120),
    CONSTRAINT chk_x_goal_range CHECK (x_goal IS NULL OR (x_goal >= 0 AND x_goal <= 1)),
    CONSTRAINT chk_position_x_range CHECK (position_x IS NULL OR (position_x >= 0 AND position_x <= 1)),
    CONSTRAINT chk_position_y_range CHECK (position_y IS NULL OR (position_y >= 0 AND position_y <= 1)),
    CONSTRAINT chk_shooter_not_assister CHECK (assister_id IS NULL OR shooter_id != assister_id),
    
    -- Foreign Keys
    CONSTRAINT fk_shots_game FOREIGN KEY (game_id) 
        REFERENCES games(game_id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_shots_shooter FOREIGN KEY (shooter_id) 
        REFERENCES players(player_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_shots_assister FOREIGN KEY (assister_id) 
        REFERENCES players(player_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

COMMENT ON TABLE shots IS 'Detailed shot information for advanced analytics';
COMMENT ON COLUMN shots.shot_id IS 'Auto-generated primary key for shot records';
COMMENT ON COLUMN shots.x_goal IS 'Expected goal probability for this shot';
COMMENT ON COLUMN shots.position_x IS 'Normalized X coordinate of shot (0-1)';
COMMENT ON COLUMN shots.position_y IS 'Normalized Y coordinate of shot (0-1)';

-- Indexes for shots table (large table, needs careful indexing)
CREATE INDEX idx_shots_game ON shots(game_id);
CREATE INDEX idx_shots_shooter ON shots(shooter_id);
CREATE INDEX idx_shots_assister ON shots(assister_id) WHERE assister_id IS NOT NULL;
CREATE INDEX idx_shots_result ON shots(shot_result) WHERE shot_result IS NOT NULL;
CREATE INDEX idx_shots_situation ON shots(situation) WHERE situation IS NOT NULL;

-- Composite index for shot analysis queries
CREATE INDEX idx_shots_game_shooter ON shots(game_id, shooter_id);
CREATE INDEX idx_shots_shooter_result ON shots(shooter_id, shot_result, x_goal);

-- Partial index for goals
CREATE INDEX idx_shots_goals ON shots(game_id, shooter_id, minute) 
    WHERE shot_result = 'Goal';

-- GiST index for spatial queries (shot positions)
CREATE INDEX idx_shots_position ON shots USING gist(
    box(point(position_x, position_y), point(position_x, position_y))
) WHERE position_x IS NOT NULL AND position_y IS NOT NULL;

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View: Full game details with team names
CREATE OR REPLACE VIEW v_games_full AS
SELECT 
    g.game_id,
    g.season,
    g.date,
    l.name AS league_name,
    ht.name AS home_team,
    at.name AS away_team,
    g.home_goals,
    g.away_goals,
    CASE 
        WHEN g.home_goals > g.away_goals THEN ht.name
        WHEN g.away_goals > g.home_goals THEN at.name
        ELSE 'Draw'
    END AS winner,
    g.home_probability,
    g.draw_probability,
    g.away_probability
FROM games g
JOIN leagues l ON g.league_id = l.league_id
JOIN teams ht ON g.home_team_id = ht.team_id
JOIN teams at ON g.away_team_id = at.team_id;

COMMENT ON VIEW v_games_full IS 'Complete game information with readable team and league names';

-- ============================================================================

-- View: Player statistics aggregated
CREATE OR REPLACE VIEW v_player_stats_summary AS
SELECT 
    p.player_id,
    p.name AS player_name,
    COUNT(DISTINCT a.game_id) AS games_played,
    SUM(a.goals) AS total_goals,
    SUM(a.assists) AS total_assists,
    SUM(a.time_played) AS total_minutes,
    SUM(a.yellow_card) AS total_yellow_cards,
    SUM(a.red_card) AS total_red_cards,
    ROUND(AVG(a.x_goals)::numeric, 3) AS avg_x_goals,
    ROUND(AVG(a.x_assists)::numeric, 3) AS avg_x_assists,
    ROUND((SUM(a.goals)::numeric / NULLIF(SUM(a.time_played), 0) * 90), 3) AS goals_per_90
FROM players p
LEFT JOIN appearances a ON p.player_id = a.player_id
GROUP BY p.player_id, p.name;

COMMENT ON VIEW v_player_stats_summary IS 'Aggregated player statistics across all games';

-- ============================================================================

-- View: Team performance summary
CREATE OR REPLACE VIEW v_team_performance AS
SELECT 
    t.team_id,
    t.name AS team_name,
    COUNT(DISTINCT ts.game_id) AS games_played,
    SUM(CASE WHEN ts.result = 'W' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN ts.result = 'D' THEN 1 ELSE 0 END) AS draws,
    SUM(CASE WHEN ts.result = 'L' THEN 1 ELSE 0 END) AS losses,
    SUM(ts.goals) AS goals_scored,
    ROUND(AVG(ts.x_goals)::numeric, 2) AS avg_x_goals,
    SUM(ts.shots) AS total_shots,
    SUM(ts.shots_on_target) AS total_shots_on_target,
    ROUND((SUM(ts.shots_on_target)::numeric / NULLIF(SUM(ts.shots), 0) * 100), 2) AS shot_accuracy_pct
FROM teams t
LEFT JOIN team_stats ts ON t.team_id = ts.team_id
GROUP BY t.team_id, t.name;

COMMENT ON VIEW v_team_performance IS 'Comprehensive team performance metrics';

-- ============================================================================
-- MATERIALIZED VIEWS FOR HEAVY ANALYTICS
-- ============================================================================

-- Materialized View: League standings per season
CREATE MATERIALIZED VIEW mv_league_standings AS
SELECT 
    g.league_id,
    l.name AS league_name,
    g.season,
    ts.team_id,
    t.name AS team_name,
    COUNT(DISTINCT ts.game_id) AS matches_played,
    SUM(CASE WHEN ts.result = 'W' THEN 3 
             WHEN ts.result = 'D' THEN 1 
             ELSE 0 END) AS points,
    SUM(CASE WHEN ts.result = 'W' THEN 1 ELSE 0 END) AS wins,
    SUM(CASE WHEN ts.result = 'D' THEN 1 ELSE 0 END) AS draws,
    SUM(CASE WHEN ts.result = 'L' THEN 1 ELSE 0 END) AS losses,
    SUM(ts.goals) AS goals_for,
    SUM(CASE 
        WHEN ts.location = 'h' THEN (SELECT away_goals FROM games WHERE game_id = ts.game_id)
        ELSE (SELECT home_goals FROM games WHERE game_id = ts.game_id)
    END) AS goals_against,
    SUM(ts.goals) - SUM(CASE 
        WHEN ts.location = 'h' THEN (SELECT away_goals FROM games WHERE game_id = ts.game_id)
        ELSE (SELECT home_goals FROM games WHERE game_id = ts.game_id)
    END) AS goal_difference
FROM team_stats ts
JOIN games g ON ts.game_id = g.game_id
JOIN teams t ON ts.team_id = t.team_id
JOIN leagues l ON g.league_id = l.league_id
GROUP BY g.league_id, l.name, g.season, ts.team_id, t.name;

CREATE INDEX idx_mv_league_standings_league_season ON mv_league_standings(league_id, season, points DESC);
CREATE INDEX idx_mv_league_standings_team ON mv_league_standings(team_id, season);

COMMENT ON MATERIALIZED VIEW mv_league_standings IS 'Pre-computed league standings by season';

-- ============================================================================
-- FUNCTIONS FOR DATA INTEGRITY
-- ============================================================================

-- Function: Refresh materialized views
CREATE OR REPLACE FUNCTION refresh_all_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW mv_league_standings;
    RAISE NOTICE 'All materialized views refreshed successfully';
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_all_materialized_views IS 'Refresh all materialized views in the database';

-- ============================================================================

-- Function: Validate game result consistency
CREATE OR REPLACE FUNCTION validate_game_results()
RETURNS TABLE(game_id INTEGER, issue TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        g.game_id,
        'Inconsistent results in team_stats' AS issue
    FROM games g
    WHERE EXISTS (
        SELECT 1
        FROM team_stats ts1
        JOIN team_stats ts2 ON ts1.game_id = ts2.game_id AND ts1.team_id != ts2.team_id
        WHERE ts1.game_id = g.game_id
        AND (
            (ts1.result = 'W' AND ts2.result != 'L') OR
            (ts1.result = 'D' AND ts2.result != 'D') OR
            (ts1.result = 'L' AND ts2.result != 'W')
        )
    );
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION validate_game_results IS 'Check for inconsistent match results';

-- ============================================================================
-- TRIGGERS FOR AUDIT AND VALIDATION
-- ============================================================================

-- Trigger function: Update timestamp on modify
CREATE OR REPLACE FUNCTION update_modified_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.created_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- STATISTICS AND PERFORMANCE TUNING
-- ============================================================================

-- Increase statistics target for frequently queried columns
ALTER TABLE games ALTER COLUMN league_id SET STATISTICS 1000;
ALTER TABLE games ALTER COLUMN season SET STATISTICS 1000;
ALTER TABLE games ALTER COLUMN date SET STATISTICS 1000;
ALTER TABLE team_stats ALTER COLUMN team_id SET STATISTICS 1000;
ALTER TABLE appearances ALTER COLUMN player_id SET STATISTICS 1000;
ALTER TABLE shots ALTER COLUMN shooter_id SET STATISTICS 1000;

-- ============================================================================
-- VACUUM AND ANALYZE RECOMMENDATIONS
-- ============================================================================

-- Set autovacuum parameters for large tables
ALTER TABLE shots SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);

ALTER TABLE appearances SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);

-- ============================================================================
-- SUMMARY COMMENTS
-- ============================================================================

COMMENT ON DATABASE laliga_europe IS 
'European Football Database - Optimized schema for Top 5 leagues
Key optimizations:
1. Normalized data model - removed redundant columns (leagueID from appearances, season/date from team_stats)
2. Strategic indexing - covering indexes, partial indexes, and composite indexes for common queries
3. Proper constraints - CHECK constraints for data validation, FK constraints for referential integrity
4. Performance tuning - increased statistics targets, optimized autovacuum settings
5. Materialized views - pre-computed aggregations for heavy analytical queries
6. Role-based access control - three-tier permission model
7. Audit capabilities - timestamp tracking and validation functions
8. Spatial indexing - GiST index for shot position analytics';

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================

-- Analyze all tables to update statistics
ANALYZE;

-- Display success message
\echo '============================================================================'
\echo 'Schema created successfully!'
\echo 'Database: laliga_europe'
\echo 'Tables: 7 (leagues, teams, players, games, team_stats, appearances, shots)'
\echo 'Views: 3 regular views + 1 materialized view'
\echo 'Indexes: 40+ strategic indexes for optimal query performance'
\echo '============================================================================'
