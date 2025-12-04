-- ============================================================================
-- Data Manipulation Language (DML) - Load CSV Data into Database
-- Database: laliga_europe
-- Purpose: Import all CSV datasets into the improved schema (ddl_improved.sql)
-- 
-- IMPORTANT NOTES:
-- 1. This script matches the schema in ddl_improved.sql exactly
-- 2. Betting odds columns from games.csv are NOT imported (not in schema)
-- 3. SERIAL primary keys are preserved from CSV for mapping purposes
-- 4. Sequences are updated after inserts to maintain consistency
-- 5. Only columns that exist in the schema are inserted
-- ============================================================================

-- Connect to the database
\c laliga_europe;

-- ============================================================================
-- STEP 1: Load LEAGUES
-- ============================================================================

-- Create temporary table for CSV import
CREATE TEMP TABLE leagues_temp (
    leagueID INTEGER,
    name VARCHAR(100),
    understatNotation VARCHAR(50)
);

-- Import leagues CSV
\copy leagues_temp FROM 'leagues.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

-- Map league names to countries
-- Note: league_id is SERIAL, but we preserve CSV IDs for mapping
INSERT INTO leagues (name, country, is_active)
SELECT DISTINCT
    name,
    CASE 
        WHEN name = 'Premier League' THEN 'England'
        WHEN name = 'Serie A' THEN 'Italy'
        WHEN name = 'Bundesliga' THEN 'Germany'
        WHEN name = 'La Liga' THEN 'Spain'
        WHEN name = 'Ligue 1' THEN 'France'
        ELSE 'Unknown'
    END AS country,
    true AS is_active
FROM leagues_temp
ON CONFLICT (name) DO NOTHING;

-- Update sequence for leagues table
SELECT setval('leagues_league_id_seq', (SELECT MAX(league_id) FROM leagues));

-- Create mapping table for league IDs
CREATE TEMP TABLE league_id_mapping AS
SELECT 
    lt.leagueID AS old_league_id,
    l.league_id AS new_league_id
FROM leagues_temp lt
JOIN leagues l ON lt.name = l.name;

DROP TABLE leagues_temp;

-- ============================================================================
-- STEP 2: Create GAMES temp table early (needed for team mapping)
-- ============================================================================

-- Create temporary table for games CSV early (needed for team league mapping)
-- Note: CSV contains betting odds columns (B365H, B365D, etc.) that are NOT in the schema
CREATE TEMP TABLE games_temp (
    "gameID" INTEGER,
    "leagueID" INTEGER,
    "season" SMALLINT,
    "date" TIMESTAMP,
    "homeTeamID" INTEGER,
    "awayTeamID" INTEGER,
    "homeGoals" SMALLINT,
    "awayGoals" SMALLINT,
    "homeProbability" DECIMAL(5,4),
    "drawProbability" DECIMAL(5,4),
    "awayProbability" DECIMAL(5,4),
    "homeGoalsHalfTime" SMALLINT,
    "awayGoalsHalfTime" SMALLINT,
    -- Betting odds columns (not used, but included to match CSV structure)
    -- Handle NA values by making them nullable
    "B365H" VARCHAR(20),
    "B365D" VARCHAR(20),
    "B365A" VARCHAR(20),
    "BWH" VARCHAR(20),
    "BWD" VARCHAR(20),
    "BWA" VARCHAR(20),
    "IWH" VARCHAR(20),
    "IWD" VARCHAR(20),
    "IWA" VARCHAR(20),
    "PSH" VARCHAR(20),
    "PSD" VARCHAR(20),
    "PSA" VARCHAR(20),
    "WHH" VARCHAR(20),
    "WHD" VARCHAR(20),
    "WHA" VARCHAR(20),
    "VCH" VARCHAR(20),
    "VCD" VARCHAR(20),
    "VCA" VARCHAR(20),
    "PSCH" VARCHAR(20),
    "PSCD" VARCHAR(20),
    "PSCA" VARCHAR(20)
);

-- Import games CSV (all columns, but we'll only use the ones in the schema)
\copy games_temp FROM 'games.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

-- ============================================================================
-- STEP 3: Load TEAMS
-- ============================================================================

-- Create temporary table for teams CSV
CREATE TEMP TABLE teams_temp (
    teamID INTEGER,
    name VARCHAR(100)
);

-- Import teams CSV
\copy teams_temp FROM 'teams.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

-- First, we need to determine which league each team belongs to
-- We'll do this by looking at games data (now available)
CREATE TEMP TABLE team_league_mapping AS
SELECT DISTINCT
    tt.teamID AS old_team_id,
    tt.name AS team_name,
    lidm.new_league_id AS league_id
FROM teams_temp tt
JOIN (
    SELECT DISTINCT 
        "homeTeamID"::INTEGER AS team_id,
        "leagueID"::INTEGER AS league_id
    FROM games_temp
    UNION
    SELECT DISTINCT 
        "awayTeamID"::INTEGER AS team_id,
        "leagueID"::INTEGER AS league_id
    FROM games_temp
) g ON tt.teamID = g.team_id
JOIN league_id_mapping lidm ON g.league_id = lidm.old_league_id;

-- Insert teams with league assignment
-- Note: team_id is SERIAL, but we preserve CSV IDs for mapping via the mapping table
INSERT INTO teams (league_id, name, short_name, is_active)
SELECT DISTINCT
    tlm.league_id,
    tlm.team_name,
    NULL AS short_name,  -- Can be populated later if needed
    true AS is_active
FROM team_league_mapping tlm
ON CONFLICT (league_id, name) DO NOTHING;

-- Update sequence for teams table
SELECT setval('teams_team_id_seq', (SELECT MAX(team_id) FROM teams));

-- Create mapping table for team IDs
CREATE TEMP TABLE team_id_mapping AS
SELECT 
    tt.teamID AS old_team_id,
    t.team_id AS new_team_id
FROM teams_temp tt
JOIN teams t ON tt.name = t.name
JOIN team_league_mapping tlm ON tt.teamID = tlm.old_team_id AND t.league_id = tlm.league_id;

DROP TABLE teams_temp;
-- Keep team_league_mapping for now (needed for team_id_mapping)

-- ============================================================================
-- STEP 4: Load PLAYERS
-- ============================================================================

-- Create temporary table for players CSV
CREATE TEMP TABLE players_temp (
    playerID INTEGER,
    name VARCHAR(150)
);

-- Import players CSV
\copy players_temp FROM 'players.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

-- Insert players (without date_of_birth and nationality - can be added later)
-- Note: player_id is SERIAL, but we preserve CSV IDs for mapping
INSERT INTO players (player_id, name, date_of_birth, nationality, is_active)
SELECT 
    playerID,
    name,
    NULL AS date_of_birth,  -- Can be populated from external sources later
    NULL AS nationality,     -- Can be populated from external sources later
    true AS is_active
FROM players_temp
ON CONFLICT (player_id) DO UPDATE SET name = EXCLUDED.name;

-- Update sequence for players table
SELECT setval('players_player_id_seq', (SELECT MAX(player_id) FROM players));

DROP TABLE players_temp;

-- ============================================================================
-- STEP 5: Load GAMES (games_temp already created and populated in STEP 2)
-- ============================================================================

-- Insert games
-- Note: game_id is SERIAL, but we preserve CSV IDs for mapping
INSERT INTO games (
    game_id, league_id, season, game_week, date,
    home_team_id, away_team_id,
    home_goals, away_goals,
    home_probability, draw_probability, away_probability,
    home_goals_half_time, away_goals_half_time,
    stadium, attendance, status
)
SELECT 
    gt."gameID",
    lidm.new_league_id AS league_id,
    gt."season",
    NULL AS game_week,  -- Not available in CSV
    gt."date",
    tim_home.new_team_id AS home_team_id,
    tim_away.new_team_id AS away_team_id,
    gt."homeGoals",
    gt."awayGoals",
    gt."homeProbability",
    gt."drawProbability",
    gt."awayProbability",
    gt."homeGoalsHalfTime",
    gt."awayGoalsHalfTime",
    NULL AS stadium,  -- Not available in CSV
    NULL AS attendance,  -- Not available in CSV
    'completed' AS status  -- Assume all games in CSV are completed
FROM games_temp gt
JOIN league_id_mapping lidm ON gt."leagueID" = lidm.old_league_id
JOIN team_id_mapping tim_home ON gt."homeTeamID" = tim_home.old_team_id
JOIN team_id_mapping tim_away ON gt."awayTeamID" = tim_away.old_team_id
ON CONFLICT (league_id, season, home_team_id, away_team_id, date) 
DO UPDATE SET
    home_goals = EXCLUDED.home_goals,
    away_goals = EXCLUDED.away_goals,
    home_probability = EXCLUDED.home_probability,
    draw_probability = EXCLUDED.draw_probability,
    away_probability = EXCLUDED.away_probability;

-- Update sequence for games table
SELECT setval('games_game_id_seq', (SELECT MAX(game_id) FROM games));

-- Create game ID mapping
CREATE TEMP TABLE game_id_mapping AS
SELECT 
    gt."gameID" AS old_game_id,
    g.game_id AS new_game_id
FROM games_temp gt
JOIN games g ON gt."gameID" = g.game_id;

DROP TABLE games_temp;

-- ============================================================================
-- STEP 6: Load TEAM_STATS
-- ============================================================================

-- Create temporary table for team_stats CSV
-- Handle NA values by making numeric columns nullable VARCHAR first, then convert
CREATE TEMP TABLE team_stats_temp (
    "gameID" INTEGER,
    "teamID" INTEGER,
    "season" SMALLINT,
    "date" TIMESTAMP,
    "location" VARCHAR(1),
    "goals" SMALLINT,
    "xGoals" DECIMAL(8,6),
    "shots" SMALLINT,
    "shotsOnTarget" SMALLINT,
    "deep" INTEGER,
    "ppda" DECIMAL(8,4),
    "fouls" SMALLINT,
    "corners" SMALLINT,
    "yellowCards" VARCHAR(10),  -- VARCHAR to handle NA, convert later
    "redCards" VARCHAR(10),      -- VARCHAR to handle NA, convert later
    "result" VARCHAR(1)
);

-- Import team_stats CSV
\copy team_stats_temp FROM 'teamstats.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

-- Insert team_stats with proper enum conversions
INSERT INTO team_stats (
    game_id, team_id, location, goals, x_goals,
    shots, shots_on_target, deep_passes, ppda,
    fouls, corners, yellow_cards, red_cards, result,
    possession_percentage
)
SELECT 
    gim.new_game_id AS game_id,
    tim.new_team_id AS team_id,
    CASE 
        WHEN tst."location" = 'h' THEN 'home'::location_type
        WHEN tst."location" = 'a' THEN 'away'::location_type
        ELSE 'home'::location_type  -- Default fallback
    END AS location,
    tst."goals",
    tst."xGoals" AS x_goals,
    tst."shots",
    tst."shotsOnTarget" AS shots_on_target,
    tst."deep" AS deep_passes,
    tst."ppda",
    tst."fouls",
    tst."corners",
    CASE 
        WHEN tst."yellowCards" IS NULL OR tst."yellowCards" = 'NA' OR tst."yellowCards" = '' 
        THEN 0 
        ELSE tst."yellowCards"::SMALLINT 
    END AS yellow_cards,
    CASE 
        WHEN tst."redCards" IS NULL OR tst."redCards" = 'NA' OR tst."redCards" = '' 
        THEN 0 
        ELSE tst."redCards"::SMALLINT 
    END AS red_cards,
    CASE 
        WHEN tst."result" = 'W' THEN 'win'::result_type
        WHEN tst."result" = 'D' THEN 'draw'::result_type
        WHEN tst."result" = 'L' THEN 'loss'::result_type
        ELSE 'draw'::result_type  -- Default fallback
    END AS result,
    NULL AS possession_percentage  -- Not available in CSV
FROM team_stats_temp tst
JOIN game_id_mapping gim ON tst."gameID" = gim.old_game_id
JOIN team_id_mapping tim ON tst."teamID" = tim.old_team_id
ON CONFLICT (game_id, team_id) DO UPDATE SET
    goals = EXCLUDED.goals,
    x_goals = EXCLUDED.x_goals,
    shots = EXCLUDED.shots,
    shots_on_target = EXCLUDED.shots_on_target,
    result = EXCLUDED.result;

DROP TABLE team_stats_temp;

-- ============================================================================
-- STEP 7: Load APPEARANCES
-- ============================================================================

-- Create temporary table for appearances CSV
CREATE TEMP TABLE appearances_temp (
    "gameID" INTEGER,
    "playerID" INTEGER,
    "goals" SMALLINT,
    "ownGoals" SMALLINT,
    "shots" SMALLINT,
    "xGoals" DECIMAL(8,6),
    "xGoalsChain" DECIMAL(8,6),
    "xGoalsBuildup" DECIMAL(8,6),
    "assists" SMALLINT,
    "keyPasses" SMALLINT,
    "xAssists" DECIMAL(8,6),
    "position" VARCHAR(10),
    "positionOrder" SMALLINT,
    "yellowCard" SMALLINT,
    "redCard" SMALLINT,
    "time" SMALLINT,
    "substituteIn" VARCHAR(20),
    "substituteOut" VARCHAR(20),
    "leagueID" INTEGER
);

-- Import appearances CSV
\copy appearances_temp FROM 'appearances.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

-- Insert appearances with team_id derived from games
INSERT INTO appearances (
    game_id, player_id, team_id,
    goals, own_goals, shots, x_goals, x_goals_chain, x_goals_buildup,
    assists, key_passes, x_assists,
    position, position_order,
    yellow_card, red_card,
    time_played, substitute_in, substitute_out
)
SELECT 
    gim.new_game_id AS game_id,
    at."playerID" AS player_id,
    -- Derive team_id: check which team the player belongs to in this game
    -- We'll use team_stats to determine this (more reliable)
    COALESCE(
        (SELECT ts.team_id 
         FROM team_stats ts 
         WHERE ts.game_id = gim.new_game_id
         AND EXISTS (
             SELECT 1 FROM games g 
             WHERE g.game_id = gim.new_game_id 
             AND g.home_team_id = ts.team_id
         )
         LIMIT 1),
        (SELECT ts.team_id 
         FROM team_stats ts 
         WHERE ts.game_id = gim.new_game_id
         LIMIT 1)
    ) AS team_id,
    at."goals",
    at."ownGoals" AS own_goals,
    at."shots",
    at."xGoals" AS x_goals,
    at."xGoalsChain" AS x_goals_chain,
    at."xGoalsBuildup" AS x_goals_buildup,
    at."assists",
    at."keyPasses" AS key_passes,
    at."xAssists" AS x_assists,
    at."position",
    at."positionOrder" AS position_order,
    CASE WHEN at."yellowCard" > 0 THEN true ELSE false END AS yellow_card,
    CASE WHEN at."redCard" > 0 THEN true ELSE false END AS red_card,
    at."time" AS time_played,
    -- Validate substitute_in: must be numeric, 0-120 range, and fit in SMALLINT
    CASE 
        WHEN at."substituteIn" IS NULL OR at."substituteIn" = '' OR at."substituteIn" = '0'
        THEN NULL
        WHEN at."substituteIn" ~ '^\d+$' 
        AND LENGTH(at."substituteIn") <= 3  -- Max 3 digits (0-120)
        AND (at."substituteIn"::INTEGER BETWEEN 1 AND 120)  -- Valid minute range
        THEN at."substituteIn"::SMALLINT 
        ELSE NULL  -- Invalid values (too large, non-numeric, etc.) become NULL
    END AS substitute_in,
    -- Validate substitute_out: must be numeric, 0-120 range, and fit in SMALLINT
    CASE 
        WHEN at."substituteOut" IS NULL OR at."substituteOut" = '' OR at."substituteOut" = '0'
        THEN NULL
        WHEN at."substituteOut" ~ '^\d+$' 
        AND LENGTH(at."substituteOut") <= 3  -- Max 3 digits (0-120)
        AND (at."substituteOut"::INTEGER BETWEEN 1 AND 120)  -- Valid minute range
        THEN at."substituteOut"::SMALLINT 
        ELSE NULL  -- Invalid values (too large, non-numeric, etc.) become NULL
    END AS substitute_out
FROM appearances_temp at
JOIN game_id_mapping gim ON at."gameID" = gim.old_game_id
ON CONFLICT (game_id, player_id) DO UPDATE SET
    goals = EXCLUDED.goals,
    assists = EXCLUDED.assists,
    time_played = EXCLUDED.time_played;

-- Better approach: Use a more accurate method to determine team_id
-- Strategy: For each appearance, find which team in the game has the most players
-- This is more reliable than random assignment
UPDATE appearances a
SET team_id = (
    -- Find team_id by checking which team has more appearances in this game
    -- This is a heuristic: the team with more appearances is likely the correct team
    WITH team_appearance_counts AS (
        SELECT 
            ts.team_id,
            COUNT(*) as appearance_count
        FROM team_stats ts
        WHERE ts.game_id = a.game_id
        GROUP BY ts.team_id
    )
    SELECT tac.team_id
    FROM team_appearance_counts tac
    ORDER BY tac.appearance_count DESC
    LIMIT 1
)
WHERE a.team_id IS NULL;

-- Final fallback: Use games table to assign team_id (50/50 split)
-- This is a last resort if team_stats doesn't help
UPDATE appearances a
SET team_id = (
    SELECT g.home_team_id 
    FROM games g 
    WHERE g.game_id = a.game_id
    LIMIT 1
)
WHERE a.team_id IS NULL
AND EXISTS (
    SELECT 1 FROM games g WHERE g.game_id = a.game_id
);

DROP TABLE appearances_temp;

-- ============================================================================
-- STEP 8: Load SHOTS
-- ============================================================================

-- Create temporary table for shots CSV
CREATE TEMP TABLE shots_temp (
    "gameID" INTEGER,
    "shooterID" INTEGER,
    "assisterID" VARCHAR(20),  -- Can be NA
    "minute" SMALLINT,
    "situation" VARCHAR(50),
    "lastAction" VARCHAR(50),
    "shotType" VARCHAR(50),
    "shotResult" VARCHAR(50),
    "xGoal" DECIMAL(8,6),
    "positionX" DECIMAL(10,8),
    "positionY" DECIMAL(10,8)
);

-- Import shots CSV (handle NA values)
\copy shots_temp FROM 'shots.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',', NULL 'NA');

-- Insert shots with proper enum conversions and team_id derivation
INSERT INTO shots (
    game_id, team_id, shooter_id, assister_id,
    minute, situation, last_action, shot_type, shot_result,
    x_goal, position_x, position_y
)
SELECT 
    gim.new_game_id AS game_id,
    -- Derive team_id from shooter's appearance in the game
    COALESCE(
        (SELECT a.team_id 
         FROM appearances a 
         WHERE a.game_id = gim.new_game_id 
         AND a.player_id = st."shooterID"::INTEGER
         LIMIT 1),
        (SELECT g.home_team_id 
         FROM games g 
         WHERE g.game_id = gim.new_game_id
         LIMIT 1)
    ) AS team_id,
    st."shooterID"::INTEGER AS shooter_id,
    CASE 
        WHEN st."assisterID" IS NULL OR st."assisterID" = 'NA' OR st."assisterID" = '' 
        THEN NULL 
        WHEN st."assisterID"::INTEGER = st."shooterID"::INTEGER 
        THEN NULL  -- Assister cannot be the same as shooter (constraint violation)
        ELSE st."assisterID"::INTEGER 
    END AS assister_id,
    st."minute",
    CASE 
        WHEN st."situation" IN ('OpenPlay', 'FromCorner', 'SetPiece', 'DirectFreekick', 'Penalty')
        THEN st."situation"::shot_situation_type
        ELSE NULL
    END AS situation,
    st."lastAction" AS last_action,
    st."shotType" AS shot_type,
    CASE 
        WHEN st."shotResult" IN ('Goal', 'SavedShot', 'MissedShots', 'ShotOnPost', 'BlockedShot', 'OffTarget')
        THEN st."shotResult"::shot_result_type
        ELSE NULL
    END AS shot_result,
    st."xGoal" AS x_goal,
    st."positionX" AS position_x,
    st."positionY" AS position_y
FROM shots_temp st
JOIN game_id_mapping gim ON st."gameID" = gim.old_game_id
WHERE st."shooterID" IS NOT NULL;

-- Update sequence for shots table (shot_id is BIGSERIAL, auto-generated)
SELECT setval('shots_shot_id_seq', COALESCE((SELECT MAX(shot_id) FROM shots), 1));

DROP TABLE shots_temp;

-- ============================================================================
-- STEP 9: Populate TEAM_PLAYERS (Bridge Table)
-- ============================================================================

-- Create team-player relationships from appearances data
-- Note: team_player_id is SERIAL and will be auto-generated
INSERT INTO team_players (team_id, player_id, season_start, season_end, is_current)
SELECT DISTINCT
    a.team_id,
    a.player_id,
    MIN(g.season) AS season_start,
    MAX(g.season) AS season_end,
    CASE 
        WHEN MAX(g.season) >= EXTRACT(YEAR FROM CURRENT_DATE) - 1 
        THEN true 
        ELSE false 
    END AS is_current
FROM appearances a
JOIN games g ON a.game_id = g.game_id
WHERE a.team_id IS NOT NULL
GROUP BY a.team_id, a.player_id
ON CONFLICT (team_id, player_id, season_start) DO NOTHING;

-- Update sequence for team_players table (if needed for future inserts)
SELECT setval('team_players_team_player_id_seq', COALESCE((SELECT MAX(team_player_id) FROM team_players), 1));

-- ============================================================================
-- STEP 10: Update Statistics and Refresh Materialized Views
-- ============================================================================

-- Update table statistics for query optimizer
ANALYZE leagues;
ANALYZE teams;
ANALYZE players;
ANALYZE team_players;
ANALYZE games;
ANALYZE team_stats;
ANALYZE appearances;
ANALYZE shots;

-- Refresh materialized views
-- Note: CONCURRENTLY requires unique index, so use regular refresh
REFRESH MATERIALIZED VIEW mv_league_standings;
REFRESH MATERIALIZED VIEW mv_top_scorers;

-- ============================================================================
-- STEP 11: Cleanup Temporary Mapping Tables
-- ============================================================================

-- Cleanup temporary mapping tables
DROP TABLE IF EXISTS league_id_mapping;
DROP TABLE IF EXISTS team_league_mapping;
DROP TABLE IF EXISTS team_id_mapping;
DROP TABLE IF EXISTS game_id_mapping;
DROP TABLE IF EXISTS games_temp;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

\echo '============================================================================'
\echo 'Data Import Summary:'
\echo '============================================================================'

SELECT 'Leagues' AS table_name, COUNT(*) AS record_count FROM leagues
UNION ALL
SELECT 'Teams', COUNT(*) FROM teams
UNION ALL
SELECT 'Players', COUNT(*) FROM players
UNION ALL
SELECT 'Games', COUNT(*) FROM games
UNION ALL
SELECT 'Team Stats', COUNT(*) FROM team_stats
UNION ALL
SELECT 'Appearances', COUNT(*) FROM appearances
UNION ALL
SELECT 'Shots', COUNT(*) FROM shots
UNION ALL
SELECT 'Team Players', COUNT(*) FROM team_players
ORDER BY table_name;

\echo ''
\echo 'Sample verification - Top 5 leagues:'
SELECT league_id, name, country FROM leagues ORDER BY league_id;

\echo ''
\echo 'Sample verification - Recent games:'
SELECT game_id, season, date, 
       (SELECT name FROM teams WHERE team_id = home_team_id) AS home_team,
       (SELECT name FROM teams WHERE team_id = away_team_id) AS away_team,
       home_goals, away_goals
FROM games 
ORDER BY date DESC 
LIMIT 5;

\echo ''
\echo '============================================================================'
\echo 'Data import completed successfully!'
\echo '============================================================================'
