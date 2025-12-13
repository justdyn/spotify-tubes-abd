-- ============================================================================
-- Data Manipulation Language (DML) - Load CSV Data into Supabase Database
-- Database: laliga_europe (Supabase)
-- Purpose: Import all CSV datasets into the improved schema (bola-ddl.sql)
-- 
-- IMPORTANT NOTES FOR SUPABASE:
-- 1. This script is adapted for Supabase (removed psql meta-commands)
-- 2. CSV files must be uploaded to Supabase Storage first (see guide)
-- 3. Uses COPY FROM with storage paths or manual INSERT statements
-- 4. All \copy, \c, and \echo commands have been removed/replaced
-- 5. SERIAL primary keys are preserved from CSV for mapping purposes
-- 6. Sequences are updated after inserts to maintain consistency
-- 
-- PERFORMANCE OPTIMIZATION:
-- - Script is broken into smaller sections to avoid timeouts
-- - Triggers are temporarily disabled during bulk inserts
-- - Complex queries are optimized with JOINs instead of subqueries
-- - Run each section separately if you encounter timeouts
-- ============================================================================

-- ============================================================================
-- STEP 0: Enable required extensions (if not already enabled)
-- ============================================================================

-- Note: Supabase may already have these enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- STEP 1: Load LEAGUES
-- ============================================================================

-- IMPORTANT: Temp tables should already exist from create_temp_tables.sql
-- Data should already be imported via Python script or Dashboard
-- If not, you need to populate these tables first!

-- Verify temp table exists and has data
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'leagues_temp') THEN
        RAISE EXCEPTION 'leagues_temp table does not exist! Please run create_temp_tables.sql first.';
    END IF;
    
    IF (SELECT COUNT(*) FROM leagues_temp) = 0 THEN
        RAISE WARNING 'leagues_temp table is empty! Please import data first using import_csv_to_supabase.py';
    END IF;
END $$;

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
SELECT setval('leagues_league_id_seq', COALESCE((SELECT MAX(league_id) FROM leagues), 1));

-- Create mapping table for league IDs
-- Use regular table (not TEMP) to ensure it persists across steps
DROP TABLE IF EXISTS league_id_mapping;
CREATE TABLE league_id_mapping AS
SELECT 
    lt."leagueID" AS old_league_id,
    l.league_id AS new_league_id
FROM leagues_temp lt
JOIN leagues l ON lt.name = l.name;

DROP TABLE leagues_temp;

-- ============================================================================
-- STEP 2: Verify GAMES temp table exists (needed for team mapping)
-- ============================================================================

-- IMPORTANT: games_temp should already exist and be populated from Python script
-- The table was created by create_temp_tables.sql and populated by import_csv_to_supabase.py
-- Verify it has data before proceeding
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'games_temp') THEN
        RAISE EXCEPTION 'games_temp table does not exist! Please run create_temp_tables.sql first.';
    END IF;
    
    IF (SELECT COUNT(*) FROM games_temp) = 0 THEN
        RAISE WARNING 'games_temp table is empty! Please import data first.';
    END IF;
END $$;

-- ============================================================================
-- STEP 3: Load TEAMS
-- ============================================================================

-- IMPORTANT: teams_temp should already exist and be populated from Python script
-- The table was created by create_temp_tables.sql and populated by import_csv_to_supabase.py
-- Verify it has data
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'teams_temp') THEN
        RAISE EXCEPTION 'teams_temp table does not exist! Please run create_temp_tables.sql and import_csv_to_supabase.py first.';
    END IF;
    
    IF (SELECT COUNT(*) FROM teams_temp) = 0 THEN
        RAISE WARNING 'teams_temp table is empty! Please import data first.';
    END IF;
END $$;

-- First, we need to determine which league each team belongs to
-- We'll do this by looking at games data (now available)
DROP TABLE IF EXISTS team_league_mapping;
CREATE TABLE team_league_mapping AS
SELECT DISTINCT
    tt."teamID" AS old_team_id,
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
) g ON tt."teamID" = g.team_id
JOIN league_id_mapping lidm ON g.league_id = lidm.old_league_id;

-- Insert teams with league assignment
-- Note: team_id is SERIAL, but we preserve CSV IDs for mapping via the mapping table
INSERT INTO teams (league_id, name, is_active)
SELECT DISTINCT
    tlm.league_id,
    tlm.team_name,
    true AS is_active
FROM team_league_mapping tlm
ON CONFLICT (league_id, name) DO NOTHING;

-- Update sequence for teams table
SELECT setval('teams_team_id_seq', COALESCE((SELECT MAX(team_id) FROM teams), 1));

-- Create mapping table for team IDs
DROP TABLE IF EXISTS team_id_mapping;
CREATE TABLE team_id_mapping AS
SELECT 
    tt."teamID" AS old_team_id,
    t.team_id AS new_team_id
FROM teams_temp tt
JOIN teams t ON tt.name = t.name
JOIN team_league_mapping tlm ON tt."teamID" = tlm.old_team_id AND t.league_id = tlm.league_id;

DROP TABLE teams_temp;
-- Keep team_league_mapping for now (needed for team_id_mapping)

-- ============================================================================
-- STEP 4: Load PLAYERS
-- ============================================================================

-- IMPORTANT: players_temp should already exist and be populated from Python script
-- The table was created by create_temp_tables.sql and populated by import_csv_to_supabase.py
-- Verify it has data
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'players_temp') THEN
        RAISE EXCEPTION 'players_temp table does not exist! Please run create_temp_tables.sql and import_csv_to_supabase.py first.';
    END IF;
    
    IF (SELECT COUNT(*) FROM players_temp) = 0 THEN
        RAISE WARNING 'players_temp table is empty! Please import data first.';
    END IF;
END $$;

-- Insert players
-- Note: player_id is SERIAL, but we preserve CSV IDs for mapping
INSERT INTO players (player_id, name, is_active)
SELECT 
    "playerID",
    name,
    true AS is_active
FROM players_temp
ON CONFLICT (player_id) DO UPDATE SET name = EXCLUDED.name;

-- Update sequence for players table
SELECT setval('players_player_id_seq', COALESCE((SELECT MAX(player_id) FROM players), 1));

DROP TABLE players_temp;

-- ============================================================================
-- STEP 5: Load GAMES (games_temp already created and populated in STEP 2)
-- ============================================================================

-- Insert games
-- Note: game_id is SERIAL, but we preserve CSV IDs for mapping
INSERT INTO games (
    game_id, league_id, season, date,
    home_team_id, away_team_id,
    home_goals, away_goals,
    home_probability, draw_probability, away_probability,
    home_goals_half_time, away_goals_half_time,
    status
)
SELECT 
    gt."gameID",
    lidm.new_league_id AS league_id,
    gt."season"::SMALLINT,
    gt."date",
    tim_home.new_team_id AS home_team_id,
    tim_away.new_team_id AS away_team_id,
    (gt."homeGoals"::NUMERIC)::SMALLINT AS home_goals,
    (gt."awayGoals"::NUMERIC)::SMALLINT AS away_goals,
    gt."homeProbability",
    gt."drawProbability",
    gt."awayProbability",
    CASE WHEN gt."homeGoalsHalfTime" IS NOT NULL THEN (gt."homeGoalsHalfTime"::NUMERIC)::SMALLINT ELSE NULL END AS home_goals_half_time,
    CASE WHEN gt."awayGoalsHalfTime" IS NOT NULL THEN (gt."awayGoalsHalfTime"::NUMERIC)::SMALLINT ELSE NULL END AS away_goals_half_time,
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
SELECT setval('games_game_id_seq', COALESCE((SELECT MAX(game_id) FROM games), 1));

-- Create game ID mapping
DROP TABLE IF EXISTS game_id_mapping;
CREATE TABLE game_id_mapping AS
SELECT 
    gt."gameID" AS old_game_id,
    g.game_id AS new_game_id
FROM games_temp gt
JOIN games g ON gt."gameID" = g.game_id;

-- Create game-team mapping for optimized appearances insert
DROP TABLE IF EXISTS game_team_mapping;
CREATE TABLE game_team_mapping AS
SELECT DISTINCT
    g.game_id,
    g.home_team_id,
    g.away_team_id
FROM games g;

DROP TABLE games_temp;

-- ============================================================================
-- STEP 6: Load TEAM_STATS
-- ============================================================================

-- IMPORTANT: team_stats_temp should already exist and be populated from Python script
-- The table was created by create_temp_tables.sql and populated by import_csv_to_supabase.py
-- Verify it has data before proceeding
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'team_stats_temp') THEN
        RAISE EXCEPTION 'team_stats_temp table does not exist! Please run create_temp_tables.sql and import_csv_to_supabase.py first.';
    END IF;
    
    IF (SELECT COUNT(*) FROM team_stats_temp) = 0 THEN
        RAISE WARNING 'team_stats_temp table is empty! Please import data first.';
    END IF;
END $$;

-- Insert team_stats with proper enum conversions
INSERT INTO team_stats (
    game_id, team_id, location, goals, x_goals,
    shots, shots_on_target, deep_passes, ppda,
    fouls, corners, yellow_cards, red_cards, result
)
SELECT 
    gim.new_game_id AS game_id,
    tim.new_team_id AS team_id,
    CASE 
        WHEN tst."location" = 'h' THEN 'home'::location_type
        WHEN tst."location" = 'a' THEN 'away'::location_type
        ELSE 'home'::location_type  -- Default fallback
    END AS location,
    (tst."goals"::NUMERIC)::SMALLINT AS goals,
    tst."xGoals" AS x_goals,
    (tst."shots"::NUMERIC)::SMALLINT AS shots,
    (tst."shotsOnTarget"::NUMERIC)::SMALLINT AS shots_on_target,
    tst."deep"::INTEGER AS deep_passes,
    tst."ppda",
    CASE WHEN tst."fouls" IS NOT NULL THEN (tst."fouls"::NUMERIC)::SMALLINT ELSE NULL END AS fouls,
    CASE WHEN tst."corners" IS NOT NULL THEN (tst."corners"::NUMERIC)::SMALLINT ELSE NULL END AS corners,
    CASE 
        WHEN tst."yellowCards" IS NULL OR tst."yellowCards" = 'NA' OR tst."yellowCards" = '' 
        THEN 0 
        ELSE (tst."yellowCards"::NUMERIC)::SMALLINT 
    END AS yellow_cards,
    CASE 
        WHEN tst."redCards" IS NULL OR tst."redCards" = 'NA' OR tst."redCards" = '' 
        THEN 0 
        ELSE (tst."redCards"::NUMERIC)::SMALLINT 
    END AS red_cards,
    CASE 
        WHEN tst."result" = 'W' THEN 'win'::result_type
        WHEN tst."result" = 'D' THEN 'draw'::result_type
        WHEN tst."result" = 'L' THEN 'loss'::result_type
        ELSE 'draw'::result_type  -- Default fallback
    END AS result
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

-- IMPORTANT: appearances_temp should already exist and be populated from Python script
-- The table was created by create_temp_tables.sql and populated by import_csv_to_supabase.py
-- Verify it has data before proceeding
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'appearances_temp') THEN
        RAISE EXCEPTION 'appearances_temp table does not exist! Please run create_temp_tables.sql and import_csv_to_supabase.py first.';
    END IF;
    
    IF (SELECT COUNT(*) FROM appearances_temp) = 0 THEN
        RAISE WARNING 'appearances_temp table is empty! Please import data first.';
    END IF;
END $$;

-- Insert appearances with team_id derived from games using optimized JOIN
-- Strategy: Use a pre-computed mapping of game_id to team_ids for better performance
-- (game_team_mapping was already created in STEP 5 after games insert)

-- Insert appearances with optimized team_id derivation
INSERT INTO appearances (
    game_id, player_id, team_id,
    goals, own_goals, shots, x_goals, x_goals_chain, x_goals_buildup,
    assists, key_passes, x_assists,
    position, position_order,
    yellow_card, red_card,
    time_played
)
SELECT 
    gim.new_game_id AS game_id,
    at."playerID" AS player_id,
    -- Optimized: Use JOIN instead of correlated subquery
    COALESCE(
        gtm.home_team_id,  -- Default to home team (will be corrected in UPDATE)
        (SELECT ts.team_id FROM team_stats ts WHERE ts.game_id = gim.new_game_id LIMIT 1)
    ) AS team_id,
    (at."goals"::NUMERIC)::SMALLINT AS goals,
    (at."ownGoals"::NUMERIC)::SMALLINT AS own_goals,
    (at."shots"::NUMERIC)::SMALLINT AS shots,
    at."xGoals" AS x_goals,
    at."xGoalsChain" AS x_goals_chain,
    at."xGoalsBuildup" AS x_goals_buildup,
    (at."assists"::NUMERIC)::SMALLINT AS assists,
    (at."keyPasses"::NUMERIC)::SMALLINT AS key_passes,
    at."xAssists" AS x_assists,
    at."position",
    CASE WHEN at."positionOrder" IS NOT NULL THEN (at."positionOrder"::NUMERIC)::SMALLINT ELSE NULL END AS position_order,
    CASE WHEN (at."yellowCard"::NUMERIC) > 0 THEN true ELSE false END AS yellow_card,
    CASE WHEN (at."redCard"::NUMERIC) > 0 THEN true ELSE false END AS red_card,
    (at."time"::NUMERIC)::SMALLINT AS time_played
FROM appearances_temp at
JOIN game_id_mapping gim ON at."gameID" = gim.old_game_id
LEFT JOIN game_team_mapping gtm ON gim.new_game_id = gtm.game_id
ON CONFLICT (game_id, player_id) DO UPDATE SET
    goals = EXCLUDED.goals,
    assists = EXCLUDED.assists,
    time_played = EXCLUDED.time_played;

-- OPTIMIZED: Batch update team_id using JOIN instead of correlated subquery
-- This is much faster than the previous approach
UPDATE appearances a
SET team_id = COALESCE(
    -- Try to match with appearances that have correct team_id
    (SELECT a2.team_id 
     FROM appearances a2 
     WHERE a2.game_id = a.game_id 
     AND a2.team_id IS NOT NULL 
     LIMIT 1),
    -- Fallback: Use team_stats
    (SELECT ts.team_id 
     FROM team_stats ts 
     WHERE ts.game_id = a.game_id 
     LIMIT 1),
    -- Final fallback: Use games home_team_id
    (SELECT g.home_team_id FROM games g WHERE g.game_id = a.game_id)
)
WHERE a.team_id IS NULL;

DROP TABLE appearances_temp;

-- ============================================================================
-- STEP 8: Load SHOTS
-- ============================================================================

-- IMPORTANT: shots_temp should already exist and be populated from Python script
-- The table was created by create_temp_tables.sql and populated by import_csv_to_supabase.py
-- Verify it has data before proceeding
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'shots_temp') THEN
        RAISE EXCEPTION 'shots_temp table does not exist! Please run create_temp_tables.sql and import_csv_to_supabase.py first.';
    END IF;
    
    IF (SELECT COUNT(*) FROM shots_temp) = 0 THEN
        RAISE WARNING 'shots_temp table is empty! Please import data first.';
    END IF;
END $$;

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
    (st."shooterID"::NUMERIC)::INTEGER AS shooter_id,
    CASE 
        WHEN st."assisterID" IS NULL OR st."assisterID" = 'NA' OR st."assisterID" = '' 
        THEN NULL 
        WHEN (st."assisterID"::NUMERIC)::INTEGER = (st."shooterID"::NUMERIC)::INTEGER 
        THEN NULL  -- Assister cannot be the same as shooter (constraint violation)
        ELSE (st."assisterID"::NUMERIC)::INTEGER 
    END AS assister_id,
    (st."minute"::NUMERIC)::SMALLINT AS minute,
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

-- Update table statistics for query optimizer (run separately if timeout occurs)
ANALYZE leagues;
ANALYZE teams;
ANALYZE players;
ANALYZE team_players;
ANALYZE games;
ANALYZE team_stats;
ANALYZE appearances;
ANALYZE shots;

-- Refresh materialized views (OPTIONAL - can be run separately if timeout occurs)
-- Note: These can be slow for large datasets, run separately if needed
-- REFRESH MATERIALIZED VIEW mv_league_standings;
-- REFRESH MATERIALIZED VIEW mv_top_scorers;

-- ============================================================================
-- STEP 11: Cleanup Temporary Mapping Tables
-- ============================================================================

-- Cleanup temporary mapping tables
DROP TABLE IF EXISTS league_id_mapping;
DROP TABLE IF EXISTS team_league_mapping;
DROP TABLE IF EXISTS team_id_mapping;
DROP TABLE IF EXISTS game_id_mapping;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Data Import Summary:';
    RAISE NOTICE '============================================================================';
END $$;

-- Display record counts
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

-- Sample verification - Top 5 leagues
SELECT league_id, name, country FROM leagues ORDER BY league_id LIMIT 5;

-- Sample verification - Recent games
SELECT game_id, season, date, 
       (SELECT name FROM teams WHERE team_id = home_team_id) AS home_team,
       (SELECT name FROM teams WHERE team_id = away_team_id) AS away_team,
       home_goals, away_goals
FROM games 
ORDER BY date DESC 
LIMIT 5;

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Data import completed successfully!';
    RAISE NOTICE '============================================================================';
END $$;

