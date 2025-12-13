-- ============================================================================
-- Create Temporary Import Tables for Supabase
-- ============================================================================
-- Run this script FIRST in Supabase SQL Editor before running the Python import
-- ============================================================================

-- Drop existing temp tables if they exist (cleanup)
DROP TABLE IF EXISTS leagues_temp CASCADE;
DROP TABLE IF EXISTS teams_temp CASCADE;
DROP TABLE IF EXISTS players_temp CASCADE;
DROP TABLE IF EXISTS games_temp CASCADE;
DROP TABLE IF EXISTS team_stats_temp CASCADE;
DROP TABLE IF EXISTS appearances_temp CASCADE;
DROP TABLE IF EXISTS shots_temp CASCADE;

-- ============================================================================
-- Create Temp Tables (matching CSV structure)
-- ============================================================================

CREATE TABLE leagues_temp (
    "leagueID" INTEGER,
    name VARCHAR(100),
    "understatNotation" VARCHAR(50)
);

CREATE TABLE teams_temp (
    "teamID" INTEGER,
    name VARCHAR(100)
);

CREATE TABLE players_temp (
    "playerID" INTEGER,
    name VARCHAR(150)
);

CREATE TABLE games_temp (
    "gameID" INTEGER,
    "leagueID" INTEGER,
    season SMALLINT,
    date TIMESTAMP,
    "homeTeamID" INTEGER,
    "awayTeamID" INTEGER,
    "homeGoals" SMALLINT,
    "awayGoals" SMALLINT,
    "homeProbability" DECIMAL(5,4),
    "drawProbability" DECIMAL(5,4),
    "awayProbability" DECIMAL(5,4),
    "homeGoalsHalfTime" SMALLINT,
    "awayGoalsHalfTime" SMALLINT,
    -- Betting odds columns (not used in final schema, but in CSV)
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

CREATE TABLE team_stats_temp (
    "gameID" INTEGER,
    "teamID" INTEGER,
    season SMALLINT,
    date TIMESTAMP,
    location VARCHAR(1),
    goals SMALLINT,
    "xGoals" DECIMAL(8,6),
    shots SMALLINT,
    "shotsOnTarget" SMALLINT,
    deep INTEGER,
    ppda DECIMAL(8,4),
    fouls SMALLINT,
    corners SMALLINT,
    "yellowCards" VARCHAR(10),  -- VARCHAR to handle NA values
    "redCards" VARCHAR(10),      -- VARCHAR to handle NA values
    result VARCHAR(1)
);

CREATE TABLE appearances_temp (
    "gameID" INTEGER,
    "playerID" INTEGER,
    goals SMALLINT,
    "ownGoals" SMALLINT,
    shots SMALLINT,
    "xGoals" DECIMAL(8,6),
    "xGoalsChain" DECIMAL(8,6),
    "xGoalsBuildup" DECIMAL(8,6),
    assists SMALLINT,
    "keyPasses" SMALLINT,
    "xAssists" DECIMAL(8,6),
    position VARCHAR(10),
    "positionOrder" SMALLINT,
    "yellowCard" SMALLINT,
    "redCard" SMALLINT,
    time SMALLINT,
    "substituteIn" VARCHAR(20),
    "substituteOut" VARCHAR(20),
    "leagueID" INTEGER
);

CREATE TABLE shots_temp (
    "gameID" INTEGER,
    "shooterID" INTEGER,
    "assisterID" VARCHAR(20),  -- Can be NA
    minute SMALLINT,
    situation VARCHAR(50),
    "lastAction" VARCHAR(50),
    "shotType" VARCHAR(50),
    "shotResult" VARCHAR(50),
    "xGoal" DECIMAL(8,6),
    "positionX" DECIMAL(10,8),
    "positionY" DECIMAL(10,8)
);

-- ============================================================================
-- Verification
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '============================================================================';
    RAISE NOTICE 'Temporary import tables created successfully!';
    RAISE NOTICE '============================================================================';
    RAISE NOTICE '';
    RAISE NOTICE 'Next steps:';
    RAISE NOTICE '1. Run: python import_csv_to_supabase.py';
    RAISE NOTICE '2. Then run: bola-dml-supabase.sql';
    RAISE NOTICE '============================================================================';
END $$;

-- Verify tables were created
SELECT 
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns 
     WHERE table_name = t.table_name) AS column_count
FROM information_schema.tables t
WHERE table_schema = 'public' 
  AND table_name LIKE '%_temp'
ORDER BY table_name;

