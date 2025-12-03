-- ============================================================================
-- MIGRATION GUIDE: From Original Schema to Improved Schema
-- ============================================================================
-- This script helps migrate existing data from the old schema to the new one
-- Run this AFTER deploying the new schema to a fresh database
-- ============================================================================

-- IMPORTANT: This is a REFERENCE guide, not a complete automated script
-- You'll need to adapt it based on your actual data and requirements

-- ============================================================================
-- STEP 1: BACKUP YOUR EXISTING DATABASE
-- ============================================================================

-- Run this in your terminal BEFORE starting migration:
-- pg_dump -U postgres -F c -b -v -f laliga_europe_backup_before_migration.dump laliga_europe

-- ============================================================================
-- STEP 2: EXPORT DATA FROM OLD SCHEMA
-- ============================================================================

-- If you have existing data, export it to CSV files:

-- Export leagues
\copy (SELECT * FROM leagues) TO 'leagues_old.csv' CSV HEADER;

-- Export teams
\copy (SELECT * FROM teams) TO 'teams_old.csv' CSV HEADER;

-- Export players
\copy (SELECT * FROM players) TO 'players_old.csv' CSV HEADER;

-- Export games
\copy (SELECT * FROM games) TO 'games_old.csv' CSV HEADER;

-- Export team_stats
\copy (SELECT * FROM team_stats) TO 'team_stats_old.csv' CSV HEADER;

-- Export appearances
\copy (SELECT * FROM appearances) TO 'appearances_old.csv' CSV HEADER;

-- Export shots
\copy (SELECT * FROM shots) TO 'shots_old.csv' CSV HEADER;

-- ============================================================================
-- STEP 3: DEPLOY NEW SCHEMA
-- ============================================================================

-- Run the improved schema script:
-- psql -U postgres -f ddl_improved.sql

-- ============================================================================
-- STEP 4: DATA TRANSFORMATION AND MIGRATION
-- ============================================================================

-- ============================================================================
-- 4.1: Migrate LEAGUES
-- ============================================================================

-- If you have existing leagues data, you'll need to add country information
-- Option A: Import with country data
/*
INSERT INTO leagues (league_id, name, country, is_active)
VALUES 
    (1, 'Premier League', 'England', true),
    (2, 'La Liga', 'Spain', true),
    (3, 'Bundesliga', 'Germany', true),
    (4, 'Serie A', 'Italy', true),
    (5, 'Ligue 1', 'France', true);
*/

-- Option B: Import from old data and update country manually
/*
-- First, create a temporary table to hold old data
CREATE TEMP TABLE leagues_old AS SELECT * FROM old_database.leagues;

-- Insert with placeholder country
INSERT INTO leagues (league_id, name, country, is_active)
SELECT league_id, name, 'Unknown', true
FROM leagues_old;

-- Update with actual countries
UPDATE leagues SET country = 'England' WHERE name = 'Premier League';
UPDATE leagues SET country = 'Spain' WHERE name = 'La Liga';
UPDATE leagues SET country = 'Germany' WHERE name = 'Bundesliga';
UPDATE leagues SET country = 'Italy' WHERE name = 'Serie A';
UPDATE leagues SET country = 'France' WHERE name = 'Ligue 1';
*/

-- ============================================================================
-- 4.2: Migrate TEAMS
-- ============================================================================

-- You need to assign league_id to each team
-- Option A: If you have a mapping file
/*
\copy teams_with_league FROM 'teams_with_league.csv' CSV HEADER;
*/

-- Option B: Manual mapping based on team names
/*
-- Create temp table
CREATE TEMP TABLE teams_old AS SELECT * FROM old_database.teams;

-- Insert teams with league assignment
-- Example for Premier League teams:
INSERT INTO teams (team_id, league_id, name, short_name, is_active)
SELECT 
    team_id, 
    1 as league_id,  -- Premier League
    name,
    CASE 
        WHEN name = 'Manchester United' THEN 'Man Utd'
        WHEN name = 'Manchester City' THEN 'Man City'
        WHEN name = 'Newcastle United' THEN 'Newcastle'
        -- Add more mappings as needed
        ELSE NULL
    END as short_name,
    true as is_active
FROM teams_old
WHERE name IN ('Manchester United', 'Manchester City', 'Arsenal', ...);

-- Repeat for other leagues (La Liga = 2, Bundesliga = 3, Serie A = 4, Ligue 1 = 5)
*/

-- ============================================================================
-- 4.3: Migrate PLAYERS
-- ============================================================================

-- If you don't have date_of_birth and nationality data, import without them
/*
CREATE TEMP TABLE players_old AS SELECT * FROM old_database.players;

INSERT INTO players (player_id, name, date_of_birth, nationality, is_active)
SELECT 
    player_id,
    name,
    NULL as date_of_birth,  -- Can be populated later
    NULL as nationality,     -- Can be populated later
    true as is_active
FROM players_old;
*/

-- If you have additional player data from external sources:
/*
-- Update with birth dates
UPDATE players p
SET date_of_birth = pd.birth_date
FROM player_data_external pd
WHERE p.name = pd.player_name;

-- Update with nationalities
UPDATE players p
SET nationality = pd.nationality
FROM player_data_external pd
WHERE p.name = pd.player_name;
*/

-- ============================================================================
-- 4.4: Populate TEAM_PLAYERS (NEW TABLE)
-- ============================================================================

-- This is a NEW table, so we need to derive data from existing records
-- Strategy: Use appearances table to determine which teams players played for

/*
-- Create team-player relationships from appearances
INSERT INTO team_players (team_id, player_id, season_start, season_end, is_current)
SELECT DISTINCT
    a.team_id,  -- You'll need to derive this from games table
    a.player_id,
    MIN(g.season) as season_start,
    MAX(g.season) as season_end,
    CASE WHEN MAX(g.season) = EXTRACT(YEAR FROM CURRENT_DATE) THEN true ELSE false END as is_current
FROM old_database.appearances a
JOIN old_database.games g ON a.game_id = g.game_id
GROUP BY a.player_id, a.team_id;
*/

-- More accurate approach: Derive team_id from games table
/*
WITH player_teams AS (
    SELECT DISTINCT
        a.player_id,
        CASE 
            WHEN EXISTS (
                SELECT 1 FROM games g 
                WHERE g.game_id = a.game_id 
                AND g.home_team_id IN (
                    -- Find which team the player appeared for
                    SELECT DISTINCT home_team_id FROM games WHERE game_id = a.game_id
                )
            ) THEN (SELECT home_team_id FROM games WHERE game_id = a.game_id LIMIT 1)
            ELSE (SELECT away_team_id FROM games WHERE game_id = a.game_id LIMIT 1)
        END as team_id,
        g.season
    FROM old_database.appearances a
    JOIN old_database.games g ON a.game_id = g.game_id
)
INSERT INTO team_players (team_id, player_id, season_start, season_end, is_current)
SELECT 
    team_id,
    player_id,
    MIN(season) as season_start,
    MAX(season) as season_end,
    CASE WHEN MAX(season) >= EXTRACT(YEAR FROM CURRENT_DATE) - 1 THEN true ELSE false END as is_current
FROM player_teams
GROUP BY team_id, player_id;
*/

-- ============================================================================
-- 4.5: Migrate GAMES
-- ============================================================================

-- Transform and migrate games data
/*
CREATE TEMP TABLE games_old AS SELECT * FROM old_database.games;

INSERT INTO games (
    game_id, league_id, season, game_week, date,
    home_team_id, away_team_id, home_goals, away_goals,
    home_probability, draw_probability, away_probability,
    home_goals_half_time, away_goals_half_time,
    stadium, attendance, status
)
SELECT 
    game_id,
    league_id,
    season,
    NULL as game_week,  -- Can be calculated or imported separately
    date,
    home_team_id,
    away_team_id,
    home_goals,
    away_goals,
    home_probability,
    draw_probability,
    away_probability,
    home_goals_half_time,
    away_goals_half_time,
    NULL as stadium,    -- Can be populated later
    NULL as attendance, -- Can be populated later
    'completed' as status  -- Assuming all old games are completed
FROM games_old;
*/

-- Calculate game_week if you have sequential games per season
/*
WITH ranked_games AS (
    SELECT 
        game_id,
        ROW_NUMBER() OVER (PARTITION BY league_id, season ORDER BY date) as game_week
    FROM games
)
UPDATE games g
SET game_week = rg.game_week
FROM ranked_games rg
WHERE g.game_id = rg.game_id;
*/

-- ============================================================================
-- 4.6: Migrate TEAM_STATS
-- ============================================================================

-- Transform location and result from CHAR(1) to ENUM
/*
CREATE TEMP TABLE team_stats_old AS SELECT * FROM old_database.team_stats;

INSERT INTO team_stats (
    game_id, team_id, location, goals, x_goals,
    shots, shots_on_target, deep_passes, ppda,
    fouls, corners, yellow_cards, red_cards, result,
    possession_percentage
)
SELECT 
    game_id,
    team_id,
    CASE location 
        WHEN 'h' THEN 'home'::location_type
        WHEN 'a' THEN 'away'::location_type
    END as location,
    goals,
    x_goals,
    shots,
    shots_on_target,
    deep_passes,
    ppda,
    fouls,
    corners,
    yellow_cards,
    red_cards,
    CASE result
        WHEN 'W' THEN 'win'::result_type
        WHEN 'D' THEN 'draw'::result_type
        WHEN 'L' THEN 'loss'::result_type
    END as result,
    NULL as possession_percentage  -- Can be calculated or imported separately
FROM team_stats_old;
*/

-- Calculate possession if you have the data
/*
-- If you have possession data from another source
UPDATE team_stats ts
SET possession_percentage = ps.possession
FROM possession_source ps
WHERE ts.game_id = ps.game_id AND ts.team_id = ps.team_id;
*/

-- ============================================================================
-- 4.7: Migrate APPEARANCES
-- ============================================================================

-- Need to add team_id and convert card fields to BOOLEAN
/*
CREATE TEMP TABLE appearances_old AS SELECT * FROM old_database.appearances;

-- First, we need to determine which team each player played for in each game
-- This requires joining with games table
INSERT INTO appearances (
    game_id, player_id, team_id,
    goals, own_goals, shots, x_goals, x_goals_chain, x_goals_buildup,
    assists, key_passes, x_assists,
    position, position_order,
    yellow_card, red_card,
    time_played, substitute_in, substitute_out
)
SELECT 
    a.game_id,
    a.player_id,
    -- Derive team_id: check if player is in home or away team
    COALESCE(
        (SELECT team_id FROM team_players tp 
         WHERE tp.player_id = a.player_id 
         AND EXISTS (SELECT 1 FROM games g WHERE g.game_id = a.game_id AND g.home_team_id = tp.team_id)
         LIMIT 1),
        (SELECT team_id FROM team_players tp 
         WHERE tp.player_id = a.player_id 
         AND EXISTS (SELECT 1 FROM games g WHERE g.game_id = a.game_id AND g.away_team_id = tp.team_id)
         LIMIT 1)
    ) as team_id,
    a.goals,
    a.own_goals,
    a.shots,
    a.x_goals,
    a.x_goals_chain,
    a.x_goals_buildup,
    a.assists,
    a.key_passes,
    a.x_assists,
    a.position,
    a.position_order,
    CASE WHEN a.yellow_card > 0 THEN true ELSE false END as yellow_card,
    CASE WHEN a.red_card > 0 THEN true ELSE false END as red_card,
    a.time_played,
    -- Convert substitute times from VARCHAR to SMALLINT
    CASE 
        WHEN a.substitute_in IS NOT NULL AND a.substitute_in ~ '^\d+$' 
        THEN a.substitute_in::SMALLINT 
        ELSE NULL 
    END as substitute_in,
    CASE 
        WHEN a.substitute_out IS NOT NULL AND a.substitute_out ~ '^\d+$' 
        THEN a.substitute_out::SMALLINT 
        ELSE NULL 
    END as substitute_out
FROM appearances_old a;
*/

-- Alternative: If you can't determine team_id from team_players
-- Use a more direct approach with games table
/*
INSERT INTO appearances (
    game_id, player_id, team_id,
    goals, own_goals, shots, x_goals, x_goals_chain, x_goals_buildup,
    assists, key_passes, x_assists,
    position, position_order,
    yellow_card, red_card,
    time_played, substitute_in, substitute_out
)
SELECT 
    a.game_id,
    a.player_id,
    -- Simple heuristic: if player scored, they're likely on the winning team
    -- This is NOT perfect and should be verified manually
    CASE 
        WHEN a.goals > 0 THEN 
            CASE 
                WHEN g.home_goals > g.away_goals THEN g.home_team_id
                ELSE g.away_team_id
            END
        ELSE g.home_team_id  -- Default to home team (needs manual verification)
    END as team_id,
    a.goals,
    a.own_goals,
    a.shots,
    a.x_goals,
    a.x_goals_chain,
    a.x_goals_buildup,
    a.assists,
    a.key_passes,
    a.x_assists,
    a.position,
    a.position_order,
    CASE WHEN a.yellow_card > 0 THEN true ELSE false END as yellow_card,
    CASE WHEN a.red_card > 0 THEN true ELSE false END as red_card,
    a.time_played,
    NULL as substitute_in,   -- Parse from VARCHAR if available
    NULL as substitute_out   -- Parse from VARCHAR if available
FROM appearances_old a
JOIN games g ON a.game_id = g.game_id;
*/

-- ============================================================================
-- 4.8: Migrate SHOTS
-- ============================================================================

-- Add team_id and convert to ENUM types
/*
CREATE TEMP TABLE shots_old AS SELECT * FROM old_database.shots;

INSERT INTO shots (
    shot_id, game_id, team_id, shooter_id, assister_id,
    minute, situation, last_action, shot_type, shot_result,
    x_goal, position_x, position_y
)
SELECT 
    s.shot_id,
    s.game_id,
    -- Derive team_id from shooter's team in that game
    COALESCE(
        (SELECT team_id FROM appearances a WHERE a.game_id = s.game_id AND a.player_id = s.shooter_id LIMIT 1),
        (SELECT home_team_id FROM games WHERE game_id = s.game_id)  -- Fallback
    ) as team_id,
    s.shooter_id,
    s.assister_id,
    s.minute,
    -- Convert situation to ENUM (only if it matches valid values)
    CASE 
        WHEN s.situation IN ('OpenPlay', 'FromCorner', 'SetPiece', 'DirectFreekick', 'Penalty') 
        THEN s.situation::shot_situation_type
        ELSE NULL
    END as situation,
    s.last_action,
    s.shot_type,
    -- Convert shot_result to ENUM (only if it matches valid values)
    CASE 
        WHEN s.shot_result IN ('Goal', 'SavedShot', 'MissedShots', 'ShotOnPost', 'BlockedShot', 'OffTarget')
        THEN s.shot_result::shot_result_type
        ELSE NULL
    END as shot_result,
    s.x_goal,
    s.position_x,
    s.position_y
FROM shots_old s;
*/

-- ============================================================================
-- STEP 5: DATA VALIDATION
-- ============================================================================

-- Run validation checks to ensure data integrity

-- Check for any games with inconsistent results
SELECT * FROM validate_game_results();
-- Should return 0 rows if everything is correct

-- Check that all team_stats have matching games
SELECT ts.game_id, ts.team_id
FROM team_stats ts
LEFT JOIN games g ON ts.game_id = g.game_id
WHERE g.game_id IS NULL;
-- Should return 0 rows

-- Check that all appearances have valid team_id
SELECT a.game_id, a.player_id, a.team_id
FROM appearances a
LEFT JOIN games g ON a.game_id = g.game_id
WHERE a.team_id NOT IN (g.home_team_id, g.away_team_id);
-- Should return 0 rows

-- Check that all shots have valid team_id
SELECT s.game_id, s.shooter_id, s.team_id
FROM shots s
LEFT JOIN games g ON s.game_id = g.game_id
WHERE s.team_id NOT IN (g.home_team_id, g.away_team_id);
-- Should return 0 rows

-- Check for NULL team_ids in appearances (these need manual fixing)
SELECT COUNT(*) FROM appearances WHERE team_id IS NULL;

-- Check for NULL team_ids in shots (these need manual fixing)
SELECT COUNT(*) FROM shots WHERE team_id IS NULL;

-- Verify all probabilities sum to ~1.0
SELECT game_id, home_probability, draw_probability, away_probability,
       (home_probability + draw_probability + away_probability) as total
FROM games
WHERE home_probability IS NOT NULL 
  AND draw_probability IS NOT NULL 
  AND away_probability IS NOT NULL
  AND ABS((home_probability + draw_probability + away_probability) - 1.0) >= 0.01;
-- Should return 0 rows

-- ============================================================================
-- STEP 6: REFRESH MATERIALIZED VIEWS
-- ============================================================================

-- After all data is migrated, refresh materialized views
SELECT refresh_all_materialized_views();

-- Verify materialized views have data
SELECT COUNT(*) FROM mv_league_standings;
SELECT COUNT(*) FROM mv_top_scorers;

-- ============================================================================
-- STEP 7: UPDATE SEQUENCES
-- ============================================================================

-- Update sequences to continue from max IDs
-- This is important if you're using SERIAL columns

SELECT setval('leagues_league_id_seq', (SELECT MAX(league_id) FROM leagues));
SELECT setval('teams_team_id_seq', (SELECT MAX(team_id) FROM teams));
SELECT setval('players_player_id_seq', (SELECT MAX(player_id) FROM players));
SELECT setval('team_players_team_player_id_seq', (SELECT MAX(team_player_id) FROM team_players));
SELECT setval('games_game_id_seq', (SELECT MAX(game_id) FROM games));
SELECT setval('shots_shot_id_seq', (SELECT MAX(shot_id) FROM shots));

-- ============================================================================
-- STEP 8: ANALYZE TABLES
-- ============================================================================

-- Update table statistics for optimal query performance
ANALYZE leagues;
ANALYZE teams;
ANALYZE players;
ANALYZE team_players;
ANALYZE games;
ANALYZE team_stats;
ANALYZE appearances;
ANALYZE shots;

-- ============================================================================
-- STEP 9: VERIFY APPLICATION COMPATIBILITY
-- ============================================================================

-- Test queries that your application uses
-- Example tests:

-- Test 1: Get league standings
SELECT * FROM mv_league_standings 
WHERE season = 2024 
ORDER BY league_id, points DESC 
LIMIT 10;

-- Test 2: Get player stats
SELECT * FROM v_player_stats_summary 
ORDER BY total_goals DESC 
LIMIT 10;

-- Test 3: Get recent games
SELECT * FROM v_games_full 
ORDER BY date DESC 
LIMIT 10;

-- Test 4: Get team form
SELECT * FROM get_team_form(1, 5);

-- ============================================================================
-- STEP 10: APPLICATION CODE UPDATES NEEDED
-- ============================================================================

/*
UPDATE YOUR APPLICATION CODE:

1. Change ENUM values:
   - location: 'h' → 'home', 'a' → 'away'
   - result: 'W' → 'win', 'D' → 'draw', 'L' → 'loss'

2. Add team_id to INSERT statements:
   - appearances: Must include team_id
   - shots: Must include team_id

3. Change card fields from integer to boolean:
   - yellow_card: 0/1 → false/true
   - red_card: 0/1 → false/true

4. Update substitute time handling:
   - substitute_in: VARCHAR → SMALLINT (minutes)
   - substitute_out: VARCHAR → SMALLINT (minutes)

5. Add new optional fields:
   - leagues: country
   - teams: league_id (required), short_name
   - players: date_of_birth, nationality
   - games: game_week, stadium, attendance, status
   - team_stats: possession_percentage

EXAMPLE CODE CHANGES:

Python (psycopg2):
OLD:
cursor.execute(
    "INSERT INTO team_stats (game_id, team_id, location, result) VALUES (%s, %s, %s, %s)",
    (game_id, team_id, 'h', 'W')
)

NEW:
cursor.execute(
    "INSERT INTO team_stats (game_id, team_id, location, result) VALUES (%s, %s, %s, %s)",
    (game_id, team_id, 'home', 'win')
)

JavaScript (node-postgres):
OLD:
await client.query(
    'INSERT INTO appearances (game_id, player_id, yellow_card) VALUES ($1, $2, $3)',
    [gameId, playerId, 1]
);

NEW:
await client.query(
    'INSERT INTO appearances (game_id, player_id, team_id, yellow_card) VALUES ($1, $2, $3, $4)',
    [gameId, playerId, teamId, true]
);
*/

-- ============================================================================
-- TROUBLESHOOTING COMMON ISSUES
-- ============================================================================

/*
ISSUE 1: "Team X is not playing in game Y"
CAUSE: Trying to insert team_stats/appearances/shots with wrong team_id
SOLUTION: Verify team_id matches home_team_id or away_team_id in games table

ISSUE 2: "Invalid input value for enum"
CAUSE: Using old values ('h', 'W', etc.)
SOLUTION: Use new enum values ('home', 'win', etc.)

ISSUE 3: "NULL value in column team_id violates not-null constraint"
CAUSE: Missing team_id in appearances or shots
SOLUTION: Ensure team_id is included in all INSERT statements

ISSUE 4: "Duplicate key value violates unique constraint"
CAUSE: Trying to insert duplicate records
SOLUTION: Check if record already exists, use UPDATE or ON CONFLICT

ISSUE 5: Triggers preventing data insertion
CAUSE: Data doesn't pass validation (e.g., team not in game)
SOLUTION: Review trigger error message and fix data before inserting
*/

-- ============================================================================
-- ROLLBACK PLAN
-- ============================================================================

/*
If migration fails and you need to rollback:

1. Restore from backup:
   pg_restore -U postgres -d laliga_europe -v laliga_europe_backup_before_migration.dump

2. Or recreate old schema:
   psql -U postgres -f ddl.sql

3. Restore data from CSV exports:
   \copy leagues FROM 'leagues_old.csv' CSV HEADER;
   \copy teams FROM 'teams_old.csv' CSV HEADER;
   ... etc
*/

-- ============================================================================
-- POST-MIGRATION CHECKLIST
-- ============================================================================

/*
□ All data migrated successfully
□ Validation queries return 0 errors
□ Materialized views refreshed
□ Sequences updated
□ Tables analyzed
□ Application code updated
□ Test queries run successfully
□ Backup of new database created
□ Team trained on new schema
□ Documentation updated
□ Monitoring set up for new triggers
*/

-- ============================================================================
-- END OF MIGRATION GUIDE
-- ============================================================================

\echo '============================================================================'
\echo 'Migration guide complete!'
\echo ''
\echo 'IMPORTANT REMINDERS:'
\echo '1. Always backup before migration'
\echo '2. Test in development environment first'
\echo '3. Update application code for enum values'
\echo '4. Add team_id to appearances and shots'
\echo '5. Run validation queries after migration'
\echo '6. Refresh materialized views'
\echo '7. Update sequences'
\echo '8. Analyze tables'
\echo ''
\echo 'For questions, refer to:'
\echo '  - IMPROVEMENTS_EXPLAINED.md (detailed explanations)'
\echo '  - QUICK_REFERENCE.md (common queries)'
\echo '  - BEFORE_AFTER_COMPARISON.md (visual comparison)'
\echo '============================================================================'

