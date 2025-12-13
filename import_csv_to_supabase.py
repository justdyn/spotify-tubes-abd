#!/usr/bin/env python3
"""
Supabase CSV Import Script
==========================
This script imports CSV files into Supabase temporary tables for processing.

Usage:
    python import_csv_to_supabase.py

Environment Variables Required:
    SUPABASE_URL: Your Supabase project URL
    SUPABASE_SERVICE_ROLE_KEY: Your Supabase service role key (for admin access)

Requirements:
    pip install supabase pandas python-dotenv
"""

import os
import sys
import math
import numpy as np
from pathlib import Path
from typing import Optional, Dict, List
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables from .env file (optional)
load_dotenv()

# Configuration
BATCH_SIZE = 1000  # Number of rows to insert per batch
CHUNK_SIZE = 5000  # Number of rows to read from CSV at a time

# CSV file mappings
CSV_FILES = {
    'leagues_temp': {
        'file': 'leagues.csv',
        'columns': ['leagueID', 'name', 'understatNotation']
    },
    'teams_temp': {
        'file': 'teams.csv',
        'columns': ['teamID', 'name']
    },
    'players_temp': {
        'file': 'players_utf8.csv',  # Prefer UTF-8 version
        'fallback': 'players.csv',
        'columns': ['playerID', 'name']
    },
    'games_temp': {
        'file': 'games.csv',
        'columns': [
            'gameID', 'leagueID', 'season', 'date',
            'homeTeamID', 'awayTeamID', 'homeGoals', 'awayGoals',
            'homeProbability', 'drawProbability', 'awayProbability',
            'homeGoalsHalfTime', 'awayGoalsHalfTime',
            'B365H', 'B365D', 'B365A', 'BWH', 'BWD', 'BWA',
            'IWH', 'IWD', 'IWA', 'PSH', 'PSD', 'PSA',
            'WHH', 'WHD', 'WHA', 'VCH', 'VCD', 'VCA',
            'PSCH', 'PSCD', 'PSCA'
        ]
    },
    'team_stats_temp': {
        'file': 'teamstats.csv',
        'columns': [
            'gameID', 'teamID', 'season', 'date', 'location',
            'goals', 'xGoals', 'shots', 'shotsOnTarget', 'deep',
            'ppda', 'fouls', 'corners', 'yellowCards', 'redCards', 'result'
        ]
    },
    'appearances_temp': {
        'file': 'appearances.csv',
        'columns': [
            'gameID', 'playerID', 'goals', 'ownGoals', 'shots',
            'xGoals', 'xGoalsChain', 'xGoalsBuildup', 'assists',
            'keyPasses', 'xAssists', 'position', 'positionOrder',
            'yellowCard', 'redCard', 'time', 'substituteIn',
            'substituteOut', 'leagueID'
        ]
    },
    'shots_temp': {
        'file': 'shots.csv',
        'columns': [
            'gameID', 'shooterID', 'assisterID', 'minute',
            'situation', 'lastAction', 'shotType', 'shotResult',
            'xGoal', 'positionX', 'positionY'
        ]
    }
}


def get_supabase_client() -> Client:
    """Initialize and return Supabase client."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    
    if not url or not key:
        print("ERROR: Missing required environment variables!")
        print("Please set:")
        print("  - SUPABASE_URL")
        print("  - SUPABASE_SERVICE_ROLE_KEY")
        print("\nYou can:")
        print("  1. Set them as environment variables")
        print("  2. Create a .env file with these variables")
        print("  3. Export them in your shell")
        sys.exit(1)
    
    try:
        return create_client(url, key)
    except Exception as e:
        print(f"ERROR: Failed to create Supabase client: {e}")
        sys.exit(1)


def create_temp_tables(supabase: Client) -> None:
    """Create temporary tables in Supabase for CSV import."""
    print("\n" + "="*70)
    print("Creating temporary import tables...")
    print("="*70)
    
    # SQL statements to create temp tables
    create_table_sql = """
    -- Drop existing temp tables if they exist
    DROP TABLE IF EXISTS leagues_temp CASCADE;
    DROP TABLE IF EXISTS teams_temp CASCADE;
    DROP TABLE IF EXISTS players_temp CASCADE;
    DROP TABLE IF EXISTS games_temp CASCADE;
    DROP TABLE IF EXISTS team_stats_temp CASCADE;
    DROP TABLE IF EXISTS appearances_temp CASCADE;
    DROP TABLE IF EXISTS shots_temp CASCADE;
    
    -- Create temp tables
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
        "yellowCards" VARCHAR(10),
        "redCards" VARCHAR(10),
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
        "assisterID" VARCHAR(20),
        minute SMALLINT,
        situation VARCHAR(50),
        "lastAction" VARCHAR(50),
        "shotType" VARCHAR(50),
        "shotResult" VARCHAR(50),
        "xGoal" DECIMAL(8,6),
        "positionX" DECIMAL(10,8),
        "positionY" DECIMAL(10,8)
    );
    """
    
    try:
        # Execute SQL using Supabase RPC (if available) or direct SQL
        # Note: Supabase Python client doesn't support raw SQL directly
        # We'll need to use the REST API or create tables via dashboard first
        print("⚠️  NOTE: Please create temp tables manually in Supabase SQL Editor")
        print("   Or use the SQL provided in SUPABASE_IMPORT_GUIDE.md")
        print("   The script will attempt to insert data into existing tables.")
    except Exception as e:
        print(f"⚠️  Warning: Could not create tables automatically: {e}")
        print("   Please create them manually in Supabase SQL Editor")


def find_csv_file(table_name: str, config: Dict) -> Optional[Path]:
    """Find CSV file, checking primary and fallback paths."""
    primary_file = Path(config['file'])
    if primary_file.exists():
        return primary_file
    
    if 'fallback' in config:
        fallback_file = Path(config['fallback'])
        if fallback_file.exists():
            print(f"  Using fallback file: {fallback_file}")
            return fallback_file
    
    return None


def clean_dataframe(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Clean and prepare DataFrame for insertion."""
    # Select only required columns
    available_columns = [col for col in columns if col in df.columns]
    df = df[available_columns].copy()
    
    # Replace 'NA' strings with None (NULL)
    df = df.replace('NA', None)
    df = df.replace('', None)
    
    # Convert data types where possible
    for col in df.columns:
        if df[col].dtype == 'object':
            # Try to convert to numeric if possible (without deprecated errors parameter)
            try:
                numeric_series = pd.to_numeric(df[col])
                # Only convert if successful (no errors)
                if not numeric_series.isna().all():
                    df[col] = numeric_series
            except (ValueError, TypeError):
                # Keep as string if conversion fails
                pass
    
    # CRITICAL: Replace all NaN/NaT values with None for JSON compatibility
    # This must happen AFTER all conversions
    df = df.replace({np.nan: None, pd.NA: None, pd.NaT: None})
    
    # Also handle float('nan') that might exist
    df = df.where(pd.notnull(df), None)
    
    return df


def import_csv_to_table(
    supabase: Client,
    table_name: str,
    csv_path: Path,
    columns: List[str],
    batch_size: int = BATCH_SIZE
) -> int:
    """
    Import CSV file to Supabase table in batches.
    
    Returns:
        Number of rows imported
    """
    print(f"\n📁 Importing {csv_path.name} → {table_name}...")
    
    if not csv_path.exists():
        print(f"  ❌ File not found: {csv_path}")
        return 0
    
    total_rows = 0
    
    try:
        # Read CSV in chunks
        chunk_iter = pd.read_csv(
            csv_path,
            chunksize=CHUNK_SIZE,
            encoding='utf-8',
            low_memory=False,
            na_values=['NA', '', 'NULL', 'null']
        )
        
        for chunk_num, chunk in enumerate(chunk_iter, 1):
            # Clean the chunk
            chunk = clean_dataframe(chunk, columns)
            
            # Convert to list of dictionaries and clean NaN values
            records = chunk.to_dict('records')
            
            # Final cleanup: ensure no NaN values in records (JSON serialization issue)
            cleaned_records = []
            for record in records:
                cleaned_record = {}
                for key, value in record.items():
                    # Convert NaN, inf, and other non-JSON-serializable values to None
                    if value is None:
                        cleaned_record[key] = None
                    elif isinstance(value, float):
                        if math.isnan(value) or math.isinf(value):
                            cleaned_record[key] = None
                        else:
                            cleaned_record[key] = value
                    else:
                        cleaned_record[key] = value
                cleaned_records.append(cleaned_record)
            
            # Insert in batches
            for i in range(0, len(cleaned_records), batch_size):
                batch = cleaned_records[i:i + batch_size]
                
                try:
                    response = supabase.table(table_name).insert(batch).execute()
                    total_rows += len(batch)
                    print(f"  ✓ Inserted batch {i//batch_size + 1} ({len(batch)} rows) - Total: {total_rows:,}")
                except Exception as e:
                    print(f"  ❌ Error inserting batch {i//batch_size + 1}: {e}")
                    # Try inserting row by row to identify problematic rows
                    for record in batch:
                        try:
                            supabase.table(table_name).insert(record).execute()
                            total_rows += 1
                        except Exception as row_error:
                            print(f"    ⚠️  Skipped problematic row: {row_error}")
                            continue
        
        print(f"  ✅ Completed: {total_rows:,} rows imported to {table_name}")
        return total_rows
        
    except Exception as e:
        print(f"  ❌ Error importing {csv_path.name}: {e}")
        import traceback
        traceback.print_exc()
        return total_rows


def main():
    """Main execution function."""
    print("="*70)
    print("Supabase CSV Import Script")
    print("="*70)
    
    # Initialize Supabase client
    supabase = get_supabase_client()
    print("✓ Connected to Supabase")
    
    # Create temp tables (or prompt user to create them)
    create_temp_tables(supabase)
    
    # Import each CSV file
    print("\n" + "="*70)
    print("Starting CSV imports...")
    print("="*70)
    
    results = {}
    
    for table_name, config in CSV_FILES.items():
        csv_file = find_csv_file(table_name, config)
        
        if csv_file:
            rows_imported = import_csv_to_table(
                supabase,
                table_name,
                csv_file,
                config['columns']
            )
            results[table_name] = rows_imported
        else:
            print(f"\n⚠️  Skipping {table_name}: CSV file not found")
            print(f"   Expected: {config['file']}")
            if 'fallback' in config:
                print(f"   Or: {config['fallback']}")
            results[table_name] = 0
    
    # Print summary
    print("\n" + "="*70)
    print("Import Summary")
    print("="*70)
    
    total_rows = 0
    for table_name, count in results.items():
        status = "✅" if count > 0 else "❌"
        print(f"{status} {table_name:25s}: {count:>10,} rows")
        total_rows += count
    
    print("-"*70)
    print(f"{'TOTAL':25s}: {total_rows:>10,} rows")
    print("="*70)
    
    if total_rows > 0:
        print("\n✅ Import completed successfully!")
        print("\n📝 Next steps:")
        print("   1. Verify data in Supabase Dashboard")
        print("   2. Run bola-dml-supabase.sql to transform and load data")
        print("   3. Check for any errors or warnings above")
    else:
        print("\n❌ No data was imported. Please check:")
        print("   - CSV files exist in current directory")
        print("   - File names match expected names")
        print("   - Supabase connection is working")
        print("   - Temp tables exist in database")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Import interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

