import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get PostgreSQL password from .env file
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

if not POSTGRES_PASSWORD:
    raise ValueError("POSTGRES_PASSWORD not found in .env file!")

try:
    # ==========================================
    # STEP 1: Create the database
    # ==========================================
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password=POSTGRES_PASSWORD
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    cursor.execute("SELECT 1 FROM pg_database WHERE datname='riot_stats'")
    if not cursor.fetchone():
        cursor.execute('CREATE DATABASE riot_stats')
        print("Database 'riot_stats' created successfully!")
    else:
        print("Database 'riot_stats' already exists")
    
    cursor.close()
    conn.close()
    
    # ==========================================
    # STEP 2: Create all tables
    # ==========================================
    conn = psycopg2.connect(
        host="localhost",
        database="riot_stats",
        user="postgres",
        password=POSTGRES_PASSWORD
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Summoners table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS summoners (
            puuid TEXT PRIMARY KEY,
            summoner_name TEXT,
            summoner_tag TEXT,
            summoner_region TEXT,
            summoner_level INTEGER,
            last_updated TIMESTAMP
        )
    ''')
    print("Table 'summoners' ready")
    
    # Matches table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT,
            puuid TEXT,
            summoner_name TEXT,
            game_date TIMESTAMP,
            champion TEXT,
            kills INTEGER,
            deaths INTEGER,
            assists INTEGER,
            kda REAL,
            win BOOLEAN,
            game_mode TEXT,
            queue_id INTEGER,
            total_damage INTEGER,
            gold_earned INTEGER,
            cs INTEGER,
            game_duration INTEGER,
            game_duration_seconds INTEGER,
            lane TEXT,
            role TEXT,
            enemy_champion TEXT,
            PRIMARY KEY (match_id, puuid)
        )
    ''')
    print("Table 'matches' ready")
    
    # Ranked stats table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ranked_stats (
            puuid TEXT,
            summoner_name TEXT,
            queue_type TEXT,
            tier TEXT,
            rank TEXT,
            lp INTEGER,
            wins INTEGER,
            losses INTEGER,
            timestamp TIMESTAMP
        )
    ''')
    print("Table 'ranked_stats' ready")
    
    # ==========================================
    # STEP 3: Create indexes for faster queries
    # ==========================================
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_matches_puuid 
        ON matches(puuid)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_matches_summoner_name 
        ON matches(summoner_name)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_matches_game_date 
        ON matches(game_date)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_matches_game_mode 
        ON matches(game_mode)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_ranked_puuid 
        ON ranked_stats(puuid)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_ranked_summoner_name 
        ON ranked_stats(summoner_name)
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_ranked_queue_type 
        ON ranked_stats(queue_type)
    ''')
    print("Indexes created for faster queries")
    
    cursor.close()
    conn.close()
    
    print("\nPostgreSQL is fully set up and ready!")
    print("Next step: Run riot_to_db_postgres.py")
    
except psycopg2.OperationalError as e:
    print("Connection failed. Check:")
    print("   1. PostgreSQL is running")
    print("   2. Password is correct")
    print(f"   Error: {e}")
except Exception as e:
    print(f"Error: {e}")