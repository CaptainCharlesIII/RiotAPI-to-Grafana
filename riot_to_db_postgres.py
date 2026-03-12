# riot_to_db_postgres.py
import psycopg2
from psycopg2 import sql
import pandas as pd
from riot_api_wrapper import *
import time
from datetime import datetime
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

if not POSTGRES_PASSWORD:
    raise ValueError("POSTGRES_PASSWORD not found in .env file!")

DB_CONFIG = {
    'host': 'localhost',
    'database': 'riot_stats',
    'user': 'postgres',
    'password': POSTGRES_PASSWORD
}

# SQLAlchemy engine for pandas
engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")

def init_database():
    """Create tables"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Summoner info table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS summoners (
            puuid TEXT PRIMARY KEY,
            summoner_name TEXT,
            summoner_tag TEXT,
            summoner_level INTEGER,
            last_updated TIMESTAMP
        )
    ''')
    
    # Match data table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT,
            puuid TEXT,
            game_date TIMESTAMP,
            champion TEXT,
            kills INTEGER,
            deaths INTEGER,
            assists INTEGER,
            kda REAL,
            win BOOLEAN,
            game_mode TEXT,
            total_damage INTEGER,
            gold_earned INTEGER,
            cs INTEGER,
            game_duration INTEGER,
            PRIMARY KEY (match_id, puuid)
        )
    ''')
    
    # Ranked stats table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ranked_stats (
            puuid TEXT,
            queue_type TEXT,
            tier TEXT,
            rank TEXT,
            lp INTEGER,
            wins INTEGER,
            losses INTEGER,
            timestamp TIMESTAMP,
            PRIMARY KEY (puuid, queue_type, timestamp)
        )
    ''')
    
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Tables initialized")

def update_summoner_data(name, tag, region='na1', clear_old_data=True):
    """Fetch and store summoner data"""
    
    # Clear all old data first
    if clear_old_data:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM matches')
        cursor.execute('DELETE FROM ranked_stats')
        cursor.execute('DELETE FROM summoners')
        conn.commit()
        cursor.close()
        conn.close()
        print("🗑️  Old data cleared")
    
    # Get summoner info
    summoner = get_summoner(name, tag, region)
    if not summoner:
        print(f"❌ Could not find {name}#{tag}")
        return
    
    puuid = summoner['puuid']
    
        # Store/update summoner info
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO summoners (puuid, summoner_name, summoner_tag, summoner_level, last_updated)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (puuid) 
        DO UPDATE SET 
            summoner_level = EXCLUDED.summoner_level,
            last_updated = EXCLUDED.last_updated
    ''', (puuid, name, tag, summoner['summonerLevel'], datetime.now()))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    # Get match history
    print(f"📊 Fetching matches for {name}#{tag}...")
    match_region = 'americas' if region in ['na1', 'br1', 'la1', 'la2'] else 'europe'
    match_ids = get_match_history(puuid, match_region, count=20)
    
    matches = []
    for match_id in match_ids:
        data = get_match_details(match_id, match_region)
        if data:
            info = data['info']
            for participant in info['participants']:
                if participant['puuid'] == puuid:
                    matches.append({
                        'match_id': match_id,
                        'puuid': puuid,
                        'game_date': pd.to_datetime(info['gameEndTimestamp'], unit='ms'),
                        'champion': participant['championName'],
                        'kills': participant['kills'],
                        'deaths': participant['deaths'],
                        'assists': participant['assists'],
                        'kda': round((participant['kills'] + participant['assists']) / max(participant['deaths'], 1), 2),
                        'win': participant['win'],
                        'game_mode': info['gameMode'],
                        'total_damage': participant['totalDamageDealtToChampions'],
                        'gold_earned': participant['goldEarned'],
                        'cs': participant['totalMinionsKilled'] + participant['neutralMinionsKilled'],
                        'game_duration': info['gameDuration'] // 60
                    })
                    break
        time.sleep(0.5)
    
    # Store matches
    if matches:
        df_matches = pd.DataFrame(matches)
        try:
            df_matches.to_sql('matches', engine, if_exists='append', index=False, method='multi')
            print(f"✅ Stored {len(matches)} matches")
        except Exception as e:
            print(f"⚠️  Some matches already in database (this is normal)")
    
        # Get ranked stats
    try:
        ranked = get_ranked_stats(puuid, region)
        ranked_data = []
        
        if ranked:
            for queue in ranked:
                ranked_data.append({
                    'puuid': puuid,
                    'queue_type': queue['queueType'],
                    'tier': queue['tier'],
                    'rank': queue['rank'],
                    'lp': queue['leaguePoints'],
                    'wins': queue['wins'],
                    'losses': queue['losses'],
                    'timestamp': datetime.now()
                })
            
            pd.DataFrame(ranked_data).to_sql('ranked_stats', engine, if_exists='append', index=False, method='multi')
            print(f"✅ Stored ranked stats: {ranked_data[0]['tier']} {ranked_data[0]['rank']} {ranked_data[0]['lp']} LP")
        else:
            print("⚠️  No ranked data (unranked this season?)")
            
    except Exception as e:
        print(f"⚠️  Ranked stats error: {e}")

if __name__ == "__main__":
    # Initialize database
    init_database()
    
    # Update data for your summoner (CHANGE THESE!)
    update_summoner_data("JustSpoon", "MID", "na1")  # THIS RESETS ALL EXISTING DATA
    # update_summoner_data("SecondPerson", "TAG2", "na1", clear_old_data=False)  ADDS DATA INSTEAD OF RESETTING
    
    print("\n🎉 Data successfully stored in PostgreSQL!")