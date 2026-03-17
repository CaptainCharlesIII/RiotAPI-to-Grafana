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
    # Determine match region based on summoner region
    if region in ['na1', 'br1', 'la1', 'la2', 'oc1']:
        match_region = 'americas'
    elif region in ['euw1', 'eun1', 'tr1', 'ru']:
        match_region = 'europe'
    elif region in ['kr', 'jp1']:
        match_region = 'asia'
    elif region in ['ph2', 'sg2', 'th2', 'tw2', 'vn2']:
        match_region = 'sea'
    else:
        match_region = 'americas'

    match_ids = get_match_history(puuid, match_region, count=20)
    
    matches = []
    for match_id in match_ids:
        data = get_match_details(match_id, match_region)
        if data:
            info = data['info']
            
            # Find this player's data
            my_participant = None
            for participant in info['participants']:
                if participant['puuid'] == puuid:
                    my_participant = participant
                    break
            
            if my_participant:
                # Find enemy laner
                my_team_id = my_participant['teamId']
                my_lane = my_participant.get('individualPosition', '') or my_participant.get('teamPosition', '') or my_participant.get('lane', '')
                
                enemy_champion = 'Unknown'
                
                # Search for enemy in same position
                for participant in info['participants']:
                    if participant['teamId'] != my_team_id:
                        enemy_lane = participant.get('individualPosition', '') or participant.get('teamPosition', '') or participant.get('lane', '')
                        if enemy_lane == my_lane and my_lane != '':
                            enemy_champion = participant['championName']
                            break
                
                matches.append({
                    'match_id': match_id,
                    'puuid': puuid,
                    'game_date': pd.to_datetime(info['gameEndTimestamp'], unit='ms'),
                    'champion': my_participant['championName'],
                    'kills': my_participant['kills'],
                    'deaths': my_participant['deaths'],
                    'assists': my_participant['assists'],
                    'kda': round((my_participant['kills'] + my_participant['assists']) / max(my_participant['deaths'], 1), 2),
                    'win': my_participant['win'],
                    'game_mode': info['gameMode'],
                    'queue_id': info.get('queueId', 0),
                    'total_damage': my_participant['totalDamageDealtToChampions'],
                    'gold_earned': my_participant['goldEarned'],
                    'cs': my_participant['totalMinionsKilled'] + my_participant['neutralMinionsKilled'],
                    'game_duration': info['gameDuration'] // 60,
                    'game_duration_seconds': info['gameDuration'],
                    'lane': my_lane,
                    'role': my_participant.get('role', ''),
                    'enemy_champion': enemy_champion
                })
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
    try:
        # Initialize database
        init_database()
        
        print("="*50)
        print("🎮 RIOT API DATA COLLECTOR")
        print("="*50)
        
        # Get summoner info from user
        print("\nEnter your Riot ID (the name before the #):")
        summoner_name = input("➡️  Summoner Name: ").strip()
        
        print("\nEnter your tagline (the part after the #):")
        summoner_tag = input("➡️  Tagline: ").strip()
        
        print("\nSelect your region:")
        print("  1. NA (North America)")
        print("  2. EUW (Europe West)")
        print("  3. EUNE (Europe Nordic & East)")
        print("  4. KR (Korea)")
        print("  5. JP (Japan)")
        print("  6. BR (Brazil)")
        print("  7. LAN (Latin America North)")
        print("  8. LAS (Latin America South)")
        print("  9. OCE (Oceania)")
        print("  10. TR (Turkey)")
        print("  11. RU (Russia)")
        
        region_choice = input("➡️  Enter number (1-11): ").strip()
        
        # Map choice to region code
        region_map = {
            '1': 'na1',
            '2': 'euw1',
            '3': 'eun1',
            '4': 'kr',
            '5': 'jp1',
            '6': 'br1',
            '7': 'la1',
            '8': 'la2',
            '9': 'oc1',
            '10': 'tr1',
            '11': 'ru'
        }
        
        region = region_map.get(region_choice, 'na1')
        
        # Confirm with user
        print(f"\n📋 Looking up: {summoner_name}#{summoner_tag} ({region.upper()})")
        print("="*50)
        
        # Update data
        update_summoner_data(summoner_name, summoner_tag, region)
        
        print("\n🎉 Data successfully stored in PostgreSQL!")
        print("Refresh your Grafana dashboard to see the updated stats.")
        
    except KeyboardInterrupt:
        print("\n\n❌ Cancelled by user")
    except Exception as e:
        print("\n" + "="*50)
        print("❌ ERROR OCCURRED:")
        print("="*50)
        print(f"{type(e).__name__}: {e}")
        print("\nFull error details:")
        import traceback
        traceback.print_exc()
    finally:
        input("\n\nPress Enter to close...")