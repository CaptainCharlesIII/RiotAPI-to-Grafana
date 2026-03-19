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

# PostgreSQL connection
DB_CONFIG = {
    'host': 'localhost',
    'database': 'riot_stats',
    'user': 'postgres',
    'password': POSTGRES_PASSWORD
}

engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")


def init_database():
    """Create tables if they don't exist"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
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
    
    # Add summoner_name column if it doesn't exist
    try:
        cursor.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS summoner_name TEXT")
        cursor.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS lane TEXT")
        cursor.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS role TEXT")
        cursor.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS enemy_champion TEXT")
        cursor.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS queue_id INTEGER")
        cursor.execute("ALTER TABLE matches ADD COLUMN IF NOT EXISTS game_duration_seconds INTEGER")
        cursor.execute("ALTER TABLE ranked_stats ADD COLUMN IF NOT EXISTS summoner_name TEXT")
        cursor.execute("ALTER TABLE summoners ADD COLUMN IF NOT EXISTS summoner_region TEXT")
    except:
        pass
    
    conn.commit()
    cursor.close()
    conn.close()


def get_all_tracked_players():
    """Get list of all tracked summoners"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute('SELECT summoner_name, summoner_tag, summoner_region, summoner_level, last_updated FROM summoners ORDER BY summoner_name')
    players = cursor.fetchall()
    cursor.close()
    conn.close()
    return players


def remove_player(name, tag):
    """Remove a player and all their data"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute('SELECT puuid FROM summoners WHERE summoner_name = %s AND summoner_tag = %s', (name, tag))
    result = cursor.fetchone()
    
    if result:
        puuid = result[0]
        cursor.execute('DELETE FROM matches WHERE puuid = %s', (puuid,))
        cursor.execute('DELETE FROM ranked_stats WHERE puuid = %s', (puuid,))
        cursor.execute('DELETE FROM summoners WHERE puuid = %s', (puuid,))
        conn.commit()
        print(f"Deleted all data for {name}#{tag}")
    else:
        print(f"Player {name}#{tag} not found in database")
    
    cursor.close()
    conn.close()


def update_summoner_data(name, tag, region='na1'):
    """Fetch and store summoner data (without clearing old data)"""
    
    # Determine match region
    if region in ['na1', 'br1', 'la1', 'la2']:
        match_region = 'americas'
    elif region in ['euw1', 'eun1', 'tr1', 'ru']:
        match_region = 'europe'
    elif region in ['kr', 'jp1']:
        match_region = 'asia'
    elif region in ['oc1', 'ph2', 'sg2', 'th2', 'tw2', 'vn2']:
        match_region = 'sea'
    else:
        match_region = 'americas'
    
    # Get summoner info
    summoner = get_summoner(name, tag, region)
    if not summoner:
        print(f"Could not find {name}#{tag}")
        return
    
    puuid = summoner['puuid']
    display_name = f"{name}#{tag}"
    
    # Update summoner info (upsert)
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO summoners (puuid, summoner_name, summoner_tag, summoner_region, summoner_level, last_updated)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (puuid) 
        DO UPDATE SET 
            summoner_level = EXCLUDED.summoner_level,
            summoner_region = EXCLUDED.summoner_region,
            last_updated = EXCLUDED.last_updated
    ''', (puuid, name, tag, region, summoner['summonerLevel'], datetime.now()))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    # Get existing match IDs for this player (to avoid re-fetching)
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute('SELECT match_id FROM matches WHERE puuid = %s', (puuid,))
    existing_matches = set(row[0] for row in cursor.fetchall())
    cursor.close()
    conn.close()
    
    # Get match history
    print(f"Fetching matches for {name}#{tag}...")
    match_ids = get_match_history(puuid, match_region, count=100)
    
    # Filter out matches we already have
    new_match_ids = [m for m in match_ids if m not in existing_matches]
    
    if not new_match_ids:
        print(f"No new matches to add for {name}#{tag}")
    else:
        print(f"Found {len(new_match_ids)} new matches (skipping {len(match_ids) - len(new_match_ids)} existing)")
        
        matches = []
        for i, match_id in enumerate(new_match_ids, 1):
            print(f"  Processing match {i}/{len(new_match_ids)}...", end='\r')
            
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
                    
                    for participant in info['participants']:
                        if participant['teamId'] != my_team_id:
                            enemy_lane = participant.get('individualPosition', '') or participant.get('teamPosition', '') or participant.get('lane', '')
                            if enemy_lane == my_lane and my_lane != '':
                                enemy_champion = participant['championName']
                                break
                    
                    matches.append({
                        'match_id': match_id,
                        'puuid': puuid,
                        'summoner_name': display_name,
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
        
        # Store new matches
        if matches:
            df_matches = pd.DataFrame(matches)
            try:
                df_matches.to_sql('matches', engine, if_exists='append', index=False, method='multi')
                print(f"\nStored {len(matches)} new matches")
            except Exception as e:
                print(f"\nSome matches already exist (this is normal)")
    
    # Get ranked stats
    try:
        ranked = get_ranked_stats(puuid, region)
        
        if ranked:
            # Delete old ranked stats for this player (keep only latest)
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute('DELETE FROM ranked_stats WHERE puuid = %s', (puuid,))
            conn.commit()
            cursor.close()
            conn.close()
            
            ranked_data = []
            for queue in ranked:
                ranked_data.append({
                    'puuid': puuid,
                    'summoner_name': display_name,
                    'queue_type': queue['queueType'],
                    'tier': queue['tier'],
                    'rank': queue['rank'],
                    'lp': queue['leaguePoints'],
                    'wins': queue['wins'],
                    'losses': queue['losses'],
                    'timestamp': datetime.now()
                })
            
            pd.DataFrame(ranked_data).to_sql('ranked_stats', engine, if_exists='append', index=False, method='multi')
            print(f"Stored ranked stats: {ranked_data[0]['tier']} {ranked_data[0]['rank']} {ranked_data[0]['lp']} LP")
        else:
            print("No ranked data (unranked this season?)")
            
    except Exception as e:
        print(f"Ranked stats error: {e}")
    
    print(f"Done updating {name}#{tag}!")


def get_region_choice():
    """Display region menu and return choice"""
    print("\nSelect region:")
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
    
    region_choice = input("Enter number (1-11): ").strip()
    
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
    
    return region_map.get(region_choice, 'na1')


if __name__ == "__main__":
    try:
        init_database()
        
        while True:
            print("\n" + "="*50)
            print("  RIOT API DATA COLLECTOR")
            print("="*50)
            print("\n  1. Add new player")
            print("  2. Refresh a specific player")
            print("  3. Refresh ALL players")
            print("  4. Remove a player")
            print("  5. List all tracked players")
            print("  6. Exit")
            
            choice = input("\nEnter choice (1-6): ").strip()
            
            if choice == '1':
                # Add new player
                print("\n--- ADD NEW PLAYER ---")
                name = input("Summoner Name: ").strip()
                tag = input("Tagline: ").strip()
                region = get_region_choice()
                
                print(f"\nAdding: {name}#{tag} ({region.upper()})")
                print("-"*40)
                update_summoner_data(name, tag, region)
            
            elif choice == '2':
                # Refresh specific player
                players = get_all_tracked_players()
                
                if not players:
                    print("\nNo players tracked yet! Add one first.")
                    continue
                
                print("\n--- REFRESH PLAYER ---")
                print("\nTracked players:")
                for i, (pname, ptag, pregion, plevel, pupdated) in enumerate(players, 1):
                    print(f"  {i}. {pname}#{ptag} ({pregion}) - Level {plevel}")
                
                player_choice = input(f"\nSelect player (1-{len(players)}): ").strip()
                
                try:
                    idx = int(player_choice) - 1
                    if 0 <= idx < len(players):
                        pname, ptag, pregion, plevel, pupdated = players[idx]
                        print(f"\nRefreshing: {pname}#{ptag}")
                        print("-"*40)
                        update_summoner_data(pname, ptag, pregion)
                    else:
                        print("Invalid selection")
                except ValueError:
                    print("Invalid input")
            
            elif choice == '3':
                # Refresh all players
                players = get_all_tracked_players()
                
                if not players:
                    print("\nNo players tracked yet! Add one first.")
                    continue
                
                print(f"\n--- REFRESHING ALL {len(players)} PLAYERS ---")
                
                for pname, ptag, pregion, plevel, pupdated in players:
                    print(f"\n{'='*40}")
                    print(f"Refreshing: {pname}#{ptag}")
                    print("-"*40)
                    update_summoner_data(pname, ptag, pregion)
                
                print(f"\nAll {len(players)} players updated!")
            
            elif choice == '4':
                # Remove player
                players = get_all_tracked_players()
                
                if not players:
                    print("\nNo players tracked yet!")
                    continue
                
                print("\n--- REMOVE PLAYER ---")
                print("\nTracked players:")
                for i, (pname, ptag, pregion, plevel, pupdated) in enumerate(players, 1):
                    print(f"  {i}. {pname}#{ptag} ({pregion})")
                
                player_choice = input(f"\nSelect player to remove (1-{len(players)}): ").strip()
                
                try:
                    idx = int(player_choice) - 1
                    if 0 <= idx < len(players):
                        pname, ptag, pregion, plevel, pupdated = players[idx]
                        confirm = input(f"\nAre you sure you want to remove {pname}#{ptag}? (y/n): ").strip().lower()
                        if confirm == 'y':
                            remove_player(pname, ptag)
                        else:
                            print("Cancelled")
                    else:
                        print("Invalid selection")
                except ValueError:
                    print("Invalid input")
            
            elif choice == '5':
                # List players
                players = get_all_tracked_players()
                
                if not players:
                    print("\nNo players tracked yet! Add one first.")
                    continue
                
                print("\n--- TRACKED PLAYERS ---")
                print(f"{'Name':<25} {'Region':<10} {'Level':<10} {'Last Updated'}")
                print("-"*70)
                for pname, ptag, pregion, plevel, pupdated in players:
                    updated_str = pupdated.strftime('%Y-%m-%d %H:%M') if pupdated else 'Never'
                    print(f"{pname}#{ptag:<20} {pregion:<10} {plevel:<10} {updated_str}")
            
            elif choice == '6':
                print("\nGoodbye!")
                break
            
            else:
                print("\nInvalid choice. Enter 1-6.")
    
    except KeyboardInterrupt:
        print("\n\nCancelled by user")
    except Exception as e:
        print("\n" + "="*50)
        print("ERROR OCCURRED:")
        print("="*50)
        print(f"{type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        input("\n\nPress Enter to close...")