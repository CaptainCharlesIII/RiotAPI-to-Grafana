# riot_api_wrapper.py
from riotwatcher import LolWatcher, RiotWatcher, ApiError
import pandas as pd
import os
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get API key from .env file
api_key = os.getenv('RIOT_API_KEY')

if not api_key:
    raise ValueError("RIOT_API_KEY not found in .env file!")

watcher = LolWatcher(api_key)
riot_watcher = RiotWatcher(api_key)

def get_summoner(name, tag, region='na1'):
    """Get summoner info by Riot ID (name#tag)"""
    try:
        if region in ['na1', 'br1', 'la1', 'la2', 'oc1']:
            routing = 'americas'
        elif region in ['euw1', 'eun1', 'tr1', 'ru']:
            routing = 'europe'
        elif region in ['kr', 'jp1']:
            routing = 'asia'
        elif region in ['ph2', 'sg2', 'th2', 'tw2', 'vn2']:
            routing = 'sea'
        else:
            routing = 'americas'  # Default to americas
        
        # Get account info (returns puuid)
        account = riot_watcher.account.by_riot_id(routing, name, tag)
        
        # Get summoner info using puuid
        summoner = watcher.summoner.by_puuid(region, account['puuid'])
        
        # Add the puuid to summoner dict (we need it for ranked)
        summoner['puuid'] = account['puuid']
        
        return summoner
    except ApiError as err:
        if err.response.status_code == 429:
            print('Rate limit exceeded, waiting...')
            time.sleep(120)
            return get_summoner(name, tag, region)
        elif err.response.status_code == 404:
            print(f"Summoner {name}#{tag} not found.")
            return None
        else:
            raise

def get_match_history(puuid, region='americas', count=20):
    try:
        matches = watcher.match.matchlist_by_puuid(region, puuid, count=count)
        return matches
    except ApiError as err:
        if err.response.status_code == 429:
            print('Rate limit, sleeping 120s...')
            time.sleep(120)
            return get_match_history(puuid, region, count)
        else:
            raise

def get_match_details(match_id, region='americas'):
    try:
        return watcher.match.by_id(region, match_id)
    except ApiError as err:
        if err.response.status_code == 429:
            time.sleep(120)
            return get_match_details(match_id, region)
        elif err.response.status_code == 404:
            return None
        else:
            raise

def get_ranked_stats(puuid, region='na1'):
    """Get ranked stats using PUUID"""
    try:
        import requests
        
        # Go directly to the league endpoint - no account lookup needed
        url = f"https://{region}.api.riotgames.com/lol/league/v4/entries/by-puuid/{puuid}"
        headers = {"X-Riot-Token": api_key}
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            print("Rate limit hit, waiting...")
            time.sleep(120)
            return get_ranked_stats(puuid, region)
        else:
            print(f"League API status: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"Ranked stats error: {e}")
        return []