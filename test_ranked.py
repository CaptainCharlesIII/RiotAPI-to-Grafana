from riot_api_wrapper import *
import json

# Your info
name = "JustSpoon"
tag = "MID"
region = "na1"

# Get summoner
summoner = get_summoner(name, tag, region)

print("Full summoner object:")
print(json.dumps(summoner, indent=2))

print("\n" + "="*50)
print("Available keys:")
for key in summoner.keys():
    print(f"  - {key}")