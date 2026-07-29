import requests
import json
from datetime import datetime

# California center point and radius
# Covers LAX, SFO, SAN, SJC, OAK — the whole state
API_URL = "https://api.airplanes.live/v2/point/36.7/-119.7/250"

# Backup if Airplanes.live is down
BACKUP_URL = "https://opendata.adsb.fi/api/v2/lat/36.7/lon/-119.7/dist/250"


def fetch_flights():
    """
    Fetch live California flight positions from Airplanes.live.
    Falls back to ADSB.fi if primary source is down.
    Returns a list of flight dictionaries.
    """
    for url in [API_URL, BACKUP_URL]:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Airplanes.live returns data in 'ac' key
            aircraft_list = data.get("ac", [])

            if not aircraft_list:
                print(f"No aircraft returned from {url}")
                continue

            flights = []
            for ac in aircraft_list:
                # Only include aircraft with valid position data
                lat = ac.get("lat")
                lon = ac.get("lon")
                if lat is None or lon is None:
                    continue

                flight = {
                    "icao24": ac.get("hex", "unknown").upper(),
                    "callsign": ac.get("flight", "").strip() or "UNKNOWN",
                    "latitude": float(lat),
                    "longitude": float(lon),
                    "altitude": ac.get("alt_baro", 0) or 0,
                    "velocity": ac.get("gs", 0) or 0,
                    "heading": ac.get("track", 0) or 0,
                    "on_ground": ac.get("alt_baro") == "ground",
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": "airplanes.live" if url == API_URL else "adsb.fi"
                }
                flights.append(flight)

            print(f"✅ Fetched {len(flights)} flights from {url}")
            return flights

        except requests.exceptions.RequestException as e:
            print(f"⚠️  Failed to fetch from {url}: {e}")
            continue

    # If both sources fail — return empty list
    print("❌ Both data sources failed")
    return []


if __name__ == "__main__":
    # Test the fetcher directly
    flights = fetch_flights()
    if flights:
        print(f"\nSample flight:")
        print(json.dumps(flights[0], indent=2))
        print(f"\nTotal: {len(flights)} flights over California")