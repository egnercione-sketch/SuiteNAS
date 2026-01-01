import requests
import re
import logging
from typing import Dict, List, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PinnacleClient")

class PinnacleClient:
    def __init__(self, api_key: str = "13e1dd2e12msh72d0553fca0e8aap16eeacjsn9d69ddb0d2bb"):
        self.api_key = api_key
        self.host = "pinnacle-odds.p.rapidapi.com"
        self.base_url = "https://pinnacle-odds.p.rapidapi.com"
        self.headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": self.host
        }
        self.SPORT_ID = 3      # Basquete
        self.LEAGUE_ID = 487   # NBA Regular Season

    def get_nba_games(self) -> List[Dict]:
        """Busca contexto dos jogos (Spread/Total)"""
        url = f"{self.base_url}/kit/v1/markets"
        params = {"sport_id": self.SPORT_ID, "league_ids": self.LEAGUE_ID, "is_have_odds": "true"}

        try:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code != 200: return []
            data = response.json()
            
            clean_games = []
            for event in data.get('events', []):
                try:
                    periods = event.get('periods', {}).get('num_0', {})
                    spread_val = 0
                    if 'spreads' in periods:
                        k = list(periods['spreads'].keys())[0]
                        spread_val = periods['spreads'][k].get('hdp', 0)
                    total_val = 0
                    if 'totals' in periods:
                        k = list(periods['totals'].keys())[0]
                        total_val = periods['totals'][k].get('points', 0)
                    
                    implied_home, implied_away = 0, 0
                    if total_val > 0:
                        h_tot = total_val / 2
                        h_spr = abs(spread_val) / 2
                        if spread_val < 0: # Home Fav
                            implied_home = h_tot + h_spr
                            implied_away = h_tot - h_spr
                        else:
                            implied_home = h_tot - h_spr
                            implied_away = h_tot + h_spr

                    clean_games.append({
                        "game_id": event.get('event_id', event.get('id')),
                        "home_team": event.get('home'),
                        "away_team": event.get('away'),
                        "spread": spread_val,
                        "total": total_val,
                        "implied_home": round(implied_home, 1),
                        "implied_away": round(implied_away, 1),
                        "start_time": event.get('starts')
                    })
                except: continue
            return clean_games
        except: return []

    def get_player_props(self, game_id: int) -> List[Dict]:
        """Busca props com filtros relaxados e mapeamento corrigido"""
        url = f"{self.base_url}/kit/v1/special-markets"
        params = {"sport_id": self.SPORT_ID, "event_id": game_id}
        
        props_list = []
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            data = response.json()
            specials = data.get('specials', [])
            
            for sp in specials:
                cat = sp.get('category', '').lower()
                name = sp.get('name', '')
                
                if "player" not in cat: continue

                player_name = "Unknown"
                market_raw = "Unknown"
                
                m2 = re.search(r"^(.*?)\s+\((.*?)\)$", name)
                m1 = re.search(r"^(.*?)\s+-\s+(.*?)$", name)
                
                if m2:
                    player_name = m2.group(1).strip()
                    market_raw = m2.group(2).lower()
                elif m1:
                    market_raw = m1.group(1).lower()
                    player_name = m1.group(2).strip()
                else:
                    continue 

                market_type = None
                if "point" in market_raw and "3" not in market_raw: market_type = "PTS"
                elif "rebound" in market_raw: market_type = "REB"
                elif "assist" in market_raw: market_type = "AST"
                elif "3 point" in market_raw or "3pt" in market_raw: market_type = "3PM"
                elif "pts+rebs+asts" in market_raw or "pra" in market_raw: market_type = "PRA"
                elif "double" in market_raw: market_type = "DD2"
                elif "block" in market_raw: market_type = "BLK"
                elif "steal" in market_raw: market_type = "STL"
                
                if not market_type: continue

                lines = sp.get('lines', {})
                if lines:
                    try:
                        k = list(lines.keys())[0]
                        v = lines[k]
                        price = v.get('price', 0)
                        
                        raw_hdp = v.get('handicap')
                        if raw_hdp: 
                            line_val = abs(float(raw_hdp))
                        else:
                            if market_type == "DD2": line_val = 1.0
                            else:
                                lm = re.search(r"(\d+\.?\d*)", v.get('name', ''))
                                line_val = float(lm.group(1)) if lm else 0.0
                            
                        if line_val > 0:
                            props_list.append({
                                "player": player_name,
                                "market": market_type,
                                "line": line_val,
                                "odds": price,
                                "sportsbook": "Pinnacle"
                            })
                    except: continue

            return props_list

        except Exception as e:
            logger.error(f"Erro props: {e}")
            return []