# injuries.py — OFF RADAR v50.0 (WEB API + ITEMS FIX)
# Módulo definitivo de lesões.
# FIXED: Troca para 'site.web.api.espn.com' (Mais estável).
# FIXED: Busca recursiva agora aceita 'items' além de 'athletes'.

import os
import json
import time
from datetime import datetime, timedelta
import requests

# Ajuste conforme seu projeto
BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# Default path
DEFAULT_CACHE_FILE = os.path.join(CACHE_DIR, "injuries_cache_v44.json")
CACHE_TTL_HOURS = 3

# Headers simulando um navegador real para evitar bloqueio
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.espn.com/",
    "Origin": "https://www.espn.com"
}

# --- MAPA DE TRADUÇÃO (NBA STANDARD -> ESPN URL CODE) ---
NBA_TO_ESPN_MAP = {
    "UTA": "utah", "UTAH": "utah",
    "NOP": "no", "NO": "no",
    "NYK": "ny", "NY": "ny",
    "GSW": "gs", "GS": "gs",
    "SAS": "sa", "SA": "sa",
    "PHX": "pho", "PHO": "pho",
    "WAS": "wsh", "WSH": "wsh",
    "BKN": "bkn", "BRK": "bkn"
}

# --- MAPA DE RETORNO (ESPN -> NBA STANDARD) ---
ESPN_TO_NBA_STANDARD = {
    "utah": "UTA", "UTAH": "UTA",
    "gs": "GSW", "GS": "GSW",
    "no": "NOP", "NO": "NOP",
    "ny": "NYK", "NY": "NYK",
    "sa": "SAS", "SA": "SAS",
    "pho": "PHX", "PHO": "PHX",
    "wsh": "WAS", "WSH": "WAS"
}

def normalize_name(n: str) -> str:
    import re, unicodedata
    if not n: return ""
    n = str(n).lower()
    n = n.replace(".", " ").replace(",", " ").replace("-", " ")
    n = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", n)
    n = unicodedata.normalize("NFKD", n).encode("ascii", "ignore").decode("ascii")
    n = " ".join(n.split())
    return n

def save_json(path, obj):
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except: pass

def load_json(path):
    if not os.path.exists(path): return {}
    try:
        with open(path, 'r', encoding='utf-8') as f: return json.load(f)
    except: return {}

class InjuryMonitor:
    def __init__(self, cache_file=None):
        self.cache_path = cache_file if cache_file else DEFAULT_CACHE_FILE
        self.cache = self._load_cache()

    def _load_cache(self):
        data = load_json(self.cache_path)
        return data if data else {"updated_at": None, "teams": {}}

    def fetch_injuries_for_team(self, team_abbr):
        """
        Busca lesões de UM time específico usando a API WEB da ESPN.
        """
        espn_code = NBA_TO_ESPN_MAP.get(team_abbr.upper(), team_abbr.lower())
        
        # URL da API WEB (Mais robusta que a API Mobile)
        url = f"https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{espn_code}/roster"
        
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            
            if r.status_code == 200:
                data = r.json()
                team_injuries = []
                
                # --- BUSCA INTELIGENTE DA LISTA DE JOGADORES ---
                # Procura por chaves 'athletes' OU 'items' recursivamente
                athletes = self._extract_list_recursive(data)

                if not athletes:
                    print(f"⚠️ {team_abbr}: Nenhum atleta encontrado na estrutura JSON.")
                    return False

                for ath in athletes:
                    # Pega dados básicos
                    full_name = ath.get("fullName") or ath.get("displayName")
                    if not full_name: continue # Pula lixo
                    
                    injuries_list = ath.get("injuries", [])
                    status_obj = ath.get("status", {})
                    
                    # Status genérico (ex: "Active", "Day-to-Day", "Out")
                    status_txt = status_obj.get("type", {}).get("name", "")
                    if not status_txt: 
                        status_txt = status_obj.get("name", "")
                    
                    # Detalhes específicos da lesão
                    details = ""
                    date_str = datetime.now().strftime("%Y-%m-%d")
                    
                    if injuries_list:
                        latest = injuries_list[0]
                        if not status_txt: status_txt = latest.get("status", "")
                        details = latest.get("details") or latest.get("shortComment") or latest.get("longComment") or ""
                        date_str = latest.get("date") or date_str

                    # LÓGICA DE FILTRO: Quem está machucado?
                    # 1. Se tem algo na lista 'injuries', provavelmente é relevante.
                    # 2. Se o status NÃO for 'Active' (ex: Out, Day-to-Day, Questionable).
                    # 3. CUIDADO: Às vezes 'Active' tem 'injuries' (ex: Day-to-Day jogando).
                    
                    is_hurt = False
                    st_lower = str(status_txt).lower()
                    
                    if len(injuries_list) > 0:
                        is_hurt = True
                    elif status_txt and "active" not in st_lower:
                        is_hurt = True
                    elif "day" in st_lower or "quest" in st_lower or "doubt" in st_lower:
                        is_hurt = True
                        
                    if is_hurt:
                        team_injuries.append({
                            "name": full_name,
                            "name_norm": normalize_name(full_name),
                            "status": status_txt,
                            "details": details,
                            "date": date_str
                        })
                
                # Salva no cache
                nba_std = ESPN_TO_NBA_STANDARD.get(espn_code.upper(), team_abbr.upper())
                
                if "teams" not in self.cache: self.cache["teams"] = {}
                self.cache["teams"][nba_std] = team_injuries
                
                # print(f"✅ {nba_std}: {len(team_injuries)} reports.")
                return True
            else:
                print(f"❌ Erro HTTP {r.status_code} para {team_abbr}")
                
        except Exception as e:
            print(f"⚠️ Exception {team_abbr}: {e}")
            return False
        
        return False

    def save_cache(self):
        self.cache["updated_at"] = datetime.now().isoformat()
        save_json(self.cache_path, self.cache)
        return True

    def get_team_injuries(self, team_abbr: str) -> list:
        return self.cache.get("teams", {}).get(team_abbr.upper(), [])

    def get_all_injuries(self) -> dict:
        return self.cache.get("teams", {})

    def is_player_out(self, player_name: str, team_abbr: str) -> bool:
        name_norm = normalize_name(player_name)
        team_list = self.get_team_injuries(team_abbr)
        for item in team_list:
            if item.get("name_norm") == name_norm:
                st = str(item.get("status", "")).lower()
                if "out" in st or "inj" in st: return True
        return False

    def is_player_blocked(self, player_name: str, team_abbr: str) -> bool:
        name_norm = normalize_name(player_name)
        team_list = self.get_team_injuries(team_abbr)
        for item in team_list:
            if name_norm in item.get("name_norm", "") or item.get("name_norm", "") in name_norm:
                st = str(item.get("status", "")).lower()
                if any(x in st for x in ['out', 'doubt', 'quest', 'day']):
                    return True
        return False

    def _extract_list_recursive(self, data):
        """
        Busca PROATIVA por listas de jogadores. 
        Aceita 'athletes' ou 'items' como chave.
        """
        if isinstance(data, dict):
            # Prioridade 1: Chaves conhecidas de lista
            if "athletes" in data and isinstance(data["athletes"], list):
                return data["athletes"]
            if "items" in data and isinstance(data["items"], list):
                # Validação rápida: parece jogador?
                if len(data["items"]) > 0 and ("fullName" in data["items"][0] or "displayName" in data["items"][0]):
                    return data["items"]
            
            # Prioridade 2: Busca profunda
            for v in data.values():
                res = self._extract_list_recursive(v)
                if res: return res
                
        elif isinstance(data, list):
            for item in data:
                res = self._extract_list_recursive(item)
                if res: return res
        return []
