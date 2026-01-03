# injuries.py — OFF RADAR v48.0 (JSON PARSER FIX)
# Módulo definitivo de lesões.
# FIXED: Correção na leitura do JSON da ESPN (data['team']['athletes']).
# FIXED: Prints de debug para visualizar o progresso no console.

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
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json"
}

# --- MAPA DE TRADUÇÃO (NBA STANDARD -> ESPN URL CODE) ---
# A ESPN usa códigos antigos/diferentes para alguns times na URL da API
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
        Busca lesões de UM time específico.
        """
        # 1. Traduz sigla NBA (ex: GSW) para sigla ESPN (ex: gs)
        espn_code = NBA_TO_ESPN_MAP.get(team_abbr.upper(), team_abbr.lower())
        
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{espn_code}/roster"
        
        try:
            r = requests.get(url, headers=HEADERS, timeout=8)
            
            if r.status_code == 200:
                data = r.json()
                team_injuries = []
                
                # --- CORREÇÃO CRÍTICA DO PARSING ---
                athletes = []
                
                # Tentativa 1: Estrutura Padrão (data -> team -> athletes)
                if 'team' in data and 'athletes' in data['team']:
                    athletes = data['team']['athletes']
                
                # Tentativa 2: Estrutura Alternativa (data -> athletes)
                elif 'athletes' in data:
                    athletes = data['athletes']
                
                # Tentativa 3: Roster antigo
                elif 'roster' in data and 'athletes' in data['roster']:
                    athletes = data['roster']['athletes']
                
                # Tentativa 4: Varredura profunda
                if not athletes:
                    athletes = self._extract_list_recursive(data)

                # DEBUG NO CONSOLE
                # print(f"🔍 {team_abbr}: Encontrados {len(athletes)} atletas.")

                for ath in athletes:
                    # Verifica lesões
                    inj = ath.get("injuries", [])
                    status_generic = ath.get("status", {}).get("type", {}).get("name", "")
                    
                    # Se tiver lista de injuries OU status não for Active
                    has_injury_data = (len(inj) > 0)
                    is_not_active = (status_generic and status_generic.lower() != 'active')
                    
                    if has_injury_data or is_not_active:
                        
                        # Pega o status mais descritivo
                        status_txt = status_generic
                        details = ""
                        date_str = datetime.now().strftime("%Y-%m-%d")
                        
                        if inj:
                            latest = inj[0]
                            status_txt = latest.get("status", status_txt)
                            details = latest.get("details") or latest.get("shortComment") or ""
                            date_str = latest.get("date") or date_str

                        # Filtro final: Só adiciona se realmente não for Active (ignora Day-to-Day irrelevante)
                        st_lower = str(status_txt).lower()
                        if 'active' in st_lower and 'day' not in st_lower and 'questionable' not in st_lower:
                            continue

                        team_injuries.append({
                            "name": ath.get("fullName") or ath.get("displayName"),
                            "name_norm": normalize_name(ath.get("fullName") or ath.get("displayName")),
                            "status": status_txt,
                            "details": details,
                            "date": date_str
                        })
                
                # Salva no cache em memória
                nba_std = ESPN_TO_NBA_STANDARD.get(espn_code.upper(), team_abbr.upper())
                
                if "teams" not in self.cache: self.cache["teams"] = {}
                self.cache["teams"][nba_std] = team_injuries
                
                # print(f"✅ {nba_std}: {len(team_injuries)} machucados registrados.")
                return True
            else:
                print(f"❌ Erro HTTP {r.status_code} para {team_abbr}")
                
        except Exception as e:
            print(f"⚠️ Exception ao baixar {team_abbr}: {e}")
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
        """Helper para achar lista de atletas em JSONs aninhados"""
        if isinstance(data, dict):
            if "athletes" in data and isinstance(data["athletes"], list):
                return data["athletes"]
            for v in data.values():
                res = self._extract_list_recursive(v)
                if res: return res
        elif isinstance(data, list):
            for item in data:
                res = self._extract_list_recursive(item)
                if res: return res
        return []
