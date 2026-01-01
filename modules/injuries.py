# injuries.py — OFF RADAR v45.0 (UNIVERSAL AUTO-DISCOVERY)
# Módulo definitivo de lesões.
# 1. Pede a lista de times para a ESPN (Auto-Discovery).
# 2. Baixa as lesões usando as siglas da própria ESPN.
# 3. Normaliza para o padrão NBA (UTA, GSW, NOP) antes de salvar no cache.

import os
import json
import time
from datetime import datetime, timedelta
import requests

# Ajuste conforme seu projeto
BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

INJURIES_CACHE_FILE = os.path.join(CACHE_DIR, "injuries_cache_v44.json")
CACHE_TTL_HOURS = 3
HEADERS = {"User-Agent": "Mozilla/5.0"}

# --- TRADUTOR DE RETORNO (ESPN -> NBA STANDARD) ---
# A ESPN usa siglas estranhas. Precisamos converter de volta para o padrão NBA
# para que o seu app (DeepDeepb.py) encontre os dados no cache.
ESPN_TO_NBA_STANDARD = {
    "UTAH": "UTA",
    "GS": "GSW",
    "NO": "NOP",
    "NY": "NYK",
    "SA": "SAS",
    "PHO": "PHX",
    "WSH": "WAS",
    "WAS": "WAS" # Às vezes eles usam o certo
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
    def __init__(self):
        self.cache = self._load_cache()

    def _load_cache(self):
        data = load_json(INJURIES_CACHE_FILE)
        return data if data else {"updated_at": None, "teams": {}}

    def _is_cache_fresh(self) -> bool:
        last = self.cache.get("updated_at")
        if not last: return False
        try:
            dt = datetime.fromisoformat(last)
            return (datetime.now() - dt) < timedelta(hours=CACHE_TTL_HOURS)
        except: return False

    def _get_espn_team_list(self):
        """Busca a lista MESTRA de times da ESPN para saber as siglas corretas deles."""
        url = "http://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams"
        try:
            r = requests.get(url, headers=HEADERS, timeout=5)
            if r.status_code == 200:
                data = r.json()
                teams = []
                for sport in data.get('sports', []):
                    for league in sport.get('leagues', []):
                        for team_entry in league.get('teams', []):
                            team = team_entry.get('team', {})
                            # Pega a abreviação que a ESPN usa (ex: UTAH, GS, NO)
                            teams.append(team.get('abbreviation'))
                return teams
        except Exception as e:
            print(f"⚠️ Erro ao buscar lista de times ESPN: {e}")
        return []

    def fetch_injuries_universal(self):
        """
        Método Universal:
        1. Descobre os times.
        2. Itera sobre todos.
        3. Salva com a sigla PADRONIZADA.
        """
        espn_teams = self._get_espn_team_list()
        
        # Fallback se a lista mestra falhar (usa uma lista básica padrão NBA)
        if not espn_teams:
            espn_teams = ['BOS','BKN','NYK','PHI','TOR','CHI','CLE','DET','IND','MIL','DEN','MIN','OKC','POR','UTA','GSW','LAC','LAL','PHX','SAC','ATL','CHA','MIA','ORL','WAS','DAL','HOU','MEM','NOP','SAS']

        print(f"🔄 [Universal] Atualizando lesões para {len(espn_teams)} times via ESPN...")
        
        updated_teams = {}
        
        for espn_abbr in espn_teams:
            if not espn_abbr: continue
            
            # URL usando a sigla DA ESPN (que acabamos de descobrir que funciona)
            url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{espn_abbr}/roster"
            
            try:
                r = requests.get(url, headers=HEADERS, timeout=3) # Timeout curto para ser rápido
                if r.status_code == 200:
                    data = r.json()
                    team_injuries = []
                    
                    for ath in data.get("athletes", []):
                        inj = ath.get("injuries", [])
                        if inj:
                            latest = inj[0]
                            status_txt = latest.get("status", "Unknown")
                            status_lower = status_txt.lower()
                            
                            # Filtra falsos positivos
                            if 'active' in status_lower and 'day-to-day' not in status_lower: continue

                            team_injuries.append({
                                "name": ath.get("fullName"),
                                "name_norm": normalize_name(ath.get("fullName")),
                                "status": status_txt,
                                "details": latest.get("details", "") or latest.get("type", {}).get("description", ""),
                                "date": latest.get("date")
                            })
                    
                    # --- CONVERSÃO CRÍTICA ---
                    # A ESPN nos deu "UTAH". Nós queremos salvar como "UTA".
                    # A ESPN nos deu "GS". Nós queremos salvar como "GSW".
                    # Se não estiver no mapa, assume que é padrão (ex: LAL -> LAL).
                    nba_standard_abbr = ESPN_TO_NBA_STANDARD.get(espn_abbr, espn_abbr)
                    
                    updated_teams[nba_standard_abbr] = team_injuries
                    
            except Exception:
                continue # Pula time com erro sem travar
            
            time.sleep(0.1) # Respeita a API

        # Salva tudo no cache
        self.cache["teams"] = updated_teams
        self.cache["updated_at"] = datetime.now().isoformat()
        save_json(INJURIES_CACHE_FILE, self.cache)
        print("✅ [Universal] Cache de lesões atualizado!")
        return updated_teams

    def get_team_injuries(self, team_abbr: str) -> list:
        # Se cache velho, roda o atualizador universal
        if not self._is_cache_fresh() or not self.cache.get("teams"):
            self.fetch_injuries_universal()
            
        # Retorna buscando pela sigla PADRÃO NBA
        # O fetch_injuries_universal já garantiu a conversão
        return self.cache.get("teams", {}).get(team_abbr, [])

    def get_all_injuries(self) -> dict:
        if not self._is_cache_fresh():
            self.fetch_injuries_universal()
        return self.cache.get("teams", {})

    def is_player_out(self, player_name: str, team_abbr: str) -> bool:
        name_norm = normalize_name(player_name)
        team_list = self.get_team_injuries(team_abbr)
        for item in team_list:
            if item.get("name_norm") == name_norm:
                st = str(item.get("status", "")).lower()
                if "out" in st or "injured" in st: return True
        return False

    def is_player_blocked(self, player_name: str, team_abbr: str) -> bool:
        name_norm = normalize_name(player_name)
        team_list = self.get_team_injuries(team_abbr)
        for item in team_list:
            if name_norm in item.get("name_norm", ""):
                st = str(item.get("status", "")).lower()
                if any(x in st for x in ['out', 'doubtful', 'questionable', 'day-to-day']):
                    return True
        return False