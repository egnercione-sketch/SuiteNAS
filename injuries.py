# injuries.py — OFF RADAR v47.0 (FULL METHOD FIX)
# Módulo definitivo de lesões.
# FIXED: Adicionado método 'fetch_injuries_for_team' que faltava.
# FIXED: Mapeamento bidirecional de siglas (NBA <-> ESPN) para evitar erros de URL.

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
HEADERS = {"User-Agent": "Mozilla/5.0"}

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

    def _is_cache_fresh(self) -> bool:
        last = self.cache.get("updated_at")
        if not last: return False
        try:
            dt = datetime.fromisoformat(last)
            return (datetime.now() - dt) < timedelta(hours=CACHE_TTL_HOURS)
        except: return False

    def fetch_injuries_for_team(self, team_abbr):
        """
        Busca lesões de UM time específico.
        Essencial para a barra de progresso na UI.
        """
        # 1. Traduz sigla NBA (ex: GSW) para sigla ESPN (ex: gs)
        espn_code = NBA_TO_ESPN_MAP.get(team_abbr.upper(), team_abbr.lower())
        
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{espn_code}/roster"
        
        try:
            r = requests.get(url, headers=HEADERS, timeout=5)
            if r.status_code == 200:
                data = r.json()
                team_injuries = []
                
                # Acha a lista de atletas (pode variar a estrutura)
                athletes = data.get("athletes", [])
                if not athletes and "roster" in data:
                    athletes = data["roster"].get("athletes", [])
                
                # Fallback: varredura profunda se a estrutura mudou
                if not athletes:
                    athletes = self._extract_list_recursive(data)

                for ath in athletes:
                    # Verifica lesões
                    inj = ath.get("injuries", [])
                    status_generic = ath.get("status", {}).get("type", {}).get("name", "")
                    
                    # Se tiver lista de injuries OU status não for Active
                    if inj or (status_generic and status_generic.lower() != 'active'):
                        
                        # Pega o status mais descritivo
                        status_txt = status_generic
                        details = ""
                        date_str = datetime.now().strftime("%Y-%m-%d")
                        
                        if inj:
                            latest = inj[0]
                            status_txt = latest.get("status", status_txt)
                            details = latest.get("details") or latest.get("shortComment") or ""
                            date_str = latest.get("date") or date_str

                        # Só adiciona se não for 100% saudável
                        if 'active' in str(status_txt).lower() and 'day' not in str(status_txt).lower():
                            continue

                        team_injuries.append({
                            "name": ath.get("fullName") or ath.get("displayName"),
                            "name_norm": normalize_name(ath.get("fullName") or ath.get("displayName")),
                            "status": status_txt,
                            "details": details,
                            "date": date_str
                        })
                
                # Salva no cache em memória
                # Usa a sigla PADRÃO NBA para salvar (ex: GSW em vez de gs)
                nba_std = ESPN_TO_NBA_STANDARD.get(espn_code.upper(), team_abbr.upper())
                
                if "teams" not in self.cache: self.cache["teams"] = {}
                self.cache["teams"][nba_std] = team_injuries
                return True
                
        except Exception as e:
            print(f"⚠️ Falha ao baixar {team_abbr}: {e}")
            return False
        
        return False

    def fetch_injuries_universal(self):
        """
        Método legado: Baixa tudo de uma vez.
        """
        # Lista padrão de siglas da ESPN para iterar
        targets = [
            "atl","bos","bkn","cha","chi","cle","dal","den","det","gs","hou","ind",
            "lac","lal","mem","mia","mil","min","no","ny","okc","orl","phi","pho",
            "por","sac","sa","tor","utah","wsh"
        ]
        
        print(f"🔄 [Injuries] Baixando dados universais...")
        for t in targets:
            self.fetch_injuries_for_team(t)
            time.sleep(0.1)
        
        self.save_cache()
        return self.cache.get("teams", {})

    def save_cache(self):
        self.cache["updated_at"] = datetime.now().isoformat()
        save_json(self.cache_path, self.cache)
        return True

    def get_team_injuries(self, team_abbr: str) -> list:
        # Se cache vazio ou velho, tenta atualizar tudo (fallback)
        if not self.cache.get("teams"):
            # Tenta carregar do disco de novo
            self.cache = self._load_cache()
            
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
            # Verifica se o nome está contido (ex: "Luka" em "Luka Doncic")
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