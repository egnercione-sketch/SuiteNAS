# injuries.py — OFF RADAR v52.0 (BACK TO BASICS + CLOUD)
# Módulo Híbrido: Lógica confiável da v47 + Salvamento em Nuvem.
# FIXED: Usa API JSON (mais estável que HTML).
# FIXED: Integração direta com db_manager para Supabase.

import os
import json
import time
from datetime import datetime, timedelta
import requests

# Tenta importar o gerenciador de banco
try:
    from db_manager import db
except ImportError:
    db = None
    print("⚠️ [Injuries] db_manager não encontrado. Rodando em modo local.")

# Ajuste conforme seu projeto
BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# Default path
DEFAULT_CACHE_FILE = os.path.join(CACHE_DIR, "injuries_cache_v44.json")
CACHE_TTL_HOURS = 3

# HEADERS DE NAVEGADOR (Anti-Bloqueio)
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
        """
        Estratégia Cloud-First:
        1. Tenta Supabase.
        2. Se falhar, tenta arquivo local.
        """
        # 1. Tenta Nuvem
        if db:
            try:
                cloud_data = db.get_data("injuries")
                if cloud_data and "teams" in cloud_data:
                    # print("☁️ [Injuries] Cache carregado do Supabase.")
                    return cloud_data
            except Exception as e:
                print(f"⚠️ Erro leitura nuvem: {e}")

        # 2. Tenta Local
        data = load_json(self.cache_path)
        return data if data else {"updated_at": None, "teams": {}}

    def fetch_injuries_for_team(self, team_abbr):
        """
        Busca lesões de UM time específico (Lógica v47).
        """
        espn_code = NBA_TO_ESPN_MAP.get(team_abbr.upper(), team_abbr.lower())
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{espn_code}/roster"
        
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code == 200:
                data = r.json()
                team_injuries = []
                
                # Busca recursiva de atletas
                athletes = self._extract_list_recursive(data)

                for ath in athletes:
                    # Verifica lesões
                    inj = ath.get("injuries", [])
                    status_generic = ath.get("status", {}).get("type", {}).get("name", "")
                    
                    # Se tiver lista de injuries OU status não for Active
                    is_hurt = False
                    st_lower = str(status_generic).lower()

                    if inj: is_hurt = True
                    elif status_generic and "active" not in st_lower: is_hurt = True
                    elif "day" in st_lower or "quest" in st_lower or "doubt" in st_lower: is_hurt = True

                    if is_hurt:
                        status_txt = status_generic
                        details = ""
                        date_str = datetime.now().strftime("%Y-%m-%d")
                        
                        if inj:
                            latest = inj[0]
                            status_txt = latest.get("status", status_txt)
                            details = latest.get("details") or latest.get("shortComment") or latest.get("longComment") or ""
                            date_str = latest.get("date") or date_str

                        team_injuries.append({
                            "name": ath.get("fullName") or ath.get("displayName"),
                            "name_norm": normalize_name(ath.get("fullName") or ath.get("displayName")),
                            "status": status_txt,
                            "details": details,
                            "date": date_str
                        })
                
                # Atualiza Cache em Memória
                nba_std = ESPN_TO_NBA_STANDARD.get(espn_code.upper(), team_abbr.upper())
                
                if "teams" not in self.cache: self.cache["teams"] = {}
                self.cache["teams"][nba_std] = team_injuries
                
                # OBS: Não salvamos na nuvem a cada time para não spammar o banco.
                # O salvamento ocorre no final do loop no arquivo principal ou chamando save_cache()
                return True
            else:
                print(f"❌ Erro HTTP {r.status_code} para {team_abbr}")
                
        except Exception as e:
            print(f"⚠️ Exception {team_abbr}: {e}")
            return False
        
        return False

    def save_cache(self):
        """
        Salva no Arquivo Local E na Nuvem.
        """
        self.cache["updated_at"] = datetime.now().isoformat()
        
        # 1. Local
        save_json(self.cache_path, self.cache)
        
        # 2. Nuvem (Supabase)
        if db:
            try:
                # print("☁️ [Injuries] Enviando para Supabase...")
                db.save_data("injuries", self.cache)
                print("✅ [Injuries] Salvo na nuvem com sucesso.")
            except Exception as e:
                print(f"❌ [Injuries] Erro upload: {e}")
        
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
            # Prioridade para chaves conhecidas
            if "athletes" in data and isinstance(data["athletes"], list):
                return data["athletes"]
            if "items" in data and isinstance(data["items"], list): # ESPN as vezes usa 'items'
                return data["items"]
            
            for v in data.values():
                res = self._extract_list_recursive(v)
                if res: return res
        elif isinstance(data, list):
            for item in data:
                res = self._extract_list_recursive(item)
                if res: return res
        return []
