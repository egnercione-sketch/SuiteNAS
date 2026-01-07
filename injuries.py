# injuries.py — OFF RADAR v52.1 (REFINED & ROBUST)
# Módulo Híbrido: Lógica confiável + Salvamento em Nuvem + Anti-Block.
# FIXED: Adicionado Delay para evitar bloqueio de IP da ESPN.
# FIXED: Matching de nomes mais flexível (Fuzzy Logic simples).

import os
import json
import time
from datetime import datetime
import requests
import unicodedata
import re

# Tenta importar o gerenciador de banco
try:
    from db_manager import db
except ImportError:
    db = None
    print("⚠️ [Injuries] db_manager não encontrado. Rodando em modo local.")

# Configurações de Diretório
BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

DEFAULT_CACHE_FILE = os.path.join(CACHE_DIR, "injuries_cache_v44.json")

# HEADERS (Anti-Bloqueio)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.espn.com/",
    "Origin": "https://www.espn.com"
}

# MAPA: NBA STANDARD -> ESPN URL CODE
NBA_TO_ESPN_MAP = {
    "UTA": "utah", "UTAH": "utah", "NOP": "no", "NO": "no",
    "NYK": "ny", "NY": "ny", "GSW": "gs", "GS": "gs",
    "SAS": "sa", "SA": "sa", "PHX": "pho", "PHO": "pho",
    "WAS": "wsh", "WSH": "wsh", "BKN": "bkn", "BRK": "bkn"
}

# MAPA INVERSO: ESPN -> NBA STANDARD
ESPN_TO_NBA_STANDARD = {
    "utah": "UTA", "gs": "GSW", "no": "NOP", "ny": "NYK",
    "sa": "SAS", "pho": "PHX", "wsh": "WAS"
}

def normalize_name(n: str) -> str:
    """Normaliza nomes removendo acentos, sufixos e pontuação."""
    if not n: return ""
    n = str(n).lower()
    n = n.replace(".", " ").replace(",", " ").replace("-", " ")
    # Remove sufixos comuns
    n = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", n)
    n = unicodedata.normalize("NFKD", n).encode("ascii", "ignore").decode("ascii")
    return " ".join(n.split())

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
        
        # Garante estrutura mínima
        if "teams" not in self.cache:
            self.cache["teams"] = {}

    def _load_cache(self):
        """Estratégia Cloud-First com Fallback Local."""
        # 1. Tenta Nuvem (Supabase)
        if db:
            try:
                cloud_data = db.get_data("injuries")
                if cloud_data and isinstance(cloud_data, dict) and "teams" in cloud_data:
                    return cloud_data
            except Exception as e:
                print(f"⚠️ [InjuryMonitor] Falha leitura nuvem: {e}")

        # 2. Tenta Local
        data = load_json(self.cache_path)
        return data if data else {"updated_at": None, "teams": {}}

    def fetch_injuries_for_team(self, team_abbr):
        """Busca lesões na API da ESPN."""
        # Delay de segurança (Anti-Throttle)
        time.sleep(0.2)
        
        espn_code = NBA_TO_ESPN_MAP.get(team_abbr.upper(), team_abbr.lower())
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{espn_code}/roster"
        
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code == 200:
                data = r.json()
                team_injuries = []
                
                # Busca recursiva de atletas no JSON aninhado
                athletes = self._extract_list_recursive(data)

                for ath in athletes:
                    inj = ath.get("injuries", [])
                    status_generic = ath.get("status", {}).get("type", {}).get("name", "")
                    
                    # Lógica de Detecção de Lesão
                    is_hurt = False
                    st_lower = str(status_generic).lower()

                    if inj: is_hurt = True
                    elif status_generic and "active" not in st_lower: is_hurt = True
                    elif any(x in st_lower for x in ["day", "quest", "doubt", "out"]): is_hurt = True

                    if is_hurt:
                        # Prioriza info específica da lesão, senão usa status genérico
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
                
                # Normaliza sigla para padrão NBA e salva
                nba_std = ESPN_TO_NBA_STANDARD.get(espn_code.upper(), team_abbr.upper())
                self.cache["teams"][nba_std] = team_injuries
                return True
            else:
                print(f"❌ [InjuryMonitor] Erro HTTP {r.status_code} para {team_abbr}")
                
        except Exception as e:
            print(f"⚠️ [InjuryMonitor] Exception {team_abbr}: {e}")
            return False
        
        return False

    def update_all_teams(self, team_list):
        """Atualiza a liga toda e salva uma vez no final."""
        print(f"🔄 [InjuryMonitor] Atualizando {len(team_list)} times...")
        count = 0
        for team in team_list:
            if self.fetch_injuries_for_team(team):
                count += 1
        
        if count > 0:
            self.save_cache()
            return True
        return False

    def save_cache(self):
        """Salva Local e Nuvem."""
        self.cache["updated_at"] = datetime.now().isoformat()
        
        # 1. Local
        save_json(self.cache_path, self.cache)
        
        # 2. Nuvem
        if db:
            try:
                db.save_data("injuries", self.cache)
                # print("✅ [InjuryMonitor] Sincronizado com Supabase.")
            except Exception as e:
                print(f"❌ [InjuryMonitor] Erro upload: {e}")
        
        return True

    def get_team_injuries(self, team_abbr: str) -> list:
        return self.cache.get("teams", {}).get(team_abbr.upper(), [])

    def get_all_injuries(self) -> dict:
        return self.cache.get("teams", {})

    def is_player_blocked(self, player_name: str, team_abbr: str) -> bool:
        """
        Verifica se o jogador está bloqueado (OUT, DOUBTFUL, ETC).
        Usa Fuzzy Match: Se 'Luka' estiver na lista, bloqueia 'Luka Doncic'.
        """
        name_norm = normalize_name(player_name)
        team_list = self.get_team_injuries(team_abbr)
        
        blocked_keywords = ['out', 'doubt', 'quest', 'day', 'injur', 'surg']
        
        for item in team_list:
            item_name = str(item.get("name_norm", ""))
            
            # Match flexível (um contém o outro)
            if name_norm in item_name or item_name in name_norm:
                st = str(item.get("status", "")).lower()
                if any(x in st for x in blocked_keywords):
                    return True
        return False

    def _extract_list_recursive(self, data):
        """Encontra listas de atletas em JSONs complexos da ESPN."""
        if isinstance(data, dict):
            if "athletes" in data and isinstance(data["athletes"], list): return data["athletes"]
            if "items" in data and isinstance(data["items"], list): return data["items"]
            
            for v in data.values():
                res = self._extract_list_recursive(v)
                if res: return res
        elif isinstance(data, list):
            for item in data:
                res = self._extract_list_recursive(item)
                if res: return res
        return []
