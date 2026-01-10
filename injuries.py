# ============================================================================
# INJURIES.PY - INTELLIGENCE MODULE v61.0 (CRITICAL PERSISTENCE FIX)
# ============================================================================
# Lógica: Híbrida (ESPN API + CBS Scraping)
# Persistência: Supabase (Primário) + Session State (Fallback)
# ============================================================================

import os
import json
import time
from datetime import datetime
import requests
import unicodedata
import re
import streamlit as st # Adicionado para debug visual se necessário

# Tenta importar BeautifulSoup
try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    print("⚠️ [Injuries] BeautifulSoup não instalado. Scraping CBS desativado.")

# Tenta importar DB
try:
    from db_manager import db
except ImportError:
    db = None
    print("⚠️ [CRITICAL] db_manager não encontrado. Persistência comprometida.")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}

# --- HELPERS ---
_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}

def normalize_name(s: str) -> str:
    s = str(s or "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    tokens = [t for t in s.split(" ") if t and t not in _SUFFIXES]
    return " ".join(tokens)

# --- SCRAPER CBS ---
def fetch_cbs_injuries():
    if not BS4_AVAILABLE: return {}
    url = "https://www.cbssports.com/nba/injuries/"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200: return {}
        soup = BeautifulSoup(r.text, "html.parser")
        injuries = {}
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) >= 4:
                    raw_name = cols[0].get_text(strip=True)
                    name_clean = normalize_name(raw_name)
                    status = cols[4].get_text(strip=True) if len(cols) > 4 else "Unknown"
                    injury_type = cols[3].get_text(strip=True)
                    injuries[name_clean] = {
                        "source": "CBS", "status": status, "injury": injury_type,
                        "updated": cols[2].get_text(strip=True)
                    }
        return injuries
    except Exception as e:
        print(f"⚠️ [Injuries] Erro CBS: {e}")
        return {}

# --- CLASSE PRINCIPAL ---
class InjuryMonitor:
    def __init__(self):
        self.cbs_data = {} 
        self.last_cbs_update = 0
        self.cache = self._load_from_cloud()

    def _load_from_cloud(self):
        """Tenta carregar do Supabase com fallback para estrutura vazia."""
        if db:
            try:
                data = db.get_data("injuries")
                if data and isinstance(data, dict) and "teams" in data:
                    print(f"✅ [Injuries] Dados carregados do Supabase ({len(data['teams'])} times).")
                    return data
            except Exception as e:
                print(f"⚠️ [Injuries] Falha leitura Supabase: {e}")
        return {"updated_at": None, "teams": {}}

    def refresh_market_intelligence(self):
        if time.time() - self.last_cbs_update > 1800: # 30 min cache CBS
            print("🔄 [Injuries] Atualizando CBS Intelligence...")
            self.cbs_data = fetch_cbs_injuries()
            self.last_cbs_update = time.time()

    def fetch_injuries_for_team(self, team_abbr):
        if not self.cbs_data: self.refresh_market_intelligence()
        
        nba_to_espn = {"UTA":"utah","NOP":"no","NYK":"ny","GSW":"gs","SAS":"sa","PHX":"pho","WAS":"wsh","BKN":"bkn"}
        team_code = nba_to_espn.get(team_abbr.upper(), team_abbr.lower())
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_code}/roster"
        
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code != 200: return False
            data = r.json()
            team_injuries = []
            
            # Extração flexível de atletas
            athletes = data.get("athletes", [])
            if not athletes and "team" in data: athletes = data["team"].get("athletes", [])
            
            for ath in athletes:
                raw_name = ath.get("fullName", "")
                norm_name = normalize_name(raw_name)
                
                # ESPN Data
                espn_status = ath.get("status", {}).get("type", {}).get("name", "Active")
                espn_injuries = ath.get("injuries", [])
                
                is_hurt = False
                final_status = espn_status
                details = ""
                
                if espn_injuries:
                    is_hurt = True
                    final_status = espn_injuries[0].get("status", final_status)
                    details = espn_injuries[0].get("shortComment", "")
                elif "active" not in str(espn_status).lower():
                    is_hurt = True
                
                # CBS Data Merge
                cbs_info = self.cbs_data.get(norm_name)
                if cbs_info:
                    is_hurt = True
                    cbs_stat = cbs_info.get('status')
                    if cbs_stat and cbs_stat != "Unknown":
                        final_status = f"{final_status} | CBS: {cbs_stat}"
                    if cbs_info.get('injury'):
                        details = f"{details} [{cbs_info['injury']}]".strip()

                if is_hurt:
                    team_injuries.append({
                        "name": raw_name, "name_norm": norm_name, "status": final_status,
                        "details": details, "source": "Hybrid" if cbs_info else "ESPN",
                        "updated": datetime.now().strftime("%Y-%m-%d %H:%M")
                    })
            
            self.cache["teams"][team_abbr.upper()] = team_injuries
            return True
        except Exception as e:
            print(f"❌ [Injuries] Erro time {team_abbr}: {e}")
            return False

    def update_all_teams(self, team_list):
        self.refresh_market_intelligence()
        success = 0
        for team in team_list:
            if self.fetch_injuries_for_team(team): success += 1
            time.sleep(0.1) # Throttling leve
            
        if success > 0:
            self.save_to_cloud()
            return True
        return False

    def save_to_cloud(self):
        self.cache["updated_at"] = datetime.now().isoformat()
        if db:
            try:
                db.save_data("injuries", self.cache)
                print("✅ [Injuries] Salvo no Supabase com sucesso.")
            except Exception as e:
                print(f"❌ [Injuries] Erro crítico ao salvar Supabase: {e}")

    def get_all_injuries(self):
        return self.cache.get("teams", {})

    def get_team_injuries(self, team_abbr):
        return self.cache.get("teams", {}).get(team_abbr.upper(), [])

    def is_player_blocked(self, player_name, team_abbr):
        target = normalize_name(player_name)
        injuries = self.get_team_injuries(team_abbr)
        block_keywords = ['out', 'surg', 'injur', 'doubt', 'protocol', 'g-league', 'quest', 'gtd']
        
        for inj in injuries:
            if target in inj.get("name_norm", ""):
                status = str(inj.get("status", "")).lower()
                if any(x in status for x in block_keywords): return True
        return False

# Instância Singleton
monitor = InjuryMonitor()
