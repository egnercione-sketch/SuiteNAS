# ============================================================================
# INJURIES.PY - INTELLIGENCE MODULE v60.2 (FIXED ATTRIBUTE ERROR)
# ============================================================================
# Lógica: Híbrida (ESPN API + CBS Scraping)
# Persistência: 100% Supabase (Key: 'injuries')
# FIX: Reintroduzido método 'get_all_injuries' necessário para a UI.
# ============================================================================

import os
import json
import time
from datetime import datetime
import requests
import unicodedata
import re

# Tenta importar BeautifulSoup (Vital para o novo scraping)
try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    print("⚠️ [Injuries] BeautifulSoup não instalado. Scraping CBS desativado.")

# Tenta importar o gerenciador de banco (Essencial para este modo)
try:
    from db_manager import db
except ImportError:
    db = None
    print("⚠️ [CRITICAL] db_manager não encontrado. O sistema não salvará no Supabase.")

# HEADERS (Anti-Bloqueio para Scraping)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}

# --- HELPERS DE NORMALIZAÇÃO ---
_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}

def normalize_name(s: str) -> str:
    """Normalização robusta para bater nomes entre fontes diferentes."""
    s = str(s or "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c)) # Remove acentos
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s) # Remove pontuação
    s = re.sub(r"\s+", " ", s).strip()
    # Remove sufixos (Jr, III, etc) para matching mais fácil
    tokens = [t for t in s.split(" ") if t and t not in _SUFFIXES]
    return " ".join(tokens)

# --- SCRAPER CBS (INTELIGÊNCIA) ---
def fetch_cbs_injuries():
    """
    Raspa a tabela de lesões da CBS Sports.
    Retorna um dicionário: {'lebron james': {'status': 'GTD', ...}}
    """
    if not BS4_AVAILABLE: return {}
    
    url = "https://www.cbssports.com/nba/injuries/"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200: return {}
        
        soup = BeautifulSoup(r.text, "html.parser")
        injuries = {}
        
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cols = row.find_all("td")
                if len(cols) >= 4:
                    raw_name = cols[0].get_text(strip=True)
                    name_clean = normalize_name(raw_name)
                    
                    status = cols[4].get_text(strip=True) if len(cols) > 4 else "Unknown"
                    injury_type = cols[3].get_text(strip=True)
                    date_update = cols[2].get_text(strip=True)
                    
                    injuries[name_clean] = {
                        "source": "CBS",
                        "status": status,
                        "injury": injury_type,
                        "updated": date_update
                    }
        return injuries
    except Exception as e:
        print(f"⚠️ [Injuries] Erro CBS Scraping: {e}")
        return {}

# --- CLASSE PRINCIPAL ---

class InjuryMonitor:
    def __init__(self):
        # Cache em memória apenas para execução corrente
        self.cbs_data = {} 
        self.last_cbs_update = 0
        
        # Carrega estado inicial do Supabase
        self.cache = self._load_from_cloud()

    def _load_from_cloud(self):
        """Carrega da chave 'injuries' do Supabase."""
        if db:
            try:
                data = db.get_data("injuries")
                if data and isinstance(data, dict) and "teams" in data:
                    return data
            except Exception as e:
                print(f"⚠️ [InjuryMonitor] Falha ao ler Supabase: {e}")
        
        # Estrutura padrão se falhar
        return {"updated_at": None, "teams": {}}

    def refresh_market_intelligence(self):
        """Atualiza dados da CBS (Scraping) a cada 30 min."""
        if time.time() - self.last_cbs_update > 1800:
            self.cbs_data = fetch_cbs_injuries()
            self.last_cbs_update = time.time()

    def fetch_injuries_for_team(self, team_abbr):
        """
        Busca na ESPN API e enriquece com dados da CBS em memória.
        """
        # Garante que temos dados da CBS atualizados
        if not self.cbs_data: self.refresh_market_intelligence()
        
        # Mapeamento ESPN
        nba_to_espn = {
            "UTA": "utah", "NOP": "no", "NYK": "ny", "GSW": "gs", "SAS": "sa", 
            "PHX": "pho", "WAS": "wsh", "BKN": "bkn"
        }
        team_code = nba_to_espn.get(team_abbr.upper(), team_abbr.lower())
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_code}/roster"
        
        time.sleep(0.2) # Delay anti-bloqueio
        
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code != 200: return False
            
            data = r.json()
            team_injuries = []
            
            # Extração segura de atletas
            athletes = []
            if "athletes" in data: athletes = data["athletes"]
            elif "team" in data and "athletes" in data["team"]: athletes = data["team"]["athletes"]
            
            for ath in athletes:
                raw_name = ath.get("fullName", "")
                norm_name = normalize_name(raw_name)
                
                # 1. Dados Oficiais (ESPN API)
                espn_status = ath.get("status", {}).get("type", {}).get("name", "Active")
                espn_injuries = ath.get("injuries", [])
                
                is_hurt = False
                final_status = espn_status
                details = ""
                
                # Detecta flag na ESPN
                if espn_injuries:
                    is_hurt = True
                    final_status = espn_injuries[0].get("status", final_status)
                    details = espn_injuries[0].get("shortComment", "")
                elif "active" not in str(espn_status).lower():
                    is_hurt = True
                
                # 2. Enriquecimento com CBS
                cbs_info = self.cbs_data.get(norm_name)
                
                if cbs_info:
                    is_hurt = True
                    cbs_status = cbs_info.get('status', '')
                    if cbs_status and cbs_status != "Unknown":
                        final_status = f"{final_status} | CBS: {cbs_status}"
                    
                    if cbs_info.get('injury'):
                        details = f"{details} [{cbs_info['injury']}]".strip()

                if is_hurt:
                    team_injuries.append({
                        "name": raw_name,
                        "name_norm": norm_name,
                        "status": final_status,
                        "details": details,
                        "source": "Hybrid (ESPN+CBS)" if cbs_info else "ESPN",
                        "updated": datetime.now().strftime("%Y-%m-%d %H:%M")
                    })
            
            # Atualiza cache local
            self.cache["teams"][team_abbr.upper()] = team_injuries
            return True
            
        except Exception as e:
            print(f"❌ [InjuryMonitor] Erro processando {team_abbr}: {e}")
            return False

    def update_all_teams(self, team_list):
        """Atualiza lista de times e salva no Supabase."""
        self.refresh_market_intelligence()
        
        success_count = 0
        
        for team in team_list:
            if self.fetch_injuries_for_team(team):
                success_count += 1
                
        if success_count > 0:
            self.save_to_cloud()
            return True
        return False

    def save_to_cloud(self):
        """Salva o estado atual na chave 'injuries' do Supabase."""
        self.cache["updated_at"] = datetime.now().isoformat()
        
        if db:
            try:
                db.save_data("injuries", self.cache)
            except Exception as e:
                print(f"❌ [InjuryMonitor] Erro ao salvar no Supabase: {e}")

    def get_team_injuries(self, team_abbr):
        """Retorna lesões de um time específico."""
        return self.cache.get("teams", {}).get(team_abbr.upper(), [])

    # --- MÉTODO RESTAURADO (CRÍTICO PARA A UI) ---
    def get_all_injuries(self):
        """Retorna o dicionário completo de times e lesões."""
        return self.cache.get("teams", {})

    def is_player_blocked(self, player_name, team_abbr):
        """
        Verifica se o jogador deve ser bloqueado.
        """
        target = normalize_name(player_name)
        injuries = self.get_team_injuries(team_abbr)
        
        block_keywords = ['out', 'surg', 'injur', 'doubt', 'protocol', 'g-league']
        block_keywords.extend(['quest', 'day', 'gtd', 'game time']) 
        
        for inj in injuries:
            inj_name = inj.get("name_norm", "")
            if target in inj_name or inj_name in target:
                status = str(inj.get("status", "")).lower()
                if any(x in status for x in block_keywords):
                    return True
        return False

# Instância Global (Opcional, mas útil para testes)
monitor = InjuryMonitor()
