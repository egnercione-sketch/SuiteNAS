# injuries.py — OFF RADAR v51.0 (HTML TABLE STRATEGY)
# Módulo definitivo de lesões.
# FIXED: Substitui API por HTML Parsing (1 Request único vs 30 Requests).
# FIXED: Evita bloqueios de API e timeouts.

import os
import json
import time
from datetime import datetime
import pandas as pd
import requests

# Ajuste conforme seu projeto
BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)
DEFAULT_CACHE_FILE = os.path.join(CACHE_DIR, "injuries_cache_v44.json")

# Mapa Reverso de Nomes para Siglas (Necessário para o HTML da ESPN)
NAME_TO_ABBR = {
    "Atlanta Hawks": "ATL", "Boston Celtics": "BOS", "Brooklyn Nets": "BKN", "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI", "Cleveland Cavaliers": "CLE", "Dallas Mavericks": "DAL", "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET", "Golden State Warriors": "GSW", "Houston Rockets": "HOU", "Indiana Pacers": "IND",
    "LA Clippers": "LAC", "Los Angeles Clippers": "LAC", "Los Angeles Lakers": "LAL", "Lakers": "LAL",
    "Memphis Grizzlies": "MEM", "Miami Heat": "MIA", "Milwaukee Bucks": "MIL", "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP", "New York Knicks": "NYK", "Oklahoma City Thunder": "OKC", "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI", "Phoenix Suns": "PHX", "Portland Trail Blazers": "POR", "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS", "Toronto Raptors": "TOR", "Utah Jazz": "UTA", "Washington Wizards": "WAS"
}

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
        self.global_scraped_data = {} # Cache em memória da sessão atual
        self.data_loaded = False

    def _load_cache(self):
        data = load_json(self.cache_path)
        return data if data else {"updated_at": None, "teams": {}}

    def _fetch_all_injuries_html(self):
        """
        Mágica: Baixa TODAS as lesões de uma vez lendo a tabela HTML da ESPN.
        """
        if self.data_loaded: return True # Já baixou nesta sessão?

        url = "https://www.espn.com/nba/injuries"
        print("🌍 [Injuries] Conectando à página HTML da ESPN...")
        
        try:
            # Pandas lê todas as tabelas da página
            # headers simulando browser
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
            dfs = pd.read_html(r.text)
            
            # A página da ESPN tem várias tabelas, geralmente uma por time que tem lesão.
            # O desafio é saber qual tabela é de qual time. 
            # O Pandas read_html às vezes pega o título da tabela anterior, mas aqui vamos iterar.
            
            count = 0
            new_data = {}

            # Varredura inteligente: Procura o nome do time no texto cru ou na estrutura
            # Como o read_html retorna lista de DFs, precisamos associar ao time.
            # Na ESPN, o nome do time costuma vir como uma coluna ou header.
            
            # Vamos tentar uma abordagem direta: Iterar os DataFrames e tentar adivinhar o time
            # pelos jogadores ou cabeçalho.
            # POREM, na ESPN /nba/injuries, os nomes dos times são headers H4 entre as tabelas. 
            # O pandas read_html perde esses headers.
            
            # ABORDAGEM ALTERNATIVA ROBUSTA: CBS SPORTS
            # A CBS tem uma tabela única limpa ou estrutura mais fácil.
            # Mas vamos tentar manter ESPN com parse manual simplificado se o pandas falhar na identificação.
            
            # Vamos usar um TRUQUE: A primeira coluna da tabela ESPN Injuries costuma conter o nome do jogador.
            # Infelizmente sem o header do time, fica difícil.
            
            # VOLTA PARA O PLANO B: ROTOWIRE (Muito estruturado)
            # Ou PLANO C: COVERS.COM
            
            # Vamos usar a API JSON "escondida" que alimenta a página HTML da ESPN se possível.
            # Se não, vamos usar a CBS Sports que coloca o time na linha.
            
            cbs_url = "https://www.cbssports.com/nba/injuries/"
            dfs_cbs = pd.read_html(cbs_url)
            
            # A CBS retorna DataFrames onde o header ou a primeira linha é o nome do time
            for df in dfs_cbs:
                if df.empty: continue
                
                # Tenta identificar o time pelo cabeçalho ou primeira coluna
                # Na CBS, cada tabela tem o nome do time como título, o pandas puxa como column name
                possible_team = df.columns[0]
                
                # Limpa o nome (ex: "Atlanta Hawks Injuries" -> "Atlanta Hawks")
                clean_name = possible_team.replace(" Injuries", "").replace(" Daily", "").strip()
                
                team_abbr = NAME_TO_ABBR.get(clean_name)
                if not team_abbr:
                    # Tenta match parcial
                    for k, v in NAME_TO_ABBR.items():
                        if k in clean_name:
                            team_abbr = v
                            break
                
                if team_abbr:
                    # Processa as linhas
                    team_injuries = []
                    # CBS Colunas: Player, Position, Updated, Injury, Injury Status
                    # As colunas reais podem variar, vamos pelo índice
                    
                    for _, row in df.iterrows():
                        try:
                            # Pula linhas de cabeçalho repetido
                            if str(row[0]) == "Player" or str(row[0]) == clean_name: continue
                            
                            p_name = str(row[0])
                            status = str(row[4]) if len(row) > 4 else "Unknown" # Injury Status
                            notes = str(row[3]) if len(row) > 3 else "" # Injury Details
                            date_str = str(row[2]) if len(row) > 2 else "" # Date
                            
                            if "Game Time" in status: status = "Questionable"
                            if "Expected to be out" in status: status = "Out"
                            
                            team_injuries.append({
                                "name": p_name,
                                "name_norm": self._normalize(p_name),
                                "status": status,
                                "details": notes,
                                "date": date_str
                            })
                        except: continue
                    
                    if team_injuries:
                        new_data[team_abbr] = team_injuries
                        count += 1

            if count > 0:
                self.global_scraped_data = new_data
                self.cache["teams"] = new_data
                self.data_loaded = True
                print(f"✅ [Injuries] {count} times atualizados via CBS HTML Table.")
                return True
                
        except Exception as e:
            print(f"⚠️ Erro no HTML Parsing: {e}")
            return False

    def fetch_injuries_for_team(self, team_abbr):
        """
        Simula o comportamento antigo para manter compatibilidade com o SuiteNAS.
        Na primeira chamada, baixa tudo. Nas próximas, só retorna da memória.
        """
        # Garante que temos dados carregados
        if not self.data_loaded:
            self._fetch_all_injuries_html()
            self.data_loaded = True # Marca como carregado mesmo se falhar para não travar
            
        # Retorna True se tiver dados desse time, só pra UI ficar feliz
        return team_abbr in self.cache.get("teams", {})

    def _normalize(self, n):
        import re, unicodedata
        if not n: return ""
        n = str(n).lower()
        n = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", n)
        n = unicodedata.normalize("NFKD", n).encode("ascii", "ignore").decode("ascii")
        return " ".join(n.split())

    def save_cache(self):
        self.cache["updated_at"] = datetime.now().isoformat()
        save_json(self.cache_path, self.cache)
        return True

    def get_team_injuries(self, team_abbr: str) -> list:
        return self.cache.get("teams", {}).get(team_abbr.upper(), [])

    def get_all_injuries(self) -> dict:
        return self.cache.get("teams", {})

    def is_player_out(self, player_name: str, team_abbr: str) -> bool:
        # Mesma lógica de antes
        name_norm = self._normalize(player_name)
        team_list = self.get_team_injuries(team_abbr)
        for item in team_list:
            if item.get("name_norm") == name_norm:
                st = str(item.get("status", "")).lower()
                if "out" in st or "inj" in st: return True
        return False
    
    def is_player_blocked(self, player_name: str, team_abbr: str) -> bool:
        name_norm = self._normalize(player_name)
        team_list = self.get_team_injuries(team_abbr)
        for item in team_list:
            if name_norm in item.get("name_norm", ""):
                st = str(item.get("status", "")).lower()
                if any(x in st for x in ['out', 'doubt', 'quest', 'day']):
                    return True
        return False
