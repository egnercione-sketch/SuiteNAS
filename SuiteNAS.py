# ============================================================================
# NBA ANALYTICS SUITE v2.0 (Cloud Enhanced - Fixed Globals)
# ============================================================================
import os
import sys
import pickle
import time
import json
import re
import random
import logging
import streamlit.components.v1 as components
from datetime import datetime, timedelta
from itertools import combinations

# --- Imports de Terceiros --
import requests
import pandas as pd
import numpy as np
import streamlit as st

# --- Configuração de Logger ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- FUNÇÕES GLOBAIS DE CORREÇÃO (Substitua as antigas) ---
def fix_team_abbr(abbr):
    """MAPPING UNIVERSAL: Corrige GS->GSW, UTAH->UTA, NO->NOP, etc."""
    if not abbr: return "UNK"
    abbr = str(abbr).upper().strip()
    mapping = {
        'GS': 'GSW', 'GOLDEN STATE': 'GSW', 'WARRIORS': 'GSW',
        'NO': 'NOP', 'NEW ORLEANS': 'NOP', 'PELICANS': 'NOP',
        'NY': 'NYK', 'NEW YORK': 'NYK', 'KNICKS': 'NYK',
        'SA': 'SAS', 'SAN ANTONIO': 'SAS', 'SPURS': 'SAS',
        'PHO': 'PHX', 'PHOENIX': 'PHX', 'SUNS': 'PHX',
        'UTAH': 'UTA', 'UTA': 'UTA', 'JAZZ': 'UTA', 'UT': 'UTA',
        'WSH': 'WAS', 'WASHINGTON': 'WAS', 'WIZARDS': 'WAS',
        'BKN': 'BKN', 'BROOKLYN': 'BKN', 'NETS': 'BKN',
        'CHA': 'CHA', 'CHO': 'CHA', 'HORNETS': 'CHA',
        'LAL': 'LAL', 'LAC': 'LAC'
    }
    return mapping.get(abbr, abbr)

def normalize_name(n):
    """Remove acentos e sufixos para comparar nomes."""
    if not n: return ""
    import unicodedata
    import re
    n = str(n).lower().replace(".", " ").replace(",", " ").replace("-", " ")
    n = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", n)
    n = unicodedata.normalize("NFKD", n).encode("ascii", "ignore").decode("ascii")
    return " ".join(n.split())

# ==============================================================================
# 1. FETCH SCOREBOARD (PRODUTOR: Extrai Jogos + Odds Brutas)
# ==============================================================================
def fetch_espn_scoreboard(progress_ui=False):
    """
    Busca jogos e ODDS BRUTAS direto da API da ESPN.
    Salva tudo no session_state['scoreboard'].
    """
    import requests
    from datetime import datetime, timedelta
    
    try:
        if progress_ui: st.toast("Sincronizando NBA...", icon="🏀")
        
        # Data de "Hoje" na NBA (UTC-5)
        et_now = datetime.utcnow() - timedelta(hours=5)
        date_str = et_now.strftime("%Y%m%d")
        
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_str}"
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        data = resp.json()
        
        games = []
        for event in data.get('events', []):
            try:
                comp = event['competitions'][0]
                
                # Times
                home_raw = comp['competitors'][0]['team']['abbreviation']
                away_raw = comp['competitors'][1]['team']['abbreviation']
                
                # Normalização (se tiver a função global, usa, senão usa string pura)
                try: 
                    home = normalize_team_signature(home_raw)
                    away = normalize_team_signature(away_raw)
                except:
                    home = fix_team_abbr(home_raw)
                    away = fix_team_abbr(away_raw)

                gid = event['id']
                
                # --- CAPTURA DE ODDS (A PEÇA QUE FALTAVA) ---
                odds_spread = "N/A"
                odds_total = 0.0
                
                if 'odds' in comp and len(comp['odds']) > 0:
                    # Pega o primeiro provider de odds (geralmente ESPN BET)
                    odds_obj = comp['odds'][0]
                    odds_spread = odds_obj.get('details', 'N/A') # Ex: "BOS -5.5"
                    odds_total = odds_obj.get('overUnder', 0.0)
                
                games.append({
                    "home": home, 
                    "away": away, 
                    "game_id": str(gid),
                    "game_str": f"{away} @ {home}",
                    "status": event['status']['type']['state'],
                    # Salvamos o dado bruto aqui para o próximo passo usar
                    "odds_spread": odds_spread, 
                    "odds_total": odds_total
                })
            except: continue
            
        # Salva na memória
        st.session_state['scoreboard'] = games
        return games
        
    except Exception as e:
        if progress_ui: st.error(f"Erro Scoreboard: {e}")
        return []
    

# ============================================================================
# 0. CONFIGURAÇÕES GLOBAIS E CONSTANTES
# ============================================================================

# Mapa de tradução NBA Oficial -> Códigos URL da ESPN
# Essencial para baixar elencos e fotos corretamente.
ESPN_TEAM_CODES = {
    "ATL": "atl", "BOS": "bos", "BKN": "bkn", "CHA": "cha", "CHI": "chi",
    "CLE": "cle", "DAL": "dal", "DEN": "den", "DET": "det", "GSW": "gs",
    "HOU": "hou", "IND": "ind", "LAC": "lac", "LAL": "lal", "MEM": "mem",
    "MIA": "mia", "MIL": "mil", "MIN": "min", "NOP": "no", "NYK": "ny",
    "OKC": "okc", "ORL": "orl", "PHI": "phi", "PHX": "pho", "POR": "por",
    "SAC": "sac", "SAS": "sa", "TOR": "tor", "UTA": "utah", "WAS": "wsh"
}
# ============================================================================
# 1. CONFIGURAÇÃO DE CAMINHOS E BANCO DE DADOS
# ============================================================================
BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# ============================================================================
# CONFIGURAÇÃO DE DIRETÓRIOS E ARQUIVOS (MANTENHA ISSO)
# ============================================================================
CACHE_DIR = "cache"
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

# Arquivos de Cache (Fallback Local - Necessários para o código não quebrar)
# Mudei L5 para .json pois abandonamos o Pickle
L5_CACHE_FILE = os.path.join(CACHE_DIR, "l5_players.json") 
SCOREBOARD_JSON_FILE = os.path.join(CACHE_DIR, "scoreboard_today.json")
TEAM_ADVANCED_FILE = os.path.join(CACHE_DIR, "team_advanced.json")
TEAM_OPPONENT_FILE = os.path.join(CACHE_DIR, "team_opponent.json")
NAME_OVERRIDES_FILE = os.path.join(CACHE_DIR, "name_overrides.json")
ODDS_CACHE_FILE = os.path.join(CACHE_DIR, "odds_today.json")
INJURIES_CACHE_FILE = os.path.join(CACHE_DIR, "injuries_cache_v44.json")
TESES_CACHE_FILE = os.path.join(CACHE_DIR, "teses_cache.json")
DVP_CACHE_FILE = os.path.join(CACHE_DIR, "dvp_cache.json")
AUDIT_CACHE_FILE = os.path.join(CACHE_DIR, "audit_trixies.json")
FEATURE_STORE_FILE = os.path.join(CACHE_DIR, "feature_store.json")
LOGS_CACHE_FILE = os.path.join(CACHE_DIR, "real_game_logs.json")

# Se o arquivo se chama db_manager.py:
from db_manager import db, DatabaseHandler
try:
    db = DatabaseHandler()
    if not db.connected:
        db = None # Garante que é None se falhar, pro código usar fallback
except:
    db = None

# ============================================================================
# 2. FUNÇÕES DE DADOS HÍBRIDOS (CLOUD FIRST)
# ============================================================================
def get_data_universal(key_db, file_fallback=None):
    """Tenta pegar do Supabase. Se falhar, tenta do arquivo local."""
    data = {}
    
    # 1. Tenta Nuvem
    if db:
        try:
            data = db.get_data(key_db)
            if data:
                # --- DEDO DURO ---
                print(f"✅ LENDO '{key_db}' DIRETO DA NUVEM (SUPABASE)!") 
                return data
        except Exception as e:
            print(f"⚠️ Erro nuvem '{key_db}': {e}")
            
    # 2. Tenta Local
    if not data and file_fallback and os.path.exists(file_fallback):
        try:
            with open(file_fallback, "r", encoding="utf-8") as f:
                # --- DEDO DURO ---
                print(f"📁 LENDO '{key_db}' DO ARQUIVO LOCAL (GITHUB)!")
                return json.load(f)
        except: pass
    
    return data

# ============================================================================
# FUNÇÃO SAVE BLINDADA v2 (COM REPORT DE ERRO DETALHADO)
# ============================================================================
def save_data_universal(key_db, data, file_path=None):
    import json
    import time
    
    sucesso_nuvem = False
    
    # 1. Limpeza
    try:
        json_str = json.dumps(data, default=str).replace("NaN", "null").replace("Infinity", "null")
        clean_data = json.loads(json_str)
        size_kb = len(json_str) / 1024
        print(f"📦 [CLEAN] '{key_db}': {size_kb:.2f} KB")
    except Exception as e:
        print(f"⚠️ Erro limpeza '{key_db}': {e}")
        clean_data = data
        size_kb = 0

    # 2. Salva na Nuvem
    if db:
        try:
            start_time = time.time()
            db.save_data(key_db, clean_data) # Tenta salvar
            duration = time.time() - start_time
            print(f"☁️ [UPLOAD] '{key_db}' salvo! ({duration:.2f}s)")
            sucesso_nuvem = True
        except Exception as e:
            # --- AQUI ESTÁ A MUDANÇA: MOSTRAR O ERRO REAL NA TELA ---
            erro_txt = str(e)
            print(f"❌ [ERRO UPLOAD] '{key_db}': {erro_txt}")
            st.error(f"❌ Erro ao salvar '{key_db}' no Supabase: {erro_txt}") 
            # Isso vai imprimir o erro técnico (ex: 413, 500, timeout)
            
    # 3. Salva Local
    if file_path:
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(json_str)
        except: pass
    
    return sucesso_nuvem

# ============================================================================
# 3. CONSTANTES E MAPAS DA NBA
# ============================================================================
SEASON = "2025-26"
TODAY = datetime.now().strftime("%Y-%m-%d")

# --- CHAVES DO BANCO DE DADOS (SUPABASE KEYS) ---
KEY_SCOREBOARD = "scoreboard"
KEY_TEAM_ADV = "team_advanced"
KEY_TEAM_OPP = "team_opponent"
KEY_ODDS = "pinnacle_odds"
KEY_LOGS = "real_game_logs"
KEY_INJURIES = "injuries"
KEY_NAME_OVERRIDES = "name_overrides"
KEY_PLAYERS_MAP = "nba_players_map"
KEY_DVP = "dvp_stats"
KEY_L5 = "l5_stats" # <--- ADICIONE ESTA LINHA NOVA

TEAM_ABBR_TO_ODDS = {
    "ATL": "Atlanta Hawks","BOS": "Boston Celtics","BKN": "Brooklyn Nets","CHA": "Charlotte Hornets",
    "CHI": "Chicago Bulls","CLE": "Cleveland Cavaliers","DAL": "Dallas Mavericks","DEN": "Denver Nuggets",
    "DET": "Detroit Pistons","GSW": "Golden State Warriors","HOU": "Houston Rockets","IND": "Indiana Pacers",
    "LAC": "Los Angeles Clippers","LAL": "Los Angeles Lakers","MEM": "Memphis Grizzlies","MIA": "Miami Heat",
    "MIL": "Milwaukee Bucks","MIN": "Minnesota Timberwolves","NOP": "New Orleans Pelicans","NYK": "New York Knicks",
    "OKC": "Oklahoma City Thunder","ORL": "Orlando Magic","PHI": "Philadelphia 76ers","PHX": "Phoenix Suns",
    "POR": "Portland Trail Blazers","SAC": "Sacramento Kings","SAS": "San Antonio Spurs","TOR": "Toronto Raptors",
    "UTA": "Utah Jazz","WAS": "Washington Wizards",
    "UTAH": "Utah Jazz","NY": "New York Knicks","SA": "San Antonio Spurs","NO": "New Orleans Pelicans"
}

TEAM_NAME_VARIATIONS = {
    "Atlanta Hawks": ["ATL", "Atlanta", "Hawks"],
    "Boston Celtics": ["BOS", "Boston", "Celtics"],
    "Brooklyn Nets": ["BKN", "BRO", "Brooklyn", "Nets"],
    "Charlotte Hornets": ["CHA", "Charlotte", "Hornets"],
    "Chicago Bulls": ["CHI", "Chicago", "Bulls"],
    "Cleveland Cavaliers": ["CLE", "Cleveland", "Cavaliers"],
    "Dallas Mavericks": ["DAL", "Dallas", "Mavericks"],
    "Denver Nuggets": ["DEN", "Denver", "Nuggets"],
    "Detroit Pistons": ["DET", "Detroit", "Pistons"],
    "Golden State Warriors": ["GSW", "GS", "Golden State", "Warriors"],
    "Houston Rockets": ["HOU", "Houston", "Rockets"],
    "Indiana Pacers": ["IND", "Indiana", "Pacers"],
    "Los Angeles Clippers": ["LAC", "LA Clippers", "Clippers"],
    "Los Angeles Lakers": ["LAL", "LA Lakers", "Lakers"],
    "Memphis Grizzlies": ["MEM", "Memphis", "Grizzlies"],
    "Miami Heat": ["MIA", "Miami", "Heat"],
    "Milwaukee Bucks": ["MIL", "Milwaukee", "Bucks"],
    "Minnesota Timberwolves": ["MIN", "Minnesota", "Timberwolves"],
    "New Orleans Pelicans": ["NOP", "NO", "New Orleans", "Pelicans"],
    "New York Knicks": ["NYK", "NY", "New York", "Knicks"],
    "Oklahoma City Thunder": ["OKC", "Oklahoma City", "Thunder"],
    "Orlando Magic": ["ORL", "Orlando", "Magic"],
    "Philadelphia 76ers": ["PHI", "Philadelphia", "76ers"],
    "Phoenix Suns": ["PHX", "PHO", "Phoenix", "Suns"],
    "Portland Trail Blazers": ["POR", "Portland", "Trail Blazers", "Blazers"],
    "Sacramento Kings": ["SAC", "Sacramento", "Kings"],
    "San Antonio Spurs": ["SAS", "SA", "San Antonio", "Spurs"],
    "Toronto Raptors": ["TOR", "Toronto", "Raptors"],
    "Utah Jazz": ["UTA", "UTAH", "Utah", "Jazz"],
    "Washington Wizards": ["WAS", "WSH", "Washington", "Wizards"]
}

TEAM_NORMALIZATION_MAP = {
    "PHO": "PHX", "GS": "GSW", "NY": "NYK", "NO": "NOP", "NOH": "NOP",
    "SA": "SAS", "WSH": "WAS", "UTAH": "UTA", "BK": "BKN", "BRK": "BKN",
    "LA": "LAL", "CHO": "CHA"
}

# ============================================================================
# 4. FUNÇÕES UTILITÁRIAS ESSENCIAIS
# ============================================================================
def normalize_team(team_code):
    if not team_code: return ""
    code = str(team_code).upper().strip()
    return TEAM_NORMALIZATION_MAP.get(code, code)

def get_full_team_name(team_abbr):
    team_abbr = team_abbr.upper() if team_abbr else ""
    full_name = TEAM_ABBR_TO_ODDS.get(team_abbr)
    if full_name: return full_name
    for full_name, variations in TEAM_NAME_VARIATIONS.items():
        if team_abbr in variations or team_abbr == full_name:
            return full_name
    return team_abbr

def ensure_dataframe(df) -> pd.DataFrame:
    if isinstance(df, pd.DataFrame): return df
    if df is None: return pd.DataFrame()
    if isinstance(df, list) and all(isinstance(x, dict) for x in df): return pd.DataFrame(df)
    if isinstance(df, dict):
        try: return pd.DataFrame.from_dict(df)
        except: return pd.DataFrame([df])
    return pd.DataFrame()

# ============================================================================
# 5. CARREGAMENTO DE MÓDULOS (BLINDAGEM TOTAL v3.1 - COM ALIAS)
# ============================================================================

# 1. Definição Inicial (Evita NameError se o import falhar)
PaceAdjuster = None
VacuumMatrixAnalyzer = None
DvPAnalyzer = None
DvpAnalyzer = None # <--- ADICIONADO PARA EVITAR O ERRO
InjuryMonitor = None
PlayerClassifier = None
CorrelationValidator = None
RotationAnalyzer = None
NarrativeFormatter = None
ThesisEngine = None
StrategyEngine = None
StrategyIdentifier = None
ArchetypeEngine = None
RotationCeilingEngine = None
SinergyEngine = None
AuditSystem = None
PinnacleClient = None
MomentumEngine = None
DesdobradorInteligente = None

# 2. Definição de Flags
NOVOS_MODULOS_DISPONIVEIS = False
PACE_ADJUSTER_AVAILABLE = False
VACUUM_MATRIX_AVAILABLE = False
DVP_ANALYZER_AVAILABLE = False
INJURY_MONITOR_AVAILABLE = False
PLAYER_CLASSIFIER_AVAILABLE = False
CORRELATION_FILTERS_AVAILABLE = False
ROTATION_CEILING_AVAILABLE = False
SINERGY_ENGINE_AVAILABLE = False
PINNACLE_AVAILABLE = False
MOMENTUM_AVAILABLE = False
AUDIT_AVAILABLE = False

print("🔄 Inicializando Módulos do Sistema...")

# --- FUNÇÃO AUXILIAR DE IMPORTAÇÃO ---
def safe_import(module_name, class_name):
    """Tenta importar de múltiplos locais (Raiz, modules, new_modules)"""
    # 1. Tenta Raiz
    try:
        mod = __import__(module_name, fromlist=[class_name])
        cls = getattr(mod, class_name)
        return cls
    except (ImportError, AttributeError): pass

    # 2. Tenta modules.new_modules
    try:
        mod = __import__(f"modules.new_modules.{module_name}", fromlist=[class_name])
        cls = getattr(mod, class_name)
        return cls
    except (ImportError, AttributeError): pass

    # 3. Tenta modules
    try:
        mod = __import__(f"modules.{module_name}", fromlist=[class_name])
        cls = getattr(mod, class_name)
        return cls
    except (ImportError, AttributeError): return None

# --- CARREGAMENTO REAL ---
try:
    # Core Estratégico
    try:
        ThesisEngine = safe_import("thesis_engine", "ThesisEngine")
        StrategyEngine = safe_import("strategy_engine", "StrategyEngine")
        NarrativeFormatter = safe_import("narrative_formatter", "NarrativeFormatter")
        RotationAnalyzer = safe_import("rotation_analyzer", "RotationAnalyzer")
        StrategyIdentifier = safe_import("strategy_identifier", "StrategyIdentifier")
        if ThesisEngine and StrategyEngine: NOVOS_MODULOS_DISPONIVEIS = True
    except: pass

    # Componentes Nexus & Stats
    PaceAdjuster = safe_import("pace_adjuster", "PaceAdjuster")
    if PaceAdjuster: PACE_ADJUSTER_AVAILABLE = True

    VacuumMatrixAnalyzer = safe_import("vacuum_matrix", "VacuumMatrixAnalyzer")
    if VacuumMatrixAnalyzer: VACUUM_MATRIX_AVAILABLE = True

    # Tenta DvP (Grafia Preferencial: DvPAnalyzer)
    DvPAnalyzer = safe_import("dvp_analyzer", "DvPAnalyzer")
    if not DvPAnalyzer: 
        DvPAnalyzer = safe_import("dvp_analyzer", "DvpAnalyzer") # Tenta grafia alternativa
    
    if DvPAnalyzer: DVP_ANALYZER_AVAILABLE = True

    PlayerClassifier = safe_import("player_classifier", "PlayerClassifier")
    if PlayerClassifier: PLAYER_CLASSIFIER_AVAILABLE = True

    SinergyEngine = safe_import("sinergy_engine", "SinergyEngine")
    if SinergyEngine: SINERGY_ENGINE_AVAILABLE = True

    CorrelationValidator = safe_import("correlation_filters", "CorrelationValidator")
    if CorrelationValidator: CORRELATION_FILTERS_AVAILABLE = True

    ArchetypeEngine = safe_import("archetype_engine", "ArchetypeEngine")

    MomentumEngine = safe_import("momentum", "MomentumEngine")
    if MomentumEngine: MOMENTUM_AVAILABLE = True

    DesdobradorInteligente = safe_import("desdobrador_inteligente", "DesdobradorInteligente")

    # Raiz / Legado
    try:
        from injuries import InjuryMonitor
        INJURY_MONITOR_AVAILABLE = True
    except ImportError:
        InjuryMonitor = safe_import("injury_monitor", "InjuryMonitor")
        if InjuryMonitor: INJURY_MONITOR_AVAILABLE = True

    # Audit System
    AuditSystem = safe_import("audit_system", "AuditSystem")
    if AuditSystem: AUDIT_AVAILABLE = True

    # Pinnacle
    try:
        from pinnacle_client import PinnacleClient
        PINNACLE_AVAILABLE = True
    except ImportError:
        class PinnacleClient: 
            def __init__(self, *args, **kwargs): pass
            def get_nba_games(self): return []
            def get_player_props(self, game_id): return []

    # ========================================================================
    # 6. ALIASES DE COMPATIBILIDADE (A CORREÇÃO DO SEU ERRO ESTÁ AQUI)
    # ========================================================================
    # Garante que 'DvpAnalyzer' (minúsculo) aponte para a classe correta
    DvpAnalyzer = DvPAnalyzer 

    print("✅ Carregamento de módulos concluído.")

except Exception as e:
    print(f"⚠️ Erro Crítico no Bloco de Imports: {e}")

# ============================================================================
# 6. AUTENTICAÇÃO E SESSION STATE
# ============================================================================

# Variáveis Globais de Controle de Features (Configuração Padrão)
FEATURE_CONFIG_DEFAULT = {
    "PACE_ADJUSTER": {"active": PACE_ADJUSTER_AVAILABLE, "min_pace_threshold": 98, "adjustment_factor": 0.02},
    "DYNAMIC_THRESHOLDS": {"active": True, "position_multipliers": {"PG": {"AST": 1.5}, "C": {"REB": 2.0}}},
    "STRATEGIC_ENGINE": {"active": NOVOS_MODULOS_DISPONIVEIS, "min_confidence": 0.6},
    "CORRELATION_FILTERS": {"active": CORRELATION_FILTERS_AVAILABLE, "max_similarity_score": 0.7}
}

# Configura Session State Padrão
if 'df_l5' not in st.session_state: st.session_state.df_l5 = pd.DataFrame()
if 'scoreboard' not in st.session_state: st.session_state.scoreboard = []
if 'use_advanced_features' not in st.session_state: st.session_state.use_advanced_features = False
if 'advanced_features_config' not in st.session_state: st.session_state.advanced_features_config = FEATURE_CONFIG_DEFAULT

# ============================================================================
# FUNÇÃO LOGS: MODO TURBO v2 (COM TIMEOUT DE 120s)
# ============================================================================
def fetch_and_upload_real_game_logs(progress_ui=True):
    from nba_api.stats.endpoints import leaguegamelog
    import pandas as pd
    import json
    from datetime import datetime
    
    SEASON_CURRENT = "2025-26"
    KEY_LOGS = "real_game_logs"

    if progress_ui:
        status_box = st.status("🚀 MODO TURBO: Baixando temporada inteira...", expanded=True)
        status_box.write("📡 Conectando ao servidor da NBA (LeagueGameLog)...")

    try:
        # --- CORREÇÃO: TIMEOUT AUMENTADO PARA 120 SEGUNDOS ---
        logs_api = leaguegamelog.LeagueGameLog(
            season=SEASON_CURRENT,
            player_or_team_abbreviation='P', 
            direction='DESC', 
            sorter='DATE',
            timeout=120  # <--- AQUI ESTÁ A CORREÇÃO DO TIMEOUT
        )
        df_all = logs_api.get_data_frames()[0]
        
        if df_all.empty:
            if progress_ui: status_box.error("A API retornou vazio.")
            return {}

        if progress_ui: status_box.write(f"📦 Processando {len(df_all)} registros de jogos...")

        # Processamento Local
        cols_int = ['PTS', 'REB', 'AST', 'STL', 'BLK', 'FGA', 'FG3M', 'TOV', 'PF']
        for c in cols_int:
            if c in df_all.columns: df_all[c] = df_all[c].fillna(0).astype(int)

        df_all['GAME_DATE'] = pd.to_datetime(df_all['GAME_DATE'])
        df_all = df_all.sort_values(by=['PLAYER_ID', 'GAME_DATE'], ascending=[True, False])

        results = {}
        unique_players = df_all['PLAYER_ID'].unique()
        total_p = len(unique_players)
        processed_count = 0
        bar = status_box.progress(0)
        
        for pid in unique_players:
            player_games = df_all[df_all['PLAYER_ID'] == pid].head(30)
            if player_games.empty: continue
            
            p_name = player_games.iloc[0]['PLAYER_NAME']
            p_team = player_games.iloc[0]['TEAM_ABBREVIATION']
            
            clean_logs = {}
            for col in cols_int:
                if col in player_games.columns:
                    clean_logs[col] = player_games[col].tolist()
            
            if 'MIN' in player_games.columns:
                def parse_min(x):
                    try: return float(x)
                    except: return 0.0
                clean_logs['MIN'] = player_games['MIN'].apply(parse_min).tolist()

            results[p_name] = {
                "name": p_name,
                "id": int(pid),
                "team": p_team,
                "logs": clean_logs,
                "updated_at": datetime.now().strftime("%Y-%m-%d")
            }
            
            processed_count += 1
            if progress_ui and processed_count % 100 == 0:
                bar.progress(processed_count / total_p)

        # Salva no Supabase
        if results:
            if progress_ui: status_box.write("☁️ Enviando pacote para Nuvem...")
            save_data_universal(KEY_LOGS, results)
            
            if progress_ui:
                status_box.update(label=f"⚡ TURBO COMPLETO! {len(results)} jogadores atualizados.", state="complete", expanded=False)
        
        return results

    except Exception as e:
        # Mostra o erro exato na tela
        if progress_ui: status_box.error(f"Erro no Modo Turbo: {e}")
        print(f"Erro Turbo: {e}")
        return {}

# ==============================================================================
# FUNÇÃO PONTE (WRAPPER) PARA COMPATIBILIDADE
# ==============================================================================
def update_batch_cache(games_list, force_all=False):
    """
    Função de compatibilidade. 
    O botão 'Reconstruir Cache' chama esta função.
    Ela redireciona para o novo Motor Turbo V3.0.
    """
    import streamlit as st
    
    # Ignoramos 'games_list' e 'force_all' porque o Turbo V3 baixa a liga inteira
    # de uma vez só, o que é mais seguro e garante que temos todos os jogadores.
    
    st.toast("Iniciando Motor Turbo V3...", icon="🚀")
    
    # Chama a função V3.0 (que você deve ter colado anteriormente)
    return fetch_and_upload_real_game_logs(progress_ui=True)
    

# ==============================================================================
# 8. DATA FETCHING & NORMALIZATION (ATUALIZADO V59.0)
# ==============================================================================    
def normalize_cache_keys(cache_data):
    """Normaliza chaves antigas se existirem no JSON."""
    if not cache_data: return {}
    for name, data in cache_data.items():
        if 'logs' not in data: continue
        logs = data['logs']
        
        # Mapeamento
        if 'FG3M' in logs and '3PM' not in logs: logs['3PM'] = logs['FG3M']
        if 'FG3A' in logs and '3PA' not in logs: logs['3PA'] = logs['FG3A']
        
        # Fallback se FG3A não existir (usa FGA como proxy aproximado ou 0)
        if '3PA' not in logs: 
            logs['3PA'] = logs.get('FGA', [0]*len(logs.get('PTS', [])))
        
        data['logs'] = logs
    return cache_data  
# ============================================================================
# 8. FUNÇÕES DE FETCH ESTATÍSTICO (STATSMANAGER)
# ============================================================================
def fetch_real_time_team_stats():
    """Busca Pace e Stats Defensivos via NBA API"""
    try:
        from nba_api.stats.endpoints import leaguedashteamstats
        stats = leaguedashteamstats.LeagueDashTeamStats(
            measure_type_detailed_defense='Advanced',
            season=SEASON,
            per_mode_detailed='PerGame'
        )
        df = stats.get_data_frames()[0]
        if df.empty: return None

        advanced_data = {}
        for _, row in df.iterrows():
            team_abbr = row.get('TEAM_ABBREVIATION', row['TEAM_NAME'][:3].upper())
            advanced_data[team_abbr] = {
                "PACE": float(row['PACE']),
                "OFF_RATING": float(row['OFF_RATING']),
                "DEF_RATING": float(row['DEF_RATING']),
                "NET_RATING": float(row['NET_RATING']),
                "TS_PCT": float(row['TS_PCT'])
            }
        
        save_data_universal("team_advanced", advanced_data, TEAM_ADVANCED_FILE)
        st.session_state.team_advanced = advanced_data
        return advanced_data
    except Exception as e:
        print(f"Erro Team Stats: {e}")
        return None

# Helpers de Inicialização
# ============================================================================
# DIAG
# ============================================================================
def show_cloud_diagnostics():
    """
    Mostra um painel de debug na Sidebar para conferir o Supabase.
    """
    st.sidebar.divider()
    with st.sidebar.expander("☁️ Diagnóstico de Nuvem", expanded=False):
        if st.button("🔍 Testar Conexão Supabase"):
            
            # Lista de chaves críticas
            keys_to_check = [
                "scoreboard", 
                "l5_stats",    # O polêmico L5
                "real_game_logs", 
                "injuries", 
                "pinnacle_odds",
                "team_advanced",
                "audit_trixies"
            ]
            
            st.write("Verificando Supabase...")
            
            for key in keys_to_check:
                try:
                    # Tenta baixar APENAS o header ou tamanho se possível, 
                    # mas como sua API é simples, baixamos e vemos se é None.
                    data = get_data_universal(key)
                    
                    if data:
                        # Calcula tamanho aproximado
                        size = len(json.dumps(data, default=str)) / 1024
                        st.success(f"✅ **{key}**: OK ({size:.1f} KB)")
                    else:
                        st.error(f"❌ **{key}**: VAZIO / NÃO ENCONTRADO")
                        
                except Exception as e:
                    st.error(f"⚠️ **{key}**: Erro ({e})")
            
            st.caption("Se 'l5_stats' estiver vermelho, ele não foi salvo.")

# ============================================================================
# ORACLE ENGINE (CÉREBRO MATEMÁTICO)
# ============================================================================
class OracleEngine:
    def __init__(self, logs_cache, injuries_data):
        self.logs = logs_cache
        self.injuries_names = self._process_injuries(injuries_data)

    def _process_injuries(self, raw_injuries):
        """Cria uma 'Blocklist' de nomes normalizados de jogadores lesionados (OUT)."""
        blacklist = set()
        if not raw_injuries: return blacklist
        
        # Flattening da lista de lesões (Mesma lógica do Depto Médico)
        flat_list = []
        stack = [raw_injuries]
        while stack:
            curr = stack.pop()
            if isinstance(curr, list):
                if curr and isinstance(curr[0], dict) and ('player' in curr[0] or 'name' in curr[0]):
                    flat_list.extend(curr)
                else: stack.extend(curr)
            elif isinstance(curr, dict):
                for v in curr.values(): stack.append(v)
        
        for item in flat_list:
            # Só bloqueia quem está OUT. GTD/Day-to-Day a gente projeta (com risco).
            status = str(item.get('status', '')).upper()
            if "OUT" in status:
                name = item.get('player') or item.get('name')
                if name:
                    # Normaliza: "LeBron James" -> "LEBRONJAMES"
                    clean = str(name).upper().replace(" ", "").replace(".", "").replace("'", "").strip()
                    blacklist.add(clean)
        return blacklist

    def generate_projections(self, limit=10):
        """
        Gera as projeções matemáticas ponderadas (50% L5, 30% L10, 20% L25).
        Retorna: Lista de dicionários ordenada pela maior projeção de Pontos (por enquanto).
        """
        projections = []
        
        for player_name, data in self.logs.items():
            # 1. Filtro de Lesão
            clean_name = str(player_name).upper().replace(" ", "").replace(".", "").replace("'", "").strip()
            if clean_name in self.injuries_names:
                continue # Pula jogador lesionado

            # 2. Dados Básicos
            team = data.get('team')
            logs = data.get('logs', {})
            pts_log = logs.get('PTS', [])
            
            # Filtro Mínimo: Precisa ter jogado pelo menos 5 jogos recentes
            if len(pts_log) < 5: continue
            
            # 3. Matemática do Oráculo (Weighted Average)
            def calculate_oracle_stat(values_list):
                if not values_list: return 0.0
                # Garante janelas
                l5 = values_list[:5]
                l10 = values_list[:10]
                l25 = values_list[:25] # Pega tudo se tiver menos que 25
                
                avg_l5 = sum(l5) / len(l5)
                avg_l10 = sum(l10) / len(l10)
                avg_l25 = sum(l25) / len(l25)
                
                # Fórmula: 50% Forma + 30% Médio Prazo + 20% Histórico
                weighted = (avg_l5 * 0.50) + (avg_l10 * 0.30) + (avg_l25 * 0.20)
                return weighted

            proj_pts = calculate_oracle_stat(logs.get('PTS', []))
            proj_reb = calculate_oracle_stat(logs.get('REB', []))
            proj_ast = calculate_oracle_stat(logs.get('AST', []))
            proj_3pm = calculate_oracle_stat(logs.get('3PM', [])) # Supondo que tenhamos 3PM, senão zerar

            # Só mostra jogadores relevantes (Projeção > 15 PTS)
            if proj_pts < 15: continue

            projections.append({
                "name": player_name,
                "team": team,
                "PTS": proj_pts,
                "REB": proj_reb,
                "AST": proj_ast,
                "3PM": proj_3pm
            })

        # Ordena pelos maiores pontuadores (Estrelas do Show)
        # Futuramente podemos ordenar por "Value" se tivermos as lines das casas
        return sorted(projections, key=lambda x: x['PTS'], reverse=True)[:limit]

# ============================================================================
# PÁGINA: ORACLE PROJECTIONS (V3.1 - FIX DUPLICATE COLS)
# ============================================================================
# ============================================================================
# PÁGINA: ORACLE PROJECTIONS (V4 - ESTRELAS BLINDADAS & LOGO FIX)
# ============================================================================
# ============================================================================
# PÁGINA: ORÁCULO (CORRIGIDA - SEM DUPLICATAS)
# ============================================================================
def show_oracle_page():
    import os
    import pandas as pd
    import streamlit as st
    import re
    import unicodedata
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
    from matplotlib.offsetbox import OffsetImage, AnnotationBbox
    import io
    from urllib.request import urlopen
    from PIL import Image

    # --- 1. CSS VISUAL ---
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Oswald:wght@400;500;600;700&family=Inter:wght@400;600&display=swap');
        
        [data-testid="stVerticalBlockBorderWrapper"] {
            background: linear-gradient(90deg, #0f172a 0%, #1e293b 100%) !important;
            border: 1px solid #334155 !important;
            border-left: 4px solid #D4AF37 !important;
            padding: 8px !important; margin-bottom: 8px !important; border-radius: 6px !important;
        }
        .oracle-name { font-family: 'Oswald'; font-size: 16px; color: #fff; line-height: 1.1; margin-bottom: 2px; }
        .oracle-meta { font-family: 'Inter'; font-size: 10px; color: #94a3b8; display: flex; align-items: center; gap: 4px; }
        .stat-box { background: rgba(30, 41, 59, 0.5); border-radius: 4px; padding: 3px 0; text-align: center; border: 1px solid #334155; }
        .stat-val { font-family: 'Oswald'; font-size: 18px; font-weight: bold; line-height: 1; }
        .stat-lbl { font-family: 'Inter'; font-size: 8px; color: #64748b; font-weight: 700; margin-top: -2px; }
        .c-gold { color: #fbbf24; } .c-red { color: #f87171; } .c-blue { color: #60a5fa; } .c-green { color: #4ade80; }
    </style>
    """, unsafe_allow_html=True)

    # --- HELPERS ---
    def clean_key(text):
        if not text: return ""
        try:
            t = str(text)
            nfkd = unicodedata.normalize('NFKD', t)
            t = "".join([c for c in nfkd if not unicodedata.combining(c)])
            # Remove sufixos comuns que atrapalham o match
            t = t.replace(" Jr.", "").replace(" Sr.", "").replace(" III", "")
            # Remove tudo que não for letra A-Z
            return re.sub(r'[^A-Z]', '', t.upper())
        except: return ""

    def get_espn_logo_url(nba_code):
        """Traduz código NBA para código de URL da ESPN"""
        if not nba_code: return "https://a.espncdn.com/i/teamlogos/nba/500/unk.png"
        
        code = str(nba_code).upper().strip()
        
        # Mapeamento Oficial ESPN URLs
        mapping = {
            "UTA": "utah", "UTAH": "utah",
            "NOP": "no", "NO": "no", "NOH": "no",
            "GSW": "gs", "GS": "gs",
            "NYK": "ny", "NY": "ny",
            "SAS": "sa", "SA": "sa",
            "PHX": "phx", "PHO": "phx",
            "WAS": "wsh", "WSH": "wsh",
            "BKN": "bkn", "BRK": "bkn",
            "CHA": "cha", "CHO": "cha"
        }
        
        slug = mapping.get(code, code.lower())
        return f"https://a.espncdn.com/i/teamlogos/nba/500/{slug}.png"

    # --- 2. HEADER ---
    c1, c2 = st.columns([8, 2])
    with c1:
        st.markdown("""
        <div style="display:flex; align-items:center; gap:10px; margin-bottom:15px;">
            <img src="https://i.ibb.co/TxfVPy49/Sem-t-tulo.png" width="35">
            <div>
                <div style="font-family:'Oswald'; font-size:22px; color:#D4AF37; line-height:1;">ORACLE PROJECTIONS</div>
                <div style="font-family:'Inter'; font-size:10px; color:#64748b;">POWERED BY DIGIBETS AI</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # --- 3. DADOS ---
    full_cache = get_data_universal("real_game_logs", os.path.join("cache", "real_game_logs.json"))
    injuries_data = get_data_universal('injuries') or get_data_universal('injuries_cache_v44')
    df_l5 = st.session_state.get('df_l5', pd.DataFrame()) 
    
    if not full_cache:
        st.warning("⚠️ Aguardando dados... Por favor, atualize o L5 na Config.")
        return

    # --- 4. MAPA DE ROSTER (DYNAMIC) ---
    roster_map = {}
    if not df_l5.empty:
        # Normaliza colunas
        df_l5.columns = [str(c).upper().strip() for c in df_l5.columns]
        # Remove duplicatas
        df_l5 = df_l5.loc[:, ~df_l5.columns.duplicated()]

        col_name = next((c for c in ['PLAYER_NAME', 'PLAYER', 'NAME', 'FULL_NAME'] if c in df_l5.columns), None)
        col_id = next((c for c in ['PLAYER_ID', 'ID', 'PERSON_ID'] if c in df_l5.columns), None)
        col_team = next((c for c in ['TEAM_ABBREVIATION', 'TEAM', 'TEAM_ID'] if c in df_l5.columns), None)
        
        if col_name and col_id:
            for _, row in df_l5.iterrows():
                try:
                    k = clean_key(row[col_name])
                    raw_id = row[col_id]
                    if isinstance(raw_id, pd.Series): raw_id = raw_id.iloc[0]
                    val_id = int(float(raw_id)) if pd.notnull(raw_id) else 0
                    
                    raw_team = row[col_team] if col_team else "UNK"
                    if isinstance(raw_team, pd.Series): raw_team = raw_team.iloc[0]

                    # Só salva se tiver ID válido
                    if val_id > 0:
                        roster_map[k] = {'id': val_id, 'team': str(raw_team).upper()}
                except: continue

    # --- 5. BUNKER DE ESTRELAS (STATIC BACKUP) ---
    ELITE_DB_BACKUP = {
        "NIKOLAJOKIC": 203999, "LUKADONCIC": 1629029, "GIANNISANTETOKOUNMPO": 203507,
        "SHAIGILGEOUSALEXANDER": 1628983, "JOELEMBIID": 203954, "JAYSONTATUM": 1628369,
        "STEPHENCURRY": 201939, "KEVINDURANT": 201142, "LEBRONJAMES": 2544,
        "ANTHONYDAVIS": 203076, "DEVINBOOKER": 1626164, "ANTHONYEDWARDS": 1630162,
        "JALENBRUNSON": 1628973, "KAWHILEONARD": 202695, "TYRESEMAXEY": 1630178,
        "DONOVANMITCHELL": 1628378, "JAYLENBROWN": 1627759, "DAMIANLILLARD": 203081,
        "KYRIEIRVING": 202681, "PAULGEORGE": 202331, "JAMESHARDEN": 201935,
        "VICTORWEMBANYAMA": 1641705, "TRAEYOUNG": 1629027, "DEARRONFOX": 1628368,
        "DOMANTASSABONIS": 1627734, "TYRESEHALIBURTON": 1630169, "BAMADEBAYO": 1628389,
        "ZIONWILLIAMSON": 1629627, "JACOMPLETE": 1629630, "JAMORANT": 1629630,
        "JIMMYBUTLER": 202710, "PAOLOBANCHERO": 1631094, "CHETHOLMGREN": 1631096,
        "SCOTTIEBARNES": 1630567, "LAMELOBALL": 1630163, "ALPERENSENGUN": 1630578,
        "JULIUSERANDLE": 203944, "PASCALSIAKAM": 1627783, "KRISTAPSPORZINGIS": 204001,
        "JRUEHOLIDAY": 201950, "DERRICKWHITE": 1628401, "JAMALMURRAY": 1627750,
        "KARLANTHONYTOWNS": 1626157, "RUDYGOBERT": 203497, "LAURIMARKKANEN": 1628374,
        "DESMONDBANE": 1630217, "JARENJACKSONJR": 1628991, "DEJOUNTEMURRAY": 1627749,
        "FRANZWAGNER": 1630532, "EVANMOBLEY": 1630596, "CADECUNNINGHAM": 1630595
    }

    # --- 6. ENGINE ---
    try:
        engine = OracleEngine(full_cache, injuries_data)
        # Pede um pouco mais de projeções para poder filtrar duplicatas depois
        projections_raw = engine.generate_projections(limit=25) 
    except Exception as e:
        st.error(f"Erro no Motor do Oráculo: {e}")
        return

    if not projections_raw:
        st.info("Calibrando o Oráculo...")
        return

    # --- 7. RENDERIZAÇÃO COM DEDUPLICAÇÃO ---
    st.markdown("""<div style="display:flex; justify-content:flex-end; padding-right:15px; margin-bottom:5px; font-family:'Oswald'; font-size:10px; color:#64748b; gap:40px;"><span>PTS</span> <span>REB</span> <span>AST</span> <span>3PM</span></div>""", unsafe_allow_html=True)

    snapshot_list = []
    seen_players = set() # SET PARA EVITAR DUPLICATAS VISUAIS
    
    # Processa e Filtra
    for p in projections_raw:
        raw_name = p['name']
        search_key = clean_key(raw_name)
        
        # --- FILTRO DE DUPLICATAS ---
        if search_key in seen_players:
            continue # Pula se já processamos esse jogador
        seen_players.add(search_key)
        # ----------------------------

        pid = 0
        real_team = "UNK"
        
        # 1. Tenta Busca Dinâmica (L5)
        if search_key in roster_map:
            pid = roster_map[search_key]['id']
            real_team = roster_map[search_key]['team']
        
        # 2. Backup Manual
        if pid == 0:
            pid = ELITE_DB_BACKUP.get(search_key, 0)
            if real_team == "UNK":
                real_team = str(p.get('team', 'UNK')).upper()

        # URL FOTO
        if pid > 0: 
            photo_url = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"
        else: 
            photo_url = "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"

        # URL LOGO
        logo_url = get_espn_logo_url(real_team)

        snapshot_list.append({
            'name': raw_name, 'team': real_team, 
            'pts': p['PTS'], 'reb': p['REB'], 'ast': p['AST'], '3pm': p['3PM'],
            'pid': pid 
        })
        
        # Limita a exibição visual a 10 cards, mesmo que tenha processado mais
        if len(snapshot_list) <= 10:
            with st.container(border=True):
                c_img, c_info, c1, c2, c3, c4 = st.columns([1, 3, 1.2, 1.2, 1.2, 1.2])
                with c_img: 
                    st.image(photo_url, use_container_width=True)
                with c_info:
                    st.markdown(f'<div class="oracle-name">{raw_name}</div>', unsafe_allow_html=True)
                    st.markdown(f"""
                    <div class="oracle-meta">
                        <img src="{logo_url}" width="14" style="vertical-align:middle;" onerror="this.style.display='none'"> 
                        <span style="font-size:10px; color:#cbd5e1;">{real_team}</span>
                    </div>""", unsafe_allow_html=True)
                
                with c1: st.markdown(f'<div class="stat-box"><div class="stat-val c-gold">{p["PTS"]:.1f}</div><div class="stat-lbl">PTS</div></div>', unsafe_allow_html=True)
                with c2: st.markdown(f'<div class="stat-box"><div class="stat-val c-red">{p["REB"]:.1f}</div><div class="stat-lbl">REB</div></div>', unsafe_allow_html=True)
                with c3: st.markdown(f'<div class="stat-box"><div class="stat-val c-blue">{p["AST"]:.1f}</div><div class="stat-lbl">AST</div></div>', unsafe_allow_html=True)
                with c4: st.markdown(f'<div class="stat-box"><div class="stat-val c-green">{p["3PM"]:.1f}</div><div class="stat-lbl">3PM</div></div>', unsafe_allow_html=True)

    # --- 8. SNAPSHOT PROFISSIONAL (DESIGNER MODE) ---
    def create_professional_snapshot(data_list):
        # Limita o snapshot aos top 10 para não ficar gigante
        data_list = data_list[:10]
        
        BG_COLOR = "#0f172a"
        CARD_BG = "#1e293b"
        TEXT_WHITE = "#F8FAFC"
        TEXT_GRAY = "#94A3B8"
        COLOR_GOLD = "#FBBF24"
        COLOR_RED = "#F87171"
        COLOR_BLUE = "#60A5FA"
        COLOR_GREEN = "#4ADE80"
        
        num_items = len(data_list)
        fig_height = 2 + (num_items * 1.2)
        fig, ax = plt.subplots(figsize=(8, fig_height))
        
        fig.patch.set_facecolor(BG_COLOR)
        ax.set_facecolor(BG_COLOR)
        ax.set_xlim(0, 100)
        ax.set_ylim(0, 100)
        ax.axis('off')

        # Header
        try:
            logo_url = "https://i.ibb.co/TxfVPy49/Sem-t-tulo.png"
            with urlopen(logo_url) as url:
                logo_img = Image.open(url)
                imagebox = OffsetImage(logo_img, zoom=0.15)
                ab = AnnotationBbox(imagebox, (10, 95), frameon=False, box_alignment=(0, 0.5))
                ax.add_artist(ab)
        except: pass

        ax.text(20, 96, "ORACLE PROJECTIONS", color=COLOR_GOLD, fontsize=20, weight='bold', fontname='DejaVu Sans')
        ax.text(20, 92, "DIGIBETS AI PREDICTIVE MODEL", color=TEXT_GRAY, fontsize=9, fontname='DejaVu Sans')
        ax.plot([5, 95], [89, 89], color=TEXT_GRAY, lw=0.5, alpha=0.5)
        
        header_y = 86
        ax.text(12, header_y, "PLAYER", color=TEXT_GRAY, fontsize=8, weight='bold')
        ax.text(55, header_y, "PTS", color=COLOR_GOLD, fontsize=8, weight='bold', ha='center')
        ax.text(68, header_y, "REB", color=COLOR_RED, fontsize=8, weight='bold', ha='center')
        ax.text(81, header_y, "AST", color=COLOR_BLUE, fontsize=8, weight='bold', ha='center')
        ax.text(94, header_y, "3PM", color=COLOR_GREEN, fontsize=8, weight='bold', ha='center')

        start_y = 82
        row_height = 8
        
        for i, item in enumerate(data_list):
            y_pos = start_y - (i * row_height)
            
            rect = patches.Rectangle((5, y_pos - 3), 90, 6, linewidth=0, edgecolor='none', facecolor=CARD_BG, alpha=0.6)
            ax.add_patch(rect)
            
            ax.text(12, y_pos + 1, item['name'], color=TEXT_WHITE, fontsize=11, weight='bold')
            ax.text(12, y_pos - 1.5, f"{item['team']} • NBA", color=TEXT_GRAY, fontsize=7)
            
            circle = patches.Circle((8, y_pos), 2, color='#334155')
            ax.add_patch(circle)
            
            ax.text(55, y_pos - 0.5, f"{item['pts']:.1f}", color=COLOR_GOLD, fontsize=12, weight='bold', ha='center')
            ax.text(68, y_pos - 0.5, f"{item['reb']:.1f}", color=COLOR_RED, fontsize=12, weight='bold', ha='center')
            ax.text(81, y_pos - 0.5, f"{item['ast']:.1f}", color=COLOR_BLUE, fontsize=12, weight='bold', ha='center')
            ax.text(94, y_pos - 0.5, f"{item['3pm']:.1f}", color=COLOR_GREEN, fontsize=12, weight='bold', ha='center')

        ax.text(50, 2, "Gerado automaticamente por DigiBets SuiteNAS", color=TEXT_GRAY, fontsize=6, ha='center', alpha=0.5)

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=200, bbox_inches='tight', facecolor=BG_COLOR)
        buf.seek(0)
        plt.close(fig)
        return buf

    with c2:
        if snapshot_list:
            img = create_professional_snapshot(snapshot_list)
            st.download_button("📸 Baixar Pro", data=img, file_name="oracle_pro_card.png", mime="image/png")
            
            
# ============================================================================
# PROPS ODDS PAGE (CORRIGIDA)
# ============================================================================
def show_props_odds_page():
    st.header("🔥 Las Vegas Sync")

    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Atualizar Jogos do Dia (Spreads/Totais)"):
            with st.spinner("Conectando à Pinnacle..."):
                # --- LÓGICA RECRIADA AQUI (Substituindo update_pinnacle_data) ---
                try:
                    # Instancia o cliente importado (certifique-se que o import está no topo)
                    client = PinnacleClient("13e1dd2e12msh72d0553fca0e8aap16eeacjsn9d69ddb0d2bb")
                    
                    # 1. Busca Jogos
                    games = client.get_nba_games()
                    
                    if not games:
                        st.warning("Nenhum jogo com odds abertas encontrado.")
                    else:
                        # 2. Salva no Session State
                        st.session_state['pinnacle_games'] = games
                        
                        # Cria mapa rápido
                        odds_map = {}
                        for g in games:
                            odds_map[g['home_team']] = g
                            odds_map[g['away_team']] = g
                        
                        st.session_state['pinnacle_odds_map'] = odds_map
                        st.success(f"✅ Odds Atualizadas! {len(games)} jogos carregados.")
                        
                except Exception as e:
                    st.error(f"Erro ao conectar na Pinnacle: {e}")

    with col2:
        if st.button("🎯 Sincronizar Player Props"):
            if 'pinnacle_games' not in st.session_state:
                st.error("Primeiro atualize os jogos do dia (Botão da esquerda)!")
            else:
                games = st.session_state['pinnacle_games']
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    client = PinnacleClient("13e1dd2e12msh72d0553fca0e8aap16eeacjsn9d69ddb0d2bb")
                    full_map = {}
                    total = len(games)
                    
                    for i, game in enumerate(games):
                        status_text.text(f"Processando {i+1}/{total}: {game['away_team']} @ {game['home_team']}")
                        props = client.get_player_props(game['game_id'])
                        for p in props:
                            name = p['player']
                            if name not in full_map:
                                full_map[name] = {}
                            
                            # Salva a linha e a odd
                            full_map[name][p['market']] = {
                                "line": p['line'],
                                "odds": p['odds']
                            }
                        progress_bar.progress((i + 1) / total)
                    
                    st.session_state['pinnacle_props_map'] = full_map
                    progress_bar.empty()
                    status_text.empty()
                    
                    total_props = sum(len(v) for v in full_map.values())
                    st.success(f"✅ {len(full_map)} jogadores | {total_props} props sincronizados!")
                    
                except Exception as e:
                    st.error(f"Erro ao buscar props: {e}")

    # Mostrar status
    st.divider()
    if 'pinnacle_games' in st.session_state:
        st.info(f"🟢 {len(st.session_state['pinnacle_games'])} jogos carregados")
    if 'pinnacle_props_map' in st.session_state:
        total_props = sum(len(v) for v in st.session_state.get('pinnacle_props_map', {}).values())
        st.success(f"🎯 {len(st.session_state['pinnacle_props_map'])} jogadores | {total_props} props disponíveis")
    
    # Opcional: busca por jogador
    if 'pinnacle_props_map' in st.session_state:
        player_name = st.text_input("Buscar props de um jogador:")
        if player_name:
            matches = {k: v for k, v in st.session_state['pinnacle_props_map'].items() if player_name.lower() in k.lower()}
            if matches:
                for name, props in matches.items():
                    st.write(f"**{name}**")
                    for market, data in props.items():
                        # Ajuste para ler dicionário ou valor direto
                        line = data.get('line') if isinstance(data, dict) else data
                        odd = data.get('odds') if isinstance(data, dict) else 1.90
                        st.write(f"• {market}: {line} (@{odd})")
            else:
                st.info("Jogador não encontrado nas props do dia.")
                

# ============================================================================
# PÁGINA: DVP TACTICAL BOARD (V41.0 - ROBUST HTML ARCHITECTURE)
# ============================================================================
def show_dvp_analysis():
    import streamlit as st
    import pandas as pd
    import numpy as np
    import unicodedata

    # --- 1. CSS BLINDADO (SINGLE BLOCK) ---
    st.markdown("""
    <style>
        .dvp-header-title { font-family: 'Oswald', sans-serif; font-size: 28px; color: #fff; margin-bottom: 5px; letter-spacing: 1px; }
        .dvp-header-sub { font-family: 'Nunito', sans-serif; font-size: 14px; color: #94a3b8; margin-bottom: 25px; }

        /* ESTRUTURA DO CARD DE JOGO */
        .dvp-game-card {
            background-color: #1e293b;
            border: 1px solid #334155;
            border-radius: 8px;
            margin-bottom: 20px;
            overflow: hidden;
            box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        }

        .dvp-game-header {
            background: linear-gradient(90deg, #0f172a 0%, #334155 50%, #0f172a 100%);
            padding: 10px;
            text-align: center;
            font-family: 'Oswald', sans-serif;
            font-size: 18px; color: #f8fafc;
            border-bottom: 1px solid #475569;
            letter-spacing: 1px;
        }

        /* GRID TÁTICO (CSS GRID PURO) */
        .dvp-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            width: 100%;
        }

        .dvp-col {
            padding: 0;
        }
        .dvp-col:first-child {
            border-right: 1px solid #334155;
        }

        .dvp-col-header {
            background: rgba(0,0,0,0.2);
            padding: 8px;
            text-align: center;
            font-size: 11px; font-weight: bold; color: #94a3b8; text-transform: uppercase;
            border-bottom: 1px solid #334155;
        }

        /* LINHA DO JOGADOR */
        .dvp-player-row {
            display: flex; align-items: center; justify-content: space-between;
            padding: 8px 12px;
            border-bottom: 1px solid rgba(255,255,255,0.05);
            transition: background 0.2s;
        }
        .dvp-player-row:last-child { border-bottom: none; }
        .dvp-player-row:hover { background: rgba(255,255,255,0.03); }

        .dvp-p-left { display: flex; align-items: center; gap: 10px; }
        
        .dvp-p-img {
            width: 38px; height: 38px; border-radius: 50%;
            object-fit: cover; background: #000; border: 1px solid #475569;
        }

        .dvp-p-info { display: flex; flex-direction: column; }
        .dvp-p-name { font-family: 'Oswald', sans-serif; font-size: 14px; color: #e2e8f0; line-height: 1.1; }
        .dvp-p-meta { font-size: 10px; color: #94a3b8; font-weight: 600; margin-top: 2px; }

        /* BADGE DE RANK */
        .dvp-rank-badge {
            font-family: 'Roboto Mono', monospace;
            font-size: 13px; font-weight: bold;
            padding: 3px 8px; border-radius: 4px;
            min-width: 42px; text-align: center;
            border: 1px solid rgba(255,255,255,0.1);
        }

        /* CORES (TIERS) */
        .tier-s { background: #22c55e; color: #000; box-shadow: 0 0 8px rgba(34, 197, 94, 0.4); border-color: #22c55e; } /* 25-30 */
        .tier-a { background: rgba(34, 197, 94, 0.15); color: #4ade80; border-color: #22c55e; } /* 20-24 */
        .tier-b { background: rgba(234, 179, 8, 0.15); color: #facc15; border-color: #eab308; } /* 11-19 */
        .tier-c { background: rgba(239, 68, 68, 0.15); color: #f87171; border-color: #ef4444; } /* 1-10 */

        .dvp-missing { padding: 20px; text-align: center; font-size: 11px; color: #64748b; font-style: italic; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="dvp-header-title">🛡️ DvP TACTICAL BOARD</div>', unsafe_allow_html=True)
    st.markdown('<div class="dvp-header-sub">Rank defensivo do oponente por posição (30 = Pior Defesa/Melhor Alvo).</div>', unsafe_allow_html=True)

    # --- 2. CARREGAMENTO DE DADOS ---
    dvp_analyzer = st.session_state.get("dvp_analyzer")
    if not dvp_analyzer:
        try:
            from modules.new_modules.dvp_analyzer import DvPAnalyzer
            st.session_state.dvp_analyzer = DvPAnalyzer()
            dvp_analyzer = st.session_state.dvp_analyzer
        except:
            st.error("Módulo DvP offline.")
            return

    games = st.session_state.get("scoreboard", [])
    if not games:
        st.warning("Aguardando jogos...")
        return

    df_l5 = st.session_state.get("df_l5", pd.DataFrame())
    real_logs = get_data_universal('real_game_logs') or {}

    # --- 3. MAPAS & NORMALIZAÇÃO ---
    def normalize_str(text):
        if not text: return ""
        try:
            text = str(text)
            text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
            return text.upper().strip()
        except: return ""

    # Mapa expandido para pegar NY e DET
    TEAM_CORRECTION = {
        "GS": "GSW", "NO": "NOP", "NY": "NYK", "SA": "SAS", "PHO": "PHX",
        "WSH": "WAS", "UTAH": "UTA", "BRK": "BKN", "CHO": "CHA", "BK": "BKN",
        "NEW YORK": "NYK", "KNICKS": "NYK", "N.Y.": "NYK",
        "DETROIT": "DET", "PISTONS": "DET"
    }

    def clean_team_code(t):
        raw = str(t).upper().strip()
        return TEAM_CORRECTION.get(raw, raw)

    # --- 4. FILTRO DE LESÕES ---
    banned_players = set()
    try:
        fresh_inj = get_data_universal('injuries_cache_v44') or get_data_universal('injuries_data')
        raw_inj = fresh_inj or []
        flat_inj = []
        if isinstance(raw_inj, dict):
            for t in raw_inj.values(): 
                if isinstance(t, list): flat_inj.extend(t)
        elif isinstance(raw_inj, list): flat_inj = raw_inj
        
        EXCLUSION = ['OUT', 'DOUBT', 'SURG', 'INJUR', 'PROTOCOL', 'DAY', 'DTD', 'QUEST']
        for item in flat_inj:
            p_name = ""
            status = ""
            if isinstance(item, dict):
                p_name = item.get('player') or item.get('name') or ""
                status = str(item.get('status', '')).upper()
            elif isinstance(item, str):
                p_name = item.split('-')[0]
                status = str(item).upper()
            if p_name and any(x in status for x in EXCLUSION):
                banned_players.add(normalize_str(p_name))
    except: pass

    # --- 5. ROSTER BUILDER ---
    
    def get_clean_pos(raw_pos_str, stats=None):
        """Limpa e infere posição se necessário."""
        raw = str(raw_pos_str).upper()
        
        # 1. Posições Explícitas
        if 'CENTER' in raw: return 'C'
        if 'POINT' in raw: return 'PG'
        if 'SHOOTING' in raw: return 'SG'
        if 'POWER' in raw: return 'PF'
        if 'SMALL' in raw: return 'SF'
        
        clean = raw.replace('-', '/').replace('GUARD', 'G').replace('FORWARD', 'F')
        parts = clean.split('/')
        main = parts[0].strip()
        
        if main in ['PG', 'SG', 'SF', 'PF', 'C']: return main
        
        # 2. Inferência por Stats (se disponível)
        if stats:
            ast = stats.get('AST', 0)
            reb = stats.get('REB', 0)
            if 'G' in main: return 'PG' if ast > 5.0 else 'SG'
            if 'F' in main: return 'PF' if reb > 7.0 else 'SF'
            if 'C' in main: return 'C'
            
        # 3. Fallback Seguro
        if main == 'G': return 'SG'
        if main == 'F': return 'SF'
        return 'SF'

    def get_team_roster(team_code):
        roster = []
        
        # Fonte 1: L5 (Dados Oficiais)
        if not df_l5.empty:
            try:
                # Busca colunas dinamicamente
                cols = [c.upper() for c in df_l5.columns]
                df_l5.columns = cols
                c_team = next((c for c in cols if 'TEAM' in c and 'ID' not in c), 'TEAM')
                c_min = next((c for c in cols if 'MIN' in c), 'MIN')
                c_name = next((c for c in cols if 'PLAYER' in c and 'NAME' in c), 'PLAYER')
                c_pos = next((c for c in cols if 'POS' in c), 'POS')
                c_id = next((c for c in cols if 'ID' in c), 'PLAYER_ID')

                # Filtra (usando clean_team_code também no dataframe se necessário)
                # Iterar é mais seguro para normalização
                for _, row in df_l5.iterrows():
                    row_team = clean_team_code(row.get(c_team, 'UNK'))
                    
                    if row_team == team_code:
                        nm = normalize_str(row.get(c_name, ''))
                        if nm in banned_players: continue
                        
                        try: mins = float(row.get(c_min, 0))
                        except: mins = 0
                        
                        if mins >= 15:
                            pos = get_clean_pos(row.get(c_pos, 'SF'))
                            roster.append({
                                "name": nm, "min": mins, "pos": pos, 
                                "id": row.get(c_id, 0)
                            })
            except: pass

        # Fonte 2: Logs (Fallback e Complemento)
        if real_logs:
            # Coleta nomes existentes para evitar duplicatas
            existing = {r['name'] for r in roster}
            
            for p_name, p_data in real_logs.items():
                if not isinstance(p_data, dict): continue
                t = clean_team_code(p_data.get('team', ''))
                
                if t == team_code:
                    nm = normalize_str(p_name)
                    if nm in existing or nm in banned_players: continue
                    
                    logs = p_data.get('logs', {})
                    mins = logs.get('MIN_AVG', 0)
                    
                    # Calcula stats médios para inferência
                    stats = {}
                    for k in ['PTS', 'REB', 'AST', 'MIN']:
                        vals = [float(x) for x in logs.get(k, []) if x is not None]
                        stats[k] = sum(vals)/len(vals) if vals else 0
                    
                    if stats['MIN'] == 0: stats['MIN'] = mins
                    
                    if stats['MIN'] >= 15:
                        # Tenta inferir posição
                        pos = get_clean_pos("SF", stats) # Passa stats para desempate
                        roster.append({"name": nm, "min": stats['MIN'], "pos": pos, "id": 0})

        # Retorna Top 8 rotação
        return sorted(roster, key=lambda x: x['min'], reverse=True)[:8]

    # --- 6. RENDER HTML (SINGLE BLOCK PER GAME) ---
    # Isso evita o "HTML explodindo"
    
    def render_player_html(p, opp_code):
        rank = dvp_analyzer.get_position_rank(opp_code, p['pos'])
        if not rank: rank = 15
        
        # Cores
        if rank >= 25: tier = "tier-s" # Verde Forte
        elif rank >= 20: tier = "tier-a" # Verde
        elif rank >= 11: tier = "tier-b" # Amarelo
        else: tier = "tier-c" # Vermelho
        
        photo = "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"
        if p.get('id'): photo = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{int(p['id'])}.png"
        
        name = p['name'].title()
        
        return f"""
        <div class="dvp-player-row">
            <div class="dvp-p-left">
                <img src="{photo}" class="dvp-p-img">
                <div class="dvp-p-info">
                    <div class="dvp-p-name">{name}</div>
                    <div class="dvp-p-meta">{p['pos']} • {p['min']:.0f}m</div>
                </div>
            </div>
            <div class="dvp-rank-badge {tier}">#{rank}</div>
        </div>
        """

    for g in games:
        home = clean_team_code(g['home'])
        away = clean_team_code(g['away'])
        
        r_away = get_team_roster(away)
        r_home = get_team_roster(home)
        
        # Constrói o HTML interno das colunas
        html_away = "".join([render_player_html(p, home) for p in r_away]) if r_away else f'<div class="dvp-missing">Sinal perdido: {away}</div>'
        html_home = "".join([render_player_html(p, away) for p in r_home]) if r_home else f'<div class="dvp-missing">Sinal perdido: {home}</div>'
        
        # Renderiza o Card Completo
        st.markdown(f"""
        <div class="dvp-game-card">
            <div class="dvp-game-header">{away} @ {home}</div>
            <div class="dvp-grid">
                <div class="dvp-col">
                    <div class="dvp-col-header">ATAQUE {away} <span style="color:#64748b">vs {home}</span></div>
                    {html_away}
                </div>
                <div class="dvp-col">
                    <div class="dvp-col-header">ATAQUE {home} <span style="color:#64748b">vs {away}</span></div>
                    {html_home}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
                
# ============================================================================
# PÁGINA: BLOWOUT RADAR (V33.0 - SMART PHOTO ENGINE)
# ============================================================================
def show_blowout_hunter_page():
    import json
    import pandas as pd
    import re
    import time
    import numpy as np
    import unicodedata
    import streamlit as st
    
    # --- 1. FUNÇÕES AUXILIARES & MATEMÁTICA ---
    def normalize_str(text):
        """Limpa texto para comparação (Remove acentos, uppercase, sufixos)."""
        if not text: return ""
        try:
            text = str(text)
            text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
            text = text.upper().strip()
            text = text.replace(".", "").replace("'", "") # Remove pontuação
            for suffix in [" JR", " SR", " III", " II", " IV"]:
                if text.endswith(suffix):
                    text = text.replace(suffix, "")
            return text.strip()
        except: return ""

    def normalize_team_code(t):
        """Padroniza siglas de times."""
        t = str(t).upper().strip()
        map_t = {
            "GS": "GSW", "NO": "NOP", "NY": "NYK", "SA": "SAS", "PHO": "PHX",
            "UTAH": "UTA", "WSH": "WAS", "BRK": "BKN", "CHO": "CHA"
        }
        return map_t.get(t, t)

    def adjust_stat(raw_val, projected_min):
        """Ajusta a estatística para a realidade (Matemática V32.0)."""
        try:
            val = float(raw_val)
            mins = float(projected_min)
            if mins <= 0: return 0.0
            
            # Normalização (Base 36)
            projected_val = (val / 36.0) * mins
            
            # Teto de Sanidade
            caps = {'pts': 0.9 * mins, 'reb': 0.4 * mins, 'ast': 0.35 * mins}
            
            if projected_val > caps.get('pts', 99): 
                projected_val = (projected_val + caps['pts']) / 2
                
            return round(projected_val, 1)
        except: return 0.0

    def get_logo_url(team_abbr):
        clean_abbr = normalize_team_code(team_abbr)
        return f"https://a.espncdn.com/i/teamlogos/nba/500/scoreboard/{clean_abbr.lower()}.png"

    # --- 2. ESTILO VISUAL ---
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Oswald:wght@400;700&display=swap');
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');

        .radar-title { font-family: 'Oswald'; font-size: 26px; color: #fff; margin-bottom: 5px; letter-spacing: 1px; }
        
        .match-container { background-color: #1e293b; border-radius: 12px; margin-bottom: 20px; border: 1px solid #334155; overflow: hidden; }
        
        .risk-header { padding: 12px 16px; display: flex; justify-content: space-between; align-items: center; }
        .risk-high { background: linear-gradient(90deg, #7f1d1d 0%, #1e293b 80%); border-left: 5px solid #EF4444; }
        .risk-med { background: linear-gradient(90deg, #78350f 0%, #1e293b 80%); border-left: 5px solid #F59E0B; }
        .risk-low { background: linear-gradient(90deg, #064e3b 0%, #1e293b 80%); border-left: 5px solid #10B981; }
        
        .game-matchup-text { font-family: 'Oswald'; font-size: 18px; color: #fff; letter-spacing: 1px; margin: 0 10px; }
        .match-logo { width: 30px; height: 30px; object-fit: contain; }
        .risk-label { font-size: 11px; font-weight: bold; color: #fff; text-transform: uppercase; }
        .spread-tag { font-size: 12px; color: #cbd5e1; background: rgba(0,0,0,0.4); padding: 2px 6px; border-radius: 4px; font-weight: bold; }
        
        .players-area { padding: 10px; background: rgba(0,0,0,0.2); }
        .team-col-header { display: flex; align-items: center; gap: 8px; border-bottom: 1px solid #334155; padding-bottom: 5px; margin-bottom: 8px; }
        .team-col-logo { width: 24px; height: 24px; object-fit: contain; }
        .team-col-text { color: #94a3b8; font-size: 11px; font-weight: bold; letter-spacing: 1px; text-transform: uppercase; }
        
        .vulture-row { display: flex; justify-content: space-between; align-items: center; padding: 8px; margin-bottom: 6px; background: rgba(255,255,255,0.03); border-radius: 6px; border: 1px solid #334155; }
        .vulture-img { width: 40px; height: 40px; border-radius: 50%; border: 2px solid #6366f1; margin-right: 10px; object-fit: cover; background: #0f172a; }
        .vulture-name { color: #e2e8f0; font-weight: 600; font-size: 13px; font-family: 'Oswald'; }
        .vulture-mins { font-size: 10px; color: #94a3b8; margin-top: 2px; font-family: 'Inter'; }
        
        .stat-box { display: flex; gap: 10px; text-align: center; }
        .stat-val { font-family: 'Oswald'; font-size: 14px; font-weight: bold; }
        .stat-lbl { font-size: 8px; color: #64748B; font-weight: bold; }
        .c-pts { color: #4ade80; } .c-reb { color: #60a5fa; } .c-ast { color: #facc15; }
        
        .badge-sniper { background: rgba(139, 92, 246, 0.2); color: #a78bfa; border: 1px solid #a78bfa; padding: 1px 4px; border-radius: 3px; font-size: 8px; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="radar-title">&#127744; BLOWOUT RADAR</div>', unsafe_allow_html=True)

    # --- HERO SECTION ---
    st.markdown("""
    <div style="background: linear-gradient(90deg, rgba(30,41,59,0.6) 0%, rgba(15,23,42,0.6) 100%); border-left: 4px solid #6366f1; border-radius: 8px; padding: 15px 20px; margin-bottom: 25px; border: 1px solid #334155;">
        <div style="font-family: 'Inter', sans-serif; color: #e2e8f0; font-size: 14px; line-height: 1.6;">
            <strong style="color: #6366f1; font-size: 15px;">O CAÇADOR DE OPORTUNIDADES (GARBAGE TIME)</strong><br>
            Jogos com Spread alto tendem a ser decididos cedo. Quando as estrelas sentam, abre-se valor nas linhas dos reservas ("Bench Mob").
            <ul style="margin-top: 8px; margin-bottom: 0; padding-left: 20px; list-style-type: none;">
                <li style="margin-bottom: 6px;">🌪️ <strong style="color: #EF4444;">Blowout Risk:</strong> Alta probabilidade de goleada. Foco total nos reservas listados.</li>
                <li>📈 <strong style="color: #4ade80;">Projeção Realista:</strong> Estatísticas ajustadas para os minutos projetados de quadra livre.</li>
            </ul>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # --- 3. MOTOR DE FOTOS INTELIGENTE (SMART ID MAP) ---
    df_l5 = st.session_state.get('df_l5', pd.DataFrame())
    NAME_TO_ID = {}
    LASTNAME_TEAM_TO_ID = {}
    
    if not df_l5.empty:
        try:
            # Garante nomes de colunas
            df_l5.columns = [str(c).upper().strip() for c in df_l5.columns]
            
            c_name = next((c for c in df_l5.columns if c in ['PLAYER_NAME', 'PLAYER', 'NAME']), 'PLAYER')
            c_id = next((c for c in df_l5.columns if c in ['PLAYER_ID', 'ID', 'PERSON_ID']), 'PLAYER_ID')
            c_team = next((c for c in df_l5.columns if c in ['TEAM', 'TEAM_ABBREVIATION', 'TEAM_CODE']), 'TEAM')

            # Popula Mapas
            for _, row in df_l5.iterrows():
                try:
                    pid = int(float(row.get(c_id, 0)))
                    if pid == 0: continue
                    
                    full_name = normalize_str(str(row.get(c_name, '')))
                    team_code = normalize_team_code(str(row.get(c_team, '')))
                    
                    # Mapa 1: Nome Completo -> ID
                    NAME_TO_ID[full_name] = pid
                    
                    # Mapa 2: Sobrenome + Time -> ID (Para fallbacks)
                    parts = full_name.split()
                    if len(parts) > 0:
                        lastname = parts[-1]
                        key = f"{lastname}_{team_code}"
                        LASTNAME_TEAM_TO_ID[key] = pid
                except: continue
        except: pass

    # --- 4. MONITOR DE LESÕES ---
    banned_players = set()
    EXCLUSION_KEYWORDS = ['OUT', 'DOUBTFUL', 'SURGERY', 'INJURED', 'PROTOCOL', 'SUSPENDED', 'G LEAGUE', 'PERSONAL']
    
    raw_inj_source = get_data_universal('injuries') or st.session_state.get('injuries_data', [])
    if raw_inj_source:
        flat_inj = []
        if isinstance(raw_inj_source, dict):
            for t in raw_inj_source.values():
                if isinstance(t, list): flat_inj.extend(t)
        elif isinstance(raw_inj_source, list): flat_inj = raw_inj_source
            
        for item in flat_inj:
            try:
                p_name = item.get('player') or item.get('name') or ""
                status = str(item.get('status', '')).upper()
                if p_name:
                    norm = normalize_str(p_name)
                    if norm and any(x in status for x in EXCLUSION_KEYWORDS):
                        banned_players.add(norm)
            except: continue

    # --- 5. ENGINE & LOOP ---
    DNA_DB = st.session_state.get('dna_final_v27', {})
    if not DNA_DB:
        DNA_DB = get_data_universal("rotation_dna_v27") or {}
        st.session_state['dna_final_v27'] = DNA_DB

    games = st.session_state.get('scoreboard', [])
    if not games:
        st.info("Aguardando jogos...")
        return

    st.markdown("---")
    force_spread = st.slider("🎛️ Simular Cenário de Blowout (Aumentar Spread Virtual):", 0, 30, 0)

    def get_team_data(query):
        q = str(query).upper().strip()
        # Mapeia query para chave do DNA_DB
        map_key = {
            "GS": "GSW", "NO": "NOP", "NY": "NYK", "SA": "SAS", "PHO": "PHX",
            "UTAH": "UTA", "WSH": "WAS", "BRK": "BKN", "CHO": "CHA"
        }
        target = map_key.get(q, q)
        
        # Tenta achar no DB
        if target in DNA_DB: return DNA_DB[target]
        # Tenta match parcial
        for k in DNA_DB.keys():
            if target in k: return DNA_DB[k]
        return []

    for g in games:
        raw_s = g.get('odds_spread', '0')
        try: real_s = abs(float(re.findall(r"[-+]?\d*\.\d+|\d+", str(raw_s))[-1]))
        except: real_s = 0.0
        
        final_spread = max(real_s, force_spread)
        
        if final_spread >= 12.5:
            risk_cls, risk_txt = "risk-high", "🔥 ALTO RISCO (BLOWOUT)"
            show_players = True
        elif final_spread >= 8.5:
            risk_cls, risk_txt = "risk-med", "⚠ RISCO MODERADO"
            show_players = True
        else:
            risk_cls, risk_txt = "risk-low", "JOGO EQUILIBRADO"
            show_players = False
        
        logo_away = get_logo_url(g['away'])
        logo_home = get_logo_url(g['home'])

        st.markdown(f"""
        <div class="match-container">
            <div class="risk-header {risk_cls}">
                <div style="display:flex; align-items:center;">
                    <img src="{logo_away}" class="match-logo">
                    <span class="game-matchup-text">{g['away']} @ {g['home']}</span>
                    <img src="{logo_home}" class="match-logo">
                </div>
                <div style="text-align:right;">
                    <div class="risk-label">{risk_txt}</div>
                    <span class="spread-tag">LINHA: {final_spread}</span>
                </div>
            </div>
        """, unsafe_allow_html=True)

        if show_players:
            c1, c2 = st.columns(2)
            
            def render_team_col(col, t_name):
                data = get_team_data(t_name)
                t_logo = get_logo_url(t_name)
                t_clean_code = normalize_team_code(t_name)
                
                with col:
                    st.markdown(f"""
                    <div class="players-area">
                        <div class="team-col-header">
                            <img src="{t_logo}" class="team-col-logo">
                            <div class="team-col-text">{t_name} RESERVAS</div>
                        </div>
                    """, unsafe_allow_html=True)
                    
                    if data:
                        valid_players = []
                        for p in data:
                            p_clean = normalize_str(p.get('clean_name') or p['name'])
                            if p_clean in banned_players: continue 
                            
                            # --- BUSCA INTELIGENTE DE ID ---
                            fresh_id = 0
                            # 1. Tenta Nome Completo
                            if p_clean in NAME_TO_ID:
                                fresh_id = NAME_TO_ID[p_clean]
                            else:
                                # 2. Tenta Sobrenome + Time
                                parts = p_clean.split()
                                if len(parts) > 0:
                                    lname = parts[-1]
                                    key = f"{lname}_{t_clean_code}"
                                    if key in LASTNAME_TEAM_TO_ID:
                                        fresh_id = LASTNAME_TEAM_TO_ID[key]
                            
                            # Atualiza ID se achou um melhor que o zero
                            if fresh_id != 0: p['id'] = fresh_id
                            elif 'id' not in p: p['id'] = 0
                                
                            valid_players.append(p)
                        
                        if valid_players:
                            for p in valid_players[:3]:
                                pid = int(p.get('id', 0))
                                nba_url = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"
                                espn_url = f"https://a.espncdn.com/combiner/i?img=/i/headshots/nba/players/full/{pid}.png"
                                fallback_url = "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"

                                badge_html = f'<span class="badge-sniper">SNIPER</span>' if p.get('type') == 'SNIPER' else ''
                                
                                img_html = f"""<img src="{nba_url}" class="vulture-img" onerror="this.src='{espn_url}'; this.onerror=function(){{this.src='{fallback_url}'}};">"""
                                if pid == 0: img_html = f"""<img src="{fallback_url}" class="vulture-img">"""

                                proj_min = float(p.get('blowout_min', 15))
                                avg_min = float(p.get('avg_min', 10))
                                
                                adj_pts = adjust_stat(p.get('pts', 0), proj_min)
                                adj_reb = adjust_stat(p.get('reb', 0), proj_min)
                                adj_ast = adjust_stat(p.get('ast', 0), proj_min)

                                st.markdown(f"""
                                <div class="vulture-row">
                                    <div style="display:flex; align-items:center;">
                                        {img_html}
                                        <div>
                                            <div class="vulture-name">{p['name']} {badge_html}</div>
                                            <div class="vulture-mins">
                                                {avg_min:.0f}m <span style="color:#64748B;">➝</span> <span style="color:#4ade80; font-weight:bold;">{proj_min:.0f}m</span>
                                            </div>
                                        </div>
                                    </div>
                                    <div class="stat-box">
                                        <div><div class="stat-val c-pts">{adj_pts:.1f}</div><div class="stat-lbl">PTS</div></div>
                                        <div><div class="stat-val c-reb">{adj_reb:.1f}</div><div class="stat-lbl">REB</div></div>
                                        <div><div class="stat-val c-ast">{adj_ast:.1f}</div><div class="stat-lbl">AST</div></div>
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                        else:
                            st.markdown("<div style='text-align:center; padding:10px; font-size:11px; color:#e11d48;'>Reservas listados estão lesionados.</div>", unsafe_allow_html=True)
                    else:
                        st.markdown("<div style='text-align:center; padding:10px; font-size:11px; color:#64748b;'>Sem dados de reservas.</div>", unsafe_allow_html=True)
                    st.markdown("</div>", unsafe_allow_html=True)

            render_team_col(c1, g['away'])
            render_team_col(c2, g['home'])
        else:
            st.markdown("""
            <div style="padding:15px; text-align:center; background:rgba(0,0,0,0.2); color:#64748b; font-size:12px; font-weight:bold;">
                🛡️ Foco nos Titulares (Spread Baixo)
            </div>
            """, unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)
# ============================================================================
# PÁGINA: MOMENTUM (V5.3 - PODIUM & THERMOMETER UX)
# ============================================================================
def show_momentum_page():
    import pandas as pd
    import streamlit as st

    # --- 1. CSS VISUAL (PODIUM STYLE) ---
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Oswald:wght@400;700&display=swap');
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');

        .mom-header { font-family: 'Oswald'; font-size: 26px; color: #fff; margin-bottom: 5px; letter-spacing: 1px; }
        
        /* CARD GRANDE (LÍDER) */
        .king-card {
            background: linear-gradient(180deg, rgba(16, 185, 129, 0.1) 0%, rgba(15, 23, 42, 1) 100%);
            border: 1px solid #10B981;
            border-radius: 12px;
            padding: 15px;
            text-align: center;
            position: relative;
            box-shadow: 0 0 20px rgba(16, 185, 129, 0.15);
            margin-bottom: 15px;
        }
        .frozen-card {
            background: linear-gradient(180deg, rgba(239, 68, 68, 0.1) 0%, rgba(15, 23, 42, 1) 100%);
            border: 1px solid #EF4444;
            border-radius: 12px;
            padding: 15px;
            text-align: center;
            position: relative;
            box-shadow: 0 0 20px rgba(239, 68, 68, 0.15);
            margin-bottom: 15px;
        }
        
        .rank-badge {
            position: absolute; top: -10px; left: 50%; transform: translateX(-50%);
            background: #0f172a; padding: 4px 12px; border-radius: 20px;
            font-family: 'Oswald'; font-size: 12px; font-weight: bold; border: 1px solid #334155;
            z-index: 2;
        }

        .king-img { width: 100px; height: 100px; border-radius: 50%; object-fit: cover; border: 3px solid #10B981; margin: 0 auto 10px auto; display: block; background:#000; }
        .frozen-img { width: 100px; height: 100px; border-radius: 50%; object-fit: cover; border: 3px solid #EF4444; margin: 0 auto 10px auto; display: block; background:#000; }

        .k-name { font-family: 'Oswald'; font-size: 20px; color: #fff; line-height: 1.1; margin-bottom: 4px; }
        .k-meta { font-family: 'Inter'; font-size: 11px; color: #94a3b8; margin-bottom: 8px; }
        .k-score { font-family: 'Oswald'; font-size: 28px; font-weight: bold; }

        /* CARD PEQUENO (LISTA) */
        .mini-card {
            display: flex; align-items: center; gap: 10px;
            background: rgba(30, 41, 59, 0.4);
            border-radius: 8px; padding: 8px; margin-bottom: 8px;
            border: 1px solid #334155;
        }
        .mini-img { width: 45px; height: 45px; border-radius: 50%; object-fit: cover; border: 2px solid #475569; background:#000; }
        .mini-info { flex: 1; }
        .mini-name { font-family: 'Oswald'; font-size: 14px; color: #e2e8f0; line-height: 1.1; }
        .mini-val { font-family: 'Oswald'; font-size: 16px; font-weight: bold; text-align: right; }

    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="mom-header">⚡ MOMENTUM RADAR</div>', unsafe_allow_html=True)

    # --- HERO SECTION (TERMÔMETRO) ---
    st.markdown("""
    <div style="
        background: linear-gradient(90deg, rgba(30,41,59,0.6) 0%, rgba(15,23,42,0.6) 100%);
        border-left: 4px solid #10B981;
        border-radius: 8px;
        padding: 15px 20px;
        margin-bottom: 25px;
        border: 1px solid #334155;
    ">
        <div style="font-family: 'Inter', sans-serif; color: #e2e8f0; font-size: 14px; line-height: 1.6;">
            <strong style="color: #10B981; font-size: 15px;">RADAR DE TENDÊNCIAS (L5)</strong><br>
            Identifique quem está vivendo um momento mágico e quem está numa maré de azar nos últimos 5 jogos:
            <ul style="margin-top: 8px; margin-bottom: 0; padding-left: 20px; list-style-type: none;">
                <li style="margin-bottom: 6px;">
                    🔥 <strong style="color: #10B981;">Em Chamas (Hot):</strong> Produção estatística (PRA) muito acima da média da liga. Ideal para surfar a boa fase (Overs).
                </li>
                <li>
                    ❄️ <strong style="color: #EF4444;">Congelado (Cold):</strong> Titulares com alta minutagem, mas baixo impacto recente. Oportunidades de explorar linhas baixas (Unders).
                </li>
            </ul>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # --- 2. DADOS ---
    if 'df_l5' not in st.session_state or st.session_state.df_l5.empty:
        st.warning("⚠️ Base de dados L5 vazia. Vá em Config > Ingestão para atualizar.")
        return

    # Cópia segura
    df = st.session_state.df_l5.copy()
    
    # 2.1 NORMALIZAÇÃO DE COLUNAS
    df.columns = [str(c).upper().strip() for c in df.columns]

    # Mapeamento de Colunas
    cols_map = {
        'PLAYER_ID': ['ID', 'PERSON_ID'],
        'PLAYER': ['PLAYER_NAME', 'NAME'],
        'TEAM': ['TEAM_ABBREVIATION', 'TEAM_CODE'],
        'MIN_AVG': ['MIN', 'MINUTES'],
        'PTS_AVG': ['PTS'],
        'REB_AVG': ['REB'],
        'AST_AVG': ['AST']
    }
    
    for target, alts in cols_map.items():
        if target not in df.columns:
            for alt in alts:
                if alt in df.columns:
                    df[target] = df[alt]
                    break
            if target not in df.columns:
                # Valores default
                if target == 'PLAYER': df[target] = 'Unknown'
                elif target == 'TEAM': df[target] = 'UNK'
                else: df[target] = 0

    # 2.2 DEDUPLICAÇÃO E LIMPEZA (A CORREÇÃO)
    # Garante números
    for col in ['MIN_AVG', 'PTS_AVG', 'REB_AVG', 'AST_AVG', 'PLAYER_ID']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Remove duplicatas de ID (mantém a linha com mais minutos, assumindo ser a mais correta)
    df = df.sort_values('MIN_AVG', ascending=False).drop_duplicates(subset=['PLAYER_ID'])

    # Filtro de Relevância (Apenas jogadores com > 18 min)
    df_calc = df[df['MIN_AVG'] >= 18].copy()
    
    if df_calc.empty:
        st.info("Nenhum jogador qualificado para análise (Mínimo 18 min).")
        return

    # --- 3. CÁLCULO ESTATÍSTICO (Z-SCORE) ---
    # Usamos PRA (Points + Rebounds + Assists) como métrica de volume
    df_calc['PRA_AVG'] = df_calc['PTS_AVG'] + df_calc['REB_AVG'] + df_calc['AST_AVG']

    mean_league = df_calc['PRA_AVG'].mean()
    std_league = df_calc['PRA_AVG'].std()
    if std_league == 0: std_league = 1

    df_calc['z_score'] = (df_calc['PRA_AVG'] - mean_league) / std_league

    # --- 4. SEPARAÇÃO PÓDIO ---
    
    # TOP 3 HOT (Ordenado do maior para o menor)
    top_hot = df_calc[df_calc['z_score'] > 0].sort_values('z_score', ascending=False).head(3)
    
    # TOP 3 COLD (Ordenado do menor para o maior - mais negativo primeiro)
    # Filtro extra: Cold precisa ter minutos altos (>24) para ser relevante (Titular mal)
    top_cold = df_calc[
        (df_calc['z_score'] < 0) & 
        (df_calc['MIN_AVG'] >= 24)
    ].sort_values('z_score', ascending=True).head(3)

    # --- 5. RENDERIZAÇÃO (PODIUM LAYOUT) ---

    c_hot, c_cold = st.columns(2)

    # === LADO QUENTE (HOT) ===
    with c_hot:
        st.markdown('<div style="color:#10B981; font-family:Oswald; font-size:18px; text-align:center; margin-bottom:10px;">🔥 EM CHAMAS (TOP 3)</div>', unsafe_allow_html=True)
        
        if not top_hot.empty:
            # #1 KING
            king = top_hot.iloc[0]
            k_pid = int(king['PLAYER_ID'])
            k_img = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{k_pid}.png"
            
            st.markdown(f"""
            <div class="king-card">
                <div class="rank-badge" style="color:#10B981; border-color:#10B981;">#1 LÍDER</div>
                <img src="{k_img}" class="king-img" onerror="this.src='https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png'">
                <div class="k-name">{king['PLAYER']}</div>
                <div class="k-meta">{king['TEAM']} • PRA {king['PRA_AVG']:.1f}</div>
                <div class="k-score" style="color:#10B981">+{king['z_score']:.2f}σ</div>
            </div>
            """, unsafe_allow_html=True)

            # #2 e #3 LISTA
            for i in range(1, len(top_hot)):
                row = top_hot.iloc[i]
                r_pid = int(row['PLAYER_ID'])
                r_img = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{r_pid}.png"
                
                st.markdown(f"""
                <div class="mini-card" style="border-left: 3px solid #10B981;">
                    <img src="{r_img}" class="mini-img" onerror="this.src='https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png'">
                    <div class="mini-info">
                        <div class="mini-name">#{i+1} {row['PLAYER']}</div>
                        <div style="font-size:10px; color:#94a3b8">{row['TEAM']}</div>
                    </div>
                    <div class="mini-val" style="color:#10B981">+{row['z_score']:.2f}</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("Nenhum destaque positivo.")

    # === LADO FRIO (COLD) ===
    with c_cold:
        st.markdown('<div style="color:#EF4444; font-family:Oswald; font-size:18px; text-align:center; margin-bottom:10px;">❄️ CONGELADOS (TOP 3)</div>', unsafe_allow_html=True)
        
        if not top_cold.empty:
            # #1 FROZEN
            frozen = top_cold.iloc[0]
            f_pid = int(frozen['PLAYER_ID'])
            f_img = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{f_pid}.png"
            
            st.markdown(f"""
            <div class="frozen-card">
                <div class="rank-badge" style="color:#EF4444; border-color:#EF4444;">#1 TRAVADO</div>
                <img src="{f_img}" class="frozen-img" onerror="this.src='https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png'">
                <div class="k-name">{frozen['PLAYER']}</div>
                <div class="k-meta">{frozen['TEAM']} • PRA {frozen['PRA_AVG']:.1f}</div>
                <div class="k-score" style="color:#EF4444">{frozen['z_score']:.2f}σ</div>
            </div>
            """, unsafe_allow_html=True)

            # #2 e #3 LISTA
            for i in range(1, len(top_cold)):
                row = top_cold.iloc[i]
                r_pid = int(row['PLAYER_ID'])
                r_img = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{r_pid}.png"
                
                st.markdown(f"""
                <div class="mini-card" style="border-left: 3px solid #EF4444;">
                    <img src="{r_img}" class="mini-img" onerror="this.src='https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png'">
                    <div class="mini-info">
                        <div class="mini-name">#{i+1} {row['PLAYER']}</div>
                        <div style="font-size:10px; color:#94a3b8">{row['TEAM']}</div>
                    </div>
                    <div class="mini-val" style="color:#EF4444">{row['z_score']:.2f}</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("Nenhum destaque negativo relevante.")
                

# ============================================================================
# CLASSE NEXUS ENGINE (v11.0 - HIGH VOLUME & ROBUST MATCHING)
# ============================================================================
import math
import json
import os
import unicodedata
import streamlit as st 

class NexusEngine:
    def __init__(self, logs_cache, games):
        self.logs = logs_cache
        self.games = games # Lista de jogos do dia (Scoreboard)
        self.player_ids = self._load_photo_map()
        
        # Módulos (com verificação segura)
        self.injury_monitor = InjuryMonitor() if 'InjuryMonitor' in globals() and InjuryMonitor else None
        self.pace_adjuster = PaceAdjuster() if 'PaceAdjuster' in globals() and PaceAdjuster else None
        self.dvp_analyzer = DvPAnalyzer() if 'DvPAnalyzer' in globals() and DvPAnalyzer else None
        
        # OTIMIZAÇÃO: Cria mapa de elenco (Roster)
        self.roster_map = self._build_roster_map()

    # --- UTILITÁRIOS ---
    def _normalize_team(self, team_raw):
        """
        Versão unificada e robusta de normalização de times.
        Garante que PHX = PHO, GS = GSW, etc.
        """
        if not team_raw: return "UNK"
        t = str(team_raw).upper().strip()
        
        mapping = {
            "ATLANTA": "ATL", "HAWKS": "ATL", "BOSTON": "BOS", "CELTICS": "BOS",
            "BROOKLYN": "BKN", "NETS": "BKN", "CHARLOTTE": "CHA", "HORNETS": "CHA",
            "CHICAGO": "CHI", "BULLS": "CHI", "CLEVELAND": "CLE", "CAVS": "CLE", "CAVALIERS": "CLE",
            "DALLAS": "DAL", "MAVS": "DAL", "MAVERICKS": "DAL", "DENVER": "DEN", "NUGGETS": "DEN",
            "DETROIT": "DET", "PISTONS": "DET", "GOLDEN STATE": "GSW", "WARRIORS": "GSW", "GS": "GSW",
            "HOUSTON": "HOU", "ROCKETS": "HOU", "INDIANA": "IND", "PACERS": "IND",
            "CLIPPERS": "LAC", "LA CLIPPERS": "LAC", "L.A. CLIPPERS": "LAC",
            "LAKERS": "LAL", "LA LAKERS": "LAL", "L.A. LAKERS": "LAL", "LOS ANGELES LAKERS": "LAL",
            "MEMPHIS": "MEM", "GRIZZLIES": "MEM", "MIAMI": "MIA", "HEAT": "MIA",
            "MILWAUKEE": "MIL", "BUCKS": "MIL", "MINNESOTA": "MIN", "WOLVES": "MIN", "TIMBERWOLVES": "MIN",
            "NEW ORLEANS": "NOP", "PELICANS": "NOP", "NO": "NOP", "N.O.": "NOP",
            "NEW YORK": "NYK", "KNICKS": "NYK", "NY": "NYK", "N.Y.": "NYK",
            "OKLAHOMA CITY": "OKC", "THUNDER": "OKC", "OKC THUNDER": "OKC",
            "ORLANDO": "ORL", "MAGIC": "ORL", "PHILADELPHIA": "PHI", "SIXERS": "PHI", "76ERS": "PHI",
            "PHOENIX": "PHX", "SUNS": "PHX", "PHO": "PHX", "PORTLAND": "POR", "BLAZERS": "POR", "TRAIL BLAZERS": "POR",
            "SACRAMENTO": "SAC", "KINGS": "SAC", "SAN ANTONIO": "SAS", "SPURS": "SAS", "SA": "SAS",
            "TORONTO": "TOR", "RAPTORS": "TOR", "UTAH": "UTA", "JAZZ": "UTA",
            "WASHINGTON": "WAS", "WIZARDS": "WAS", "WSH": "WAS"
        }
        # Tenta mapear, se não der, pega os 3 primeiros caracteres
        return mapping.get(t, t[:3])

    def _strip_accents(self, text):
        try:
            text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")
            return str(text)
        except: return str(text)

    def _load_photo_map(self):
        if os.path.exists("nba_players_map.json"):
            try:
                with open("nba_players_map.json", "r", encoding="utf-8") as f:
                    return json.load(f)
            except: pass
        return {}

    def get_photo(self, name):
        pid = self.player_ids.get(name) or self.player_ids.get(self._strip_accents(name))
        if pid: return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"
        return "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"

    def get_team_logo(self, team_abbr):
        abbr = self._normalize_team(team_abbr).lower()
        if abbr == 'uta': abbr = 'utah'
        if abbr == 'nop': abbr = 'no'
        return f"https://a.espncdn.com/i/teamlogos/nba/500/{abbr}.png"

    def _build_roster_map(self):
        """Organiza os jogadores por time para busca rápida O(1)."""
        roster = {}
        for name, data in self.logs.items():
            if not isinstance(data, dict): continue
            raw_t = data.get('team', 'UNK')
            team = self._normalize_team(raw_t)
            if team not in roster: roster[team] = []
            roster[team].append(name)
        return roster

    # --- MOTOR PRINCIPAL ---
    def run_nexus_scan(self):
        opportunities = []
        
        # 1. SGP (Estratégia Sinergia)
        opportunities.extend(self._scan_sgp_opportunities())

        # 2. Vácuo (Estratégia Lesão)
        if self.injury_monitor:
            try: 
                opportunities.extend(self._scan_vacuum_opportunities())
            except Exception as e: 
                print(f"⚠️ Erro Vacuum: {e}") 
        
        return sorted(opportunities, key=lambda x: x['score'], reverse=True)

    def _scan_sgp_opportunities(self):
        found = []
        
        # Identifica times ativos hoje
        active_teams = set()
        for g in self.games:
            active_teams.add(self._normalize_team(g.get('home')))
            active_teams.add(self._normalize_team(g.get('away')))

        for team in active_teams:
            players_list = self.roster_map.get(team, [])
            if not players_list: continue

            # --- ETAPA 1: Identificar Candidatos ---
            motors = []
            finishers = []
            
            # Filtros Mínimos (Bem baixos para capturar volume)
            min_ast_req = 3.5
            min_pts_req = 12.0
            
            for p in players_list:
                avg_ast = self._get_avg_stat(p, 'AST')
                avg_pts = self._get_avg_stat(p, 'PTS')
                
                if avg_ast >= min_ast_req:
                    motors.append({'name': p, 'val': avg_ast})
                
                if avg_pts >= min_pts_req:
                    finishers.append({'name': p, 'val': avg_pts})
            
            if not motors or not finishers: continue
            
            # --- ETAPA 2: Formar a Melhor Dupla ---
            # Ordena pelos melhores
            motors.sort(key=lambda x: x['val'], reverse=True)
            finishers.sort(key=lambda x: x['val'], reverse=True)
            
            best_motor = motors[0]
            best_finisher = None
            
            # Garante que o finalizador não é o próprio motor
            for f in finishers:
                if f['name'] != best_motor['name']:
                    best_finisher = f
                    break
            
            if not best_finisher: continue

            # --- ETAPA 3: Calcular Score e Badge ---
            m_val = best_motor['val']
            f_val = best_finisher['val']
            
            t_ast = math.ceil(m_val - 0.5)
            t_pts = math.floor(f_val) 
            
            # Score Base
            score = 45 
            badges = []
            
            # Bonificações de Qualidade
            if m_val >= 7.0: score += 10; badges.append("🧠 Elite Playmaker")
            elif m_val >= 5.0: score += 5
            
            if f_val >= 25.0: score += 10; badges.append("🎯 Elite Scorer")
            elif f_val >= 18.0: score += 5
            
            # Analisa Pace
            opp = self._get_opponent(team)
            if opp and self.pace_adjuster:
                pace = self.pace_adjuster.calculate_game_pace(team, opp)
                if pace >= 100: 
                    score += 10
                    badges.append(f"🏎️ Pace: {int(pace)}")
            
            # Filtro Final (Aceita quase tudo que é decente)
            if score >= 50:
                found.append({
                    "type": "SGP",
                    "title": "SGP: SINERGIA OFENSIVA",
                    "score": score,
                    "color": "#eab308",
                    "hero": {
                        "name": best_motor['name'], 
                        "photo": self.get_photo(best_motor['name']), 
                        "role": "PASSADOR", 
                        "stat": "AST", 
                        "target": f"{t_ast}+", 
                        "logo": self.get_team_logo(team)
                    },
                    "partner": {
                        "name": best_finisher['name'], 
                        "photo": self.get_photo(best_finisher['name']), 
                        "role": "CESTINHA", 
                        "stat": "PTS", 
                        "target": f"{t_pts}+", 
                        "logo": self.get_team_logo(team)
                    },
                    "badges": badges
                })
        return found

    def _scan_vacuum_opportunities(self):
        """LÓGICA VÁCUO 2.1 (Mantida pois funcionou bem)"""
        found = []
        if not self.games: return []

        matchups = {}
        for g in self.games:
            h = self._normalize_team(g.get('home'))
            a = self._normalize_team(g.get('away'))
            matchups[h] = a; matchups[a] = h
        
        try: all_injuries = self.injury_monitor.get_all_injuries()
        except: return []
        
        if not all_injuries: return []

        for team_raw, injuries in all_injuries.items():
            victim_team = self._normalize_team(team_raw)
            if victim_team not in matchups: continue
            predator_team = matchups[victim_team]
            
            for inj in injuries:
                status = str(inj.get('status', '')).upper()
                name = inj.get('name', '')
                pos_raw = str(inj.get('position', '')).upper()
                
                if any(x in status for x in ['OUT', 'INJ', 'DOUBT']):
                    is_center = False
                    if 'C' in pos_raw or 'CENTER' in pos_raw: is_center = True
                    log_avg_reb = self._get_avg_stat(name, 'REB')
                    if log_avg_reb >= 7.0: is_center = True
                    
                    vip_centers = [
                        "NIKOLA JOKIC", "DOMANTAS SABONIS", "JAKOB POELTL", "WALKER KESSLER", 
                        "JUSUF NURKIC", "ZACH EDEY", "ISAIAH HARTENSTEIN", "IVICA ZUBAC", 
                        "ALPEREN SENGUN", "JOEL EMBIID", "DEANDRE AYTON", "JALEN DUREN",
                        "ANTHONY DAVIS", "BAM ADEBAYO", "GIANNIS ANTETOKOUNMPO", "VICTOR WEMBANYAMA",
                        "KARL-ANTHONY TOWNS", "RUDY GOBERT", "JARRETT ALLEN", "EVAN MOBLEY", "DRAYMOND GREEN"
                    ]
                    if name.upper() in vip_centers: is_center = True

                    if is_center:
                        predator = self._find_best_rebounder(predator_team)
                        if predator:
                            avg_reb = self._get_avg_stat(predator, 'REB')
                            if avg_reb >= 6.0:
                                boost = 2.0 if avg_reb > 9 else 1.5
                                target = math.ceil(avg_reb + boost)
                                moon = math.ceil(avg_reb + boost + 3)
                                score = 75 
                                if avg_reb > 10: score += 10
                                
                                found.append({
                                    "type": "VACUUM",
                                    "title": "VÁCUO DE REBOTE",
                                    "score": score,
                                    "color": "#a855f7",
                                    "hero": {
                                        "name": predator, 
                                        "photo": self.get_photo(predator), 
                                        "status": "🧨 PREDADOR", 
                                        "stat": "REB", 
                                        "target": f"{target}+",
                                        "logo": self.get_team_logo(predator_team)
                                    },
                                    "villain": {
                                        "name": victim_team, "missing": name, "status": "🚨 OUT", "logo": self.get_team_logo(victim_team)
                                    },
                                    "ladder": [f"✅ Base: {int(avg_reb)}+", f"💰 Alvo: {target}+", f"🚀 Lua: {moon}+"],
                                    "impact": f"Sem {name} ({log_avg_reb:.1f} reb/j), {victim_team} perde proteção de aro."
                                })
                                break 
        return found

    # --- AUXILIARES ---
    def _get_avg_stat(self, player, stat):
        try:
            player_data = self.logs.get(player)
            if not player_data: return 0
            vals = player_data.get('logs', {}).get(stat, [])
            if not vals: return 0
            limit = min(len(vals), 10)
            return sum(vals[:limit])/limit
        except: return 0

    def _get_opponent(self, team):
        target = self._normalize_team(team)
        for g in self.games:
            h = self._normalize_team(g.get('home'))
            a = self._normalize_team(g.get('away'))
            if h == target: return a
            if a == target: return h
        return None

    def _find_best_rebounder(self, team):
        team_players = self.roster_map.get(team, [])
        best, max_reb = None, 0
        for name in team_players:
            val = self._get_avg_stat(name, 'REB')
            if val > max_reb: 
                max_reb = val
                best = name
        return best

# ============================================================================
# PÁGINA: TRINITY CLUB (V17.0 - NUCLEAR ID MATCHING)
# ============================================================================
def show_trinity_club_page():
    import os
    import pandas as pd
    import streamlit as st
    import re
    import unicodedata

    # --- 1. CSS VISUAL (MANTIDO) ---
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Oswald:wght@400;500;600;700&family=Inter:wght@400;600&display=swap');
        
        .trin-card {
            background-color: #0f172a;
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 12px;
            margin-bottom: 15px;
            box-shadow: 0 4px 10px rgba(0,0,0,0.4);
            transition: all 0.3s ease;
        }
        .trin-card.gold-member {
            border: 1px solid #D4AF37;
            box-shadow: 0 0 15px rgba(212, 175, 55, 0.15);
            background: linear-gradient(180deg, rgba(212, 175, 55, 0.05) 0%, #0f172a 100%);
        }
        .trin-name { font-family: 'Oswald'; font-size: 18px; color: #fff; line-height: 1.1; margin-bottom: 2px; }
        .trin-meta { font-family: 'Inter'; font-size: 11px; color: #94a3b8; display: flex; align-items: center; gap: 6px; margin-bottom: 10px; }
        
        .col-header { text-align: center; font-family: 'Oswald'; font-size: 11px; font-weight: bold; padding: 3px; border-radius: 4px; margin-bottom: 6px; letter-spacing: 1px; }
        .head-l5 { background: rgba(239, 68, 68, 0.2); color: #fca5a5; border: 1px solid #ef4444; }
        .head-l10 { background: rgba(234, 179, 8, 0.2); color: #fde047; border: 1px solid #eab308; }
        .head-l15 { background: rgba(59, 130, 246, 0.2); color: #93c5fd; border: 1px solid #3b82f6; }

        .stat-box { text-align: center; background: rgba(30, 41, 59, 0.5); border-radius: 6px; padding: 4px; border: 1px solid #334155; margin-bottom: 4px; }
        .stat-val { font-family: 'Oswald'; font-size: 16px; font-weight: bold; line-height: 1; }
        .stat-lbl { font-size: 8px; color: #94a3b8; text-transform: uppercase; margin-top: 2px; font-weight: 600; }
        
        .color-pts { color: #fbbf24; } 
        .color-reb { color: #f87171; } 
        .color-ast { color: #60a5fa; } 
        .color-def { color: #e2e8f0; } 

        .context-bar { display: flex; gap: 8px; margin-top: 10px; padding-top: 10px; border-top: 1px dashed #334155; flex-wrap: wrap; }
        .ctx-pill { background: rgba(15, 23, 42, 0.8); border: 1px solid #475569; border-radius: 4px; padding: 2px 8px; display: flex; align-items: center; gap: 6px; font-size: 10px; color: #cbd5e1; font-family: 'Inter'; }
        .ctx-val { color: #fff; font-weight: bold; font-family: 'Oswald'; margin-left: 2px; }
        
        .trin-img { border-radius: 50%; border: 2px solid #334155; width: 65px; height: 65px; object-fit: cover; background: #0f172a; }
        .gold-member .trin-img { border-color: #D4AF37; }
        .gold-badge { background: #D4AF37; color: #000; font-family: 'Oswald'; font-size: 10px; padding: 2px 6px; border-radius: 4px; font-weight: bold; margin-left: 8px; display: inline-block; vertical-align: middle; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("## 🏆 Trinity Club")

    # HERO SECTION
    st.markdown("""
    <div style="background: linear-gradient(90deg, rgba(30,41,59,0.6) 0%, rgba(15,23,42,0.6) 100%); border-left: 4px solid #D4AF37; border-radius: 8px; padding: 15px 20px; margin-bottom: 25px; border: 1px solid #334155;">
        <div style="font-family: 'Inter', sans-serif; color: #e2e8f0; font-size: 14px; line-height: 1.6;">
            <strong style="color: #D4AF37; font-size: 15px;">A ELITE DA CONSISTÊNCIA</strong><br>
            Ao contrário das médias, o Trinity Club busca o <strong>Piso Seguro</strong>. Identificamos linhas estatísticas que o jogador bateu em <strong>100%</strong> dos jogos nas janelas abaixo:
            <ul style="margin-top: 8px; margin-bottom: 0; padding-left: 20px; list-style-type: none;">
                <li style="margin-bottom: 4px;">🔥 <strong style="color: #fca5a5;">L5:</strong> Forma Imediata.</li>
                <li style="margin-bottom: 4px;">⚖️ <strong style="color: #fde047;">L10:</strong> Tendência Sólida.</li>
                <li style="margin-bottom: 4px;">🏛️ <strong style="color: #93c5fd;">L15:</strong> Pilar Histórico.</li>
            </ul>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # 2. CARREGAMENTO DE DADOS
    full_cache = get_data_universal("real_game_logs", os.path.join("cache", "real_game_logs.json"))
    df_l5 = st.session_state.get('df_l5', pd.DataFrame()) 
    
    if not full_cache:
        st.warning("Aguardando sincronização de logs...")
        return

    # ==============================================================================
    # 3. MOTOR NUCLEAR DE IDENTIFICAÇÃO (ID VAULT V2)
    # ==============================================================================
    
    def nuclear_normalize(text):
        """
        Remove TUDO que não for letra ou número.
        Ex: 'Luka Dončić' -> 'LUKADONCIC'
        Ex: 'Shai Gilgeous-Alexander' -> 'SHAIGILGEOUSALEXANDER'
        Ex: 'C.J. McCollum' -> 'CJMCCOLLUM'
        """
        if not text: return ""
        try:
            # Normaliza Unicode (remove acentos)
            text = unicodedata.normalize('NFKD', str(text)).encode('ASCII', 'ignore').decode('utf-8')
            text = text.upper()
            # Remove sufixos comuns (Jr, Sr, etc) ANTES de limpar tudo
            for suffix in [" JR", " SR", " III", " II", " IV"]:
                if text.endswith(suffix):
                    text = text.replace(suffix, "")
            # Remove tudo que não é letra ou numero
            text = re.sub(r'[^A-Z0-9]', '', text)
            return text
        except: return ""

    ID_VAULT = {}
    
    if not df_l5.empty:
        try:
            # Padroniza Colunas
            df_l5.columns = [str(c).upper().strip() for c in df_l5.columns]
            
            # Encontra colunas vitais
            c_name = next((c for c in df_l5.columns if c in ['PLAYER_NAME', 'PLAYER', 'NAME']), 'PLAYER')
            c_id = next((c for c in df_l5.columns if c in ['PLAYER_ID', 'ID', 'PERSON_ID']), 'PLAYER_ID')
            c_team = next((c for c in df_l5.columns if c in ['TEAM', 'TEAM_ABBREVIATION', 'TEAM_CODE']), 'TEAM')

            for _, row in df_l5.iterrows():
                try:
                    raw_id = row.get(c_id, 0)
                    pid = int(float(raw_id))
                    
                    if pid > 0:
                        raw_name = str(row.get(c_name, ''))
                        raw_team = str(row.get(c_team, 'UNK')).upper().strip()
                        
                        # Chave 1: Nome Completo Nuclear (LUKADONCIC)
                        k1 = nuclear_normalize(raw_name)
                        ID_VAULT[k1] = pid
                        
                        # Chave 2: Sobrenome Nuclear + Time (DONCIC_DAL)
                        parts = raw_name.split()
                        if len(parts) > 1:
                            lname = nuclear_normalize(parts[-1])
                            k2 = f"{lname}_{raw_team}"
                            ID_VAULT[k2] = pid
                            
                except: continue
        except Exception as e: pass

    def resolve_player_id(player_name, team_abbr):
        """Tenta encontrar o ID da NBA a todo custo."""
        # 1. Tenta Nome Completo Limpo
        k1 = nuclear_normalize(player_name)
        if k1 in ID_VAULT: return ID_VAULT[k1]
        
        # 2. Tenta Sobrenome + Time
        parts = player_name.split()
        if len(parts) > 0:
            lname = nuclear_normalize(parts[-1])
            k2 = f"{lname}_{team_abbr}"
            if k2 in ID_VAULT: return ID_VAULT[k2]
        
        # 3. Tenta Match Parcial (Lento, mas salva)
        # Ex: "Nicolas Claxton" vs "Nic Claxton"
        for vault_key, vault_id in ID_VAULT.items():
            if k1 in vault_key or vault_key in k1:
                # Verifica se o sobrenome bate para não dar falso positivo
                if len(parts) > 1:
                    lname = nuclear_normalize(parts[-1])
                    if lname in vault_key:
                        return vault_id

        return 0

    # ==============================================================================

    # 4. ENGINE TRINITY
    class TrinityEngine:
        def __init__(self, logs_cache, games):
            self.logs = logs_cache
            self.games_map = self._map_games(games)
            
        def _normalize_team(self, team_code):
            mapping = {"NY": "NYK", "GS": "GSW", "PHO": "PHX", "NO": "NOP", "SA": "SAS", "WSH": "WAS", "UTAH": "UTA", "NOH": "NOP", "BRK": "BKN", "CHO": "CHA"}
            return mapping.get(str(team_code).upper(), str(team_code).upper())

        def _map_games(self, games):
            mapping = {}
            for g in games:
                home = self._normalize_team(g.get('home'))
                away = self._normalize_team(g.get('away'))
                gid = g.get('game_id') or g.get('id') or "UNK"
                if home and away:
                    mapping[home] = {"opp": away, "is_home": True, "game_str": f"{away} @ {home}", "game_id": gid}
                    mapping[away] = {"opp": home, "is_home": False, "game_str": f"{away} @ {home}", "game_id": gid}
            return mapping

        def scan_market(self, window=10):
            candidates = []
            if not self.logs: return []

            for player_name, data in self.logs.items():
                raw_team = data.get('team')
                if not raw_team: continue
                team = self._normalize_team(raw_team)
                if team not in self.games_map: continue
                
                logs = data.get('logs', {})
                if not logs: continue
                ctx = self.games_map[team]
                
                # Armazena ID do ESPN se existir no log (Fallback)
                espn_id = data.get('id', 0)
                
                for stat in ['PTS', 'REB', 'AST']:
                    values = logs.get(stat, [])
                    if len(values) < window: continue 
                    
                    current_window_values = values[:window]
                    floor_form = min(current_window_values)
                    safe_floor = int(floor_form * 0.95) 
                    
                    min_req = 10 if stat == 'PTS' else 4
                    if safe_floor >= min_req:
                        candidates.append({
                            "player": player_name,
                            "team": team, # Time normalizado
                            "raw_team": raw_team,
                            "opp": ctx['opp'],
                            "stat": stat,
                            "line": safe_floor - 1,
                            "floors": {"Form": floor_form, "Venue": floor_form, "H2H": int(floor_form*0.9)},
                            "score": safe_floor,
                            "game_str": ctx['game_str'],
                            "espn_id": espn_id # Passamos o ID original caso precise
                        })
                            
            return sorted(candidates, key=lambda x: x['score'], reverse=True)

    engine = TrinityEngine(full_cache, st.session_state.get('scoreboard', []))
    res_l5 = engine.scan_market(window=5)
    res_l10 = engine.scan_market(window=10)
    res_l15 = engine.scan_market(window=15)

    games_dict = {}
    def consolidate(results, label):
        for r in results:
            g_str = r['game_str']
            p_name = r['player']
            if g_str not in games_dict: games_dict[g_str] = {}
            if p_name not in games_dict[g_str]: 
                games_dict[g_str][p_name] = {'meta': r, 'L5': [], 'L10': [], 'L15': []}
            games_dict[g_str][p_name][label].append(r)

    consolidate(res_l5, 'L5')
    consolidate(res_l10, 'L10')
    consolidate(res_l15, 'L15')

    if not games_dict:
        st.info("Nenhum padrão estatístico Trinity encontrado hoje.")
        return

    # 5. RENDERIZAÇÃO
    logo_base = "https://a.espncdn.com/i/teamlogos/nba/500"

    for game_name, players in games_dict.items():
        st.markdown(f"""
        <div style="font-family:'Oswald'; font-size:18px; color:#F8FAFC; border-left:4px solid #94a3b8; padding-left:10px; margin-top:25px; margin-bottom:10px;">
            🏀 {game_name}
        </div>
        """, unsafe_allow_html=True)
        
        for p_name, data in players.items():
            meta = data['meta']
            is_gold = len(data['L5']) > 0 and len(data['L10']) > 0 and len(data['L15']) > 0
            gold_class = "gold-member" if is_gold else ""
            gold_badge_html = "<span class='gold-badge'>GOLD MEMBER</span>" if is_gold else ""

            # --- BUSCA DE FOTO HÍBRIDA ---
            # 1. Tenta achar o ID NBA via Nome (Alta qualidade)
            nba_id = resolve_player_id(p_name, meta['team'])
            
            # 2. Se não achar, vê se veio ID da ESPN nos logs
            espn_id = meta.get('espn_id', 0)
            
            # Define URL da imagem
            if nba_id > 0:
                # Prioridade: NBA ID (Foto melhor)
                img_src = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{nba_id}.png"
            elif espn_id and int(espn_id) > 0:
                # Fallback: ESPN ID (Foto ok)
                img_src = f"https://a.espncdn.com/combiner/i?img=/i/headshots/nba/players/full/{espn_id}.png"
            else:
                # Fallback Final: Boneco
                img_src = "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"
            
            # Renderiza Imagem com tratamento de erro (se a URL da NBA falhar, tenta ESPN, depois fallback)
            img_html = f"""
            <img src="{img_src}" class="trin-img" 
                 onerror="this.src='https://a.espncdn.com/combiner/i?img=/i/headshots/nba/players/full/{nba_id}.png'; 
                 this.onerror=function(){{this.src='https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png'}};">
            """

            # Logo
            tm_low = meta['team'].lower()
            logo_url = f"{logo_base}/{tm_low}.png"

            st.markdown(f'<div class="trin-card {gold_class}">', unsafe_allow_html=True)
            
            c1, c2, c3, c4, c5 = st.columns([1.3, 2.7, 2, 2, 2])
            
            with c1:
                st.markdown(f'<div style="text-align:center;">{img_html}</div>', unsafe_allow_html=True)
            
            with c2:
                st.markdown(f'<div class="trin-name">{p_name} {gold_badge_html}</div>', unsafe_allow_html=True)
                st.markdown(f"""
                <div class="trin-meta">
                    <img src="{logo_url}" width="18" style="vertical-align:middle; margin-right:4px;"> 
                    <b>{meta['team']}</b> vs {meta['opp']}
                </div>
                """, unsafe_allow_html=True)
                
                floors = meta['floors']
                st.markdown(f"""
                <div class="context-bar">
                    <div class="ctx-pill" title="Forma Atual">
                        <span class="ctx-icon">📈</span><span>Forma</span><span class="ctx-val">{int(floors['Form'])}</span>
                    </div>
                    <div class="ctx-pill" title="Desempenho no Local">
                        <span class="ctx-icon">🏠</span><span>Local</span><span class="ctx-val">{int(floors['Venue'])}</span>
                    </div>
                    <div class="ctx-pill" title="Histórico vs Oponente">
                        <span class="ctx-icon">🆚</span><span>H2H</span><span class="ctx-val">{int(floors['H2H'])}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            def render_col(col, title, css_class, items):
                with col:
                    st.markdown(f'<div class="col-header {css_class}">{title}</div>', unsafe_allow_html=True)
                    if not items:
                        st.markdown("<div style='text-align:center; color:#334155; font-size:20px;'>-</div>", unsafe_allow_html=True)
                    else:
                        for item in items:
                            s_txt = item['stat']
                            c_cls = "color-pts" if 'PTS' in s_txt else ("color-reb" if 'REB' in s_txt else ("color-ast" if 'AST' in s_txt else "color-def"))
                            st.markdown(f"""
                            <div class="stat-box">
                                <div class="stat-val {c_cls}">{item['line']}+</div>
                                <div class="stat-lbl">{s_txt}</div>
                            </div>
                            """, unsafe_allow_html=True)

            render_col(c3, "🔥 L5", "head-l5", data['L5'])
            render_col(c4, "⚖️ L10", "head-l10", data['L10'])
            render_col(c5, "🏛️ L15", "head-l15", data['L15'])
            
            st.markdown("</div>", unsafe_allow_html=True)
                
# ============================================================================
# PÁGINA: NEXUS PAGE (V10.0 - NATIVE STREAMLIT / BULLETPROOF)
# ============================================================================
# ============================================================================
# PÁGINA: NEXUS PAGE (V10.3 - UI COMERCIAL & HERO SECTION)
# ============================================================================
def show_nexus_page():
    # --- CSS MÍNIMO ---
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Oswald:wght@400;700&family=Inter:wght@400;600&display=swap');
        
        .nx-title { font-family: 'Oswald', sans-serif; font-size: 20px; font-weight: bold; letter-spacing: 1px; color: #FFFFFF; }
        .nx-score { font-family: monospace; font-weight: bold; color: #FBBF24; background: #1e293b; padding: 4px 8px; border-radius: 4px; border: 1px solid #334155; }
        .nx-big-stat { font-family: 'Oswald', sans-serif; font-size: 24px; font-weight: bold; line-height: 1.1; }
        .nx-meta { font-family: monospace; font-size: 11px; color: #94a3b8; text-transform: uppercase; }
        
        .c-pts { color: #FBBF24; }
        .c-ast { color: #38BDF8; }
        .c-reb { color: #F87171; }
        .c-def { color: #A3E635; }
        
        .nx-avatar { border-radius: 50%; width: 60px; height: 60px; object-fit: cover; border: 2px solid #334155; }
        .nx-logo { width: 60px; height: 60px; object-fit: contain; }
    </style>
    """, unsafe_allow_html=True)

    # 1. Carregar Dados
    full_cache = get_data_universal("real_game_logs")
    scoreboard = get_data_universal("scoreboard")

    # Header Nativo
    st.markdown("<h1 style='text-align:center; margin-bottom:0;'>Sinergia & Vácuo</h1>", unsafe_allow_html=True)
    st.markdown("<div style='text-align:center; color:#64748b; font-size:12px; margin-bottom:20px;'>SISTEMA TÁTICO • ONLINE</div>", unsafe_allow_html=True)

    # --- HERO SECTION: COPY "A VANTAGEM" ---
    st.markdown("""
    <div style="
        background: linear-gradient(90deg, rgba(30,41,59,0.7) 0%, rgba(15,23,42,0.7) 100%);
        border-left: 4px solid #FBBF24;
        border-radius: 8px;
        padding: 15px 20px;
        margin-bottom: 25px;
        border: 1px solid #334155;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    ">
        <div style="font-family: 'Inter', sans-serif; color: #e2e8f0; font-size: 14px; line-height: 1.6;">
            <strong style="color: #FBBF24; font-size: 15px;">ENCONTRE AS FALHAS NA MATRIZ.</strong><br>
            O Nexus vai além das médias simples. Ele cruza cenários táticos para encontrar narrativas lucrativas:
            <ul style="margin-top: 8px; margin-bottom: 0; padding-left: 20px; list-style-type: none;">
                <li style="margin-bottom: 6px;">
                    ⚡ <strong style="color: #eab308;">Sinergia (SGP):</strong> Detecta <em>Conexões Letais</em> entre Armadores e Cestinhas do mesmo time que estão em sintonia máxima.
                </li>
                <li>
                    🟣 <strong style="color: #a855f7;">Vácuo Tático:</strong> Monitora lesões críticas (ex: Pivôs OUT) e aponta instantaneamente o <em>Predador</em> que vai dominar os rebotes no garrafão exposto.
                </li>
            </ul>
        </div>
    </div>
    """, unsafe_allow_html=True)

    if not full_cache:
        st.info("ℹ️ Aguardando dados de estatísticas...")
        return

    # 2. Engine
    nexus = NexusEngine(full_cache, scoreboard or [])
    
    try:
        all_ops = nexus.run_nexus_scan()
        # Filtro de exibição final (pode ser ajustado)
        opportunities = sorted(
            [op for op in all_ops if op.get('score', 0) >= 50],
            key=lambda x: x.get('score', 0),
            reverse=True
        )
    except Exception as e:
        st.error(f"Erro ao processar Nexus: {e}")
        return

    if not opportunities:
        st.info("🔎 Nenhuma oportunidade de alta sinergia ou vácuo encontrada para os jogos de hoje.")
        return

    # Helper de Cores
    def get_color_class(stat_name):
        s = str(stat_name).upper()
        if 'PTS' in s: return "c-pts"
        if 'AST' in s: return "c-ast"
        if 'REB' in s: return "c-reb"
        return "c-def"

    # 3. RENDERIZAÇÃO
    for op in opportunities:
        score = op.get('score', 0)
        color = op.get('color', '#38BDF8')
        raw_title = op.get('title', 'OPORTUNIDADE')
        op_type = op.get('type', 'Standard')
        is_sgp = (op_type == 'SGP')
        
        if is_sgp:
            main_title = "SINERGIA"
            icon = "⚡"
            center_label = ""
        else:
            main_title = str(raw_title).replace("VÁCUO DE REBOTE", "VÁCUO TÁTICO")
            icon = "⚔️"
            center_label = "VS"

        hero = op.get('hero', {})
        h_name = hero.get('name', 'Unknown')
        h_photo = hero.get('photo', '')
        h_role = hero.get('role', 'PLAYER') 
        h_val = hero.get('target', '-')
        h_stat = hero.get('stat', '')
        h_cls = get_color_class(h_stat)

        if is_sgp:
            partner = op.get('partner', {})
            t_name = partner.get('name', 'Parceiro')
            t_photo = partner.get('photo', '')
            t_role = "PARCEIRO"
            t_val_html = f"<span class='nx-big-stat {get_color_class(partner.get('stat'))}'>{partner.get('target')}</span> <span style='font-size:12px; color:#94a3b8'>{partner.get('stat')}</span>"
            t_img_class = "nx-avatar"
        else:
            villain = op.get('villain', {})
            t_name = villain.get('name', 'Adversário')
            t_photo = villain.get('logo', '')
            t_role = "DEFESA"
            v_status = villain.get('status', '')
            status_alert = f"<span style='color:#F87171; background:#3f1a1a; padding:2px 6px; border-radius:4px; font-weight:bold; font-size:11px;'>🚨 {v_status}</span>" if v_status else ""
            t_val_html = f"<div style='margin-top:5px'>{status_alert}</div>"
            t_img_class = "nx-logo"

        with st.container(border=True):
            c_head_L, c_head_R = st.columns([3, 1])
            with c_head_L:
                st.markdown(f"<span style='color:{color}; font-size:18px;'>{icon}</span> <span class='nx-title'>{main_title}</span>", unsafe_allow_html=True)
            with c_head_R:
                st.markdown(f"<div style='text-align:right'><span class='nx-score'>SCORE {score}</span></div>", unsafe_allow_html=True)
            
            st.divider()

            c1, c2, c3 = st.columns([2, 0.4, 2])
            
            with c1:
                sc1, sc2 = st.columns([0.8, 2])
                with sc1:
                    st.markdown(f"<img src='{h_photo}' class='nx-avatar' style='border-color:{color}'>", unsafe_allow_html=True)
                with sc2:
                    st.markdown(f"""
                    <div style='line-height:1.2'>
                        <div style='font-weight:bold; font-size:15px; color:#fff'>{h_name}</div>
                        <div class='nx-meta'>{h_role}</div>
                        <div class='nx-big-stat {h_cls}'>{h_val} <span style='font-size:12px; font-weight:normal; color:#94a3b8'>{h_stat}</span></div>
                    </div>
                    """, unsafe_allow_html=True)
            
            with c2:
                if center_label:
                    st.markdown(f"<div style='text-align:center; margin-top:15px; font-weight:bold; color:#475569;'>{center_label}</div>", unsafe_allow_html=True)
            
            with c3:
                sc_txt, sc_img = st.columns([2, 0.8])
                with sc_txt:
                    st.markdown(f"""
                    <div style='text-align:right; line-height:1.2'>
                        <div style='font-weight:bold; font-size:15px; color:#fff'>{t_name}</div>
                        <div class='nx-meta'>{t_role}</div>
                        {t_val_html}
                    </div>
                    """, unsafe_allow_html=True)
                with sc_img:
                    st.markdown(f"<div style='display:flex; justify-content:flex-end'><img src='{t_photo}' class='{t_img_class}'></div>", unsafe_allow_html=True)

            if not is_sgp:
                ladder_raw = op.get('ladder', [])
                ladder_clean = [str(l).split(":")[-1].strip() for l in ladder_raw if ":" in str(l)]
                if len(ladder_clean) >= 3:
                    ladder_html = f"""
                    <span style='color:#64748b'>Base</span> <strong style='color:#fff'>{ladder_clean[0]}</strong> <span style='color:#334155'>|</span> 
                    <span style='color:#FBBF24'>Alvo</span> <strong style='color:#FBBF24'>{ladder_clean[1]}</strong> <span style='color:#334155'>|</span> 
                    <span style='color:#64748b'>Teto</span> <strong style='color:#fff'>{ladder_clean[2]}</strong>
                    """
                else:
                    ladder_html = " • ".join(ladder_clean)
                
                impact_txt = op.get('impact', '')
                st.markdown(f"""
                <div style='margin-top:10px; background:#111827; border-radius:6px; padding:8px; border:1px dashed #334155; display:flex; justify-content:space-between; align-items:center; font-size:12px;'>
                    <div>{ladder_html}</div>
                    <div style='color:#94a3b8; font-style:italic; text-align:right; max-width:40%;'>{impact_txt}</div>
                </div>
                """, unsafe_allow_html=True)
            
            # Badges para SGP
            if is_sgp and 'badges' in op:
                badges_html = "".join([f"<span style='background:#1e293b; color:#94a3b8; padding:2px 6px; border-radius:4px; font-size:10px; margin-right:4px; border:1px solid #334155'>{b}</span>" for b in op['badges']])
                st.markdown(f"<div style='margin-top:8px;'>{badges_html}</div>", unsafe_allow_html=True)
# ============================================================================
# STRATEGY ENGINE: 5/7/10 (VERSÃO 3.5 - COMPATÍVEL COM LAYOUT ANTERIOR)
# ============================================================================
import os
import json
import unicodedata

class FiveSevenTenEngine:
    def __init__(self, logs_cache, games):
        self.logs = logs_cache
        self.games_map = self._map_games(games)
        
        # Carrega mapa de fotos
        self.player_ids = {}
        if os.path.exists("nba_players_map.json"):
            try:
                with open("nba_players_map.json", "r", encoding="utf-8") as f:
                    raw_data = json.load(f)
                    # Normaliza nomes
                    self.player_ids = {self._normalize_name(k): v for k, v in raw_data.items()}
            except: pass

    def _normalize_name(self, name):
        """Remove acentos: 'Luka Dončić' vira 'Luka Doncic'"""
        if not name: return ""
        return ''.join(c for c in unicodedata.normalize('NFD', name) if unicodedata.category(c) != 'Mn')

    def _normalize_team(self, team_code):
        """Padroniza siglas"""
        mapping = {
            "NY": "NYK", "GS": "GSW", "PHO": "PHX", "NO": "NOP", "SA": "SAS", 
            "WSH": "WAS", "UTAH": "UTA", "NOH": "NOP", "BKN": "BRK"
        }
        return mapping.get(team_code, team_code)

    def _map_games(self, games):
        """Mapeia jogos de HOJE"""
        mapping = {}
        if not games: return {}
        for g in games:
            home = self._normalize_team(g.get('home'))
            away = self._normalize_team(g.get('away'))
            if home and away:
                mapping[home] = {"opp": away, "venue": "CASA"}
                mapping[away] = {"opp": home, "venue": "FORA"}
        return mapping

    def get_photo_url(self, player_name):
        clean_name = self._normalize_name(player_name)
        pid = self.player_ids.get(clean_name)
        
        # Fallback
        if not pid:
            parts = clean_name.split()
            if len(parts) >= 2: 
                pid = self.player_ids.get(f"{parts[0]} {parts[-1]}")
        
        if pid: 
            return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"
        return "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"

    def analyze_market(self):
        candidates = [] # Volta a ser uma lista simples (sem agrupamento)
        diagnostics = {
            "total_players": len(self.logs),
            "playing_today": 0,
            "insufficient_data": 0,
            "failed_criteria": 0
        }

        if not self.logs: return [], diagnostics

        for player_name, data in self.logs.items():
            raw_team = data.get('team')
            if not raw_team: continue
            
            team = self._normalize_team(raw_team)
            if team not in self.games_map: continue
            
            diagnostics["playing_today"] += 1
            logs = data.get('logs', {})
            
            # Detecta Estrela
            avg_pts = 0
            if 'PTS' in logs and len(logs['PTS']) > 0:
                avg_pts = sum(logs['PTS'][:10]) / len(logs['PTS'][:10])
            is_star = avg_pts >= 20

            # Loop por Stats
            for stat_type in ['AST', 'REB']:
                values = logs.get(stat_type, [])
                if len(values) < 10: 
                    diagnostics["insufficient_data"] += 1
                    continue

                l25 = values[:25]
                total = len(l25)
                
                # Cálculos
                pct_5 = (sum(1 for x in l25 if x >= 5) / total) * 100
                pct_7 = (sum(1 for x in l25 if x >= 7) / total) * 100
                pct_10 = (sum(1 for x in l25 if x >= 10) / total) * 100

                # Critérios (Mantendo a inteligência V4)
                min_safe = 40 if is_star else 50
                min_explosion = 5 if stat_type == 'AST' else 8

                if pct_5 >= min_safe and pct_10 >= min_explosion:
                    
                    # Define Arquétipo
                    arch = "GLUE GUY"
                    if is_star: arch = "⭐ SUPERSTAR"
                    elif pct_10 > 25: arch = "DYNAMITE 🧨"
                    elif pct_5 > 85 and pct_10 < 15: arch = "RELOGINHO 🕰️"
                    
                    # FORMATO ANTIGO (FLAT) PARA COMPATIBILIDADE
                    candidates.append({
                        "player": player_name,
                        "team": raw_team,
                        "opp": self.games_map[team]['opp'],
                        "venue": self.games_map[team]['venue'],
                        "stat": stat_type,
                        "photo": self.get_photo_url(player_name),
                        "metrics": {
                            "Safe_5": int(pct_5),
                            "Target_7": int(pct_7),
                            "Ceiling_10": int(pct_10)
                        },
                        "archetype": arch # A chave que estava faltando!
                    })
                else:
                    diagnostics["failed_criteria"] += 1

        # Ordena: Superstars primeiro, depois Teto de Explosão
        return sorted(candidates, key=lambda x: (x['archetype'] == "⭐ SUPERSTAR", x['metrics']['Ceiling_10']), reverse=True), diagnostics

# ============================================================================
# PÁGINA: O GARIMPO (SGP FACTORY) - V3.0 (L25 PRECISION + HIT RATES)
# ============================================================================
def show_garimpo_page():
    import streamlit as st
    import pandas as pd
    import numpy as np
    import re
    import unicodedata
    import requests
    
    # --- 1. CONFIGURAÇÃO & CSS ---
    try:
        from modules.new_modules.vacuum_matrix import VacuumMatrixAnalyzer
    except ImportError:
        class VacuumMatrixAnalyzer:
            def analyze_team_vacuum(self, roster, team): return {}

    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Oswald:wght@400;600&family=Inter:wght@400;600&display=swap');
        .garimpo-header { font-family: 'Oswald'; font-size: 32px; color: #fbbf24; margin:0; text-transform: uppercase; text-shadow: 0 0 10px rgba(251,191,36,0.3); }
        .garimpo-sub { font-family: 'Inter'; font-size: 13px; color: #94a3b8; margin-bottom: 20px; }
        
        .nugget-card { 
            background: linear-gradient(145deg, #1e293b 0%, #0f172a 100%); 
            border: 1px solid #334155; 
            border-radius: 12px; 
            padding: 12px; 
            margin-bottom: 12px; 
            border-left: 4px solid #fbbf24; 
            transition: transform 0.2s;
        }
        .nugget-card:hover { transform: translateY(-2px); border-color: #fbbf24; }
        
        .nugget-header { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; border-bottom: 1px solid rgba(255,255,255,0.05); padding-bottom: 6px; }
        .nugget-img { width: 40px; height: 40px; border-radius: 50%; border: 2px solid #fbbf24; object-fit: cover; background:#000; }
        .nugget-name { font-family: 'Oswald'; font-size: 15px; color: #fff; line-height: 1.1; margin: 0; }
        .nugget-meta { font-size: 10px; color: #94a3b8; }
        
        .stat-row { display: flex; align-items: center; margin-bottom: 4px; font-size: 10px; color: #cbd5e1; }
        .stat-label { width: 60px; font-weight: bold; }
        .stat-bar-bg { flex: 1; height: 5px; background: #334155; border-radius: 3px; overflow: hidden; margin: 0 8px; }
        .stat-bar-fill { height: 100%; border-radius: 3px; }
        .stat-val { width: 30px; text-align: right; font-family: 'Oswald'; color: #fff; }
        .fill-pts { background: #ef4444; } .fill-reb { background: #3b82f6; } .fill-ast { background: #fbbf24; }
        
        .nugget-footer { margin-top: 8px; display: flex; justify-content: space-between; align-items: center; font-size: 10px; border-top: 1px solid #334155; padding-top: 6px; }
        .conf-tag { color: #10b981; font-weight: bold; font-family: 'Oswald'; font-size: 13px; }
        .hit-tag { background: #064e3b; color: #6ee7b7; padding: 1px 4px; border-radius: 3px; font-size: 9px; border: 1px solid #059669; }
        
        .badge-est { background: #451a03; color: #fdba74; padding: 1px 4px; border-radius: 3px; font-size: 8px; border: 1px solid #f97316; }
        .badge-type { background: rgba(251, 191, 36, 0.1); color: #fbbf24; padding: 2px 5px; border-radius: 4px; font-weight: bold; border: 1px solid rgba(251, 191, 36, 0.3); }
    </style>
    """, unsafe_allow_html=True)

    # --- 2. LAYOUT DE FILTROS ---
    c_head, c_filt = st.columns([2, 3])
    with c_head:
        st.markdown('<div class="garimpo-header">⚒️ O GARIMPO</div>', unsafe_allow_html=True)
        st.markdown('<div class="garimpo-sub">L25 Precision V3.0 (Real Data)</div>', unsafe_allow_html=True)
    
    with c_filt:
        cf1, cf2 = st.columns(2)
        with cf1:
            tier_filter = st.multiselect(
                "🎯 Kits Desejados:",
                ['👶 BASE', '🛡️ SEGURANÇA', '⚙️ OPERÁRIO', '🚀 ELITE'],
                default=['🛡️ SEGURANÇA', '⚙️ OPERÁRIO', '🚀 ELITE']
            )
        with cf2:
            min_conf = st.slider("🎚️ Confiança Mínima:", 50, 95, 65, 5)

    # --- 3. DADOS (L25 REAL) ---
    if 'scoreboard' not in st.session_state or not st.session_state.scoreboard:
        st.warning("⚠️ Scoreboard vazio. Atualize na aba Config.")
        return

    # Tenta pegar os LOGS REAIS (L25)
    try:
        from SuiteNAS import get_data_universal
        cache_logs = get_data_universal("real_game_logs")
    except:
        cache_logs = st.session_state.get("real_game_logs", {})

    if not cache_logs:
        st.error("❌ Cache L25 vazio. Vá em Config > Reconstruir Cache (V3.5).")
        return

    # --- 4. ENGINE LOCAL ---
    def nuclear_normalize(text):
        if not text: return ""
        try: return re.sub(r'[^A-Z0-9]', '', unicodedata.normalize('NFKD', str(text)).encode('ASCII', 'ignore').decode('utf-8').upper())
        except: return ""

    # Recupera ID map do L5 apenas para fotos (se disponível)
    df_l5 = st.session_state.get('df_l5', pd.DataFrame())
    ID_VAULT = {}
    if not df_l5.empty:
        try:
            for _, row in df_l5.iterrows():
                if row.get('PLAYER_ID'): ID_VAULT[nuclear_normalize(row['PLAYER'])] = int(row['PLAYER_ID'])
        except: pass

    def get_photo(name, pid_direct=None):
        if pid_direct: return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid_direct}.png"
        pid = ID_VAULT.get(nuclear_normalize(name), 0)
        return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png" if pid else "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"

    class LocalMonteCarlo:
        def __init__(self, sims=1000): self.sims = sims
        
        def analyze_series(self, data_series, target):
            """Analisa uma série real de dados (L25)"""
            if not data_series: return 0
            
            # Hit Rate Real (Quantas vezes bateu na amostra)
            hits = sum(1 for x in data_series if x >= target)
            hit_rate = (hits / len(data_series)) * 100
            
            # Monte Carlo Baseado na Distribuição Real
            mean = np.mean(data_series)
            std = np.std(data_series)
            if std == 0: std = 0.1 # Evita div por zero se for constante
            
            simulations = np.random.normal(mean, std, self.sims)
            successes = np.sum(simulations >= target)
            mc_prob = (successes / self.sims) * 100
            
            # Média Ponderada: 40% Hit Rate Histórico + 60% Probabilidade Estatística
            final_prob = (hit_rate * 0.4) + (mc_prob * 0.6)
            
            return min(99, max(1, final_prob)), hit_rate, len(data_series)

    @st.cache_data(ttl=600)
    def scan_injuries_live(games):
        blacklist = set()
        active_rosters = {}
        map_espn = {"UTA":"utah","NOP":"no","NYK":"ny","GSW":"gs","SAS":"sa","PHX":"pho","WAS":"wsh","BKN":"bkn"}
        teams = set()
        for g in games: teams.add(g['home']); teams.add(g['away'])
        
        for t in teams:
            t_code = map_espn.get(t.upper(), t.lower())
            try:
                url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{t_code}/roster"
                r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2)
                if r.status_code == 200:
                    active_rosters[t] = []
                    for ath in r.json().get('athletes', []):
                        name = ath.get('fullName', '')
                        status = ath.get('status', {}).get('type', {}).get('name', 'Active')
                        active_rosters[t].append({'name': name, 'status': status, 'pos': ath.get('position',{}).get('abbreviation','F')})
                        if status != 'Active': blacklist.add(nuclear_normalize(name))
            except: pass
        return blacklist, active_rosters

    # --- 5. MINER ENGINE (L25 REAL DATA) ---
    class GoldMinerL25:
        def __init__(self, logs_data, blacklist, rosters):
            self.logs = logs_data
            self.blacklist = blacklist
            self.rosters = rosters
            self.monte_carlo = LocalMonteCarlo(sims=800)
            self.vacuum = VacuumMatrixAnalyzer()

        def _safe_parse_list(self, data_list, max_len=25):
            clean = []
            if not isinstance(data_list, list): return []
            for x in data_list[:max_len]:
                if x is None: continue
                try: clean.append(float(str(x).replace(':', '.')))
                except: pass
            return clean

        def _smart_estimate_minutes(self, pts_avg, reb_avg, ast_avg):
            prod = pts_avg + (reb_avg * 1.2) + (ast_avg * 1.5)
            if prod >= 30: return 34.0
            if prod >= 20: return 29.0
            if prod >= 12: return 24.0
            return 16.0

        def mine_nuggets(self):
            best_nuggets = {} 
            
            for player_name, p_data in self.logs.items():
                norm = nuclear_normalize(player_name)
                if norm in self.blacklist: continue
                
                raw_logs = p_data.get('logs', {})
                if not raw_logs: continue
                
                # Extrai Séries Reais (Até 25 jogos)
                s_pts = self._safe_parse_list(raw_logs.get('PTS', []), 25)
                s_reb = self._safe_parse_list(raw_logs.get('REB', []), 25)
                s_ast = self._safe_parse_list(raw_logs.get('AST', []), 25)
                s_min = self._safe_parse_list(raw_logs.get('MIN', []), 25)
                
                if len(s_pts) < 3: continue # Amostra mínima
                
                # Médias
                avg_pts = np.mean(s_pts)
                avg_reb = np.mean(s_reb)
                avg_ast = np.mean(s_ast)
                
                # Minutos (Real ou Estimado)
                min_source = "L25"
                if len(s_min) >= 3:
                    avg_min = np.mean(s_min)
                else:
                    avg_min = self._smart_estimate_minutes(avg_pts, avg_reb, avg_ast)
                    min_source = "EST"

                # Vacuum (Simplificado para o exemplo)
                # No código real, passaríamos o roster aqui
                vacuum_boost = 1.0
                
                # Aplica boost nas médias APENAS para verificação de tier
                proj_pts = avg_pts * vacuum_boost
                proj_reb = avg_reb * vacuum_boost
                proj_ast = avg_ast * vacuum_boost
                
                # Filtro de Relevância
                if avg_min < 18 or (proj_pts + proj_reb + proj_ast < 10): continue

                kits_priority = [
                    {'name': 'Kit Teto', 'req': (15, 5, 4), 'label': '🚀 ELITE', 'rank': 4},
                    {'name': 'Kit Padrão', 'req': (12, 4, 2), 'label': '⚙️ OPERÁRIO', 'rank': 3},
                    {'name': 'Kit Piso', 'req': (8, 3, 2), 'label': '🛡️ SEGURANÇA', 'rank': 2},
                    {'name': 'Kit Iniciante', 'req': (6, 2, 1), 'label': '👶 BASE', 'rank': 1},
                ]

                selected_kit = None
                
                for kit in kits_priority:
                    if kit['label'] not in tier_filter: continue
                    
                    r_pts, r_reb, r_ast = kit['req']
                    
                    # Pré-filtro rápido na média
                    if proj_pts < r_pts or proj_reb < r_reb or proj_ast < r_ast: continue
                    
                    # Análise Monte Carlo com Dados Reais
                    prob_pts, hit_pts, _ = self.monte_carlo.analyze_series(s_pts, r_pts)
                    prob_reb, hit_reb, _ = self.monte_carlo.analyze_series(s_reb, r_reb)
                    prob_ast, hit_ast, _ = self.monte_carlo.analyze_series(s_ast, r_ast)
                    
                    min_prob = min(prob_pts, prob_reb, prob_ast)
                    avg_prob = (prob_pts + prob_reb + prob_ast) / 3
                    
                    # Filtro de Confiança
                    if avg_prob >= min_conf and min_prob >= (min_conf - 15): 
                        combined = (prob_pts/100) * (prob_reb/100) * (prob_ast/100) * 100
                        
                        # Hit Rate Combinado (Média dos Hits das 3 pernas)
                        avg_hit_rate = (hit_pts + hit_reb + hit_ast) / 3
                        
                        selected_kit = {
                            'kit': kit,
                            'lines': {'PTS': r_pts, 'REB': r_reb, 'AST': r_ast},
                            'probs': {'PTS': prob_pts, 'REB': prob_reb, 'AST': prob_ast},
                            'combined_prob': combined,
                            'avg_hit_rate': avg_hit_rate,
                            'sample_size': len(s_pts)
                        }
                        break 

                if selected_kit:
                    tm = p_data.get('team', 'UNK')
                    best_nuggets[norm] = {
                        'player': player_name, 
                        'team': tm, 
                        'id': p_data.get('id'),
                        'kit_type': selected_kit['kit']['label'],
                        'lines': selected_kit['lines'],
                        'probs': selected_kit['probs'],
                        'proj_min': avg_min, 
                        'min_source': min_source,
                        'combined_prob': selected_kit['combined_prob'],
                        'rank': selected_kit['kit']['rank'],
                        'avg_hit_rate': selected_kit['avg_hit_rate'],
                        'sample': selected_kit['sample_size']
                    }
                            
            return list(best_nuggets.values())

    # --- 6. EXECUÇÃO ---
    if st.button("⚒️ PROSPECTAR (L25 MODE)", type="primary", use_container_width=True):
        blacklist, rosters = scan_injuries_live(st.session_state.scoreboard)
        miner = GoldMinerL25(cache_logs, blacklist, rosters)
        nuggets = miner.mine_nuggets()
        st.session_state.garimpo_results = nuggets

    # --- 7. EXIBIÇÃO ---
    if 'garimpo_results' in st.session_state:
        results = st.session_state.garimpo_results
        
        if not results:
            st.warning(f"Nenhum jogador encontrado com Confiança > {min_conf}% nos tiers selecionados.")
            return
            
        # Ordenação
        results.sort(key=lambda x: (x['rank'], x['combined_prob']), reverse=True)
        st.success(f"✅ {len(results)} Cards Gerados (Base L25 Real)")
        
        cols = st.columns(3)
        for i, item in enumerate(results):
            col = cols[i % 3]
            lines = item['lines']
            probs = item['probs']
            
            min_badge = ""
            if item.get('min_source') == "EST": min_badge = '<span class="badge-est">⚠️ EST</span>'
            
            # Badge de Hit Rate
            hit_badge = f'<span class="hit-tag">HIT {int(item["avg_hit_rate"])}% (L{item["sample"]})</span>'
            
            card_html = f"""
<div class="nugget-card">
    <div class="nugget-header">
        <img src="{get_photo(item['player'], item.get('id'))}" class="nugget-img">
        <div>
            <div class="nugget-name">{item['player']}</div>
            <div class="nugget-meta">{item['team']} • ~{int(item['proj_min'])}' {min_badge} {hit_badge}</div>
        </div>
    </div>
    <div class="stat-row">
        <div class="stat-label">{lines['PTS']}+ PTS</div>
        <div class="stat-bar-bg"><div class="stat-bar-fill fill-pts" style="width: {probs['PTS']}%;"></div></div>
        <div class="stat-val">{int(probs['PTS'])}%</div>
    </div>
    <div class="stat-row">
        <div class="stat-label">{lines['REB']}+ REB</div>
        <div class="stat-bar-bg"><div class="stat-bar-fill fill-reb" style="width: {probs['REB']}%;"></div></div>
        <div class="stat-val">{int(probs['REB'])}%</div>
    </div>
    <div class="stat-row">
        <div class="stat-label">{lines['AST']}+ AST</div>
        <div class="stat-bar-bg"><div class="stat-bar-fill fill-ast" style="width: {probs['AST']}%;"></div></div>
        <div class="stat-val">{int(probs['AST'])}%</div>
    </div>
    <div class="nugget-footer">
        <span class="badge-type">{item['kit_type']}</span>
        <span class="conf-tag">{int(item['combined_prob'])}% CONF</span>
    </div>
</div>
"""
            with col:
                st.markdown(card_html, unsafe_allow_html=True)
        
        
        
# ==============================================================================
# ☢️ HIT PROP HUNTER V72.0 - UNIFIED DESIGN SYSTEM (MARKET VIEW)
# ==============================================================================

def show_hit_prop_page():
    import concurrent.futures
    import statistics
    import random
    import time
    import pandas as pd
    import streamlit as st
    import os
    import re
    import unicodedata
    from itertools import combinations
    from datetime import datetime
    from collections import defaultdict

    # ==============================================================================
    # 0. HELPER GLOBAL
    # ==============================================================================
    def normalize_team_signature(abbr):
        if not abbr: return "UNK"
        abbr = str(abbr).upper().strip()
        mapping = {
            "GS": "GSW", "PHX": "PHX", "PHO": "PHX", "NO": "NOP", "NOP": "NOP",
            "NY": "NYK", "NYK": "NYK", "SA": "SAS", "SAS": "SAS", "UTAH": "UTA",
            "UTA": "UTA", "WSH": "WAS", "WAS": "WAS", "BK": "BKN", "BKN": "BKN",
            "CHA": "CHA", "CHO": "CHA"
        }
        return mapping.get(abbr, abbr)

    def get_stat_color(stat):
        s = stat.upper()
        if 'PTS' in s: return "#fbbf24" # Amber
        if 'REB' in s: return "#60a5fa" # Blue
        if 'AST' in s: return "#facc15" # Yellow
        if '3PM' in s: return "#22d3ee" # Cyan
        if 'STL' in s or 'BLK' in s: return "#f87171" # Red
        return "#e2e8f0"

    # ==============================================================================
    # 1. FETCHING & CACHE
    # ==============================================================================
    def normalize_cache_keys(cache_data):
        if not cache_data: return {}
        for name, data in cache_data.items():
            if 'logs' not in data: continue
            logs = data['logs']
            if 'FG3M' in logs and '3PM' not in logs: logs['3PM'] = logs['FG3M']
            if 'FG3A' in logs and '3PA' not in logs: logs['3PA'] = logs['FG3A']
            if '3PA' not in logs: logs['3PA'] = logs.get('FGA', [])
            data['logs'] = logs
        return cache_data

    # ==============================================================================
    # 2. ENGINES
    # ==============================================================================

    def generate_atomic_props(cache_data, games):
        atomic_props = []
        game_info_map = {}
        
        if games:
            for g in games:
                try:
                    h = normalize_team_signature(g.get('home'))
                    a = normalize_team_signature(g.get('away'))
                    gid = str(g.get('game_id') or g.get('id') or 'UNK')
                    info = {"game_id": gid, "game_str": f"{a} @ {h}", "home": h, "away": a, "opp_map": {h: a, a: h}}
                    if h != "UNK": game_info_map[h] = info
                    if a != "UNK": game_info_map[a] = info
                except: continue

        teams_active = set(game_info_map.keys())
        min_thresholds = {"PTS": 10, "REB": 4, "AST": 3, "3PM": 1, "STL": 1, "BLK": 1}
        
        for name, data in cache_data.items():
            if not isinstance(data, dict): continue
            raw_team = data.get('team', 'UNK')
            team = normalize_team_signature(raw_team)
            
            is_active = team in teams_active
            opp = "UNK"
            if is_active:
                g_info = game_info_map.get(team)
                g_str = g_info.get('game_str')
                g_id = g_info.get('game_id')
                opp = g_info.get('opp_map', {}).get(team, "UNK")
            else:
                g_info = {}; g_str = "OFF"; g_id = "0"

            logs = data.get('logs', {})
            pid = data.get('id', 0)
            
            for stat, min_req in min_thresholds.items():
                vals = logs.get(stat, [])
                if not vals: continue
                
                for period in [5, 10]:
                    if len(vals) >= period:
                        cut = vals[:period]
                        sorted_cut = sorted(cut)
                        idx = 1 if period == 5 else 2
                        adjusted_floor = sorted_cut[idx]
                        
                        if adjusted_floor >= min_req:
                            real_min = min(cut)
                            if real_min >= adjusted_floor:
                                tag = "💎"
                                hit_txt = f"100% L{period}"
                                score = period + 2
                            else:
                                tag = "🔥"
                                hit_txt = f"80% L{period}"
                                score = period
                                
                            atomic_props.append({
                                "player": name, "team": team, "stat": stat, "opp": opp,
                                "line": int(adjusted_floor),
                                "record_str": f"{tag} {hit_txt}",
                                "hit_simple": hit_txt,
                                "tag": tag, "score": score,
                                "game_info": g_info, "game_display": g_str, 
                                "game_id": g_id, "player_id": pid, "active": is_active
                            })
                            
        return sorted(atomic_props, key=lambda x: (x['active'], x['score'], x['line']), reverse=True)

    def organize_sgp_lab(atomic_props):
        sgp_structure = {}
        for p in atomic_props:
            game_key = p.get('game_display', 'UNK')
            if 'UNK' in game_key or 'OFF' in game_key: continue
            
            if game_key not in sgp_structure: sgp_structure[game_key] = {}
            
            p_name = p['player']
            if p_name not in sgp_structure[game_key]:
                sgp_structure[game_key][p_name] = {
                    "player": p_name, "team": p['team'], "id": p.get('player_id', 0),
                    "props": []
                }
            sgp_structure[game_key][p_name]['props'].append(p)

        final_output = {}
        for game, players_dict in sgp_structure.items():
            player_list = list(players_dict.values())
            player_list.sort(key=lambda x: sum(3 if '💎' in prop['tag'] else 1 for prop in x['props']), reverse=True)
            final_output[game] = player_list
        return final_output

    def generate_specialties(cache_data, games):
        specs_3pm = []
        specs_def = []
        active_teams = set()
        game_info_map = {}
        
        for g in games:
            h = normalize_team_signature(g.get('home'))
            a = normalize_team_signature(g.get('away'))
            active_teams.add(h); active_teams.add(a)
            game_info_map[h] = a; game_info_map[a] = h
            
        for name, data in cache_data.items():
            if not isinstance(data, dict): continue
            raw_team = data.get('team', 'UNK')
            team = normalize_team_signature(raw_team)
            if team not in active_teams: continue
            opp = game_info_map.get(team, "UNK")
            
            logs = data.get('logs', {})
            pid = data.get('id', 0)
            
            threes = logs.get('3PM', [])
            attempts = logs.get('3PA', []) 
            if len(threes) >= 5:
                avg_vol = sum(attempts[:10])/len(attempts[:10]) if attempts else 0
                floor = min(threes[:5])
                if floor >= 2 or (sum(threes[:5])/5 >= 2.5):
                    specs_3pm.append({
                        "player": name, "team": team, "id": pid, "stat": "3PM", "opp": opp,
                        "line": max(2, int(floor)), "sub_text": f"Vol: {avg_vol:.1f}"
                    })

            for stat, lbl in [('STL','STL'), ('BLK','BLK')]:
                vals = logs.get(stat, [])
                if len(vals) >= 5 and min(vals[:5]) >= 1:
                    specs_def.append({
                        "player": name, "team": team, "id": pid, "stat": lbl, "opp": opp,
                        "line": 1, "sub_text": "🔒 100% L5"
                    })
                        
        return {"3PM": sorted(specs_3pm, key=lambda x: x['line'], reverse=True), 
                "DEF": sorted(specs_def, key=lambda x: x['player'])}

    # --- SQUADRON ENGINE V4 ---
    class SquadronEngine:
        def generate_combos(self, sgp_data):
            tickets = []
            usage_counter = defaultdict(int)
            global_pool = [] 
            
            for game_str, players in sgp_data.items():
                anchors = []
                role_players = []
                for p in players:
                    unique_stats = set()
                    clean_props = []
                    for prop in p['props']:
                        if prop['stat'] not in unique_stats:
                            unique_stats.add(prop['stat'])
                            clean_props.append(prop)
                    
                    score = sum(3 if "💎" in prop['tag'] else 1 for prop in clean_props)
                    has_pts = any(x['stat'] == 'PTS' for x in clean_props)
                    p_obj = {"player": p['player'], "team": p['team'], "id": p['id'], "score": score, "props": clean_props}
                    global_pool.append(p_obj)
                    if score >= 3 and has_pts: anchors.append(p_obj)
                    elif score >= 1: role_players.append(p_obj)
                
                anchors.sort(key=lambda x: x['score'], reverse=True)
                role_players.sort(key=lambda x: x['score'], reverse=True)
                
                for anchor in anchors:
                    if usage_counter[anchor['player']] >= 2: continue 
                    partners = [rp for rp in role_players if rp['player'] != anchor['player'] and usage_counter[rp['player']] < 2]
                    
                    if len(partners) >= 1:
                        legs = []
                        ap = anchor['props'][0]
                        legs.append({"player": anchor['player'], "team": anchor['team'], "id": anchor['id'], 
                                     "stat": ap['stat'], "line": ap['line'], "record": ap.get('record_str', 'N/A'), "role": "👑 LÍDER"})
                        
                        p1 = partners[0]
                        pr1 = p1['props'][0]
                        legs.append({"player": p1['player'], "team": p1['team'], "id": p1['id'], 
                                     "stat": pr1['stat'], "line": pr1['line'], "record": pr1.get('record_str', 'N/A'), "role": "🚀 BOOSTER"})
                        
                        if len(partners) > 1:
                            p2 = partners[1]
                            pr2 = p2['props'][0]
                            legs.append({"player": p2['player'], "team": p2['team'], "id": p2['id'], 
                                         "stat": pr2['stat'], "line": pr2['line'], "record": pr2.get('record_str', 'N/A'), "role": "⚙️ MOTOR"})
                        
                        tickets.append({
                            "id": f"SGP_{len(tickets)}",
                            "title": f"SGP: {game_str.split('@')[0]} vs {game_str.split('@')[1]}",
                            "legs": legs,
                            "total_props": len(legs)
                        })
                        usage_counter[anchor['player']] += 1
                        usage_counter[partners[0]['player']] += 1
                        if len(partners) > 1: usage_counter[partners[1]['player']] += 1
            
            if len(tickets) < 10:
                available = [p for p in global_pool if usage_counter[p['player']] < 2]
                available.sort(key=lambda x: x['score'], reverse=True)
                while len(available) >= 2 and len(tickets) < 12:
                    p1 = available.pop(0)
                    p2 = next((p for p in available if p['team'] != p1['team']), None)
                    if p2: available.remove(p2)
                    if p1 and p2:
                        legs = []
                        pr1 = p1['props'][0]
                        legs.append({"player": p1['player'], "team": p1['team'], "id": p1['id'], "stat": pr1['stat'], "line": pr1['line'], "record": pr1.get('record_str', 'N/A'), "role": "🔥 PICK 1"})
                        pr2 = p2['props'][0]
                        legs.append({"player": p2['player'], "team": p2['team'], "id": p2['id'], "stat": pr2['stat'], "line": pr2['line'], "record": pr2.get('record_str', 'N/A'), "role": "🔥 PICK 2"})
                        tickets.append({"id": f"MIX_{len(tickets)}", "title": "⚡ MIX: Híbrido", "legs": legs, "total_props": len(legs)})
            return tickets

    # ==============================================================================
    # 3. RENDER UI (DESIGN SYSTEM UNIFICADO)
    # ==============================================================================
    
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Oswald:wght@400;600&family=Inter:wght@400;600&display=swap');
        
        .prop-title { font-family: 'Oswald'; font-size: 30px; color: #fff; margin-bottom: 5px; }
        
        /* HEADER DO JOGO (SGP / COMBOS) */
        .game-header {
            background: #0f172a; border-left: 4px solid #3b82f6; padding: 8px 15px; margin-top: 20px; margin-bottom: 10px;
            font-family: 'Oswald'; font-size: 18px; color: #fff; border-radius: 0 4px 4px 0;
        }
        
        /* CHIP UNIFICADO (O ESTILO MESTRE) */
        .stat-chip-compact {
            display: inline-flex; flex-direction: column; align-items: center; justify-content: center;
            background: #1e293b; border: 1px solid #334155; border-radius: 6px;
            padding: 4px 8px; margin-right: 5px; margin-bottom: 5px; min-width: 65px;
            transition: transform 0.2s;
        }
        .stat-chip-compact:hover { transform: translateY(-2px); border-color: #94a3b8; }
        .scc-top { font-family: 'Oswald'; font-size: 15px; font-weight: bold; line-height: 1; margin-bottom: 2px; }
        .scc-bot { font-family: 'Inter'; font-size: 8px; color: #94a3b8; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }
        
        /* LINHA DE JOGADOR UNIFICADA (O CARD MESTRE) */
        .sgp-row { display: flex; align-items: center; gap: 12px; margin-bottom: 8px; border-bottom: 1px dashed rgba(255,255,255,0.1); padding-bottom: 8px; }
        .sgp-row:last-child { border-bottom: none; }
        .sgp-img { width: 45px; height: 45px; border-radius: 50%; object-fit: cover; border: 2px solid #475569; background:#000; flex-shrink: 0; }
        
        .sgp-name { font-family: 'Oswald'; font-size: 15px; color: #fff; line-height: 1.1; margin-bottom: 4px; }
        .sgp-sub { font-size: 10px; color: #94a3b8; font-family: 'Inter'; margin-bottom: 4px; }
        .sgp-role { font-size: 9px; color: #3b82f6; font-weight: bold; background: rgba(59,130,246,0.1); padding: 1px 4px; border-radius: 3px; margin-left: 4px; }
        
        /* CONTAINER COMBO */
        .combo-container { border: 1px solid #334155; background: rgba(15,23,42,0.4); border-radius: 8px; padding: 10px; margin-bottom: 15px; }
        .combo-title { font-family: 'Oswald'; font-size: 14px; color: #e2e8f0; margin-bottom: 8px; padding-bottom: 4px; border-bottom: 1px solid #334155; }
    </style>
    """, unsafe_allow_html=True)

    # SETUP
    games = st.session_state.get('scoreboard', [])
    if not games:
        st.warning("⚠️ Scoreboard vazio. Atualize na aba Config.")
        return

    cache_raw = get_data_universal("real_game_logs") or {}
    cache_data = normalize_cache_keys(cache_raw)
    if not cache_data:
        st.error("❌ Cache vazio.")
        return

    df_l5 = st.session_state.get('df_l5', pd.DataFrame())
    ID_VAULT = {}
    if not df_l5.empty:
        try:
            c_id = next((c for c in df_l5.columns if 'ID' in c), 'PLAYER_ID')
            c_name = next((c for c in df_l5.columns if 'PLAYER' in c), 'PLAYER')
            for _, row in df_l5.iterrows():
                pid = int(float(row.get(c_id, 0)))
                if pid > 0:
                    nm = str(row.get(c_name, '')).upper().replace('.','')
                    ID_VAULT[nm] = pid
                    if len(nm.split()) > 1: ID_VAULT[nm.split()[-1]] = pid
        except: pass

    def get_photo(name, pid=0):
        if pid > 0: return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"
        clean = str(name).upper().replace('.','')
        pid = ID_VAULT.get(clean, 0)
        if pid == 0 and len(clean.split()) > 1: pid = ID_VAULT.get(clean.split()[-1], 0)
        if pid > 0: return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"
        return "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"

    # RUN
    atomic_props = generate_atomic_props(cache_data, games)
    sgp_data = organize_sgp_lab(atomic_props)
    specs = generate_specialties(cache_data, games)
    sq_engine = SquadronEngine()
    combo_tickets = sq_engine.generate_combos(sgp_data)

    # RENDER
    st.markdown('<div class="prop-title">🎯 HIT PROP HUNTER</div>', unsafe_allow_html=True)
    
    st.markdown("""
    <div style="background:rgba(30,41,59,0.5); padding:10px; border-radius:6px; font-size:12px; color:#cbd5e1; margin-bottom:20px; border:1px solid #334155;">
        <strong>LEGENDA:</strong> 💎 <strong>100%</strong> (Invicto na Janela) • 🔥 <strong>80%</strong> (1 Erro na Janela) • <span style="color:#fbbf24">PTS</span> <span style="color:#60a5fa">REB</span> <span style="color:#facc15">AST</span>
    </div>
    """, unsafe_allow_html=True)

    tab_combos, tab_trends, tab_specs, tab_sgp, tab_radar = st.tabs([
        "🧬 COMBOS", "🔥 TOP TRENDS", "💎 ESPECIALIDADES", "🧪 SUPERBILHETE", "📋 RADAR"
    ])

# --- ABA 1: COMBOS (VISUAL NATIVE STANDARD V18) ---
    with tab_combos:
        # CSS Específico para garantir a consistência nesta aba
        st.markdown("""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Oswald:wght@400;600;700&family=Inter:wght@400;600&display=swap');
            
            .sgp-name { 
                font-family: 'Oswald'; font-size: 14px; color: #ffffff !important; 
                font-weight: 700; line-height: 1.1; margin-bottom: 4px;
                text-shadow: 0 1px 2px rgba(0,0,0,0.5);
            }
            .role-pill { 
                font-size: 8px; padding: 1px 5px; border-radius: 3px; 
                background: #334155; color: #cbd5e1; display: inline-block; 
                margin-right: 5px; vertical-align: middle; 
            }
            .stat-chip-simple {
                display: inline-block;
                background: #1e293b; border: 1px solid #475569; 
                padding: 3px 8px; border-radius: 4px; 
                margin-right: 4px; margin-bottom: 4px;
            }
        </style>
        """, unsafe_allow_html=True)

        if not combo_tickets:
            st.info("Nenhum combo automático encontrado hoje.")
        
        # Grid Nativo
        t_col1, t_col2 = st.columns(2)
        
        for i, ticket in enumerate(combo_tickets):
            col_target = t_col1 if i % 2 == 0 else t_col2
            
            with col_target:
                # Container Nativo (Anti-Explosão)
                with st.container(border=True):
                    # Header HTML Seguro
                    # Define uma cor padrão azul/roxa para combos
                    header_color = "#8b5cf6" 
                    st.markdown(f"<div style='border-left: 4px solid {header_color}; padding-left: 10px; margin-bottom: 10px;'>"
                                f"<div style='font-family:Oswald; font-size:16px; color:white;'>{ticket['title']}</div>"
                                f"<div style='font-size:11px; color:#94a3b8;'>Combo Automático • {len(ticket['legs'])} Legs</div>"
                                f"</div>", unsafe_allow_html=True)

                    # 1. Agrupamento por Jogador (Lógica SGP)
                    # Isso junta stats se o mesmo jogador aparecer 2x no combo
                    from collections import defaultdict
                    player_legs = defaultdict(list)
                    player_meta = {}
                    
                    for leg in ticket['legs']:
                        p_name = leg['player']
                        player_legs[p_name].append(leg)
                        # Salva metadados (ID, Role, Time se tiver)
                        player_meta[p_name] = {
                            'id': leg.get('id', 0),
                            'role': leg.get('role', 'BASE'),
                            'team': leg.get('team', '') # Se tiver info de time
                        }

                    # 2. Renderização dos Jogadores
                    for p_name, legs in player_legs.items():
                        meta = player_meta[p_name]
                        
                        # Layout Rígido: Imagem (1) | Texto (4)
                        c_img, c_data = st.columns([1, 4])
                        
                        with c_img:
                            # Usa a função get_photo que já está no escopo global
                            photo_url = get_photo(p_name, meta['id'])
                            st.image(photo_url, width=42)
                        
                        with c_data:
                            # Ícones de Role
                            role_map = {'ANCHOR':'👑','MOTOR':'⚙️','WORKER':'👷','BASE':'🛡️'}
                            # Tenta mapear o role (pode vir minúsculo ou diferente, normalizamos)
                            role_key = str(meta['role']).upper()
                            icon = role_map.get(role_key, '🎯')
                            
                            # Título (Nome Branco)
                            st.markdown(f"<div class='sgp-name'>{p_name}</div>", unsafe_allow_html=True)
                            
                            # Role Pill
                            st.markdown(f"<div style='margin-bottom:4px'><span class='role-pill'>{icon} {role_key}</span></div>", unsafe_allow_html=True)
                            
                            # Chips de Stats (Visual Limpo V18)
                            chips_str = ""
                            for l in legs:
                                clr = "#fbbf24" if l['stat'] == 'PTS' else ("#60a5fa" if l['stat'] == 'REB' else "#facc15")
                                line_display = f"{l['line']}+ {l['stat']}"
                                
                                # Adiciona hit rate pequeno se existir, senão só a linha
                                rec_val = l.get('record', '').replace('💎','').replace('🔥','').strip()
                                # Se quiser super limpo igual V18, remove o rec_val. 
                                # Vou deixar apenas a linha para manter o padrão "Clean UI" que você aprovou.
                                
                                chips_str += f"""<span class='stat-chip-simple'>
                                    <strong style='font-family:Oswald; color:{clr}; font-size:12px'>{line_display}</strong>
                                </span>"""
                            
                            st.markdown(chips_str, unsafe_allow_html=True)

                        # Espaçamento entre jogadores
                        st.markdown("<div style='margin-bottom:6px'></div>", unsafe_allow_html=True)

    # --- ABA 2: TOP TRENDS (VISÃO DE MERCADO / RANKING) ---
    with tab_trends:
        def filter_redundant_props(props_list):
            best_props = {}
            for p in props_list:
                stat = p['stat']
                if stat not in best_props: best_props[stat] = p
                else:
                    curr = best_props[stat]
                    if "💎" in p['tag'] and "💎" not in curr['tag']: best_props[stat] = p
                    elif p['tag'] == curr['tag'] and p['line'] > curr['line']: best_props[stat] = p
            return list(best_props.values())

        # Agrupa por STAT em vez de Jogo
        trends_by_stat = defaultdict(list)
        for p in atomic_props:
            trends_by_stat[p['stat']].append(p)
        
        stat_cols = st.columns(3) # PTS | REB | AST
        display_stats = ["PTS", "REB", "AST"]
        
        for i, stat in enumerate(display_stats):
            with stat_cols[i]:
                st.markdown(f"#### 🏆 Top {stat}")
                props_list = trends_by_stat.get(stat, [])
                # Filtra: Apenas 100% ou 80% muito bons, e remove duplicatas de jogador (pega melhor linha)
                unique_players = {}
                for p in props_list:
                    if p['player'] not in unique_players: unique_players[p['player']] = p
                    else:
                        curr = unique_players[p['player']]
                        if p['score'] > curr['score']: unique_players[p['player']] = p
                
                final_list = sorted(list(unique_players.values()), key=lambda x: x['score'], reverse=True)[:10] # Top 10
                
                for p in final_list:
                    clr = get_stat_color(p['stat'])
                    photo_url = get_photo(p['player'], p.get('player_id', 0))
                    rec_val = p.get('hit_simple', '')
                    
                    st.markdown(f"""
                    <div class="sgp-row">
                        <img src="{photo_url}" class="sgp-img">
                        <div style="flex:1">
                            <div class="sgp-name">{p['player']}</div>
                            <div class="sgp-sub">{p['team']} vs {p.get('opp','UNK')}</div>
                            <div class="stat-chip-compact">
                                <div class="scc-top" style="color:{clr}">{p['line']}+ {p['stat']}</div>
                                <div class="scc-bot">{rec_val}</div>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

    # --- ABA 3: ESPECIALIDADES (VISUAL SGP) ---
    with tab_specs:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### 🎯 Snipers")
            for s in specs['3PM']:
                photo_url = get_photo(s['player'], s.get('id', 0))
                clr = get_stat_color("3PM")
                st.markdown(f"""
                <div class="sgp-row">
                    <img src="{photo_url}" class="sgp-img">
                    <div style="flex:1">
                        <div class="sgp-name">{s['player']}</div>
                        <div class="sgp-sub">{s['team']} vs {s.get('opp','UNK')}</div>
                        <div class="stat-chip-compact">
                            <div class="scc-top" style="color:{clr}">{s['line']}+ 3PM</div>
                            <div class="scc-bot">{s['sub_text']}</div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        with c2:
            st.markdown("#### 🛡️ Defenders")
            for s in specs['DEF']:
                photo_url = get_photo(s['player'], s.get('id', 0))
                clr = get_stat_color(s['stat'])
                st.markdown(f"""
                <div class="sgp-row">
                    <img src="{photo_url}" class="sgp-img">
                    <div style="flex:1">
                        <div class="sgp-name">{s['player']}</div>
                        <div class="sgp-sub">{s['team']} vs {s.get('opp','UNK')}</div>
                        <div class="stat-chip-compact">
                            <div class="scc-top" style="color:{clr}">1+ {s['stat']}</div>
                            <div class="scc-bot">{s['sub_text']}</div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

    # --- ABA 4: SUPERBILHETE ---
    with tab_sgp:
        if not sgp_data: st.info("Vazio.")
        for game_str, players in sgp_data.items():
            st.markdown(f"<div class='game-header'>🏀 {game_str}</div>", unsafe_allow_html=True)
            for p in players:
                st.markdown(f"""
                <div class="sgp-row">
                    <img src="{get_photo(p['player'], p.get('id', 0))}" class="sgp-img">
                    <div style="flex:1">
                        <div class="sgp-name">{p['player']}</div>
                        <div style="display:flex; flex-wrap:wrap;">
                """, unsafe_allow_html=True)
                
                chips_html = ""
                unique_display = filter_redundant_props(p['props'])
                unique_display.sort(key=lambda x: {"PTS":1, "REB":2, "AST":3}.get(x['stat'], 99))

                for prop in unique_display:
                    clr = get_stat_color(prop['stat'])
                    rec_val = prop.get('record_str', 'N/A').replace('💎','').replace('🔥','').strip()
                    chips_html += f"""
                    <div class="stat-chip-compact">
                        <div class="scc-top" style="color:{clr}">{prop['line']}+ {prop['stat']}</div>
                        <div class="scc-bot">{rec_val}</div>
                    </div>
                    """
                st.markdown(chips_html + "</div></div></div>", unsafe_allow_html=True)

    # --- ABA 5: RADAR ---
    with tab_radar:
        if atomic_props:
            df = pd.DataFrame(atomic_props)
            df_disp = df[['player', 'team', 'game_display', 'stat', 'line', 'record_str']].copy()
            st.dataframe(df_disp, use_container_width=True)
            
#============================================================================
# DEFINIÇÕES E NORMALIZAÇÕES
# ============================================================================
def safe_abs_spread(val):
    if val is None: return 0.0
    try: return abs(float(val))
    except Exception: return 0.0
def _status_is_out_or_questionable(status: str) -> bool:
    s = (status or "").lower()
    return ("out" in s) or ("questionable" in s) or ("injur" in s) or ("ir" in s)
def atomic_save(path, obj_bytes):
    dirpath = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=dirpath)
    try:
        with os.fdopen(fd, "wb") as f: f.write(obj_bytes)
        os.replace(tmp, path); return True
    except Exception:
        try:
            if os.path.exists(tmp): os.remove(tmp)
        except Exception: pass
        return False
def save_pickle(path, obj):
    try:
        data = pickle.dumps(obj)
        return atomic_save(path, data)
    except Exception:
        return False

def load_pinnacle_data_auto():
    """Carrega jogos e props da Pinnacle automaticamente na inicialização"""
    if 'pinnacle_last_update' in st.session_state:
        # Cache de 10 minutos (odds não mudam tanto intraday)
        last_update = st.session_state['pinnacle_last_update']
        if (datetime.now() - last_update).total_seconds() < 14400:  # 4hrs
            st.caption("🟢 Odds Pinnacle: Cache recente (atualizado há menos de 10min)")
            return
    
    with st.spinner("🔄 Carregando odds reais da Pinnacle automaticamente..."):
        try:
            client = PinnacleClient("13e1dd2e12msh72d0553fca0e8aap16eeacjsn9d69ddb0d2bb")  # Sua chave
            
            # 1. Jogos do dia (spreads/totals)
            games = client.get_nba_games()
            if games:
                st.session_state['pinnacle_games'] = games
                st.success(f"✅ {len(games)} jogos carregados da Pinnacle")
            
            # 2. Props de todos os jogos (com barra de progresso)
            if games:
                progress_bar = st.progress(0)
                status_text = st.empty()
                full_map = {}
                total = len(games)
                
                for i, game in enumerate(games):
                    status_text.text(f"Props: {i+1}/{total} - {game['away_team']} @ {game['home_team']}")
                    props = client.get_player_props(game['game_id'])
                    for p in props:
                        name = p['player']
                        if name not in full_map:
                            full_map[name] = {}
                        full_map[name][p['market']] = {
                            "line": p['line'],
                            "odds": p['odds']
                        }
                    progress_bar.progress((i + 1) / total)
                
                st.session_state['pinnacle_props_map'] = full_map
                total_props = sum(len(v) for v in full_map.values())
                st.success(f"🎯 {len(full_map)} jogadores | {total_props} props sincronizados!")
                
                progress_bar.empty()
                status_text.empty()
            
            # Marca timestamp
            st.session_state['pinnacle_last_update'] = datetime.now()
            
        except Exception as e:
            st.error(f"Erro ao carregar Pinnacle: {e}")
            st.info("Usando modo offline (projeções internas)")

# Chame isso logo no início do main(), após session_state basics
if __name__ == "__main__":
    # ... seu código de init ...
    load_pinnacle_data_auto()  # <-- Automático!
    # ... resto do app ...


def load_pickle(path):
    try:
        if not os.path.exists(path): return None
        with open(path, "rb") as f: return pickle.load(f)
    except Exception:
        return None
def save_json(path, obj):
    try:
        data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        return atomic_save(path, data)
    except Exception:
        return False
def load_json(path):
    try:
        if not os.path.exists(path): return None
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except Exception:
        return None
class SafetyUtils:
    @staticmethod
    def safe_get(data, keys, default=None):
        try:
            if isinstance(keys, str):
                keys = [keys]
            current = data
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                elif isinstance(current, list) and isinstance(key, int) and 0 <= key < len(current):
                    current = current[key]
                else:
                    return default
            return current if current is not None else default
        except:
            return default
    @staticmethod
    def safe_float(value, default=0.0):
        try:
            if value is None:
                return default
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                cleaned = ''.join(c for c in value if c.isdigit() or c in '.-')
                return float(cleaned) if cleaned else default
            return float(value)
        except:
            return default
def safe_get(dictionary, key, default=None):
    """Acesso seguro a dicionários com fallback"""
    return dictionary.get(key, default)

def calculate_percentiles(values, percentiles=[90, 95]):
    """Calcula percentis de uma lista de valores"""
    if not values:
        return {}
    results = {}
    for p in percentiles:
        results[f'p{p}'] = np.percentile(values, p)
    return results

def exponential_backoff(attempt, max_delay=60):
    """Calcula delay para retry com backoff exponencial"""
    return min(max_delay, (2 ** attempt))

# ============================================================================
# INICIALIZAÇÃO DO GERENCIADOR DE LESÕES (CORRIGIDO FINAL)
# ============================================================================

try:
    # Importa a classe correta: InjuryMonitor
    from injuries import InjuryMonitor
    InjuriesManager = InjuryMonitor
except ImportError:
    try:
        from modules.injuries import InjuryMonitor
        InjuriesManager = InjuryMonitor
    except ImportError:
        InjuriesManager = None


def init_injury_manager():
    """
    Inicializa o InjuryMonitor de forma robusta.
    """
    if InjuriesManager is None:
        return None

    try:
        # Usa a constante de cache se existir, senão vai no padrão
        if 'INJURIES_CACHE_FILE' in globals():
            return InjuriesManager(cache_file=INJURIES_CACHE_FILE)
        else:
            return InjuriesManager()
    except Exception as e:
        if 'st' in globals():
            st.error(f"Erro crítico ao iniciar InjuryMonitor: {e}")
        return None


# ============================================================================
# FUNÇÕES AUXILIARES DE OVERRIDES
# ============================================================================
def load_name_overrides():
    # Certifique-se de que load_json e NAME_OVERRIDES_FILE estão definidos no arquivo
    try:
        data = load_json(NAME_OVERRIDES_FILE)
        return data or {}
    except:
        return {}

def save_name_overrides(overrides):
    try:
        save_json(NAME_OVERRIDES_FILE, overrides)
    except:
        pass

# ============================================================================
# NOVO: FUNÇÕES DE THRESHOLDS DINÂMICOS (FASE 2.1)
# ============================================================================

def calculate_dynamic_threshold(stat, player_data=None, team_data=None):
    league_avg = TEAM_STATS_AVG.get(stat, 0)
    threshold = league_avg
    pos_multiplier = 1.0
    if player_data:
        position = player_data.get("position", "").upper()
        pos_map = {
            "PG": {"PTS": 1.3, "AST": 1.5, "REB": 0.8},
            "SG": {"PTS": 1.3, "AST": 1.1, "REB": 0.8},
            "SF": {"PTS": 1.1, "AST": 0.9, "REB": 1.2},
            "PF": {"PTS": 1.0, "AST": 0.8, "REB": 1.5},
            "C":  {"PTS": 1.0, "AST": 0.6, "REB": 2.0}
        }
        if position in pos_map and stat in pos_map[position]:
            pos_multiplier = pos_map[position][stat]
    adjusted = threshold * pos_multiplier
    # Remover multiplicação duplicada
    adjusted = max(adjusted * 0.7, min(adjusted * 1.3, adjusted))
    return round(adjusted, 1)  # ← Arredondar para 1 casa decimal


def get_team_average_stats(team_abbr, df_l5):
    """Calcula estatísticas médias do time"""
    if df_l5.empty:
        return TEAM_STATS_AVG.copy()
    
    team_players = df_l5[df_l5["TEAM"] == team_abbr]
    if team_players.empty:
        return TEAM_STATS_AVG.copy()
    
    return {
        "PTS": team_players["PTS_AVG"].mean(),
        "REB": team_players["REB_AVG"].mean(),
        "AST": team_players["AST_AVG"].mean(),
        "PRA": team_players["PRA_AVG"].mean()
    }

# ============================================================================
# NOVO: FEATURE STORE LEVE (FASE 2.2)
# ============================================================================

class FeatureStore:
    """Cache centralizado de features"""
    def __init__(self, cache_file=FEATURE_STORE_FILE):
        self.cache_file = cache_file
        self.data = self._load_data()
    
    def _load_data(self):
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    cache_time = datetime.fromisoformat(cache_data.get("timestamp", "1970-01-01"))
                    if (datetime.now() - cache_time).total_seconds() < 900:  # 15 minutos
                        return cache_data.get("data", {})
        except Exception:
            pass
        return {}
    
    def _save_data(self):
        try:
            cache_data = {
                "timestamp": datetime.now().isoformat(),
                "data": self.data
            }
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
        except Exception:
            pass
    
    def get_game_features(self, game_id, away_abbr, home_abbr, df_l5, odds_map, team_advanced, team_opponent):
        """Obtém features para um jogo específico"""
        cache_key = f"{game_id}_{away_abbr}_{home_abbr}"
        
        if cache_key in self.data:
            return self.data[cache_key]
        
        # Construir features (simplificado)
        game_ctx = build_game_context(away_abbr, home_abbr, odds_map, team_advanced, team_opponent)
        
        # Adicionar features avançadas
        home_pace = TEAM_PACE_DATA.get(home_abbr, 100.0)
        away_pace = TEAM_PACE_DATA.get(away_abbr, 100.0)
        game_pace = (home_pace + away_pace) / 2.0
        
        result = {
            "game_ctx": {
                **game_ctx,
                "game_pace": game_pace,
                "is_high_pace": game_pace > 102,
                "is_low_pace": game_pace < 98
            },
            "calculated_at": datetime.now().isoformat()
        }
        
        self.data[cache_key] = result
        self._save_data()
        
        return result

# ============================================================================
# CORREÇÃO: PROCESS ROSTER (AGORA COM ID PARA FOTOS)
# ============================================================================
def process_roster(roster_list, team_abbr, is_home):
    processed = []
    df_l5 = st.session_state.get("df_l5")
    
    # Normalização preventiva das colunas do L5
    if df_l5 is not None and not df_l5.empty:
        df_l5.columns = [str(c).upper().strip() for c in df_l5.columns]

    for entry in roster_list:
        player = normalize_roster_entry(entry)
        player_name = player.get("PLAYER", "N/A")
        
        # ... (Overrides de posição mantidos) ...
        position_overrides = {
            "LeBron James": "SF", "Nikola Jokić": "C", "Luka Dončić": "PG",
            "Giannis Antetokounmpo": "PF", "Jimmy Butler": "SF", "Stephen Curry": "PG",
            "Joel Embiid": "C", "Jayson Tatum": "SF", "Kevin Durant": "SF", 
            "Anthony Davis": "PF", "Bam Adebayo": "C", "Domantas Sabonis": "C"
        }
        pos = position_overrides.get(player_name, player.get("POSITION", "").upper())
        starter = player.get("STARTER", False)
        
        # ... (Lógica de Status mantida) ...
        status_raw = player.get("STATUS", "").lower()
        badge_color = "#9CA3AF"
        status_display = "ACTIVE"
        if any(k in status_raw for k in ["out", "ir", "injur"]):
            badge_color = "#EF4444"; status_display = "OUT"
        elif "questionable" in status_raw or "doubt" in status_raw or "gtd" in status_raw:
            badge_color = "#F59E0B"; status_display = "QUEST"
        elif any(k in status_raw for k in ["active", "available", "probable"]):
            badge_color = "#10B981"; status_display = "ACTIVE"

        # Stats Iniciais
        stats = {
            "MIN_AVG": 0, "PRA_AVG": 0, "PTS_AVG": 0, "REB_AVG": 0, "AST_AVG": 0,
            "ID": 0 # <--- INICIALIZA ID COMO 0
        }
        
        archetypes_clean_list = [] 
        
        if df_l5 is not None and not df_l5.empty:
            matches = df_l5[df_l5["PLAYER"].str.contains(player_name, case=False, na=False)]
            if not matches.empty:
                row = matches.iloc[0]
                
                # --- AQUI ESTAVA O ERRO: AGORA PEGAMOS O ID ---
                try:
                    raw_id = row.get("PLAYER_ID") or row.get("ID") or 0
                    stats["ID"] = int(float(raw_id))
                except: stats["ID"] = 0
                # ----------------------------------------------

                stats["MIN_AVG"] = row.get("MIN_AVG", 0)
                stats["PRA_AVG"] = row.get("PRA_AVG", 0)
                stats["PTS_AVG"] = row.get("PTS_AVG", 0)
                stats["REB_AVG"] = row.get("REB_AVG", 0)
                stats["AST_AVG"] = row.get("AST_AVG", 0)
                
                # ... (Lógica de Arquétipos mantida) ...

        # Role Logic
        role = "deep_bench"
        if starter: role = "starter"
        elif stats["MIN_AVG"] >= 20: role = "rotation"
        elif stats["MIN_AVG"] >= 12: role = "bench"
        
        profile_str = ", ".join(archetypes_clean_list[:2]) if archetypes_clean_list else "-"

        processed.append({
            "PLAYER": player_name,
            "POSITION": pos,
            "ROLE": role,
            "STATUS": status_display,
            "STATUS_BADGE": badge_color,
            "ARCHETYPES": archetypes_clean_list,
            "ID": stats["ID"], # <--- SALVA O ID NO OBJETO FINAL
            
            "MIN_AVG": stats["MIN_AVG"],
            "PRA_AVG": stats["PRA_AVG"],
            "PTS_AVG": stats["PTS_AVG"],
            "REB_AVG": stats["REB_AVG"],
            "AST_AVG": stats["AST_AVG"]
        })
    
    return processed

def validate_pipeline_integrity(required_components=None):
    """
    Valida se os dados necessários para o pipeline estão disponíveis.
    """
    if required_components is None:
        required_components = ['l5', 'scoreboard']
    
    checks = {
        'l5': {
            'name': 'Dados L5 (últimos 5 jogos)',
            'critical': True,
            'status': False,
            'message': ''
        },
        'scoreboard': {
            'name': 'Scoreboard do dia',
            'critical': True,
            'status': False,
            'message': ''
        },
        'odds': {
            'name': 'Odds das casas',
            'critical': False,
            'status': False,
            'message': ''
        },
        'dvp': {
            'name': 'Dados Defense vs Position',
            'critical': False,
            'status': False,
            'message': ''
        },
        'injuries': {
            'name': 'Dados de lesões',
            'critical': False,
            'status': False,
            'message': ''
        },
        'advanced_system': {
            'name': 'Sistema Avançado',
            'critical': False,
            'status': False,
            'message': ''
        }
    }
    
    # Validar L5
    if 'l5' in required_components:
        df_l5 = st.session_state.get('df_l5')
        if df_l5 is not None and hasattr(df_l5, 'shape') and not df_l5.empty:
            checks['l5']['status'] = True
            checks['l5']['message'] = f'Carregados {len(df_l5)} jogadores'
        else:
            checks['l5']['message'] = 'Dados L5 não disponíveis'
    
    # Validar scoreboard
    if 'scoreboard' in required_components:
        scoreboard = st.session_state.get('scoreboard')
        if scoreboard and len(scoreboard) > 0:
            checks['scoreboard']['status'] = True
            checks['scoreboard']['message'] = f'{len(scoreboard)} jogos hoje'
        else:
            checks['scoreboard']['message'] = 'Nenhum jogo encontrado para hoje'
    
    # Validar odds
    if 'odds' in required_components:
        odds = st.session_state.get('odds')
        if odds and len(odds) > 0:
            checks['odds']['status'] = True
            checks['odds']['message'] = f'{len(odds)} jogos com odds'
        else:
            checks['odds']['message'] = 'Odds não disponíveis'
    
    # Validar DvP
    if 'dvp' in required_components:
        dvp_analyzer = st.session_state.get('dvp_analyzer')
        if dvp_analyzer and hasattr(dvp_analyzer, 'defense_data') and dvp_analyzer.defense_data:
            checks['dvp']['status'] = True
            checks['dvp']['message'] = f'Dados de {len(dvp_analyzer.defense_data)} times'
        else:
            checks['dvp']['message'] = 'Dados DvP não disponíveis'
    
    # Validar lesões
    if 'injuries' in required_components:
        injuries = st.session_state.get('injuries_data')
        if injuries and len(injuries) > 0:
            checks['injuries']['status'] = True
            checks['injuries']['message'] = f'Lesões carregadas'
        else:
            checks['injuries']['message'] = 'Dados de lesões não disponíveis'
    
    # Validar sistema avançado
    if 'advanced_system' in required_components:
        if st.session_state.get("use_advanced_features", False):
            checks['advanced_system']['status'] = True
            checks['advanced_system']['message'] = 'Ativo'
        else:
            checks['advanced_system']['message'] = 'Inativo'
    
    # Determinar se todos os componentes críticos estão ok
    all_critical_ok = all(
        check['status'] for key, check in checks.items() 
        if key in required_components and check['critical']
    )
    
    return all_critical_ok, checks

# ============================================================================
# DATA FETCHERS
# ============================================================================
def get_scoreboard_data():
    """
    Função Mestra do Scoreboard (Fuso Horário Brasil Forçado).
    """
    from datetime import datetime, timedelta
    import pandas as pd
    import requests
    import pytz # Obrigatório para corrigir o erro de data

    # --- 1. LÓGICA DE DATA (FUSO SÃO PAULO) ---
    try:
        tz_br = pytz.timezone('America/Sao_Paulo')
        now_br = datetime.now(tz_br)
    except:
        # Fallback se pytz falhar
        now_br = datetime.now()

    # Se for antes das 04:00 da manhã (madrugada), consideramos que ainda é a "noite" do dia anterior
    # Ex: 01:00 AM do dia 05 ainda é "jogo do dia 04"
    if now_br.hour < 4:
        target_date = now_br - timedelta(days=1)
    else:
        target_date = now_br
    
    date_str = target_date.strftime("%Y%m%d")
    
    # --- 2. TENTA LER DO SUPABASE ---
    try:
        # Opcional: Adicionar lógica para verificar se o cache do supabase bate com a data de hoje
        cached = get_data_universal("scoreboard")
        if cached and isinstance(cached, list) and len(cached) > 0:
            # Verifica se o primeiro jogo do cache é da data correta
            first_game_date = cached[0].get('date_str', '')
            if first_game_date == date_str:
                return pd.DataFrame(cached)
            else:
                pass # Cache velho, busca novo
    except: pass

    # --- 3. BAIXA DA ESPN ---
    url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
    params = {"dates": date_str, "limit": 100}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=5)
        if r.status_code != 200: return pd.DataFrame()
        
        data = r.json()
        games_list = []
        
        events = data.get("events", [])
        for evt in events:
            comp = evt["competitions"][0]
            home = next((t for t in comp["competitors"] if t["homeAway"] == "home"), {})
            away = next((t for t in comp["competitors"] if t["homeAway"] == "away"), {})
            
            status_text = evt["status"]["type"]["shortDetail"]
            odds_txt = comp["odds"][0].get("details", "") if "odds" in comp and comp["odds"] else ""
            odds_total = comp["odds"][0].get("overUnder", "") if "odds" in comp and comp["odds"] else ""

            game_dict = {
                "gameId": evt["id"],
                "date_str": date_str,
                "startTimeUTC": comp.get("date"),
                "home": home["team"]["abbreviation"],
                "away": away["team"]["abbreviation"],
                "status": status_text,
                "odds_spread": odds_txt,
                "odds_total": odds_total,
                "home_logo": home["team"].get("logo", ""),
                "away_logo": away["team"].get("logo", "")
            }
            games_list.append(game_dict)

        if games_list:
            save_data_universal("scoreboard", games_list)
            
        return pd.DataFrame(games_list)

    except Exception as e:
        print(f"⚠️ Erro no Scoreboard ESPN: {e}")
        return pd.DataFrame()
        
def fetch_team_roster(team_abbr_or_id, progress_ui=True):
    cache_path = os.path.join(CACHE_DIR, f"roster_{team_abbr_or_id}.json")
    cached = load_json(cache_path)
    if cached:
        return cached
    
    espn_code = ESPN_TEAM_CODES.get(team_abbr_or_id, team_abbr_or_id.lower())
    url = ESPN_TEAM_ROSTER_TEMPLATE.format(team=espn_code)
    
    try:
        if progress_ui:
            st.info(f"Buscando roster para {team_abbr_or_id}...")
        
        r = requests.get(url, timeout=10, headers=HEADERS)
        r.raise_for_status()
        jr = r.json()
        save_json(cache_path, jr)
        return jr
    except Exception as e:
        if progress_ui:
            st.warning(f"Falha ao buscar roster para {team_abbr_or_id}: {e}")
        return {}

# ==============================================================================
# 2. PARSE ODDS (CONSUMIDOR: Transforma Texto em Números)
# ==============================================================================
def fetch_odds_for_today():
    """
    Não baixa nada! Apenas lê o 'scoreboard' já carregado e formata
    as strings de odds (ex: 'LAL -5.5') para números usáveis (-5.5).
    """
    # 1. Pega os dados que acabamos de baixar
    games = st.session_state.get('scoreboard', [])
    
    # Se não tiver jogos, não tem o que fazer
    if not games: return {}

    odds_map = {}
    
    for game in games:
        try:
            away = game.get("away")
            home = game.get("home")
            
            # Pega o dado bruto que salvamos no passo anterior
            spread_str = game.get("odds_spread", "")
            total_val = game.get("odds_total", 0)
            
            spread_val = 0.0
            
            # Lógica de Parsing (Limpeza da String)
            if spread_str and spread_str != "N/A":
                try:
                    # Caso 1: "EVEN" (0.0)
                    if "EVEN" in spread_str.upper() or "PK" in spread_str.upper():
                        spread_val = 0.0
                    else:
                        # Caso 2: "BOS -5.5" -> Pega o último pedaço
                        # Removemos o time e ficamos só com o número
                        parts = spread_str.split()
                        if len(parts) >= 1:
                            # Tenta pegar o último elemento (ex: -5.5)
                            spread_val = float(parts[-1])
                except:
                    spread_val = 0.0
            
            # Parsing do Total
            try: total_float = float(total_val)
            except: total_float = 0.0

            # Cria o objeto limpo
            entry = {
                "spread": spread_val,
                "total": total_float,
                "spread_str": spread_str, # Mantém o texto visual
                "bookmaker": "ESPN"
            }
            
            # --- CRIA CHAVES DE ACESSO ---
            # O sistema pode buscar por "GSW@LAL" ou "GSW @ LAL"
            # Criamos todas as variações para garantir que o Dashboard encontre
            key_clean = f"{away}@{home}"
            key_spaced = f"{away} @ {home}"
            
            odds_map[key_clean] = entry
            odds_map[key_spaced] = entry
            
        except Exception:
            continue
            
    # Atualiza o session state com o mapa pronto
    st.session_state['odds'] = odds_map
    return odds_map

def fetch_team_advanced_stats():
    data = load_json(TEAM_ADVANCED_FILE)
    return data or {}

def fetch_team_opponent_stats():
    data = load_json(TEAM_OPPONENT_FILE)
    return data or {}

# ============================================================================
# L5 (NBA API) (de data_fetchers.py)
# ============================================================================

def fetch_player_stats_safe(pid, name):
    try:
        from nba_api.stats.endpoints import commonplayerinfo, playergamelog
        
        info_df = commonplayerinfo.CommonPlayerInfo(player_id=pid).get_data_frames()[0]
        team = info_df["TEAM_ABBREVIATION"].iloc[0] if "TEAM_ABBREVIATION" in info_df.columns else None
        exp = int(info_df["SEASON_EXP"].iloc[0]) if "SEASON_EXP" in info_df.columns else 0
        logs = playergamelog.PlayerGameLog(player_id=pid, season=SEASON).get_data_frames()[0]
        if logs is None or logs.empty: return None
        logs = logs.head(10)
        for c in ["PTS","REB","AST","MIN"]:
            if c in logs.columns: logs[c] = pd.to_numeric(logs[c], errors="coerce")
        last5 = logs.head(5)
        def cv_of(s):
            s = s.dropna()
            if s.size==0 or s.mean()==0: return 1.0
            return float(s.std(ddof=0)/s.mean())
        pts_avg = float(last5["PTS"].mean()) if "PTS" in last5.columns else 0.0
        reb_avg = float(last5["REB"].mean()) if "REB" in last5.columns else 0.0
        ast_avg = float(last5["AST"].mean()) if "AST" in last5.columns else 0.0
        min_avg = float(last5["MIN"].mean()) if "MIN" in last5.columns else 0.0
        pra_avg = float((last5["PTS"]+last5["REB"]+last5["AST"]).mean()) if all(x in last5.columns for x in ["PTS","REB","AST"]) else 0.0
        pts_cv = cv_of(last5["PTS"]) if "PTS" in last5.columns else 1.0
        reb_cv = cv_of(last5["REB"]) if "REB" in last5.columns else 1.0
        ast_cv = cv_of(last5["AST"]) if "AST" in last5.columns else 1.0
        min_cv = cv_of(last5["MIN"]) if "MIN" in last5.columns else 1.0
        last_min = float(last5["MIN"].iloc[0]) if "MIN" in last5.columns and not last5["MIN"].isna().all() else min_avg
        return {
            "PLAYER_ID": int(pid), "PLAYER": name, "TEAM": team, "EXP": exp,
            "MIN_AVG": min_avg, "PTS_AVG": pts_avg, "REB_AVG": reb_avg,
            "AST_AVG": ast_avg, "PRA_AVG": pra_avg,
            "MIN_CV": min_cv, "PTS_CV": pts_cv, "REB_CV": reb_cv, "AST_CV": ast_cv,
            "LAST_MIN": last_min,
            "min_L5": min_avg, "pts_L5": pts_avg, "reb_L5": reb_avg,
            "ast_L5": ast_avg, "pra_L5": pra_avg,
        }
    except Exception:
        return None

def try_fetch_with_retry(pid, name, tries=3, delay=0.6):
    for attempt in range(tries):
        res = fetch_player_stats_safe(pid, name)
        if res: return res
        time.sleep(delay*(attempt+1))
    return None

# ============================================================================
# FUNÇÃO L5: FOCADA NO SUPABASE (CORRIGIDA - DEDUPLICAÇÃO DE COLUNAS)
# ============================================================================
def get_players_l5(progress_ui=True, force_update=False, incremental=False):
    from nba_api.stats.static import players
    from nba_api.stats.endpoints import playergamelog, scoreboardv2
    import concurrent.futures
    import time
    import json
    import pandas as pd
    from datetime import datetime, timedelta
    
    # CONSTANTES
    SEASON_CURRENT = "2025-26" 
    MAX_WORKERS = 8 
    KEY_L5 = "l5_stats" # Nome exato da chave no Supabase

    # --- 1. TENTA CARREGAR DO SUPABASE ---
    df_cached = pd.DataFrame()
    
    if not force_update:
        try:
            cloud_data = get_data_universal(KEY_L5) 
            
            if cloud_data:
                if isinstance(cloud_data, dict) and "records" in cloud_data:
                    df_cached = pd.DataFrame.from_records(cloud_data["records"])
                    print(f"✅ [SUPABASE] L5 carregado: {len(df_cached)} linhas.")
                elif isinstance(cloud_data, list):
                    df_cached = pd.DataFrame.from_records(cloud_data)
                    print(f"✅ [SUPABASE] L5 carregado (lista): {len(df_cached)} linhas.")
                
                if not df_cached.empty:
                    df_cached.columns = [str(c).upper().strip() for c in df_cached.columns]
                    # Remove duplicatas ao carregar também
                    df_cached = df_cached.loc[:, ~df_cached.columns.duplicated()]
            else:
                print("⚠️ [SUPABASE] Retornou vazio para l5_stats.")
                
        except Exception as e:
            print(f"❌ [SUPABASE ERROR] Falha ao ler L5: {e}")

    if not df_cached.empty and not force_update and not incremental:
        return df_cached

    # --- 2. SE PRECISAR BAIXAR (FORCE OU VAZIO) ---
    if progress_ui: st.toast("Iniciando download da NBA API...", icon="🏀")

    act_players = players.get_active_players()
    
    pending_players = []
    if force_update or df_cached.empty:
        pending_players = act_players
    elif incremental and not df_cached.empty:
        dates_to_check = [datetime.now().strftime('%Y-%m-%d')]
        target_team_ids = set()
        for d in dates_to_check:
            try:
                board = scoreboardv2.ScoreboardV2(game_date=d).get_data_frames()[0]
                if not board.empty:
                    target_team_ids.update(board['HOME_TEAM_ID'].tolist() + board['VISITOR_TEAM_ID'].tolist())
            except: pass
        
        existing_ids = set(df_cached["PLAYER_ID"].unique()) if 'PLAYER_ID' in df_cached.columns else set()
        pending_players = [p for p in act_players if p['id'] not in existing_ids]

    total_needed = len(pending_players)
    if total_needed == 0:
        return df_cached

    # UI
    if progress_ui:
        status_box = st.status(f"☁️ Atualizando Nuvem ({total_needed} jogadores)...", expanded=True)
        p_bar = status_box.progress(0)
        
    # WORKER FETCH
    def fetch_one_player(player_info):
        pid = player_info['id']
        pname = player_info['full_name']
        try:
            time.sleep(0.1)
            log = playergamelog.PlayerGameLog(player_id=pid, season=SEASON_CURRENT, timeout=10)
            df = log.get_data_frames()[0]
            if not df.empty:
                df_l5 = df.head(5).copy()
                int_cols = ['PTS', 'REB', 'AST', 'STL', 'BLK', 'TOV']
                for col in int_cols:
                    if col in df_l5.columns: df_l5[col] = df_l5[col].fillna(0).astype('int64')
                
                df_l5['PLAYER_NAME'] = pname
                df_l5['PLAYER_ID'] = pid
                
                if 'Team_ID' in df.columns: df_l5['TEAM_ID'] = df['Team_ID']
                elif 'TEAM_ID' in df.columns: df_l5['TEAM_ID'] = df['TEAM_ID']
                
                return df_l5
        except: return None
        return None

    # Execução Paralela
    df_new_batch = pd.DataFrame()
    count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_p = {executor.submit(fetch_one_player, p): p for p in pending_players}
        for future in concurrent.futures.as_completed(future_to_p):
            res = future.result()
            if res is not None:
                df_new_batch = pd.concat([df_new_batch, res], ignore_index=True)
            count += 1
            if progress_ui: p_bar.progress(count / total_needed)

    # Merge Final
    if not df_new_batch.empty:
        if not df_cached.empty:
            # Remove antigos para atualizar (se a coluna PLAYER_ID existir)
            if 'PLAYER_ID' in df_cached.columns:
                upd_ids = df_new_batch['PLAYER_ID'].unique()
                df_cached = df_cached[~df_cached['PLAYER_ID'].isin(upd_ids)]
        df_final = pd.concat([df_cached, df_new_batch], ignore_index=True)
    else:
        df_final = df_cached

    # --- 3. SALVA NO SUPABASE (COM A CORREÇÃO DE DUPLICATAS) ---
    if not df_final.empty:
        # 1. Normaliza para Maiúsculo
        df_final.columns = [str(c).upper().strip() for c in df_final.columns]
        
        # 2. REMOVE DUPLICATAS DE COLUNA (FIX DO ERRO)
        # Isso remove colunas como "TEAM_ID" se ela aparecer 2 vezes
        df_final = df_final.loc[:, ~df_final.columns.duplicated()]
        
        # 3. Converte para JSON puro
        try:
            records = json.loads(df_final.to_json(orient="records", date_format="iso"))
            payload = {
                "records": records,
                "count": len(records),
                "last_update": datetime.now().isoformat()
            }
            
            if progress_ui: status_box.write("💾 Enviando para Supabase...")
            save_data_universal(KEY_L5, payload)
            
            if progress_ui:
                status_box.update(label=f"✅ Sucesso! {len(df_final)} jogadores salvos.", state="complete", expanded=False)
        except Exception as e:
            print(f"❌ Erro ao converter JSON L5: {e}")
            if progress_ui: status_box.error(f"Erro JSON: {e}")
            
    return df_final
# ============================================================================
# FUNÇÃO PARA CALCULAR RISCO DE BLOWOUT (ADICIONE ESTA FUNÇÃO)
# ============================================================================

def calculate_blowout_risk(spread_val, total_val=None):
    """Calcula o risco de blowout baseado no spread e total"""
    if spread_val is None:
        return {"nivel": "DESCONHECIDO", "icon": "⚪", "desc": "Spread não disponível", "color": "#9CA3AF"}
    
    try:
        spread = float(spread_val)
        abs_spread = abs(spread)
        
        if abs_spread >= 12:
            return {
                "nivel": "ALTO",
                "icon": "🔴", 
                "desc": "Alto risco de blowout (≥12 pts)",
                "color": "#FF4F4F"
            }
        elif abs_spread >= 8:
            return {
                "nivel": "MÉDIO",
                "icon": "🟡",
                "desc": "Risco moderado de blowout (8-11 pts)",
                "color": "#FFA500"
            }
        elif abs_spread >= 5:
            return {
                "nivel": "BAIXO",
                "icon": "🟢",
                "desc": "Baixo risco de blowout (5-7 pts)",
                "color": "#00FF9C"
            }
        else:
            return {
                "nivel": "MÍNIMO",
                "icon": "🔵",
                "desc": "Jogo equilibrado (<5 pts)",
                "color": "#1E90FF"
            }
    except:
        return {"nivel": "DESCONHECIDO", "icon": "⚪", "desc": "Spread inválido", "color": "#9CA3AF"}

def display_strategic_category(formatted_narrative, category_name, game_ctx):
    """Exibe uma categoria estratégica com formatação aprimorada"""
    if not formatted_narrative.get("players"):
        st.info(f"Nenhuma recomendação disponível para a categoria {category_name}.")
        return
    
    # Exibir visão geral
    overview = formatted_narrative["overview"]
    st.markdown(overview["text"])
    
    # Exibir jogadores
    st.subheader(f"🏀 Jogadores Recomendados ({category_name})")
    
    for i, player in enumerate(formatted_narrative["players"], 1):
        with st.expander(f"{i}. {player['name']} ({player['position']} - {player['team']}) - Confiança: {player['confidence']:.1f}%"):
            # Informações básicas
            col1, col2 = st.columns([2, 1])
            with col1:
                st.markdown(f"**{player['name']}** ({player['position']})")
                st.caption(f"Time: {player['team']} | Confiança: {player['confidence']:.1f}%")
                st.markdown(player['narrative'])
                
                # Estatísticas principais
                stats = player.get('stats', {})
                if stats:
                    stats_text = f"**Estatísticas:** "
                    stats_text += f"{stats.get('pts_avg', 0):.1f} PTS, "
                    stats_text += f"{stats.get('reb_avg', 0):.1f} REB, "
                    stats_text += f"{stats.get('ast_avg', 0):.1f} AST"
                    st.write(stats_text)
            
            with col2:
                # Mercado principal
                primary_mercado = player.get('raw_data', {}).get('primary_thesis', 'PRA')
                primary_value = stats.get('pra_avg', 0) if primary_mercado == 'PRA' else stats.get('pts_avg', 0)
                
                st.metric(
                    label=primary_mercado,
                    value=f"{primary_value:.1f}",
                    help=f"Tese principal: {primary_mercado}"
                )
                
                # Strategy tags
                strategy_tags = player.get('raw_data', {}).get('strategy_tags', [])
                if strategy_tags:
                    st.write("**Estratégias identificadas:**")
                    for tag in strategy_tags[:3]:  # Mostrar só as 3 principais
                        st.caption(f"• {tag}")
    
    # Tabela compacta
    with st.expander("📊 Tabela Compacta"):
        table_df = st.session_state.narrative_formatter.generate_compact_table(
            category_name.lower(), 
            formatted_narrative["players"]
        )
        if not table_df.empty:
            st.dataframe(table_df)

   
def show_config_page():
    # --- ENFORCE: Forçar tudo ligado ---
    st.session_state.use_advanced_features = True
    import os
    import time
    import requests
    from datetime import datetime
    
    st.header("⚙️ PAINEL DE CONTROLE (CLOUD NATIVE)")
    
    # ==============================================================================
    # 1. STATUS DO SISTEMA
    # ==============================================================================
    st.markdown("### 📡 Status da Nuvem")
    c1, c2, c3, c4 = st.columns(4)
    
    # A. Base L5 (Memória RAM)
    l5_ok = not st.session_state.get('df_l5', pd.DataFrame()).empty
    
    # B. Scoreboard (Jogos de Hoje) - CRÍTICO PARA EXIBIR DADOS
    sb_len = len(st.session_state.get('scoreboard', []))
    sb_ok = sb_len > 0
    
    # C. Props Cache
    props_cache = get_data_universal("real_game_logs")
    props_ok = props_cache is not None and len(props_cache) > 0
    
    # D. Injuries
    inj_cache = get_data_universal("injuries")
    inj_ok = inj_cache is not None and len(inj_cache.get('teams', {})) > 0
    
    def render_mini_status(col, label, is_ok, extra_text=""):
        color = "#00FF9C" if is_ok else "#FF4F4F"
        icon = "🟢 ONLINE" if is_ok else "🔴 OFFLINE"
        col.markdown(f"""
        <div style="border:1px solid {color}40; background:rgba(0,0,0,0.2); padding:10px; border-radius:8px; text-align:center;">
            <div style="font-weight:bold; color:#E2E8F0; font-size:14px;">{label}</div>
            <div style="color:{color}; font-size:11px; font-weight:bold; margin-top:5px;">{icon} {extra_text}</div>
        </div>
        """, unsafe_allow_html=True)

    render_mini_status(c1, "RAM: L5 Base", l5_ok)
    render_mini_status(c2, "Jogos Hoje", sb_ok, f"({sb_len})")
    render_mini_status(c3, "Cloud: Props", props_ok)
    render_mini_status(c4, "Cloud: Lesões", inj_ok)
    st.markdown("---")

    # ==============================================================================
    # 2. ÁREA DE AÇÕES
    # ==============================================================================
    col_act1, col_act2 = st.columns(2, gap="large")

    # --- COLUNA DA ESQUERDA: INGESTÃO DE DADOS ---
    with col_act1:
        st.subheader("📥 Ingestão de Dados")
        
        # A. JOGOS (SCOREBOARD) - NOVO BOTÃO CRÍTICO
        if st.button("🏀 ATUALIZAR JOGOS DE HOJE (SCOREBOARD)", use_container_width=True):
            games = fetch_espn_scoreboard(progress_ui=True)
            if games:
                st.success(f"✅ {len(games)} Jogos encontrados!")
                time.sleep(1); st.rerun()
            else:
                st.warning("Nenhum jogo encontrado ou erro na API.")

        st.divider()

        # B. MOTORES ESTATÍSTICOS
        st.markdown("##### 2. Motores Estatísticos")

        # BOTÃO 1: RECONSTRUIR (Agora força a atualização dos jogos antes)
        if st.button("🔄 RECONSTRUIR CACHE DE PROPS", type="primary", use_container_width=True):
            try:
                # 1. FORÇA ATUALIZAÇÃO DO SCOREBOARD PRIMEIRO
                st.toast("Atualizando lista de jogos...", icon="🏀")
                games = fetch_espn_scoreboard()
                
                if not games:
                    st.error("❌ Não foi possível encontrar jogos hoje. O cache será atualizado, mas as abas ficarão vazias até haver jogos.")
                
                # 2. Roda o Update V65 (Master Roster)
                # Passamos force_all=True para baixar a liga toda
                update_batch_cache(games, force_all=True)
                
                st.success("✅ Cache Recalibrado com Sucesso!")
                time.sleep(1)
                st.rerun()
                    
            except Exception as e:
                st.error(f"Erro crítico: {e}")

        # BOTÃO 2: HARD RESET
        if st.button("🧨 APAGAR CACHE DE PROPS (HARD RESET)", use_container_width=True):
            try:
                if "real_game_logs" in st.session_state:
                    del st.session_state["real_game_logs"]
                save_data_universal("real_game_logs", {})
                st.warning("⚠️ Cache apagado!")
                time.sleep(1); st.rerun()
            except Exception as e:
                st.error(f"Erro: {e}")

    # --- COLUNA DA DIREITA: MANUTENÇÃO ---
    with col_act2:
        st.subheader("⚙️ Manutenção")
        
        # Botões de L5 (Movidos para cá para limpar a esquerda)
        c_l5_1, c_l5_2 = st.columns(2)
        with c_l5_1:
            if st.button("⚡ UPDATE L5", use_container_width=True):
                try:
                    with st.spinner("Atualizando L5..."):
                        new_l5 = get_players_l5(progress_ui=True, incremental=True)
                        st.session_state.df_l5 = new_l5
                        st.success("OK!")
                except: pass
        with c_l5_2:
            if st.button("🐢 RESET L5", use_container_width=True):
                try:
                    with st.spinner("Baixando L5 Zero..."):
                        new_l5 = get_players_l5(progress_ui=True, force_update=True)
                        st.session_state.df_l5 = new_l5
                        st.success("OK!")
                except: pass

        st.divider()
        
        if st.button("🗑️ LIMPAR MEMÓRIA (RAM)", use_container_width=True):
            st.cache_data.clear()
            st.cache_resource.clear()
            st.success("✅ Memória Cache limpa!")
            time.sleep(1); st.rerun()

        st.divider()
        
        c_a, c_b = st.columns(2)
        with c_a:
            if st.button("🔄 SYNC PACE", use_container_width=True):
                st.info("Cloud Mode Ativo.")
        with c_b:
            if st.button("🛡️ SYNC DVP", use_container_width=True):
                st.info("Cloud Mode Ativo.")
        
        if st.button("🚑 ATUALIZAR LESÕES", use_container_width=True):
            with st.spinner("Consultando Depto. Médico..."):
                try:
                    from injuries import InjuryMonitor
                    monitor = InjuryMonitor() 
                    ALL_TEAMS = ["ATL","BOS","BKN","CHA","CHI","CLE","DAL","DEN","DET","GSW","HOU","IND","LAC","LAL","MEM","MIA","MIL","MIN","NOP","NYK","OKC","ORL","PHI","PHX","POR","SAC","SAS","TOR","UTA","WAS"]
                    p = st.progress(0)
                    for i, team in enumerate(ALL_TEAMS):
                        monitor.fetch_injuries_for_team(team)
                        p.progress((i+1)/len(ALL_TEAMS))
                    p.empty()
                    fresh_data = monitor.get_all_injuries()
                    if fresh_data:
                        save_data_universal("injuries", {"teams": fresh_data, "updated_at": datetime.now().isoformat()})
                        st.session_state.injuries_data = fresh_data 
                        st.success("✅ Lesões Atualizadas!")
                except Exception as e: st.error(f"Erro: {e}")

    # ==============================================================================
    # 3. DASHBOARD DE VOLUMETRIA
    # ==============================================================================
    st.markdown("---")
    df_l5 = st.session_state.get('df_l5', pd.DataFrame())
    games = st.session_state.get('scoreboard', [])
    
    col_m1, col_m2, col_m3 = st.columns(3)
    col_m1.metric("Jogadores Base", len(df_l5))
    col_m2.metric("Jogos Hoje", len(games))
    
    try:
        from injuries import InjuryMonitor
        m = InjuryMonitor(cache_file=INJURIES_CACHE_FILE)
        all_inj = m.get_all_injuries()
        total_out = sum(len(v) for v in all_inj.values())
        col_m3.metric("Lesionados (Total)", total_out)
    except:
        col_m3.metric("Lesionados", "N/A")

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🗑️ LIMPAR CACHE DE LESÕES", type="secondary", use_container_width=True):
        if os.path.exists(INJURIES_CACHE_FILE):
            os.remove(INJURIES_CACHE_FILE)
            st.success("Cache removido. Re-sincronize os 30 times.")
            time.sleep(1)
            st.rerun()

# ============================================================================
# PÁGINA: ANALYTICS DASHBOARD (RANKING & ROI)
# ============================================================================
def show_analytics_page():
    st.header("📊 ANALYTICS DASHBOARD")
    st.markdown("### Inteligência de Performance & Ranking de Teses")
    
    # 1. Carregamento de Dados
    if 'audit_system' not in st.session_state or not hasattr(st.session_state.audit_system, 'audit_data'):
        from modules.audit_system import AuditSystem
        st.session_state.audit_system = AuditSystem()
    
    audit = st.session_state.audit_system
    # Agora o acesso abaixo será seguro
    history = audit.audit_data
    
    if not history:
        st.info("📭 Sem dados suficientes. Salve e valide alguns bilhetes para gerar inteligência.")
        return

    # 2. Processamento (Pandas)
    # Transformamos o JSON em DataFrames para análise rápida
    tickets_data = []
    legs_data = []

    for t in history:
        # --- BLINDAGEM DE DADOS ---
        # Usamos .get() para evitar KeyError se o campo não existir no JSON antigo
        t_status = t.get('status', 'PENDING')
        t_odd = float(t.get('total_odd', 0))
        t_cat = t.get('category', 'UNK').upper()
        t_id = t.get('id', 'N/A')
        t_date = t.get('date', 'N/A')

        # Cálculo de Profit
        profit = 0.0
        if t_status == 'WIN':
            profit = t_odd - 1.0
        elif t_status == 'LOSS':
            profit = -1.0
        
        tickets_data.append({
            "id": t_id,
            "category": t_cat,
            "status": t_status,
            "profit": profit,
            "odd": t_odd,
            "date": t_date
        })
        
        # Dados das Pernas (Legs)
        for leg in t.get('legs', []):
            leg_status = leg.get('status', 'PENDING')
            hit = 1 if leg_status == 'WIN' else 0
            
            # Hook Analysis (Perdeu por pouco?)
            hook = False
            if leg_status == 'LOSS':
                try:
                    val = float(leg.get('actual_value', 0))
                    line = float(leg.get('line', 0))
                    diff = abs(val - line)
                    
                    # Hook se perdeu por 0.5 ou 1.0 e o jogador jogou (val > 0)
                    if diff <= 1.0 and val > 0:
                        hook = True
                except (ValueError, TypeError):
                    # Se houver erro de conversão, ignora o hook
                    hook = False

            legs_data.append({
                "player": leg.get('name', leg.get('player_name', 'Unknown')),
                "market": leg.get('market_type', 'UNK'),
                "thesis": leg.get('thesis', 'General'),
                "status": leg_status,
                "hit": hit,
                "hook": hook,
                "odds": float(leg.get('odds', 0))
            })

    # Criação dos DataFrames
    df_tickets = pd.DataFrame(tickets_data)
    df_legs = pd.DataFrame(legs_data)

    # 3. MACRO VISÃO (ROI & BANCA)
    st.subheader("💰 Performance Financeira (ROI)")
    
    # KPIs Gerais
    total_bets = len(df_tickets)
    resolved_bets = df_tickets[df_tickets['status'].isin(['WIN', 'LOSS'])]
    net_profit = resolved_bets['profit'].sum() if not resolved_bets.empty else 0.0
    roi_percent = (net_profit / len(resolved_bets) * 100) if len(resolved_bets) > 0 else 0.0
    
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bilhetes Totais", total_bets)
    k2.metric("Bilhetes Resolvidos", len(resolved_bets))
    k3.metric("Lucro Líquido (u)", f"{net_profit:+.2f}u", delta_color="normal")
    k4.metric("ROI Global", f"{roi_percent:+.1f}%")
    
    st.markdown("---")

    # 4. ANÁLISE POR CATEGORIA (Gráfico)
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.markdown("#### 📈 Lucro/Prejuízo por Categoria")
        if not resolved_bets.empty:
            # Agrupa por categoria e soma o lucro
            category_roi = resolved_bets.groupby('category')['profit'].sum().reset_index()
            st.bar_chart(category_roi, x="category", y="profit", color="#00FF9C")
        else:
            st.caption("Valide bilhetes para ver o gráfico de lucro.")
            
    with c2:
        st.markdown("#### 🍰 Distribuição de Volume")
        if not df_tickets.empty:
            cat_counts = df_tickets['category'].value_counts()
            st.dataframe(cat_counts, use_container_width=True)

    # 5. RANKING DE TESES (O OURO)
    st.markdown("---")
    st.subheader("🏆 Ranking de Teses & Mercados (Legs)")
    st.caption("Quais estratégias individuais estão batendo mais?")

    if not df_legs.empty:
        # Filtra apenas legs resolvidas (WIN/LOSS)
        resolved_legs = df_legs[df_legs['status'].isin(['WIN', 'LOSS'])]
        
        if not resolved_legs.empty:
            t1, t2 = st.tabs(["🏛️ Por Tese (Estratégia)", "🏀 Por Mercado (Stat)"])
            
            with t1:
                # Agrupa por Tese
                thesis_stats = resolved_legs.groupby('thesis').agg(
                    Tentativas=('hit', 'count'),
                    Acertos=('hit', 'sum')
                ).reset_index()
                
                thesis_stats['Win Rate %'] = (thesis_stats['Acertos'] / thesis_stats['Tentativas'] * 100).round(1)
                thesis_stats = thesis_stats.sort_values(by='Win Rate %', ascending=False)
                
                # Formatação visual
                st.dataframe(
                    thesis_stats,
                    column_config={
                        "Win Rate %": st.column_config.ProgressColumn(
                            "Win Rate",
                            format="%.1f%%",
                            min_value=0,
                            max_value=100,
                        )
                    },
                    use_container_width=True,
                    hide_index=True
                )
            
            with t2:
                # Agrupa por Mercado (PTS, REB, AST)
                mkt_stats = resolved_legs.groupby('market').agg(
                    Tentativas=('hit', 'count'),
                    Acertos=('hit', 'sum')
                ).reset_index()
                
                mkt_stats['Win Rate %'] = (mkt_stats['Acertos'] / mkt_stats['Tentativas'] * 100).round(1)
                mkt_stats = mkt_stats.sort_values(by='Win Rate %', ascending=False)
                
                st.dataframe(
                    mkt_stats,
                    column_config={
                        "Win Rate %": st.column_config.ProgressColumn(
                            "Win Rate",
                            format="%.1f%%",
                            min_value=0,
                            max_value=100,
                        )
                    },
                    use_container_width=True,
                    hide_index=True
                )
        else:
            st.warning("Nenhuma perna (leg) validada ainda. Use a aba Auditoria para validar resultados.")
    else:
        st.warning("Nenhum dado de pernas encontrado.")

    # 6. ZONA DE DOR (HOOK ANALYSIS)
    st.markdown("---")
    st.subheader("🪝 Análise de 'Hooks' (O Quase)")
    st.caption("Apostas perdidas por margem mínima (0.5 ou 1.0). Indica que a leitura foi boa, mas faltou sorte.")
    
    if not df_legs.empty:
        hooks = df_legs[df_legs['hook'] == True]
        if not hooks.empty:
            st.error(f"Detectamos {len(hooks)} 'Hooks' dolorosos.")
            for i, row in hooks.iterrows():
                st.markdown(f"- **{row['player']}** ({row['market']}): Perdeu por margem mínima (Tese: {row['thesis']})")
        else:
            st.success("Nenhum 'Hook' detectado recentemente. Ou Green ou Red claro.")
# ============================================================================
# ROSTER HELPERS (mantidas do original)
# ============================================================================

def _extract_str_field(val):
    if not val: return ""
    if isinstance(val, str): return val.strip()
    if isinstance(val, dict):
        for k in ("displayName","fullName","shortName","name"):
            v = val.get(k)
            if v and isinstance(v, str): return v.strip()
    return str(val)

def normalize_roster_entry(a):
    if not a or not isinstance(a, dict):
        return {"PLAYER":"", "POSITION":"", "STARTER":False, "STATUS":""}
    candidate = a.get("athlete") if a.get("athlete") else a
    player = _extract_str_field(candidate.get("displayName") or candidate.get("fullName") or candidate.get("name") or "")
    pos_raw = candidate.get("position") or candidate.get("positionType") or candidate.get("positionName") or ""
    position = _extract_str_field(pos_raw)
    status_raw = a.get("status") or a.get("injuryStatus") or candidate.get("status") or candidate.get("injuryStatus") or ""
    if isinstance(status_raw, dict):
        status = status_raw.get("name") or status_raw.get("abbreviation") or json.dumps(status_raw, ensure_ascii=False)
    else:
        status = str(status_raw)
    starter = a.get("starter")
    if starter is None:
        starter = a.get("isStarter") or candidate.get("starter") or candidate.get("isStarter") or False
    starter_flag = bool(starter) if not isinstance(starter, str) else starter.lower() in ("true","1","yes")
    return {"PLAYER": player, "POSITION": position, "STARTER": starter_flag, "STATUS": status or ""}

def derive_availability_and_expected_minutes(roster_entry, df_l5_row, treat_unknown_as_available=True):
    status = (roster_entry.get("STATUS") or "").lower()
    starter = bool(roster_entry.get("STARTER"))
    min_avg = float(df_l5_row.get("MIN_AVG", 0)) if df_l5_row is not None else 0.0
    last_min = float(df_l5_row.get("LAST_MIN", 0)) if df_l5_row is not None else 0.0
    availability = "unknown"; expected_minutes = min_avg
    if any(k in status for k in ("out","ir","injur")):
        availability = "out"; expected_minutes = 0.0
    elif starter:
        availability = "available"; expected_minutes = max(min_avg, last_min, 28.0)
    elif status and any(k in status for k in ("active","available")):
        availability = "available"; expected_minutes = max(min_avg*0.8, last_min*0.9)
    else:
        availability = "probable"; expected_minutes = max(min_avg*0.8, last_min*0.6) if treat_unknown_as_available else min_avg*0.6
    expected_minutes = float(max(0.0, expected_minutes))
    return {"availability": availability, "expected_minutes": expected_minutes}

def extract_list(jr):
    if not jr or not isinstance(jr, dict):
        return []
    
    if "athletes" in jr and isinstance(jr["athletes"], list):
        return jr["athletes"]
    
    if "roster" in jr and isinstance(jr["roster"], dict):
        if "athletes" in jr["roster"] and isinstance(jr["roster"]["athletes"], list):
            return jr["roster"]["athletes"]
    
    possible_paths = [
        ["team", "roster", "athletes"],
        ["team", "athletes"],
        ["players"],
        ["items"],
        ["results", "athletes"],
        ["data", "athletes"]
    ]
    
    for path in possible_paths:
        current = jr
        found = True
        
        for key in path:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                found = False
                break
        
        if found and isinstance(current, list):
            return current
    
    def find_athletes_recursive(obj, depth=0, max_depth=3):
        if depth > max_depth:
            return None
        
        if isinstance(obj, list) and len(obj) > 0:
            first_item = obj[0]
            if isinstance(first_item, dict):
                athlete_keys = ['athlete', 'player', 'displayName', 'fullName', 'firstName', 'lastName']
                if any(key in first_item for key in athlete_keys):
                    return obj
                if 'athlete' in first_item and isinstance(first_item['athlete'], dict):
                    return obj
        
        elif isinstance(obj, dict):
            for key, value in obj.items():
                if key in ['athletes', 'players', 'roster']:
                    result = find_athletes_recursive(value, depth + 1, max_depth)
                    if result:
                        return result
                result = find_athletes_recursive(value, depth + 1, max_depth)
                if result:
                    return result
        
        return None
    
    athletes = find_athletes_recursive(jr)
    if athletes:
        return athletes
    
    return []

def build_l5_indices(df_l5):
    """Constrói índices para busca rápida de dados L5"""
    l5_index = {}
    l5_index_norm = {}
    
    if df_l5 is None or not isinstance(df_l5, pd.DataFrame) or df_l5.empty:
        return l5_index, l5_index_norm  # Retornar índices vazios
    
    for _, r in df_l5.iterrows():
        try:
            rowd = r.to_dict()
        except Exception:
            rowd = dict(r)
        
        name = rowd.get("PLAYER") or rowd.get("player") or ""
        if not name:
            continue
        
        l5_index[name] = rowd
        l5_index_norm[normalize_name(name)] = rowd
    
    return l5_index, l5_index_norm

def resolve_l5_row(name_espn, l5_index, l5_index_norm, overrides):
    name_norm = normalize_name(name_espn)
    if name_norm in overrides:
        target = overrides[name_norm]
        row = l5_index.get(target)
        if row is not None: return row, "override"
    row = l5_index.get(name_espn)
    if row is not None: return row, "exact"
    row = l5_index_norm.get(name_norm)
    if row is not None: return row, "normalized"
    candidates = list(l5_index.keys())
    match = difflib.get_close_matches(name_espn, candidates, n=1, cutoff=0.75)
    if match: return l5_index.get(match[0]), "fuzzy"
    candidates_norm = list(l5_index_norm.keys())
    match2 = difflib.get_close_matches(name_norm, candidates_norm, n=1, cutoff=0.75)
    if match2: return l5_index_norm.get(match2[0]), "fuzzy_norm"
    return None, "none"

# ============================================================================
# GERAR TAGS ATUALIZADA (FASE 3.2)
# ============================================================================
def gerar_tags_para_jogador(player_ctx, game_ctx):
    """
    Gera tags estratégicas a partir das teses do ThesisEngine novo.
    Retorna uma lista de nomes de teses ativas (tags).
    """
    try:
        # Verificar se temos engine disponível
        if "thesis_engine" not in st.session_state:
            # Tentar criar uma instância se não existir
            try:
                from modules.new_modules.thesis_engine import ThesisEngine
                st.session_state.thesis_engine = ThesisEngine()
            except:
                return []
        
        thesis_engine = st.session_state.thesis_engine
        theses = thesis_engine.evaluate_player(player_ctx, game_ctx)
        
        if not theses:
            return []
            
        tags = []
        for t in theses:
            name = t.get("name") or t.get("thesis_type")
            if name:
                tags.append(name)
        
        # Remover duplicatas preservando ordem
        seen = set()
        unique_tags = [x for x in tags if not (x in seen or seen.add(x))]
        return unique_tags
        
    except Exception:
        return []

# ============================================================================
# NOVAS FUNÇÕES DE EXIBIÇÃO ESTRATÉGICA
# ============================================================================
def display_player_theses(player_ctx, game_ctx, thesis_engine=None):
    """
    Retorna uma lista estruturada das teses do jogador para exibição na UI.
    Não escreve na UI diretamente; entrega dados prontos para renderização.
    """
    engine = thesis_engine or st.session_state.get("thesis_engine") or ThesisEngine()
    try:
        theses = engine.generate_all_theses(player_ctx, game_ctx)
        if not theses:
            return []
        out = []
        for thesis in theses:
            out.append({
                "type": thesis.get("thesis_type"),
                "market": thesis.get("market"),
                "confidence": float(thesis.get("confidence", 0)),
                "reason": thesis.get("reason"),
                "evidences": thesis.get("evidences", []),
                "suggested_line": thesis.get("suggested_line")
            })
        return out
    except Exception:
        return []

# ============================================================================
# GAME CONTEXT
# ============================================================================

def build_game_context(away_abbr, home_abbr, odds_map, team_advanced, team_opponent):
    """
    Constrói o contexto do jogo usando dados gratuitos da ESPN (via odds_map convertido).
    """
    away_full = TEAM_ABBR_TO_ODDS.get(away_abbr, away_abbr)
    home_full = TEAM_ABBR_TO_ODDS.get(home_abbr, home_abbr)
    
    odds = {}
    if odds_map:
        # Tenta várias chaves para encontrar o jogo
        possible_keys = [
            f"{away_abbr}@{home_abbr}", 
            f"{away_abbr} @ {home_abbr}",
            f"{away_full}@{home_full}"
        ]
        for k in possible_keys:
            if k in odds_map:
                odds = odds_map[k]
                break
    
    # Se não achou no map, tenta valores default ou zerados
    spread = odds.get("spread", 0.0)
    total = odds.get("total", 225.0)
    
    # Pace
    adv_home = team_advanced.get(home_abbr, {}) if team_advanced else {}
    adv_away = team_advanced.get(away_abbr, {}) if team_advanced else {}
    
    pace_home = adv_home.get("pace")
    pace_away = adv_away.get("pace")
    
    # Se não tiver dados avançados, usa a constante TEAM_PACE_DATA (fallback seguro)
    if not pace_home: pace_home = TEAM_PACE_DATA.get(home_abbr, 100.0)
    if not pace_away: pace_away = TEAM_PACE_DATA.get(away_abbr, 100.0)
    
    pace_expected = (float(pace_home) + float(pace_away)) / 2.0
    
    # Flags Booleanas para o Motor Estratégico
    is_high_pace = pace_expected >= 101.5
    is_low_pace = pace_expected <= 98.5
    is_blowout_risk = abs(float(spread)) >= 12.0
    
    ctx = {
        "away_abbr": away_abbr, 
        "home_abbr": home_abbr,
        "spread": spread, 
        "total": total, 
        "pace_expected": pace_expected,
        "is_high_pace": is_high_pace,
        "is_low_pace": is_low_pace,
        "is_blowout_risk": is_blowout_risk
    }
    return ctx

# ============================================================================
# FUNÇÕES DE SCORING ATUALIZADAS
# ============================================================================
def fetch_espn_boxscore(game_id):
    """
    Busca o boxscore de um jogo da ESPN.
    
    Args:
        game_id: ID do jogo na ESPN
    
    Returns:
        Dicionário com as estatísticas do jogo, ou None se não encontrado.
    """
    params = {"event": game_id}
    
    try:
        response = requests.get(ESPN_BOXSCORE_URL, params=params, headers=HEADERS, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Erro ao buscar boxscore da ESPN: {e}")
        return None
   
    def __init__(self, cache_file=None):
        self.cache_file = cache_file or AUDIT_CACHE_FILE
        self.audit_data = self._load_audit_data()
    
    def _load_audit_data(self):
        """Carrega dados de auditoria do cache"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception:
            pass
        return []
    
    def _save_audit_data(self):
        """Salva dados de auditoria no cache"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.audit_data, f, indent=2, ensure_ascii=False)
        except Exception:
            pass
    
    def log_trixie(self, trixie, game_info, trixie_type, source="system"):
        """
        Registra uma trixie na auditoria.
        
        Args:
            trixie: Dicionário da trixie
            game_info: Informações do jogo
            trixie_type: Tipo de trixie
            source: Fonte da trixie
        """
        audit_entry = {
            "id": f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{hash(str(trixie))}",
            "timestamp": datetime.now().isoformat(),
            "date": TODAY,
            "game": game_info,
            "trixie_type": trixie_type,
            "score": trixie.get("score", 0),
            "enhanced": trixie.get("enhanced", False),
            "source": source,
            "players": []
        }
        
        for p in trixie.get("players", []):
            player_entry = {
                "name": p.get("name"),
                "team": p.get("team"),
                "position": p.get("position"),
                "mercado": p.get("mercado", {}),
                "tese_efetiva": p.get("tese_efetiva", ""),  # Nova informação
                "stats": {
                    "min_L5": p.get("min_L5", 0),
                    "pts_L5": p.get("pts_L5", 0),
                    "reb_L5": p.get("reb_L5", 0),
                    "ast_L5": p.get("ast_L5", 0),
                    "pra_L5": p.get("pra_L5", 0),
                    "stl_L5": p.get("stl_L5", 0) if "stl_L5" in p else 0,
                    "blk_L5": p.get("blk_L5", 0) if "blk_L5" in p else 0,
                    "3pm_L5": p.get("3pm_L5", 0) if "3pm_L5" in p else 0
                },
                "tags": p.get("tags", []),
                "dvp_data": p.get("dvp_data", {})
            }
            audit_entry["players"].append(player_entry)
        
        self.audit_data.append(audit_entry)
        
        # Manter apenas os últimos 1000 registros
        if len(self.audit_data) > 1000:
            self.audit_data = self.audit_data[-1000:]
        
        self._save_audit_data()
    
    def validate_trixie_with_boxscore(self, audit_entry):
        """
        Valida uma trixie com base nos dados reais do jogo (boxscore).
        
        Args:
            audit_entry: Entrada de auditoria
        
        Returns:
            A mesma entrada com validação atualizada
        """
        game_info = audit_entry.get("game", {})
        game_id = game_info.get("gameId")
        
        if not game_id:
            return audit_entry
        
        # Buscar boxscore
        boxscore = fetch_espn_boxscore(game_id)
        if not boxscore:
            return audit_entry
        
        # Inicializar estrutura de validação
        validation = {
            "validated": True,
            "validation_date": datetime.now().isoformat(),
            "game_id": game_id,
            "players": [],
            "overall": "UNKNOWN"
        }
        
        all_green = True
        any_red = False
        
        for player in audit_entry.get("players", []):
            player_name = player.get("name")
            player_team = player.get("team")
            mercado = player.get("mercado", {})
            
            # Buscar estatísticas do jogador no boxscore
            player_stats = self._extract_player_stats_from_boxscore(boxscore, player_name, player_team)
            
            if not player_stats:
                # Jogador não encontrado no boxscore (não jogou?)
                player_validation = {
                    "name": player_name,
                    "team": player_team,
                    "found": False,
                    "status": "NO_DATA",
                    "target": mercado.get("base_valor", 0),
                    "actual": 0
                }
                all_green = False
            else:
                # Verificar se a leg foi GREEN ou RED
                leg_type = mercado.get("tipo")
                target_value = mercado.get("base_valor", 0)
                
                # Mapear tipo de leg para estatística do boxscore
                stat_mapping = {
                    "PTS": "points",
                    "REB": "rebounds", 
                    "AST": "assists",
                    "PRA": "pra",
                    "STL": "steals",
                    "BLK": "blocks",
                    "3PM": "three_pointers"
                }
                
                stat_key = stat_mapping.get(leg_type, "points")
                actual_value = player_stats.get(stat_key, 0)
                
                if leg_type == "PRA":
                    actual_value = player_stats.get("points", 0) + player_stats.get("rebounds", 0) + player_stats.get("assists", 0)
                
                # Determinar status
                if actual_value >= target_value:
                    status = "GREEN"
                else:
                    status = "RED"
                    all_green = False
                    any_red = True
                
                player_validation = {
                    "name": player_name,
                    "team": player_team,
                    "found": True,
                    "status": status,
                    "target": target_value,
                    "actual": actual_value,
                    "stats": player_stats,
                    "leg_type": leg_type
                }
            
            validation["players"].append(player_validation)
        
        # Determinar status geral da trixie
        if all_green:
            validation["overall"] = "GREEN"
        elif any_red:
            validation["overall"] = "RED"
        else:
            validation["overall"] = "PARTIAL"
        
        audit_entry["validation"] = validation
        return audit_entry
    
    def _extract_player_stats_from_boxscore(self, boxscore, player_name, team_abbr):
        """
        Extrai as estatísticas de um jogador do boxscore da ESPN.
        
        Args:
            boxscore: Dicionário do boxscore da ESPN
            player_name: Nome do jogador
            team_abbr: Abreviação do time
        
        Returns:
            Dicionário com as estatísticas ou None se não encontrado
        """
        try:
            # A estrutura do boxscore da ESPN varia
            # Vamos procurar em diferentes locais
            players_data = []
            
            # Tentar obter jogadores do boxscore
            if "boxscore" in boxscore:
                teams = boxscore["boxscore"].get("teams", [])
                for team in teams:
                    team_players = team.get("statistics", [])
                    for player in team_players:
                        athlete = player.get("athlete", {})
                        name = athlete.get("displayName", "")
                        if normalize_name(name) == normalize_name(player_name):
                            # Extrair estatísticas
                            stats = {}
                            for stat in player.get("stats", []):
                                if "name" in stat and "value" in stat:
                                    stats[stat["name"].lower()] = float(stat["value"])
                            
                            # Mapear nomes comuns
                            mapped_stats = {
                                "points": stats.get("points", 0),
                                "rebounds": stats.get("rebounds", stats.get("reb", 0)),
                                "assists": stats.get("assists", stats.get("ast", 0)),
                                "minutes": stats.get("minutes", stats.get("min", 0)),
                                "steals": stats.get("steals", stats.get("stl", 0)),
                                "blocks": stats.get("blocks", stats.get("blk", 0)),
                                "turnovers": stats.get("turnovers", stats.get("to", 0)),
                                "three_pointers": stats.get("3pt", stats.get("three_pointers", 0))
                            }
                            return mapped_stats
            
            return None
        except Exception:
            return None
    
    def get_audit_data(self, date_filter=None, trixie_type_filter=None, team_filter=None, status_filter=None):
        """
        Obtém dados de auditoria filtrados.
        
        Args:
            date_filter: Filtrar por data (YYYY-MM-DD)
            trixie_type_filter: Filtrar por tipo de trixie
            team_filter: Filtrar por time
            status_filter: Filtrar por status ("GREEN", "RED", "PARTIAL")
        
        Returns:
            Lista de entradas de auditoria filtradas
        """
        filtered = self.audit_data
        
        if date_filter:
            filtered = [entry for entry in filtered if entry.get("date") == date_filter]
        
        if trixie_type_filter:
            filtered = [entry for entry in filtered if entry.get("trixie_type") == trixie_type_filter]
        
        if team_filter:
            filtered = [
                entry for entry in filtered 
                if any(p.get("team") == team_filter for p in entry.get("players", []))
            ]
        
        if status_filter:
            filtered_with_validation = []
            for entry in filtered:
                if "validation" in entry:
                    if entry["validation"].get("overall") == status_filter:
                        filtered_with_validation.append(entry)
                elif status_filter == "UNKNOWN":
                    filtered_with_validation.append(entry)
            filtered = filtered_with_validation
        
        return filtered
    
    def get_validation_stats(self):
        """
        Retorna estatísticas de validação.
        
        Returns:
            Dicionário com estatísticas
        """
        stats = {
            "total": len(self.audit_data),
            "validated": 0,
            "green": 0,
            "red": 0,
            "partial": 0,
            "unknown": 0
        }
        
        for entry in self.audit_data:
            if "validation" in entry:
                stats["validated"] += 1
                overall = entry["validation"].get("overall", "UNKNOWN")
                if overall == "GREEN":
                    stats["green"] += 1
                elif overall == "RED":
                    stats["red"] += 1
                elif overall == "PARTIAL":
                    stats["partial"] += 1
                else:
                    stats["unknown"] += 1
            else:
                stats["unknown"] += 1
        
        return stats

# ============================================================================
# PÁGINA: AUDITORIA COMMAND CENTER (V3.2 - BATCH VALIDATION)
# ============================================================================
def show_audit_page():
    st.header("📋 AUDITORIA COMMAND CENTER")
    
    # 1. Carregar Sistema
    if 'audit_system' not in st.session_state or not hasattr(st.session_state.audit_system, 'audit_data'):
        try:
            from modules.audit_system import AuditSystem
            st.session_state.audit_system = AuditSystem()
        except Exception as e:
            st.error(f"Erro ao carregar AuditSystem: {e}")
            return

    audit = st.session_state.audit_system
    
    # Sincroniza
    fresh_data = audit._load_data()
    audit.audit_data = fresh_data 
    history = audit.audit_data
    
    # --- KPI DASHBOARD ---
    total = len(history)
    if total == 0:
        st.info("📭 O Cofre de Auditoria está vazio. Gere e salve novas SGPs/Trixies.")
        return

    wins = sum(1 for t in history if t.get('status') == 'WIN')
    losses = sum(1 for t in history if t.get('status') == 'LOSS')
    pending_list = [t for t in history if t.get('status', 'PENDING') == 'PENDING']
    pending_count = len(pending_list)
    
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bilhetes Totais", total)
    k2.metric("✅ Greens", wins)
    k3.metric("❌ Reds", losses)
    k4.metric("⏳ Pendentes", pending_count)
    
    st.markdown("---")

    # --- ÁREA DE AÇÕES EM LOTE (NOVO) ---
    if pending_count > 0:
        with st.expander(f"⚡ Ações em Lote ({pending_count} Pendentes)", expanded=True):
            c_batch_1, c_batch_info = st.columns([1, 3])
            
            if c_batch_1.button(f"🔄 VALIDAR TODOS ({pending_count})", type="primary", use_container_width=True):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                updated_count = 0
                errors = 0
                
                for i, ticket in enumerate(pending_list):
                    t_id = ticket.get('id')
                    # Atualiza UI
                    pct = int(((i + 1) / pending_count) * 100)
                    progress_bar.progress(pct)
                    status_text.caption(f"Validando bilhete {i+1}/{pending_count}: {t_id}...")
                    
                    # Chama validação inteligente
                    try:
                        success, msg = audit.smart_validate_ticket(t_id)
                        if success and "Atualizado" in msg:
                            updated_count += 1
                    except Exception as e:
                        errors += 1
                
                progress_bar.progress(100)
                status_text.empty()
                
                if updated_count > 0:
                    st.success(f"✅ Processo finalizado! {updated_count} bilhetes tiveram status atualizado.")
                else:
                    st.info("Processo finalizado. Nenhum bilhete mudou de status (jogos ainda não começaram ou dados indisponíveis).")
                
                if errors > 0:
                    st.warning(f"{errors} erros durante a validação.")
                    
                time.sleep(2)
                st.rerun()
            
            with c_batch_info:
                st.caption("ℹ️ Isso irá verificar o boxscore na ESPN para todos os bilhetes pendentes. Pode levar alguns segundos.")

    # --- FILTRO DE DATA ---
    all_dates = sorted(list(set([t.get('date', 'Unknown') for t in history])), reverse=True)
    c_date, c_sel = st.columns([1, 2])
    
    with c_date:
        selected_date = st.selectbox("📅 Filtrar por Data:", ["TODAS"] + all_dates)

    # Aplica Filtro
    filtered_history = history
    if selected_date != "TODAS":
        filtered_history = [t for t in history if t.get('date') == selected_date]

    if not filtered_history:
        st.warning("Nenhum bilhete encontrado nesta data.")
        return

    # --- SELETOR DE BILHETE ---
    def get_label(t):
        try:
            g = t.get('game_info', {})
            home = g.get('home','?')
            away = g.get('away','?')
            if home == "MIX": game_str = "SGP Mix / Múltipla"
            else: game_str = f"{away} @ {home}"
            
            cat = t.get('sub_category', t.get('category', 'UNK')).upper()
            status = t.get('status', 'PENDING')
            
            icon = "✅" if status == 'WIN' else "❌" if status == 'LOSS' else "⏳"
            profit_loss = ""
            if status == 'WIN':
                pl = (float(t.get('total_odd', 0)) - 1)
                profit_loss = f"(+{pl:.2f}u)"
            
            return f"{icon} {cat} | {game_str} {profit_loss}"
        except: return f"⚠️ ID: {t.get('id')}"

    reversed_history = filtered_history[::-1]
    
    with c_sel:
        selected_id = st.selectbox(
            "Selecione o bilhete para ver detalhes:", 
            [t.get('id') for t in reversed_history], 
            format_func=lambda x: get_label(next((t for t in filtered_history if t['id'] == x), {}))
        )
    
    target = next((t for t in filtered_history if t['id'] == selected_id), None)
    
    if target:
        # --- CABEÇALHO DO BILHETE ---
        with st.container():
            st.markdown(f"#### 🎫 Detalhes: {target.get('id')}")
            
            c1, c2, c3 = st.columns([2, 1, 1])
            
            cat = target.get('category', 'UNK').upper()
            sub = target.get('sub_category', '').upper()
            odd = float(target.get('total_odd', 0))
            status = target.get('status', 'PENDING')
            
            c1.markdown(f"**Estratégia:** {cat} {f'({sub})' if sub else ''}")
            c1.markdown(f"**Odd Total:** @{odd:.2f}")
            
            # Botão Individual (Mantido para correções pontuais)
            if c2.button("🔄 Validar Este", key=f"val_{selected_id}"):
                with st.spinner("Validando..."):
                    success, msg = audit.smart_validate_ticket(selected_id)
                    if success:
                        st.success(msg)
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(msg)

            if c3.button("🗑️ Excluir", key=f"del_{selected_id}"):
                audit.delete_ticket(selected_id)
                st.success("Excluído.")
                time.sleep(0.5)
                st.rerun()

        st.markdown("---")

        # --- EXIBIÇÃO DAS PERNAS (LEGS) ---
        legs = target.get('legs', [])
        if not legs:
            st.error("⚠️ ESTE BILHETE ESTÁ SEM PERNAS.")
        else:
            cols = st.columns(3)
            
            for i, leg in enumerate(legs):
                with cols[i % 3]:
                    l_status = leg.get('status', 'PENDING')
                    
                    border_color = "#475569"
                    bg_color = "rgba(255,255,255,0.02)"
                    status_icon = "⏳"
                    
                    if l_status == 'WIN': 
                        border_color = "#00FF9C"
                        bg_color = "rgba(0, 255, 156, 0.05)"
                        status_icon = "✅"
                    elif l_status == 'LOSS': 
                        border_color = "#FF4F4F"
                        bg_color = "rgba(255, 79, 79, 0.05)"
                        status_icon = "❌"
                    
                    mkt_type = leg.get('market_type', 'UNK')
                    p_name = leg.get('player_name', 'Unknown')
                    mkt_display = leg.get('market_display', '-')
                    line_val = leg.get('line', 0)
                    actual_val = leg.get('actual_value', '-')
                    if actual_val == 0 and l_status == 'PENDING': actual_val = "-"
                    
                    thesis = leg.get('thesis', 'N/A')

                    st.markdown(f"""
<div style="border: 1px solid {border_color}; border-radius: 6px; padding: 10px; background: {bg_color}; margin-bottom: 10px;">
    <div style="display: flex; justify-content: space-between; margin-bottom: 5px;">
        <span style="font-size: 10px; color: #94A3B8; font-weight: bold; background: #1e293b; padding: 2px 4px; border-radius: 3px;">{mkt_type}</span>
        <span style="font-size: 14px;">{status_icon}</span>
    </div>
    <div style="font-size: 15px; font-weight: bold; color: #E2E8F0; line-height: 1.2;">{p_name}</div>
    <div style="font-size: 14px; color: {border_color}; font-weight: bold; margin-bottom: 5px;">{mkt_display}</div>
    <div style="background: rgba(0,0,0,0.2); padding: 5px; border-radius: 4px; display: flex; justify-content: space-between;">
        <div style="font-size: 11px; color: #CBD5E1;">Meta: <b>{line_val}</b></div>
        <div style="font-size: 11px; color: #CBD5E1;">Real: <b style="color: {border_color}">{actual_val}</b></div>
    </div>
    <div style="margin-top: 6px; font-size: 10px; color: #64748B; font-style: italic;">
        Tese: {thesis[:40]}...
    </div>
</div>
""", unsafe_allow_html=True)
# ============================================================================
# FUNÇÃO DE RENDERIZAÇÃO DO CARD DE JOGO (REVERTIDO PARA COMPONENTS)
# ============================================================================
def render_game_card(away_team, home_team, game_data, odds_map=None):
    # --- CORREÇÃO DO NAMEERROR ---
    # Importamos aqui para garantir que funcione sem mexer no topo do arquivo
    import streamlit.components.v1 as components 
    import dateutil.parser
    import pytz
    
    # Mapeamento de Logos
    def get_logo(abbr):
        return f"https://a.espncdn.com/i/teamlogos/nba/500/{abbr.lower()}.png"

    # --- 1. HORÁRIO BRASIL ---
    game_time = "HOJE"
    try:
        raw_time = game_data.get("startTimeUTC") or game_data.get("date")
        if raw_time:
            dt_utc = dateutil.parser.parse(raw_time)
            dt_br = dt_utc.astimezone(pytz.timezone('America/Sao_Paulo'))
            game_time = dt_br.strftime("%H:%M")
    except: pass

    # Odds
    spread_display = game_data.get("odds_spread", "N/A")
    total_display = game_data.get("odds_total", "N/A")
    spread_val = 0.0
    try:
        if spread_display and spread_display not in ["N/A", "EVEN"]:
            spread_val = float(spread_display.split()[-1])
    except: pass

    status = game_data.get('status', 'Agendado')
    if "Final" in status: status = "FIM"

    # --- 2. PACE & DADOS ---
    adv_stats = st.session_state.get('team_advanced', {})
    def get_team_pace(abbr):
        t_data = adv_stats.get(abbr, {})
        if not t_data: return 100.0
        return float(t_data.get('PACE') or t_data.get('pace') or 100.0)

    pace_home = get_team_pace(home_team)
    pace_away = get_team_pace(away_team)
    avg_pace = (pace_home + pace_away) / 2
    
    if avg_pace >= 101.5: pace_color, pace_icon = "#00FF9C", "⚡"
    elif avg_pace <= 98.5: pace_color, pace_icon = "#FFA500", "🐌"
    else: pace_color, pace_icon = "#9CA3AF", "⚖️"

    # Tenta calcular blowout se a função existir, senão usa padrão
    try:
        blowout = calculate_blowout_risk(spread_val)
    except:
        blowout = {"color": "#9CA3AF", "desc": "-", "icon": "", "nivel": "-"}

    # --- 3. HTML DO CARD (ESTRUTURA ISOLADA) ---
    # Adicionei imports de fonte dentro do HTML para garantir o visual no iframe
    card_html = f"""
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&family=Oswald:wght@400;700&display=swap" rel="stylesheet">
    <div style="
        background: linear-gradient(135deg, #0F172A 0%, #1E293B 100%);
        border-radius: 12px;
        padding: 12px;
        border: 1px solid #334155;
        font-family: 'Inter', sans-serif;
        box-sizing: border-box;
        height: 190px;
        overflow: hidden;
    ">
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 5px;">
        <div style="background: rgba(30, 144, 255, 0.15); color: #60A5FA; padding: 2px 8px; border-radius: 4px; font-size: 10px; font-weight: 700; text-transform: uppercase;">
            {status}
        </div>
        <div style="font-family: 'Oswald', sans-serif; font-size: 12px; color: #E2E8F0; letter-spacing: 0.5px;">
            🕒 {game_time} BR
        </div>
      </div>

      <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px;">
          <div style="text-align: center; width: 30%;">
            <img src="{get_logo(away_team)}" style="width: 40px; height: 40px; margin-bottom: 2px; object-fit: contain;">
            <div style="font-weight: 800; font-size: 14px; color: #F1F5F9; font-family: 'Oswald';">{away_team}</div>
            <div style="font-size: 9px; color: #94A3B8;">PACE: {int(pace_away)}</div>
          </div>
          
          <div style="text-align: center; width: 34%; background: rgba(0,0,0,0.2); border-radius: 8px; padding: 4px 0;">
            <div style="font-size: 9px; color: #64748B; font-weight: 600;">SPREAD</div>
            <div style="font-size: 14px; color: #FFD700; font-weight: bold; font-family: 'Oswald'; margin-bottom: 2px;">{spread_display}</div>
            <div style="font-size: 9px; color: #64748B; font-weight: 600;">TOTAL</div>
            <div style="font-size: 14px; color: #E2E8F0; font-weight: bold; font-family: 'Oswald';">{total_display}</div>
          </div>
          
          <div style="text-align: center; width: 30%;">
            <img src="{get_logo(home_team)}" style="width: 40px; height: 40px; margin-bottom: 2px; object-fit: contain;">
            <div style="font-weight: 800; font-size: 14px; color: #F1F5F9; font-family: 'Oswald';">{home_team}</div>
            <div style="font-size: 9px; color: #94A3B8;">PACE: {int(pace_home)}</div>
          </div>
      </div>

      <div style="background: rgba(255, 255, 255, 0.03); border-radius: 6px; padding: 5px; display: flex; justify-content: space-around; align-items: center; border: 1px solid rgba(255,255,255,0.05);">
        <div style="text-align: center;">
            <div style="font-size: 8px; color: #94A3B8; font-weight: 600; text-transform: uppercase;">Ritmo Est. ({int(avg_pace)})</div>
            <div style="color: {pace_color}; font-weight: bold; font-size: 11px;">{pace_icon}</div>
        </div>
        <div style="width: 1px; height: 15px; background: rgba(255,255,255,0.1);"></div>
        <div style="text-align: center;">
            <div style="font-size: 8px; color: #94A3B8; font-weight: 600; text-transform: uppercase;">Risco Blowout</div>
            <div style="color: {blowout['color']}; font-weight: bold; font-size: 11px;">{blowout['icon']} {blowout['nivel']}</div>
        </div>
      </div>
    </div>
    """
    
    # --- VOLTANDO AO ORIGINAL ---
    # Usando components.html para isolar o layout (iframe)
    # Altura fixa de 210px evita barras de rolagem
    components.html(card_html, height=210, scrolling=False)
# ============================================================================
# RENDERIZADORES VISUAIS 
# ============================================================================
def render_team_header(team_abbr, is_home):
    """Cabeçalho do time com impacto visual forte"""
    # Cores vibrantes
    color = "#00E5FF" if is_home else "#FF4F4F" 
    bg_gradient = f"linear-gradient(90deg, {color}40 0%, transparent 100%)"
    icon = "🏠 HOME" if is_home else "✈️ AWAY"
    
    st.markdown(f"""
    <div style="
        background: {bg_gradient};
        border-left: 6px solid {color};
        padding: 15px;
        margin-bottom: 20px;
        border-radius: 0 8px 8px 0;
        box-shadow: 0 4px 15px rgba(0,0,0,0.3);
    ">
        <div style="font-family: 'Oswald', sans-serif; font-size: 32px; color: #FFF; font-weight: 700; line-height: 1;">
            {team_abbr}
        </div>
        <div style="font-family: 'Inter', sans-serif; font-size: 12px; color: {color}; font-weight: 800; letter-spacing: 2px; margin-top: 5px;">
            {icon}
        </div>
    </div>
    """, unsafe_allow_html=True)

def render_player_card_starter(player, injured_names=None):
    """Card de Titular estilo 'Ficha Técnica' (Correção de HTML e Argumentos)"""
    injured_names = injured_names or []
    p_name = player['PLAYER']
    
    # Lógica de Status
    status_raw = player.get('STATUS', '').lower()
    is_injured = any(inj in p_name for inj in injured_names)
    
    if is_injured or 'out' in status_raw or 'inj' in status_raw:
        s_color = "#FF4F4F"; opacity = "0.6"
    elif 'gtd' in status_raw or 'quest' in status_raw:
        s_color = "#FFA500"; opacity = "1.0"
    else:
        s_color = "#00FF9C"; opacity = "1.0"

    pos = player['POSITION']
    min_avg = player.get('MIN_AVG', 0)
    pra_avg = player.get('PRA_AVG', 0)
    
    # Archetype
    archs = player.get('ARCHETYPES', [])
    arch_html = ""
    if archs:
        # Pega até 2
        safe_archs = archs[:2]
        for a in safe_archs:
            val = a.get('name', '') if isinstance(a, dict) else str(a)
            if val:
                arch_html += f'<span style="background:rgba(255,255,255,0.1); padding:2px 6px; border-radius:4px; font-size:9px; margin-right:4px; color:#A3B3BC;">{val}</span>'

    # HTML Linear (Seguro contra erros de renderização)
    html = f'<div style="background: #0F172A; border: 1px solid rgba(255,255,255,0.1); border-left: 4px solid {s_color}; border-radius: 6px; padding: 12px; margin-bottom: 10px; position: relative; overflow: hidden; opacity: {opacity};">'
    html += f'<div style="position: absolute; right: -10px; top: -10px; font-size: 60px; font-weight: 900; color: rgba(255,255,255,0.03); font-family: \'Oswald\';">{pos}</div>'
    html += f'<div style="display: flex; justify-content: space-between; align-items: center; position: relative; z-index: 1;">'
    html += f'<div><div style="font-family: \'Oswald\', sans-serif; font-size: 16px; color: #F1F5F9; font-weight: 600; text-transform: uppercase;">{p_name}</div>'
    html += f'<div style="margin-top: 4px;">{arch_html}</div></div>'
    html += f'<div style="text-align: right;"><div style="font-family: \'Inter\'; font-size: 11px; color: #94A3B8;">MIN / PRA</div>'
    html += f'<div style="font-family: \'Oswald\'; font-size: 18px; color: {s_color};">{min_avg:.0f}<span style="font-size:12px; color:#64748B;">m</span> <span style="color:#334155;">|</span> {pra_avg:.0f}</div>'
    html += f'</div></div></div>'

    st.markdown(html, unsafe_allow_html=True)

def render_stat_leader_card(player, stat_name, stat_value, rank, color="#00E5FF"):
    """Renderiza um mini-card para líderes de estatísticas"""
    trophy = ["🥇", "🥈", "🥉"][rank] if rank < 3 else f"#{rank+1}"
    
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, {color}15 0%, rgba(15, 23, 42, 0.8) 100%);
        border: 1px solid {color}40;
        border-radius: 8px;
        padding: 10px;
        margin-bottom: 8px;
        display: flex;
        align-items: center;
        justify-content: space-between;
    ">
        <div style="display: flex; align-items: center; gap: 10px;">
            <div style="font-size: 20px;">{trophy}</div>
            <div>
                <div style="font-family: 'Oswald'; font-size: 14px; color: #F8FAFC; line-height: 1.1;">
                    {player['PLAYER']}
                </div>
                <div style="font-size: 10px; color: #94A3B8;">
                    {player['TEAM']} • {player.get('POSITION', '')}
                </div>
            </div>
        </div>
        <div style="text-align: right;">
            <div style="font-family: 'Oswald'; font-size: 18px; color: {color}; font-weight: bold;">
                {stat_value:.1f}
            </div>
            <div style="font-size: 8px; color: {color}; text-transform: uppercase;">
                {stat_name}
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

def render_minute_bar(player_name, position, minutes, role):
    """Renderiza uma barra visual de minutos (Estilo Stamina)"""
    # Cores baseadas na role
    if role == "starter":
        color = "#00FF9C" # Verde Neon
        label = "TITULAR"
    elif role == "rotation":
        color = "#00E5FF" # Azul Cyan
        label = "ROTAÇÃO"
    else:
        color = "#FFA500" # Laranja
        label = "BANCO"
        
    # Cálculo da largura (Base 48 min)
    width_pct = min((minutes / 48) * 100, 100)
    
    st.markdown(f"""
    <div style="margin-bottom: 8px;">
        <div style="display: flex; justify-content: space-between; font-size: 12px; color: #E2E8F0; margin-bottom: 2px;">
            <div><span style="color:{color}; font-weight:bold;">{position}</span> {player_name}</div>
            <div style="font-family: monospace;">{minutes:.1f} min</div>
        </div>
        <div style="
            width: 100%; 
            height: 6px; 
            background: rgba(255,255,255,0.1); 
            border-radius: 3px; 
            overflow: hidden;
        ">
            <div style="
                width: {width_pct}%; 
                height: 100%; 
                background: linear-gradient(90deg, {color} 0%, {color}80 100%);
                box-shadow: 0 0 8px {color}40;
            "></div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_projection_card(player_name, team, opp, proj_val, ceiling_val, volatility):
    """Renderiza card de projeção com HTML linear para evitar erros de renderização"""
    # Definição de cores baseada na volatilidade
    if volatility < 0.4:
        vol_color = "#00FF9C" # Verde (Seguro)
    elif volatility < 0.7:
        vol_color = "#FFA500" # Laranja (Médio)
    else:
        vol_color = "#FF4F4F" # Vermelho (Volátil)

    # Construção Linear do HTML (Sem indentação para não quebrar o Streamlit)
    html = '<div style="background: #0F172A; border: 1px solid rgba(255,255,255,0.1); border-radius: 8px; padding: 12px; margin-bottom: 10px;">'
    
    # Cabeçalho
    html += '<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 5px;">'
    html += f'<div><div style="font-family: \'Oswald\'; font-size: 16px; color: #F8FAFC;">{player_name}</div>'
    html += f'<div style="font-size: 10px; color: #94A3B8;">{team} vs {opp}</div></div>'
    html += f'<div style="text-align: right;"><div style="font-family: \'Oswald\'; font-size: 20px; color: #00E5FF;">{proj_val:.1f} <span style="font-size:12px; color:#64748B;">PRA</span></div></div>'
    html += '</div>'
    
    # Labels
    html += '<div style="display: flex; justify-content: space-between; font-size: 9px; color: #64748B; margin-bottom: 2px;">'
    html += f'<span>BASE</span><span>TETO ({ceiling_val:.1f})</span>'
    html += '</div>'
    
    # Barra Visual
    html += '<div style="width: 100%; height: 6px; background: rgba(255,255,255,0.1); border-radius: 3px; overflow: hidden; display: flex;">'
    html += '<div style="width: 70%; background: #00E5FF; opacity: 0.7;"></div>' # Base
    html += f'<div style="width: 30%; background: {vol_color}; opacity: 0.5;"></div>' # Upside
    html += '</div>'
    
    html += '</div>'

    st.markdown(html, unsafe_allow_html=True)
      
def show_estatisticas_jogador():
    st.header("📈 DATA COMMAND CENTER")
    
    # 1. Carregamento de Dados
    df_l5 = st.session_state.get('df_l5', pd.DataFrame())
    
    if df_l5 is None or df_l5.empty:
        st.warning("⚠️ Cache L5 vazio.")
        if st.button("🔄 Inicializar Base de Dados (L5)", type="primary"):
            with st.spinner("Conectando aos servidores da NBA..."):
                df_l5 = get_players_l5(progress_ui=True)
                st.session_state.df_l5 = df_l5
                st.rerun()
        return

    # 2. Hall of Fame (Top Performers L5)
    st.markdown('<div style="font-family: Oswald; color: #94A3B8; margin-bottom: 10px; letter-spacing: 1px;">🔥 LÍDERES RECENTES (ÚLTIMOS 5 JOGOS)</div>', unsafe_allow_html=True)
    
    col_pts, col_reb, col_ast, col_pra = st.columns(4)
    
    with col_pts:
        st.caption("PONTUAÇÃO (PTS)")
        top_pts = df_l5.nlargest(3, 'PTS_AVG')
        for i, (_, p) in enumerate(top_pts.iterrows()):
            render_stat_leader_card(p, "PTS", p['PTS_AVG'], i, "#FF4F4F") # Vermelho
            
    with col_reb:
        st.caption("REBOTES (REB)")
        top_reb = df_l5.nlargest(3, 'REB_AVG')
        for i, (_, p) in enumerate(top_reb.iterrows()):
            render_stat_leader_card(p, "REB", p['REB_AVG'], i, "#00E5FF") # Azul Cyan
            
    with col_ast:
        st.caption("ASSISTÊNCIAS (AST)")
        top_ast = df_l5.nlargest(3, 'AST_AVG')
        for i, (_, p) in enumerate(top_ast.iterrows()):
            render_stat_leader_card(p, "AST", p['AST_AVG'], i, "#FFA500") # Laranja
            
    with col_pra:
        st.caption("COMBO (PRA)")
        top_pra = df_l5.nlargest(3, 'PRA_AVG')
        for i, (_, p) in enumerate(top_pra.iterrows()):
            render_stat_leader_card(p, "PRA", p['PRA_AVG'], i, "#00FF9C") # Verde

    st.markdown("---")

    # 3. Painel de Controle (Filtros)
    with st.expander("🎛️ Painel de Filtros e Busca", expanded=True):
        col_f1, col_f2 = st.columns([1, 3])
        
        with col_f1:
            teams = sorted(df_l5["TEAM"].dropna().unique().tolist())
            teams.insert(0, "Todos")
            sel_team = st.selectbox("Filtrar por Time", teams)
            
            player_search = st.text_input("Buscar Jogador", placeholder="Ex: LeBron...")
            
        with col_f2:
            st.caption("Definir Mínimos (Filtro Rápido)")
            c1, c2, c3, c4 = st.columns(4)
            min_min = c1.slider("Minutos", 0, 40, 15)
            min_pts = c2.slider("Pontos", 0, 35, 0)
            min_reb = c3.slider("Rebotes", 0, 15, 0)
            min_ast = c4.slider("Assists", 0, 12, 0)

    # 4. Processamento da Tabela
    df_view = df_l5.copy()
    
    # Aplicar Filtros
    if sel_team != "Todos":
        df_view = df_view[df_view["TEAM"] == sel_team]
    
    if player_search:
        df_view = df_view[df_view["PLAYER"].str.contains(player_search, case=False, na=False)]
        
    df_view = df_view[
        (df_view["MIN_AVG"] >= min_min) &
        (df_view["PTS_AVG"] >= min_pts) &
        (df_view["REB_AVG"] >= min_reb) &
        (df_view["AST_AVG"] >= min_ast)
    ]
    
    # Seleção e Renomeação de Colunas
    cols_to_show = ["PLAYER", "TEAM", "MIN_AVG", "PTS_AVG", "REB_AVG", "AST_AVG", "PRA_AVG", "STL_AVG", "BLK_AVG", "3PM_AVG"]
    # Garantir que colunas existem
    cols_final = [c for c in cols_to_show if c in df_view.columns]
    
    df_display = df_view[cols_final].sort_values("PRA_AVG", ascending=False).reset_index(drop=True)
    
    # Formatação Visual (Heatmap)
    st.markdown(f"### 📋 Banco de Dados ({len(df_display)} Jogadores)")
    
    if not df_display.empty:
        # Configurar estilos do Pandas
        formatted_df = df_display.style\
            .format("{:.1f}", subset=[c for c in cols_final if c not in ["PLAYER", "TEAM"]])\
            .background_gradient(cmap="Blues", subset=["MIN_AVG"])\
            .background_gradient(cmap="Reds", subset=["PTS_AVG"])\
            .background_gradient(cmap="Greens", subset=["AST_AVG"])\
            .background_gradient(cmap="Purples", subset=["REB_AVG"])\
            .background_gradient(cmap="Oranges", subset=["PRA_AVG"])
            
        st.dataframe(
            formatted_df,
            use_container_width=True,
            height=600,
            column_config={
                "PLAYER": "Jogador",
                "TEAM": "Time",
                "MIN_AVG": st.column_config.NumberColumn("Minutos", help="Média de minutos L5"),
                "PTS_AVG": st.column_config.NumberColumn("PTS", help="Pontos L5"),
                "REB_AVG": st.column_config.NumberColumn("REB", help="Rebotes L5"),
                "AST_AVG": st.column_config.NumberColumn("AST", help="Assistências L5"),
                "PRA_AVG": st.column_config.NumberColumn("PRA", help="PTS + REB + AST"),
                "STL_AVG": st.column_config.NumberColumn("STL", help="Roubos L5"),
                "BLK_AVG": st.column_config.NumberColumn("BLK", help="Tocos L5"),
                "3PM_AVG": st.column_config.NumberColumn("3PM", help="3 Pontos L5"),
            }
        )
    else:
        st.info("Nenhum jogador encontrado com os filtros atuais.")

# ============================================================================
# PÁGINA: DESDOBRAMENTOS DO DIA (V18.0 - PHOTO FIX & CLEAN UI)
# ============================================================================
def show_desdobramentos_inteligentes():
    import streamlit as st
    import pandas as pd
    from datetime import datetime
    import statistics
    import re
    import unicodedata
    from collections import defaultdict

    # --- 1. INFRAESTRUTURA ---
    try: from db_manager import db 
    except ImportError: db = None
    
    try: from injuries import InjuryMonitor; monitor = InjuryMonitor()
    except ImportError: monitor = None
        
    try: from SuiteNAS import get_data_universal
    except ImportError:
        def get_data_universal(key): return {}

    # --- 2. HELPERS & FOTOS (LÓGICA SUPERBILHETE) ---
    
    # Carrega DataFrame para popular o Vault de IDs
    df_l5 = st.session_state.get('df_l5', pd.DataFrame())
    if df_l5.empty: df_l5 = get_data_universal('df_l5')

    ID_VAULT = {}
    if isinstance(df_l5, pd.DataFrame) and not df_l5.empty:
        # Normalização simplificada para bater com o get_photo do usuário
        cols = df_l5.columns
        id_col = next((c for c in cols if str(c).upper() in ['PLAYER_ID', 'PERSON_ID', 'ID']), None)
        name_col = next((c for c in cols if str(c).upper() in ['PLAYER_NAME', 'PLAYER', 'NAME']), None)
        
        if id_col and name_col:
            for _, row in df_l5.iterrows():
                try:
                    # Normalização: Upper e sem pontos (ex: C.J. -> CJ)
                    p_name = str(row[name_col]).upper().replace('.', '').strip()
                    p_id = int(float(row[id_col]))
                    ID_VAULT[p_name] = p_id
                    
                    # Salva também só pelo sobrenome para fallback
                    parts = p_name.split()
                    if len(parts) > 1:
                        ID_VAULT[parts[-1]] = p_id
                except: pass

    # A função exata solicitada
    def get_photo(name, pid=0):
        # Tenta usar o PID passado se for válido
        if pid > 0: return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"
        
        # Limpeza do nome
        clean = str(name).upper().replace('.', '').strip()
        
        # Tenta achar no Vault pelo nome completo
        pid = ID_VAULT.get(clean, 0)
        
        # Fallback: Se não achou, tenta pelo último nome
        if pid == 0 and len(clean.split()) > 1: 
            pid = ID_VAULT.get(clean.split()[-1], 0)
        
        if pid > 0: return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"
        return "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"

    # --- 3. CSS (VISUAL LIMPO) ---
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Oswald:wght@400;600;700&family=Inter:wght@400;600&display=swap');
        
        .strat-header { font-family: 'Oswald'; font-size: 30px; color: #fbbf24; margin: 0; text-transform: uppercase; }
        .strat-meta { font-family: 'Inter'; font-size: 11px; color: #94a3b8; margin-bottom: 15px; }
        
        /* CARD TICKET */
        .ticket-box {
            background: #0f172a; border: 1px solid #334155; border-radius: 8px;
            margin-bottom: 15px; overflow: hidden;
        }
        
        /* CORES TEMAS */
        .th-attack { background: linear-gradient(90deg, rgba(220, 38, 38, 0.2), rgba(15, 23, 42, 0)); border-left: 3px solid #dc2626; }
        .th-pyramid { background: linear-gradient(90deg, rgba(37, 99, 235, 0.2), rgba(15, 23, 42, 0)); border-left: 3px solid #2563eb; }
        .th-wall { background: linear-gradient(90deg, rgba(5, 150, 105, 0.2), rgba(15, 23, 42, 0)); border-left: 3px solid #059669; }
        .th-scavenge { background: linear-gradient(90deg, rgba(147, 51, 234, 0.2), rgba(15, 23, 42, 0)); border-left: 3px solid #9333ea; }

        /* LINHA DO JOGADOR */
        .sgp-row {
            display: flex; align-items: flex-start; gap: 10px;
            padding: 10px 12px; border-bottom: 1px solid #1e293b;
        }
        .sgp-row:last-child { border-bottom: none; }
        
        .sgp-img { width: 42px; height: 42px; border-radius: 50%; border: 2px solid #334155; object-fit: cover; background: #000; flex-shrink: 0; }
        
        .p-info { flex: 1; min-width: 0; }
        
        .sgp-name { 
            font-family: 'Oswald'; font-size: 14px; color: #ffffff !important; font-weight: 700; 
            line-height: 1.1; margin-bottom: 4px; text-shadow: 0 1px 2px rgba(0,0,0,0.5);
        }
        .sgp-team { font-family: 'Inter'; font-size: 10px; color: #94a3b8; font-weight: normal; margin-left: 4px; }
        
        .role-pill { font-size: 8px; padding: 1px 5px; border-radius: 3px; background: #334155; color: #cbd5e1; display: inline-block; margin-right: 5px; vertical-align: middle; }
        .barrel-icon { color: #facc15; font-size: 10px; margin-left: 2px; }
        
        /* CHIP DE STATS SIMPLIFICADO (SEM O HIT RATE) */
        .stat-chip-simple {
            display: inline-block;
            background: #1e293b; 
            border: 1px solid #475569; 
            padding: 3px 8px; 
            border-radius: 4px; 
            margin-right: 4px;
            margin-bottom: 4px;
        }
    </style>
    """, unsafe_allow_html=True)

    # --- 4. ENGINE (V17 Logic) ---
    class LocalPlayerClassifier:
        def get_role_classification(self, ctx):
            pts = ctx.get('pts_L5', 0)
            combined = pts + ctx.get('reb_L5', 0) + ctx.get('ast_L5', 0)
            is_starter = ctx.get('is_starter', False)
            if is_starter and combined >= 25: return "star"
            elif is_starter and combined >= 15: return "starter"
            elif not is_starter and combined >= 20: return "bench_scorer"
            return "rotation"

        def get_play_style(self, ctx):
            reb, ast = ctx.get('reb_L5', 0), ctx.get('ast_L5', 0)
            if ast >= 5.0: return "playmaker"
            if reb >= 7.0: return "rebounder"
            if reb >= 4.5 and ast >= 3.5: return "hustle"
            return "scorer"

    class OrchestratorV18:
        def __init__(self, logs, games):
            self.logs = logs
            self.games = games
            self.active_teams = self._get_active_teams()
            self.classifier = LocalPlayerClassifier()
            self.master_inventory = [] 
            self.diag = {"analyzed": 0, "approved_players": 0}

        def _get_active_teams(self):
            teams = set()
            for g in self.games:
                h, a = g.get('home_abbr', g.get('home')), g.get('away_abbr', g.get('away'))
                if h: teams.add(self._norm(h))
                if a: teams.add(self._norm(a))
            return teams

        def _norm(self, t):
            if not t: return ""
            return str(t).upper().strip()

        def ingest_and_bundle(self):
            for name, data in self.logs.items():
                self.diag['analyzed'] += 1
                team = self._norm(data.get('team'))
                if team not in self.active_teams: continue
                if monitor and monitor.is_player_blocked(name, team): continue
                
                logs = data.get('logs', {})
                if not logs: continue

                l5_stats = {k: statistics.mean(logs.get(k, [0])[:5]) for k in ['PTS','REB','AST','min']}
                is_starter = l5_stats['min'] > 22
                ctx = {'pts_L5': l5_stats['PTS'], 'reb_L5': l5_stats['REB'], 'ast_L5': l5_stats['AST'], 'min_L5': l5_stats['min'], 'is_starter': is_starter}
                
                role_tag = self.classifier.get_role_classification(ctx)
                style_tag = self.classifier.get_play_style(ctx)
                
                valid_legs = []
                for stat in ['PTS', 'REB', 'AST']:
                    vals = logs.get(stat, [])
                    if len(vals) < 5: continue
                    l10 = vals[:10]
                    try: floor_val = sorted(l10)[1] 
                    except: continue
                    min_req = {'PTS': 8, 'REB': 3, 'AST': 2}
                    if floor_val < min_req[stat]: continue
                    hits_l10 = sum(1 for v in l10 if v >= floor_val)
                    if hits_l10 >= 8:
                        valid_legs.append({
                            'stat': stat, 'line': floor_val, 
                            'hits': hits_l10,
                            'score': (hits_l10 * 8) + l5_stats[stat]
                        })

                if not valid_legs: continue
                
                role = 'BASE'
                if (role_tag in ['star', 'starter'] and any(l['stat']=='PTS' and l['line']>=18 for l in valid_legs)):
                    role = 'ANCHOR'
                elif len(valid_legs) >= 2 and style_tag in ['playmaker', 'hustle', 'rebounder']:
                    role = 'MOTOR'
                elif any(l['stat']=='PTS' and l['line']>=10 for l in valid_legs):
                    role = 'WORKER'
                
                valid_legs.sort(key=lambda x: x['score'], reverse=True)
                
                # Tenta pegar o ID do jogador aqui se disponível nos logs (alguns logs trazem meta info)
                # Se não, o get_photo vai resolver com o Vault
                
                self.master_inventory.append({
                    'player': name, 'team': team, 
                    'legs': valid_legs,
                    'role': role,
                    'score': sum(l['score'] for l in valid_legs),
                    'usage': 0
                })
                self.diag['approved_players'] += 1

        def manufacture_tickets(self):
            self.master_inventory.sort(key=lambda x: x['score'], reverse=True)
            tickets = []
            
            formulas = ['ATTACK', 'PYRAMID', 'WALL', 'ATTACK', 'PYRAMID', 'WALL', 'ATTACK', 'PYRAMID', 'WALL']
            
            def find_candidate(roles, exclude_teams, exclude_players, max_usage=2):
                for p in self.master_inventory:
                    usage_limit = max_usage + 1 if p['role'] in ['ANCHOR', 'MOTOR'] else max_usage
                    if p['role'] in roles and p['usage'] < usage_limit:
                        if p['team'] not in exclude_teams and p['player'] not in exclude_players:
                            return p
                return None

            # FASE 1: Fórmulas
            for form_type in formulas:
                candidates = []
                u_teams, u_players = set(), set()
                meta = {}
                slots = []
                
                if form_type == 'ATTACK':
                    slots = [(['ANCHOR'], 3), (['ANCHOR'], 3), (['WORKER', 'BASE'], 2), (['WORKER', 'BASE'], 2)]
                    meta = {'title': '🚀 ATAQUE TOTAL', 'desc': 'Foco em pontuadores.', 'color': '#ef4444'}
                elif form_type == 'PYRAMID':
                    slots = [(['ANCHOR'], 3), (['MOTOR', 'WORKER'], 3), (['WORKER', 'BASE'], 2), (['BASE', 'WORKER'], 2)]
                    meta = {'title': '🛡️ A PIRÂMIDE', 'desc': 'Equilíbrio tático.', 'color': '#3b82f6'}
                elif form_type == 'WALL':
                    slots = [(['MOTOR', 'WORKER'], 3), (['WORKER'], 2), (['BASE'], 2), (['BASE'], 2)]
                    meta = {'title': '🧱 O PAREDÃO', 'desc': 'Segurança máxima.', 'color': '#10b981'}
                
                valid_ticket = True
                for roles, limit in slots:
                    cand = find_candidate(roles, u_teams, u_players, limit)
                    if cand:
                        candidates.append(cand)
                        u_teams.add(cand['team']); u_players.add(cand['player'])
                    else:
                        valid_ticket = False; break
                
                if valid_ticket and len(candidates) == 4:
                    self._commit_ticket(tickets, candidates, meta)

            # FASE 2: Reciclagem
            while len(tickets) < 20:
                scavenge = []
                u_teams, u_players = set(), set()
                for p in self.master_inventory:
                    if len(scavenge) == 4: break
                    limit = 4 if p['role'] == 'ANCHOR' else 3
                    if p['usage'] < limit and p['team'] not in u_teams and p['player'] not in u_players:
                        scavenge.append(p)
                        u_teams.add(p['team']); u_players.add(p['player'])
                
                if len(scavenge) == 4:
                    self._commit_ticket(tickets, scavenge, {'title': '♻️ RECICLAGEM', 'desc': 'Oportunidades extras.', 'color': '#a855f7'})
                else: break
            
            return tickets

        def _commit_ticket(self, tickets, players, meta):
            final_legs = []
            for p in players:
                p['usage'] += 1
                legs_to_use = p['legs'][:2] if (len(p['legs']) > 1 and p['role'] in ['ANCHOR', 'MOTOR']) else p['legs'][:1]
                for l in legs_to_use:
                    final_legs.append({
                        'player': p['player'], 'team': p['team'], 'role': p['role'],
                        'stat': l['stat'], 'line': l['line'],
                        'is_double': len(legs_to_use) > 1
                    })
            if len(final_legs) > 7: final_legs = final_legs[:7]
            meta['title'] = f"{meta['title']} #{len(tickets)+1}"
            tickets.append({**meta, 'legs': final_legs})

    # --- 5. EXECUÇÃO ---
    c_head, c_tog = st.columns([4, 1])
    with c_head:
        st.markdown('<div style="font-family:Oswald; font-size:30px; color:#fbbf24; text-transform:uppercase;">DESDOBRAMENTOS DO DIA</div>', unsafe_allow_html=True)
        st.caption(f"📅 {datetime.now().strftime('%d/%m/%Y')} • 🧩 V18.0 Clean UI")
    with c_tog:
        debug_mode = st.toggle("Debug", value=False)

    st.markdown("**Legenda:** 👑 Anchor (Estrela) | ⚙️ Motor (Glue Guy) | 👷 Worker (Rotação) | 🛡️ Base (Segurança) | ⚡ Double Barrel")

    logs = st.session_state.get("real_game_logs") or get_data_universal("real_game_logs")
    games = st.session_state.get("scoreboard") or get_data_universal("scoreboard")

    if not logs or not games:
        st.warning("⚠️ Dados insuficientes. Atualize em Config.")
        return

    maestro = OrchestratorV18(logs, games)
    maestro.ingest_and_bundle()
    tickets = maestro.manufacture_tickets()

    if debug_mode:
        st.info(f"⚙️ DIAGNOSTICS: {maestro.diag['approved_players']} Jogadores. {len(tickets)} Bilhetes.")

    if not tickets:
        st.info("😴 Sem combinações de alta confiança.")
        return

    # --- 6. RENDERIZAÇÃO ---
    cols = st.columns(3)
    for i, t in enumerate(tickets):
        with cols[i % 3]:
            with st.container(border=True):
                # Header Seguro
                st.markdown(f"<div style='border-left: 4px solid {t['color']}; padding-left: 10px; margin-bottom: 10px;'>"
                            f"<div style='font-family:Oswald; font-size:16px; color:white;'>{t['title']}</div>"
                            f"<div style='font-size:11px; color:#94a3b8;'>{t['desc']} • {len(t['legs'])} Legs</div>"
                            f"</div>", unsafe_allow_html=True)

                player_legs = defaultdict(list)
                player_meta = {}
                for leg in t['legs']:
                    player_legs[leg['player']].append(leg)
                    player_meta[leg['player']] = {'team': leg['team'], 'role': leg['role'], 'dbl': leg['is_double']}

                for p_name, legs in player_legs.items():
                    meta = player_meta[p_name]
                    c_img, c_data = st.columns([1, 4])
                    
                    with c_img:
                        # Tenta pegar ID do Vault global ou 0
                        st.image(get_photo(p_name), width=42)
                    
                    with c_data:
                        role_icon = {'ANCHOR':'👑','MOTOR':'⚙️','WORKER':'👷','BASE':'🛡️'}.get(meta['role'],'')
                        barrel = "⚡" if meta['dbl'] else ""
                        
                        st.markdown(f"<div class='sgp-name'>{p_name} <span class='barrel-icon'>{barrel}</span> <span class='sgp-team'>({meta['team']})</span></div>", unsafe_allow_html=True)
                        st.markdown(f"<div style='margin-bottom:4px'><span class='role-pill'>{role_icon} {meta['role']}</span></div>", unsafe_allow_html=True)
                        
                        # CHIPS LIMPOS (SEM HIT RATE)
                        chips_str = ""
                        for l in legs:
                            color = "#fbbf24" if l['stat'] == 'PTS' else "#60a5fa"
                            # Removido o L10/10 conforme pedido
                            chips_str += f"""<span class='stat-chip-simple'>
                                <strong style='font-family:Oswald; color:{color}; font-size:12px'>{l['line']}+ {l['stat']}</strong>
                            </span>"""
                        
                        st.markdown(chips_str, unsafe_allow_html=True)
                    
                    st.markdown("<div style='margin-bottom:8px'></div>", unsafe_allow_html=True)
# ============================================================================
# PÁGINA: MATCHUP CENTER (V3.3 - HIERARCHY SORT FIX)
# ============================================================================
def show_escalacoes():
    import streamlit as st
    import html
    import pandas as pd
    import requests
    import re
    import unicodedata

    # --- 1. CSS VISUAL (MANTIDO) ---
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Oswald:wght@400;600&family=Inter:wght@400;600&display=swap');
        
        .war-room-header { font-family: 'Oswald'; font-size: 26px; color: #fff; margin-bottom: 5px; letter-spacing: 1px; }
        
        .game-block {
            background-color: #0f172a;
            border: 1px solid #334155;
            border-radius: 8px;
            margin-bottom: 20px;
            overflow: hidden;
        }
        
        .game-header-bar {
            background: #1e293b;
            padding: 8px 15px;
            border-bottom: 1px solid #334155;
            display: flex; justify-content: space-between; align-items: center;
        }
        .gh-title { font-family: 'Oswald'; font-size: 16px; color: #fff; }
        .gh-meta { font-family: 'Inter'; font-size: 11px; color: #94a3b8; }

        .player-row {
            display: flex; align-items: center;
            padding: 6px 8px;
            border-bottom: 1px solid #1e293b;
            transition: background 0.2s;
        }
        .player-row:hover { background: rgba(255,255,255,0.03); }
        .player-row:last-child { border-bottom: none; }
        
        .p-img { width: 32px; height: 32px; border-radius: 50%; object-fit: cover; border: 1px solid #475569; background: #000; margin-right: 10px; }
        .p-info { flex: 1; }
        .p-name { font-family: 'Oswald'; font-size: 13px; color: #e2e8f0; line-height: 1.1; }
        .p-pos { font-size: 9px; color: #64748b; font-weight: bold; background: rgba(255,255,255,0.1); padding: 1px 4px; border-radius: 3px; margin-left: 4px; }
        .p-mins { font-size: 10px; color: #10B981; font-weight: bold; margin-left: auto; font-family: 'Inter'; }
        
        .border-left-home { border-left: 3px solid #00E5FF; }
        .border-left-away { border-left: 3px solid #FF4F4F; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="war-room-header">👥 MATCHUP CENTER</div>', unsafe_allow_html=True)

    # --- HERO SECTION ---
    st.markdown("""
    <div style="background: linear-gradient(90deg, rgba(30,41,59,0.6) 0%, rgba(15,23,42,0.6) 100%); border-left: 4px solid #3b82f6; border-radius: 8px; padding: 15px 20px; margin-bottom: 25px; border: 1px solid #334155;">
        <div style="font-family: 'Inter', sans-serif; color: #e2e8f0; font-size: 14px; line-height: 1.6;">
            <strong style="color: #3b82f6; font-size: 15px;">ESCALAÇÕES - CONFRONTOS</strong><br>
            Acompanhe os duelos ordenados pela hierarquia de minutos (L5).
            <ul style="margin-top: 8px; margin-bottom: 0; padding-left: 20px; list-style-type: none;">
                <li style="margin-bottom: 4px;">✅ <strong style="color: #10B981;">OFICIAL:</strong> Escalação confirmada.</li>
                <li>⚠️ <strong style="color: #F59E0B;">PROJETADO:</strong> Ordenado por minutagem média.</li>
            </ul>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # --- 2. CONFIGURAÇÃO & HELPERS ---
    if 'scoreboard' not in st.session_state or not st.session_state.scoreboard:
        st.warning("⚠️ Scoreboard vazio. Atualize os jogos na aba Config.")
        return

    # Helper Nuclear
    def nuclear_normalize(text):
        if not text: return ""
        try:
            text = unicodedata.normalize('NFKD', str(text)).encode('ASCII', 'ignore').decode('utf-8')
            text = text.upper()
            text = re.sub(r'[^A-Z0-9]', '', text)
            return text
        except: return ""

    # PREPARAÇÃO DO MAPA DE MINUTOS E IDs
    # (Isso é crucial para saber que LeBron joga 35 min e Bronny joga 10 min)
    ID_VAULT = {}
    MINS_VAULT = {} # Mapa de Minutos
    
    df_l5 = st.session_state.get('df_l5', pd.DataFrame())
    
    if not df_l5.empty:
        try:
            df_l5.columns = [str(c).upper().strip() for c in df_l5.columns]
            c_name = next((c for c in df_l5.columns if 'PLAYER' in c), 'PLAYER')
            c_id = next((c for c in df_l5.columns if 'ID' in c and 'TEAM' not in c), 'PLAYER_ID')
            c_team = next((c for c in df_l5.columns if 'TEAM' in c and 'ID' not in c), 'TEAM')
            c_min = next((c for c in df_l5.columns if c in ['MIN_AVG', 'MIN', 'MINUTES']), None)
            
            for _, row in df_l5.iterrows():
                try:
                    pid = int(float(row.get(c_id, 0)))
                    nm = str(row.get(c_name, ''))
                    tm = str(row.get(c_team, 'UNK')).upper()
                    mins = float(row.get(c_min, 0)) if c_min else 0.0
                    
                    key = nuclear_normalize(nm)
                    
                    # Salva ID e Minutos
                    if pid > 0: ID_VAULT[key] = pid
                    MINS_VAULT[key] = mins
                    
                    # Fallback Sobrenome
                    parts = nm.split()
                    if len(parts) > 1: 
                        k2 = f"{nuclear_normalize(parts[-1])}_{tm}"
                        if pid > 0: ID_VAULT[k2] = pid
                        MINS_VAULT[k2] = mins
                except: continue
        except: pass

    def resolve_meta(name, team):
        """Retorna (ID, Minutos)"""
        k1 = nuclear_normalize(name)
        id_val = ID_VAULT.get(k1, 0)
        min_val = MINS_VAULT.get(k1, 0.0)
        
        if id_val == 0 or min_val == 0:
            parts = name.split()
            if len(parts) > 0:
                k2 = f"{nuclear_normalize(parts[-1])}_{team}"
                if id_val == 0: id_val = ID_VAULT.get(k2, 0)
                if min_val == 0: min_val = MINS_VAULT.get(k2, 0.0)
                
        return id_val, min_val

    # FETCHER EMBUTIDO
    def fetch_roster_internal(team_abbr):
        map_espn = {"UTA": "utah", "NOP": "no", "NYK": "ny", "GSW": "gs", "SAS": "sa", "PHX": "pho", "WAS": "wsh", "BKN": "bkn"}
        t_code = map_espn.get(team_abbr.upper(), team_abbr.lower())
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{t_code}/roster"
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=3)
            if r.status_code == 200:
                return r.json().get('athletes', [])
        except: pass
        return []

    # --- 3. PROCESSAMENTO DOS JOGOS ---
    games = st.session_state.scoreboard
    progress_bar = st.progress(0)
    
    for idx, game in enumerate(games):
        away = game.get('away', 'UNK')
        home = game.get('home', 'UNK')
        
        r_home = fetch_roster_internal(home)
        r_away = fetch_roster_internal(away)
        
        # --- PROCESSAMENTO COM ORDENAÇÃO POR MINUTOS ---
        def process_team(roster, team_abbr):
            processed = []
            if not roster: return []
            
            for p in roster:
                if not isinstance(p, dict): continue
                
                name = p.get('fullName', p.get('displayName', 'Unknown'))
                
                pos_obj = p.get('position')
                if not isinstance(pos_obj, dict): pos_obj = {}
                pos = pos_obj.get('abbreviation', '-')
                
                status_obj = p.get('status')
                status_text = 'Active'
                if isinstance(status_obj, dict):
                    type_obj = status_obj.get('type')
                    if isinstance(type_obj, dict):
                        status_text = type_obj.get('name', 'Active')
                
                # Busca Metadata (ID e Minutos)
                pid_nba, mins_avg = resolve_meta(name, team_abbr)
                
                # Se não achou na NBA, usa ID ESPN
                final_id = pid_nba if pid_nba > 0 else p.get('id', 0)
                
                processed.append({
                    "name": name, 
                    "pos": pos, 
                    "status": status_text, 
                    "id": final_id,
                    "minutes": mins_avg # O Segredo está aqui!
                })
            
            # --- A CORREÇÃO MAGISTRAL ---
            # Ordena: Quem tem mais minutos fica no topo
            # Jogadores sem minutos (0.0) vão pro fundo do banco
            return sorted(processed, key=lambda x: x['minutes'], reverse=True)

        h_players = process_team(r_home, home)
        a_players = process_team(r_away, away)
        
        # Agora o Top 5 é garantido serem os jogadores com mais minutos
        h_starters = h_players[:5]
        h_bench = h_players[5:]
        
        a_starters = a_players[:5]
        a_bench = a_players[5:]
        
        # --- 4. RENDERIZAÇÃO ---
        st.markdown(f"""
        <div class="game-block">
            <div class="game-header-bar">
                <span class="gh-title">{away} @ {home}</span>
                <span class="gh-meta">MATCHUP CENTER</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        c_away, c_home = st.columns(2)
        
        def render_squad(col, team_abbr, starters, bench, side):
            css_border = "border-left-away" if side == "away" else "border-left-home"
            color_tit = "#FF4F4F" if side == "away" else "#00E5FF"
            
            with col:
                st.markdown(f"<div style='margin-bottom:8px; color:{color_tit}; font-family:Oswald; font-size:18px;'>{team_abbr}</div>", unsafe_allow_html=True)
                
                for p in starters:
                    pid = p['id']
                    photo = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"
                    fallback = "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"
                    mins_display = f"{int(p['minutes'])}m" if p['minutes'] > 0 else ""
                    
                    st.markdown(f"""
                    <div class="player-row {css_border}">
                        <img src="{photo}" class="p-img" onerror="this.src='{fallback}'">
                        <div class="p-info">
                            <div class="p-name">{p['name']} <span class="p-pos">{p['pos']}</span></div>
                        </div>
                        <div class="p-mins">{mins_display}</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with st.expander(f"Reserves ({len(bench)})"):
                    for p in bench:
                         st.markdown(f"""
                        <div style="display:flex; align-items:center; padding:4px 0; border-bottom:1px dashed #334155;">
                            <div style="font-size:12px; color:#94a3b8; flex:1;">{p['name']}</div>
                            <div style="font-size:10px; color:#64748b;">{p['pos']}</div>
                        </div>
                        """, unsafe_allow_html=True)

        render_squad(c_away, away, a_starters, a_bench, "away")
        render_squad(c_home, home, h_starters, h_bench, "home")
        
        st.divider()
        progress_bar.progress((idx + 1) / len(games))

    progress_bar.empty()
# ============================================================================
# PÁGINA: DEPTO MÉDICO (V51.1 - IMPORT FIX)
# ============================================================================
def show_depto_medico():
    import streamlit as st
    import pandas as pd
    import unicodedata
    import re
    from datetime import datetime, timedelta
    
    # --- CORREÇÃO DO IMPORT ---
    # Importamos a CLASSE, não a variável. Mais seguro.
    try:
        from injuries import InjuryMonitor
        monitor = InjuryMonitor() # Instancia aqui na hora
    except ImportError:
        monitor = None
        st.error("⚠️ Módulo 'injuries.py' ou classe 'InjuryMonitor' não encontrados.")
        return

    # --- 1. CSS VISUAL ---
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Oswald:wght@400;700&family=Inter:wght@400;600&display=swap');
        
        .med-title { font-family: 'Oswald'; font-size: 24px; color: #fff; margin-bottom: 5px; }
        .med-sub { font-family: 'Inter'; font-size: 12px; color: #94a3b8; margin-bottom: 20px; }
        
        /* CARD VIP (Estrelas) */
        .vip-card {
            background: linear-gradient(145deg, #1e293b 0%, #0f172a 100%);
            border-left: 4px solid #ef4444;
            border-radius: 10px;
            padding: 12px;
            margin-bottom: 12px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.3);
            display: flex; align-items: flex-start; gap: 12px;
            transition: transform 0.2s;
        }
        .vip-card:hover { transform: translateY(-2px); }
        
        .vip-img { width: 60px; height: 60px; border-radius: 50%; border: 2px solid #ef4444; object-fit: cover; background: #000; flex-shrink: 0; }
        .vip-info { flex: 1; min-width: 0; } 
        
        .vip-name { font-family: 'Oswald'; font-size: 16px; color: #fff; line-height: 1.1; margin-bottom: 2px; }
        .vip-meta { font-family: 'Inter'; font-size: 11px; color: #94a3b8; margin-bottom: 6px; }
        
        .vip-status-row { display: flex; gap: 6px; align-items: center; margin-bottom: 6px; }
        .vip-status { font-family: 'Oswald'; font-size: 11px; font-weight: bold; padding: 2px 6px; border-radius: 4px; text-transform: uppercase; }
        .vip-source { font-family: 'Inter'; font-size: 9px; color: #64748b; background: #1e293b; padding: 2px 5px; border-radius: 3px; border: 1px solid #334155; }
        
        .vip-desc { font-family: 'Inter'; font-size: 11px; color: #cbd5e1; line-height: 1.3; font-style: italic; background: rgba(0,0,0,0.2); padding: 4px; border-radius: 4px; border-left: 2px solid #475569; }
        
        /* LISTA GERAL */
        .team-block { background: rgba(30, 41, 59, 0.3); border-radius: 8px; padding: 10px; margin-bottom: 10px; border: 1px solid #334155; }
        .team-header { font-family: 'Oswald'; color: #e2e8f0; font-size: 14px; border-bottom: 1px solid #475569; padding-bottom: 4px; margin-bottom: 8px; display: flex; justify-content: space-between; }
        
        .inj-row { display: flex; justify-content: space-between; align-items: center; font-size: 12px; padding: 5px 0; border-bottom: 1px dashed #334155; cursor: help; }
        .inj-row:hover { background: rgba(255,255,255,0.03); }
        .inj-name { color: #cbd5e1; font-weight: 500; }
        .inj-stat-out { color: #f87171; font-weight: bold; font-size: 11px; }
        .inj-stat-gtd { color: #facc15; font-weight: bold; font-size: 11px; }
    </style>
    """, unsafe_allow_html=True)

    # --- 2. LÓGICA DE AUTO-ATUALIZAÇÃO (3 HORAS) ---
    def check_and_update_injuries(monitor_instance):
        if not monitor_instance: return {}

        # Carrega dados atuais da memória (Supabase)
        data = monitor_instance.get_all_injuries()
        meta = monitor_instance.cache.get('updated_at')
        
        need_update = False
        time_diff_str = "Desconhecido"

        if not data or not meta:
            need_update = True
            time_diff_str = "Dados inexistentes"
        else:
            try:
                last_update = datetime.fromisoformat(meta)
                diff = datetime.now() - last_update
                hours = diff.total_seconds() / 3600
                time_diff_str = f"{int(hours)}h atrás"
                
                # REGRA DAS 3 HORAS
                if hours > 3.0: 
                    need_update = True
            except:
                need_update = True

        if need_update:
            with st.spinner(f"⏳ Atualizando Depto Médico com CBS & ESPN (Última: {time_diff_str})..."):
                # Busca lista de times ativos hoje para priorizar
                games = st.session_state.get('scoreboard', [])
                priority_teams = []
                if games:
                    for g in games:
                        priority_teams.append(g.get('home_abbr', g.get('home')))
                        priority_teams.append(g.get('away_abbr', g.get('away')))
                else:
                    priority_teams = ["LAL", "GSW", "BOS", "PHI", "MIL", "DEN", "PHX", "DAL"]

                monitor_instance.update_all_teams(priority_teams)
                st.toast("Depto Médico Atualizado via Satélite!", icon="📡")
                
                return monitor_instance.get_all_injuries()
        
        return data

    # --- 3. PROCESSAMENTO DE DADOS ---
    raw_teams_data = check_and_update_injuries(monitor)
    
    if not raw_teams_data:
        st.warning("⚠️ Não foi possível carregar dados de lesões.")
        return

    # Flatten data
    injuries_flat = []
    for team, players in raw_teams_data.items():
        if isinstance(players, list):
            for p in players:
                p['team'] = team
                injuries_flat.append(p)

    # --- 4. MOTOR DE FOTOS (ID RECOVERY) ---
    df_l5 = st.session_state.get('df_l5', pd.DataFrame())
    
    def normalize_name_simple(n):
        if not n: return ""
        n = str(n).lower()
        n = unicodedata.normalize("NFKD", n).encode("ascii", "ignore").decode("ascii")
        return "".join(e for e in n if e.isalnum())

    NAME_TO_ID = {}
    if not df_l5.empty:
        try:
            df_l5.columns = [str(c).upper().strip() for c in df_l5.columns]
            c_name = next((c for c in df_l5.columns if c in ['PLAYER_NAME', 'PLAYER', 'NAME']), 'PLAYER')
            c_id = next((c for c in df_l5.columns if c in ['PLAYER_ID', 'ID', 'PERSON_ID']), 'PLAYER_ID')
            c_min = next((c for c in df_l5.columns if c in ['MIN_AVG', 'MIN']), None)

            for _, row in df_l5.iterrows():
                pid = int(float(row.get(c_id, 0)))
                if pid == 0: continue
                name_norm = normalize_name_simple(str(row.get(c_name, '')))
                NAME_TO_ID[name_norm] = {'id': pid, 'min': row.get(c_min, 0) if c_min else 0}
        except: pass

    # --- 5. CLASSIFICAÇÃO (VIP vs GERAL) ---
    vip_ward = []
    general_ward = {}

    for p in injuries_flat:
        raw_name = p.get('name') or "Unknown"
        norm_name = normalize_name_simple(raw_name)
        status = str(p.get('status', '')).upper()
        details = p.get('details', 'Sem detalhes reportados.')
        source = p.get('source', 'ESPN')
        
        # Ignora disponíveis
        if "AVAILABLE" in status and "NOT" not in status: continue

        # Recupera ID
        player_stats = NAME_TO_ID.get(norm_name, {'id': 0, 'min': 0})
        pid = player_stats['id']
        minutes = float(player_stats['min'])
        
        # Define Cores
        is_out = any(x in status for x in ['OUT', 'SURG', 'INJURED'])
        status_display = "OUT" if is_out else "GTD / DÚVIDA"
        color_hex = "#ef4444" if is_out else "#facc15"
        bg_hex = "rgba(239, 68, 68, 0.15)" if is_out else "rgba(250, 204, 21, 0.15)"
        
        obj = {
            "name": raw_name,
            "team": p['team'],
            "id": pid,
            "status": status_display,
            "color": color_hex,
            "bg": bg_hex,
            "desc": details[:100] + "..." if len(details) > 100 else details,
            "source": source,
            "min": minutes
        }

        if minutes >= 25 or (pid > 0 and minutes >= 18):
            vip_ward.append(obj)
        else:
            if p['team'] not in general_ward: general_ward[p['team']] = []
            general_ward[p['team']].append(obj)

    vip_ward.sort(key=lambda x: x['min'], reverse=True)

    # --- 6. RENDERIZAÇÃO ---
    st.markdown(f'<div class="med-title">🚑 HOSPITAL HUB <span style="font-size:14px; background:#ef4444; padding:2px 6px; border-radius:4px; margin-left:10px;">LIVE</span></div>', unsafe_allow_html=True)
    
    last_ts = monitor.cache.get('updated_at', '')
    if last_ts:
        try:
            dt_ts = datetime.fromisoformat(last_ts)
            st.markdown(f'<div class="med-sub">Monitoramento Inteligente (CBS & ESPN). Atualizado em: {dt_ts.strftime("%d/%m %H:%M")}</div>', unsafe_allow_html=True)
        except: pass

    # SEÇÃO 1: UTI VIP
    if vip_ward:
        st.markdown("**🚨 IMPACTO ALTO (Titulares/Rotação)**")
        cols = st.columns(3)
        for i, p in enumerate(vip_ward):
            with cols[i % 3]:
                nba_img = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{p['id']}.png"
                fallback = "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"
                img_src = nba_img if p['id'] > 0 else fallback
                
                st.markdown(f"""
                <div class="vip-card" style="border-left-color: {p['color']}">
                    <img src="{img_src}" class="vip-img" onerror="this.src='{fallback}'">
                    <div class="vip-info">
                        <div class="vip-name">{p['name']}</div>
                        <div class="vip-meta">{p['team']} • {p['min']:.0f} MPG</div>
                        <div class="vip-status-row">
                            <span class="vip-status" style="color:{p['color']}; background:{p['bg']}; border:1px solid {p['color']}40;">{p['status']}</span>
                            <span class="vip-source">{p['source']}</span>
                        </div>
                        <div class="vip-desc" title="{p['desc']}">{p['desc'] if p['desc'] else 'Sem detalhes adicionais.'}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
    
    st.divider()

    # SEÇÃO 2: GERAL
    st.markdown("**📋 RELATÓRIO GERAL (Reservas & Banco)**")
    if not general_ward:
        st.info("Nenhuma lesão secundária reportada.")
    else:
        sorted_teams = sorted(general_ward.keys())
        row_cols = st.columns(3)
        for idx, team in enumerate(sorted_teams):
            players = general_ward[team]
            with row_cols[idx % 3]:
                st.markdown(f"""<div class="team-block"><div class="team-header"><span>{team}</span><span style="color:#94a3b8">{len(players)}</span></div>""", unsafe_allow_html=True)
                for p in players:
                    cls = "inj-stat-out" if "OUT" in p['status'] else "inj-stat-gtd"
                    detail_tooltip = p['desc'].replace('"', "'")
                    st.markdown(f"""<div class="inj-row" title="{detail_tooltip}"><span class="inj-name">{p['name']}</span><span class="{cls}">{p['status']}</span></div>""", unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)
            
# ============================================================================
# FUNÇÕES AUXILIARES E SESSION STATE (CORRIGIDA)
# ============================================================================
def process_espn_json_to_games(json_data):
    """
    Converte o JSON bruto da ESPN na lista limpa de jogos que o sistema espera.
    """
    if not json_data:
        return []
        
    events = json_data.get("events", []) if isinstance(json_data, dict) else json_data
    if not isinstance(events, list):
        return []

    processed_games = []
    
    for ev in events:
        try:
            # Pula se não for um dicionário válido
            if not isinstance(ev, dict): continue

            comp_list = ev.get("competitions", [])
            if not comp_list: continue
            
            comp = comp_list[0]
            teams_comp = comp.get("competitors", [])
            if len(teams_comp) < 2: continue
            
            # Extração segura dos times
            home_team = next((t for t in teams_comp if t.get("homeAway") == "home"), teams_comp[0])
            away_team = next((t for t in teams_comp if t.get("homeAway") == "away"), teams_comp[-1])
            
            home_abbr = home_team.get("team", {}).get("abbreviation")
            away_abbr = away_team.get("team", {}).get("abbreviation")
            
            # Extração de Odds (Gratuito)
            odds_data = comp.get("odds", [])
            espn_spread_detail = "N/A"
            espn_total = None
            
            if odds_data and isinstance(odds_data, list):
                primary_odd = odds_data[0]
                espn_spread_detail = primary_odd.get("details", "N/A")
                espn_total = primary_odd.get("overUnder")

            # Cria o objeto limpo que o DeepDeep espera
            game_obj = {
                "gameId": ev.get("id"),
                "away": away_abbr,
                "home": home_abbr,
                "status": comp.get("status", {}).get("type", {}).get("description", ""),
                "startTimeUTC": comp.get("date"),
                "score_home": home_team.get("score", "0"),
                "score_away": away_team.get("score", "0"),
                "odds_spread": espn_spread_detail,
                "odds_total": espn_total,
                "odds_source": "ESPN"
            }
            
            processed_games.append(game_obj)
            
        except Exception:
            continue
            
    return processed_games

# ============================================================================
# FUNÇÃO DE CARREGAMENTO SEGURO (VERSÃO FINAL BLINDADA)
# ============================================================================
def safe_load_initial_data():
    """
    Carrega dados e inicializa variáveis de sessão com prioridade na Nuvem (Supabase).
    Evita AttributeErrors inicializando chaves vazias antes do uso.
    """
    
    # ------------------------------------------------------------------------
    # 1. INICIALIZAÇÃO DE VARIÁVEIS (PREVINE ATTRIBUTE ERRORS)
    # ------------------------------------------------------------------------
    # Lista de chaves que PRECISAM existir no st.session_state
    keys_defaults = {
        'scoreboard': [], 
        'df_l5': pd.DataFrame(), 
        'team_advanced': {}, 
        'odds': {}, 
        'name_overrides': {}, 
        'player_ids': {},
        # Módulos (Iniciam como None)
        'injuries_manager': None,
        'pace_adjuster': None,
        'vacuum_analyzer': None,
        'dvp_analyzer': None,
        'feature_store': None,
        'audit_system': None,
        'archetype_engine': None,
        'rotation_analyzer': None,
        'thesis_engine': None
    }

    for key, default_val in keys_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_val

    # ------------------------------------------------------------------------
    # 2. DADOS DINÂMICOS (AUTO-HEALING: NUVEM -> API -> SAVE)
    # ------------------------------------------------------------------------

    # A. Scoreboard (Jogos de Hoje)
    if not st.session_state.scoreboard:
        data = get_data_universal(KEY_SCOREBOARD)
        if data:
            st.session_state.scoreboard = data
        else:
            # Falhou nuvem? Busca API e salva na nuvem
            try:
                live_data = fetch_espn_scoreboard(progress_ui=False)
                if live_data:
                    st.session_state.scoreboard = live_data
                    save_data_universal(KEY_SCOREBOARD, live_data)
            except: pass

    # B. Stats Avançados de Times
    if not st.session_state.team_advanced:
        data = get_data_universal(KEY_TEAM_ADV)
        if data:
            st.session_state.team_advanced = data
        else:
            try:
                live_data = fetch_real_time_team_stats()
                if live_data:
                    st.session_state.team_advanced = live_data
                    save_data_universal(KEY_TEAM_ADV, live_data)
            except: pass

    # C. Odds
    if not st.session_state.odds:
        data = get_data_universal(KEY_ODDS)
        if data:
            st.session_state.odds = data
        else:
            try:
                if 'fetch_odds_for_today' in globals():
                    live_data = fetch_odds_for_today()
                    if live_data:
                        st.session_state.odds = live_data
                        save_data_universal(KEY_ODDS, live_data)
            except: pass

    # D. Dados L5 (Estatísticas de Jogadores) - CORREÇÃO DE LEITURA JSON
    if st.session_state.df_l5.empty:
        # 1. Tenta Nuvem (Formato JSON Records)
        cloud_l5 = get_data_universal(KEY_L5)
        if cloud_l5 and "records" in cloud_l5:
            try:
                st.session_state.df_l5 = pd.DataFrame.from_records(cloud_l5["records"])
            except Exception as e:
                print(f"⚠️ Erro convertendo L5 da nuvem: {e}")
        
        # 2. Fallback: Tenta Pickle Local (Se nuvem falhar)
        if st.session_state.df_l5.empty and os.path.exists(L5_CACHE_FILE):
            try:
                saved = load_pickle(L5_CACHE_FILE)
                if saved and isinstance(saved, dict) and "df" in saved:
                    st.session_state.df_l5 = saved["df"]
            except: pass

    # ------------------------------------------------------------------------
    # 3. DADOS ESTÁTICOS (MIGRAÇÃO AUTOMÁTICA LOCAL -> NUVEM)
    # ------------------------------------------------------------------------
    # Se não existe na nuvem mas existe local, sobe automaticamente.
    static_files_map = {
        KEY_PLAYERS_MAP: "cache/nba_players_map.json",
        KEY_NAME_OVERRIDES: "cache/name_overrides.json",
        KEY_DVP: "cache/dvp_data_v4_static.json"
    }

    for key_db, local_path in static_files_map.items():
        if not get_data_universal(key_db): 
            if os.path.exists(local_path):
                try:
                    with open(local_path, "r", encoding="utf-8") as f:
                        local_data = json.load(f)
                    if local_data:
                        print(f"🚀 Migrando '{key_db}' do Local para Supabase...")
                        save_data_universal(key_db, local_data)
                except: pass

    # ------------------------------------------------------------------------
    # 4. INICIALIZAÇÃO DE MÓDULOS (INSTANCIAÇÃO SEGURA)
    # ------------------------------------------------------------------------
    
    # Pace Adjuster
    if st.session_state.pace_adjuster is None and PACE_ADJUSTER_AVAILABLE:
        st.session_state.pace_adjuster = PaceAdjuster()
    
    # Vacuum Matrix
    if st.session_state.vacuum_analyzer is None and VACUUM_MATRIX_AVAILABLE:
        st.session_state.vacuum_analyzer = VacuumMatrixAnalyzer()

    # Injury Monitor
    if st.session_state.injuries_manager is None and INJURY_MONITOR_AVAILABLE:
        try:
            # Instancia a classe InjuryMonitor e guarda na variável injuries_manager
            st.session_state.injuries_manager = InjuryMonitor()
        except Exception as e:
            print(f"Erro ao iniciar InjuryMonitor: {e}")

    # Dvp Analyzer
    if st.session_state.dvp_analyzer is None and DVP_ANALYZER_AVAILABLE:
        st.session_state.dvp_analyzer = DvpAnalyzer()

    # Audit System
    if st.session_state.audit_system is None:
        try:
            if AuditSystem: st.session_state.audit_system = AuditSystem()
        except: pass

    # Feature Store
    if st.session_state.feature_store is None:
        try: 
            if 'FeatureStore' in globals():
                st.session_state.feature_store = FeatureStore()
        except: pass


def load_all_data():
    """
    Função chamada pelo botão de 'Update' manual.
    Busca dados novos e SINCRONIZA COM O SUPABASE IMEDIATAMENTE.
    """
    try:
        # 1. Scoreboard
        with st.spinner("📥 Buscando scoreboard..."):
            new_scoreboard = fetch_espn_scoreboard(progress_ui=True) or []
            st.session_state.scoreboard = new_scoreboard
            # FORÇA O SALVAMENTO NA NUVEM
            save_data_universal("scoreboard", new_scoreboard, SCOREBOARD_JSON_FILE)

        # 2. Dados L5
        with st.spinner("📊 Buscando dados L5..."):
            new_l5 = get_players_l5(progress_ui=True)
            if new_l5 is not None and not new_l5.empty:
                st.session_state.df_l5 = new_l5
                # O L5 geralmente é Pickle, mas vamos salvar o JSON dos logs brutos se possível
                # Se get_players_l5 retornar o DF, assumimos que o 'real_game_logs.json' foi atualizado internamente.
                # Para garantir, precisaríamos acessar o dict de logs brutos, mas vamos focar no que temos acesso.
                
                # Se você tiver a variável com o dict de logs brutos aqui, salve-a:
                # save_data_universal("real_game_logs", logs_dict, LOGS_CACHE_FILE) 
            else:
                st.session_state.df_l5 = pd.DataFrame()

        # 3. Odds
        with st.spinner("🎰 Buscando odds..."):
            new_odds = fetch_odds_for_today() or {}
            st.session_state.odds = new_odds
            save_data_universal("pinnacle_odds", new_odds, ODDS_CACHE_FILE)

        # 4. Dados de Time
        with st.spinner("🏀 Buscando estatísticas avançadas..."):
            adv = fetch_team_advanced_stats() or {}
            opp = fetch_team_opponent_stats() or {}
            st.session_state.team_advanced = adv
            st.session_state.team_opponent = opp
            
            save_data_universal("team_advanced", adv, TEAM_ADVANCED_FILE)
            save_data_universal("team_opponent", opp, TEAM_OPPONENT_FILE)

        # 5. Re-Inicializar Sistemas com os novos dados
        if "dvp_analyzer" not in st.session_state and DVP_ANALYZER_AVAILABLE:
            st.session_state.dvp_analyzer = DvpAnalyzer()
            
        st.success("✅ Dados atualizados e SINCRONIZADOS NA NUVEM!")
        return True

    except Exception as e:
        st.error(f"❌ Erro ao carregar dados: {e}")
        print(f"Erro detalhado: {e}")
        return False

# --- FONTS & CSS GLOBAL (ATUALIZADO v5.0) ---
FONT_LINKS = """
<link href="https://fonts.googleapis.com/css2?family=Oswald:wght@400;600;700&display=swap" rel="stylesheet">
<link href="https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;800&display=swap" rel="stylesheet">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap" rel="stylesheet">
"""

CUSTOM_CSS = """
<style>
    /* 1. FORÇAR FUNDO PRETO GLOBAL (FIM DO FUNDO BRANCO) */
    .stApp {
        background-color: #000000 !important;
    }
    
    /* 2. REMOVER PADDING DO TOPO (PRINCIPAL E SIDEBAR) */
    header[data-testid="stHeader"] { visibility: hidden; height: 0px; }
    
    .block-container {
        padding-top: 0rem !important;
        padding-bottom: 2rem !important;
        margin-top: -60px !important;
    }
    
    /* Padding da Sidebar - Ataque direto */
    section[data-testid="stSidebar"] .block-container {
        padding-top: 0rem !important;
        margin-top: -40px !important;
    }

    /* 3. MENU LATERAL - FONTE ARREDONDADA (NUNITO) & BOOST VISUAL */
    div[role="radiogroup"] label {
        font-family: 'Nunito', sans-serif !important; /* Fonte Arredondada */
        font-size: 1rem !important; /* Maior (Boost) */
        font-weight: 600 !important;
        padding: 8px 12px !important;
        color: #94a3b8 !important;
        border-radius: 8px !important;
        transition: all 0.3s ease;
    }
    
    /* Hover no Menu */
    div[role="radiogroup"] label:hover {
        background: rgba(34, 211, 238, 0.1) !important;
        color: #ffffff !important;
        padding-left: 18px !important;
    }

    /* Item Selecionado no Menu */
    div[role="radiogroup"] label[data-checked="true"] {
        background: linear-gradient(90deg, rgba(34, 211, 238, 0.2) 0%, transparent 100%) !important;
        border-left: 4px solid #22d3ee !important;
        color: #22d3ee !important;
        font-weight: 800 !important; /* Extra Bold */
        text-shadow: 0 0 10px rgba(34, 211, 238, 0.4);
    }

    /* 4. DASHBOARD - PADRONIZAÇÃO OSWALD */
    /* Força Oswald em headers e textos chave */
    h1, h2, h3, .stMarkdown div, .stMarkdown p {
        font-family: 'Oswald', sans-serif !important;
    }
    
    /* Exceção: Textos muito pequenos podem usar Inter para leitura */
    .small-text { font-family: 'Inter', sans-serif !important; }
</style>
"""
# ============================================================================
# PÁGINA: LAB DE NARRATIVAS (V5.3 - CLOUD PERSISTENCE)
# ============================================================================
def show_narrative_lab():
    import time
    import datetime
    
    # --- CSS TÁTICO & HERO ---
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Oswald:wght@400;700&display=swap');
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&display=swap');
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');

        .wr-header { font-family: 'Oswald', sans-serif; font-size: 32px; color: #fff; letter-spacing: 2px; text-transform: uppercase; }
        .wr-sub { font-family: monospace; font-size: 12px; color: #94a3b8; margin-bottom: 10px; }
        
        .wr-table { width: 100%; border-collapse: collapse; }
        .wr-table td { padding: 8px 4px; vertical-align: middle; border-bottom: 1px dashed #334155; }
        .wr-table tr:last-child td { border-bottom: none; }
        .wr-name { font-family: 'Oswald', sans-serif; font-size: 15px; color: #fff; font-weight: bold; line-height: 1.1; }
        .wr-stat { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: #64748b; }
        
        .wr-val-killer { font-family: 'Oswald', sans-serif; font-size: 18px; color: #F87171; font-weight: bold; text-align: right; }
        .wr-val-cold { font-family: 'Oswald', sans-serif; font-size: 18px; color: #00E5FF; font-weight: bold; text-align: right; }
        
        .wr-tag { font-size: 10px; padding: 2px 6px; border-radius: 4px; font-weight: bold; display: inline-block; margin-top: 4px; }
        .tag-killer { background: rgba(248, 113, 113, 0.15); color: #F87171; border: 1px solid rgba(248, 113, 113, 0.3); }
        .tag-cold { background: rgba(6, 182, 212, 0.15); color: #00E5FF; border: 1px solid rgba(6, 182, 212, 0.3); }
        
        .wr-img { width: 45px; height: 45px; border-radius: 50%; object-fit: cover; border: 2px solid #334155; display: block; }
        .wr-game-title { font-family: 'Oswald', sans-serif; font-size: 18px; color: #E2E8F0; margin-bottom: 10px; border-bottom: 1px solid #334155; padding-bottom: 5px; }
    </style>
    """, unsafe_allow_html=True)

    # 1. SETUP & ENGINE CHECK
    try:
        if "narrative_engine" not in st.session_state:
            try:
                from modules.new_modules.narrative_intelligence import NarrativeIntelligence
                st.session_state.narrative_engine = NarrativeIntelligence()
            except:
                st.warning("⚠️ Módulo de Inteligência Narrativa não encontrado.")
                return
        engine = st.session_state.narrative_engine
    except:
        st.error("⚠️ Erro ao carregar Engine Narrativa.")
        return

    # Dados Básicos
    games = st.session_state.get("scoreboard", [])
    df_l5 = st.session_state.get('df_l5', pd.DataFrame())

    # HEADER
    st.markdown('<div class="wr-header">⚔️ LAB NARRATIVAS</div>', unsafe_allow_html=True)
    st.markdown('<div class="wr-sub">ANOMALIAS ESTATÍSTICAS HISTÓRICAS (H2H)</div>', unsafe_allow_html=True)

    # HERO SECTION
    st.markdown("""
    <div style="
        background: linear-gradient(90deg, rgba(30,41,59,0.6) 0%, rgba(15,23,42,0.6) 100%);
        border-left: 4px solid #F87171;
        border-radius: 8px;
        padding: 15px 20px;
        margin-bottom: 25px;
        border: 1px solid #334155;
    ">
        <div style="font-family: 'Inter', sans-serif; color: #e2e8f0; font-size: 14px; line-height: 1.6;">
            <strong style="color: #F87171; font-size: 15px;">O FATOR PSICOLÓGICO.</strong><br>
            Alguns jogadores elevam seu nível contra certos oponentes (ex: ex-times, rivais). Esta ferramenta analisa o histórico direto (Head-to-Head):
            <ul style="margin-top: 8px; margin-bottom: 0; padding-left: 20px; list-style-type: none;">
                <li style="margin-bottom: 6px;">
                    🔥 <strong style="color: #F87171;">Carrasco (Killer):</strong> Jogador que historicamente supera sua média de pontos em <strong>+15%</strong> contra este adversário.
                </li>
                <li>
                    ❄️ <strong style="color: #00E5FF;">Trauma (Cold):</strong> Jogador que historicamente joga <strong>-15%</strong> abaixo da sua média contra esta defesa.
                </li>
            </ul>
        </div>
    </div>
    """, unsafe_allow_html=True)

    if not games or df_l5.empty:
        st.info("ℹ️ Aguardando dados de jogos e estatísticas...")
        return

    # --- NORMALIZAÇÃO DE DADOS ---
    df_l5.columns = [str(c).upper().strip() for c in df_l5.columns]
    
    col_team = None
    for c in ['TEAM', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'ABBREVIATION']:
        if c in df_l5.columns: col_team = c; break
            
    if not col_team: return

    TEAM_MAP_FIX = {
        'GS': 'GSW', 'GOLDEN STATE': 'GSW', 'NO': 'NOP', 'NEW ORLEANS': 'NOP',
        'NY': 'NYK', 'NEW YORK': 'NYK', 'SA': 'SAS', 'SAN ANTONIO': 'SAS',
        'PHO': 'PHX', 'PHOENIX': 'PHX', 'UTAH': 'UTA', 'UTA': 'UTA',
        'WSH': 'WAS', 'WASHINGTON': 'WAS', 'BKN': 'BKN', 'BROOKLYN': 'BKN'
    }
    def normalize_t(x):
        x = str(x).upper().strip()
        return TEAM_MAP_FIX.get(x, x)

    df_l5['TEAM_NORMALIZED'] = df_l5[col_team].apply(normalize_t)
    
    if 'PTS_AVG' not in df_l5.columns:
        df_l5['PTS_AVG'] = df_l5['PTS'] if 'PTS' in df_l5.columns else 0.0
    df_l5['PTS_AVG'] = pd.to_numeric(df_l5['PTS_AVG'], errors='coerce').fillna(0)

    # 2. SCAN INTELIGENTE (HIERARQUIA: SESSÃO -> NUVEM -> CÁLCULO)
    today_str = datetime.datetime.now().strftime("%Y%m%d")
    cache_key = f"wr_scan_{today_str}_{len(games)}"
    
    scan_results = None
    
    # A. Verifica Sessão (Memória RAM)
    if "narrative_cache" in st.session_state and cache_key in st.session_state.narrative_cache:
        scan_results = st.session_state.narrative_cache[cache_key]
    
    # B. Verifica Nuvem (Supabase) - Se não achou na Sessão
    if scan_results is None:
        try:
            cloud_cache = get_data_universal("narrative_cache") or {}
            if cache_key in cloud_cache:
                scan_results = cloud_cache[cache_key]
                # Atualiza Sessão para ficar rápido nos próximos cliques
                if "narrative_cache" not in st.session_state: st.session_state.narrative_cache = {}
                st.session_state.narrative_cache[cache_key] = scan_results
        except: pass

    # C. Se não achou em lugar nenhum, Calcula (e Salva na Nuvem)
    if scan_results is None:
        loading_ph = st.empty()
        with loading_ph.container():
            st.info(f"📡 Calculando anomalias H2H para {len(games)} jogos... (Isso é feito uma vez por dia)")
            prog = st.progress(0)
            scan_results = []
            
            for i, game in enumerate(games):
                try:
                    away_raw = normalize_t(game.get('away', 'UNK'))
                    home_raw = normalize_t(game.get('home', 'UNK'))
                    if away_raw == 'UNK' or home_raw == 'UNK': continue
                    
                    r_away = df_l5[df_l5['TEAM_NORMALIZED'] == away_raw].sort_values('PTS_AVG', ascending=False).head(10)
                    r_home = df_l5[df_l5['TEAM_NORMALIZED'] == home_raw].sort_values('PTS_AVG', ascending=False).head(10)

                    def analyze_player(row, opp_team, my_team):
                        try:
                            c_id = next((c for c in df_l5.columns if c in ['PLAYER_ID', 'ID', 'PERSON_ID']), None)
                            c_name = next((c for c in df_l5.columns if c in ['PLAYER', 'PLAYER_NAME', 'NAME']), 'PLAYER')
                            pid = int(float(row.get(c_id, 0))) if c_id else 0
                            pname = row.get(c_name, 'Unknown')
                            avg_pts = float(row.get('PTS_AVG', 0))
                            
                            if avg_pts < 8: return

                            data = engine.get_player_matchup_history(pid, pname, opp_team)
                            if data and 'comparison' in data:
                                diff = data['comparison'].get('diff_pct', 0)
                                n_type = "NEUTRAL"
                                if diff >= 15: n_type = "KILLER"
                                elif diff <= -15: n_type = "COLD"
                                
                                if n_type != "NEUTRAL":
                                    scan_results.append({
                                        "game_id": f"{away_raw} @ {home_raw}",
                                        "player": pname,
                                        "team": my_team,
                                        "opponent": opp_team,
                                        "diff": diff,
                                        "avg": avg_pts,
                                        "type": n_type,
                                        "pid": pid,
                                        "badge": data.get('badge', 'H2H')
                                    })
                        except: pass

                    for _, p in r_away.iterrows(): analyze_player(p, home_raw, away_raw)
                    for _, p in r_home.iterrows(): analyze_player(p, away_raw, home_raw)
                    
                    prog.progress((i+1)/len(games))
                except: continue
            
            # --- SALVAMENTO ---
            # 1. Salva na Sessão Local
            if "narrative_cache" not in st.session_state: st.session_state.narrative_cache = {}
            st.session_state.narrative_cache[cache_key] = scan_results
            
            # 2. Salva na Nuvem (Supabase)
            try:
                # Baixa cache atual da nuvem para não apagar outros dias (opcional, ou sobrescreve)
                current_cloud = get_data_universal("narrative_cache") or {}
                # Limpa chaves muito antigas para economizar espaço (opcional)
                if len(current_cloud) > 5: current_cloud = {} 
                
                current_cloud[cache_key] = scan_results
                save_data_universal("narrative_cache", current_cloud)
                # st.toast("Análise salva na nuvem!", icon="☁️")
            except: pass
            
        loading_ph.empty()

    # 3. EXIBIÇÃO DE RESULTADOS
    if not scan_results:
        st.success("✅ Nenhuma anomalia estatística crítica detectada para os jogos de hoje.")
        return

    # UI: TOP THREATS
    killers = [x for x in scan_results if x['type'] == "KILLER"]
    top_threats = sorted(killers, key=lambda x: x['diff'], reverse=True)[:3]

    if top_threats:
        st.markdown("<div style='margin-bottom:10px; font-family:monospace; color:#F87171; font-weight:bold;'>🚨 ALVOS DE ALTO VALOR (TOP 3)</div>", unsafe_allow_html=True)
        cols = st.columns(3)
        for idx, t in enumerate(top_threats):
            with cols[idx]:
                with st.container(border=True):
                    c_img, c_info = st.columns([1, 2])
                    with c_img:
                        st.markdown(f"<img src='https://cdn.nba.com/headshots/nba/latest/1040x760/{t['pid']}.png' class='wr-img' style='border-color:#F87171' onerror=\"this.src='https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png'\">", unsafe_allow_html=True)
                    with c_info:
                        st.markdown(f"""
                        <div style='line-height:1.1'>
                            <div style='font-family:Oswald; font-size:14px; color:#fff; white-space:nowrap; overflow:hidden;'>{t['player']}</div>
                            <div style='font-size:10px; color:#94a3b8;'>vs {t['opponent']}</div>
                            <div style='color:#F87171; font-weight:bold; font-size:20px; font-family:Oswald;'>+{t['diff']:.0f}%</div>
                        </div>
                        """, unsafe_allow_html=True)

    # UI: RELATÓRIOS
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<div style='margin-bottom:10px; font-family:monospace; color:#94a3b8; font-weight:bold;'>📂 RELATÓRIOS DE JOGO</div>", unsafe_allow_html=True)

    games_dict = {}
    for item in scan_results:
        gid = item['game_id']
        if gid not in games_dict: games_dict[gid] = {"killers": [], "cold": []}
        target_list = games_dict[gid]["killers"] if item['type'] == "KILLER" else games_dict[gid]["cold"]
        if not any(existing['pid'] == item['pid'] for existing in target_list):
            target_list.append(item)

    for gid, rosters in games_dict.items():
        n_kill = len(rosters['killers'])
        n_cold = len(rosters['cold'])
        if n_kill == 0 and n_cold == 0: continue

        with st.container(border=True):
            st.markdown(f"""
            <div class="wr-game-title">
                {gid} <span style="font-size:12px; color:#64748b; margin-left:10px;">(🚨 {n_kill} Killers | ❄️ {n_cold} Cold)</span>
            </div>
            """, unsafe_allow_html=True)
            
            c_kill, c_cold = st.columns(2)
            with c_kill:
                st.markdown("<div style='color:#F87171; font-family:Oswald; font-size:14px; margin-bottom:10px;'>🔥 CARRASCOS</div>", unsafe_allow_html=True)
                if not rosters['killers']: 
                    st.markdown("<div style='font-size:12px; color:#475569;'>Nada relevante.</div>", unsafe_allow_html=True)
                else:
                    rows = ""
                    for p in rosters['killers']:
                        rows += f"""<tr><td style="width:50px;"><img src="https://cdn.nba.com/headshots/nba/latest/1040x760/{p['pid']}.png" class="wr-img" style="border-color:#F87171" onerror="this.src='https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png'"></td><td><div class="wr-name">{p['player']}</div><div class="wr-stat">Méd: {p['avg']:.1f}</div><span class="wr-tag tag-killer">HISTÓRICO</span></td><td><div class="wr-val-killer">+{p['diff']:.0f}%</div></td></tr>"""
                    st.markdown(f"<table class='wr-table'>{rows}</table>", unsafe_allow_html=True)

            with c_cold:
                st.markdown("<div style='color:#00E5FF; font-family:Oswald; font-size:14px; margin-bottom:10px;'>❄️ GELADOS</div>", unsafe_allow_html=True)
                if not rosters['cold']: 
                    st.markdown("<div style='font-size:12px; color:#475569;'>Nada relevante.</div>", unsafe_allow_html=True)
                else:
                    rows = ""
                    for p in rosters['cold']:
                        rows += f"""<tr><td style="width:50px;"><img src="https://cdn.nba.com/headshots/nba/latest/1040x760/{p['pid']}.png" class="wr-img" style="border-color:#00E5FF" onerror="this.src='https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png'"></td><td><div class="wr-name">{p['player']}</div><div class="wr-stat">Méd: {p['avg']:.1f}</div><span class="wr-tag tag-cold">TRAUMA</span></td><td><div class="wr-val-cold">{p['diff']:.0f}%</div></td></tr>"""
                    st.markdown(f"<table class='wr-table'>{rows}</table>", unsafe_allow_html=True)
# ============================================================================
# PÁGINA: DASHBOARD (V10.5 - AUTO-SYNC COM VALIDAÇÃO DE DATA)
# ============================================================================
def show_dashboard_page():
    import datetime
    from datetime import timedelta

    # --- CSS Helper ---
    st.markdown("""
    <style>
        .dash-title { font-family: 'Oswald'; font-size: 20px; color: #E2E8F0; margin-bottom: 10px; letter-spacing: 1px; text-transform: uppercase; }
        .gold-text { color: #D4AF37; }
    </style>
    """, unsafe_allow_html=True)

    # --- HELPER DE DATA NBA ---
    def get_nba_today_str():
        # Define "Hoje" no fuso da NBA (UTC-5)
        # Se for 1h da manhã no Brasil, ainda pode ser o dia anterior na NBA.
        et_now = datetime.datetime.utcnow() - timedelta(hours=5)
        return et_now.strftime("%Y%m%d")

    # ========================================================================
    # 1. AUTO-LOADER INTELIGENTE (COM VALIDAÇÃO DE VALIDADE)
    # ========================================================================
    
    # Flag para controlar se precisamos baixar dados novos
    need_refresh = False
    
    # 1. Tenta carregar da Nuvem primeiro
    cloud_games = get_data_universal("scoreboard")
    today_str = get_nba_today_str()

    if cloud_games and isinstance(cloud_games, list) and len(cloud_games) > 0:
        # --- AQUI ESTÁ A CORREÇÃO CRÍTICA ---
        # Verifica a data do primeiro jogo salvo no Supabase
        db_date = cloud_games[0].get('date_str', '00000000')
        
        if db_date == today_str:
            # O banco está atualizado (é de hoje)! Pode usar.
            st.session_state['scoreboard'] = cloud_games
            # Tenta carregar odds da nuvem também
            st.session_state['odds'] = get_data_universal("odds") or {}
        else:
            # O banco tem dados, MAS são velhos. Forçar atualização.
            # st.toast(f"Dados antigos detectados ({db_date}). Atualizando para {today_str}...", icon="🔄")
            need_refresh = True
    else:
        # Banco vazio. Precisa atualizar.
        need_refresh = True

    # 2. Se precisar atualizar (Banco vazio ou Dados Velhos), vai na API
    if need_refresh:
        try:
            with st.spinner("🔄 Atualizando Scoreboard e Odds do dia..."):
                # Baixa Scoreboard Novo
                games_data = fetch_espn_scoreboard(False)
                
                if games_data:
                    st.session_state['scoreboard'] = games_data
                    save_data_universal("scoreboard", games_data) # Atualiza o Supabase com a data nova!
                    
                    # Baixa Odds Novas
                    try:
                        odds_data = fetch_odds_for_today() # Função consumidora refatorada
                        if odds_data:
                            st.session_state['odds'] = odds_data
                            save_data_universal("odds", odds_data)
                    except: 
                        st.session_state['odds'] = {}
                else:
                    st.session_state['scoreboard'] = []
        except Exception as e:
            st.error(f"Erro na sincronização automática: {e}")

    # Carrega dados para variáveis locais
    games_list = st.session_state.get('scoreboard', [])
    odds_cache = st.session_state.get('odds', {})
    
    # Converte lista para DataFrame
    games = pd.DataFrame(games_list) if games_list else pd.DataFrame()

    # 2. Carrega Stats de Jogadores (L5)
    df_l5 = st.session_state.get('df_l5', pd.DataFrame())
    
    # Fallback: Tenta carregar L5 da nuvem se a sessão estiver vazia
    if df_l5 is None or df_l5.empty:
        cloud_l5 = get_data_universal("df_l5")
        if cloud_l5: 
             try: df_l5 = pd.DataFrame(cloud_l5)
             except: pass

    if df_l5 is None or df_l5.empty:
        st.warning("⚠️ Base de dados L5 vazia. Vá em Config > Ingestão para carregar os stats dos jogadores.")
    
    # ========================================================================
    # 🛠️ FIX 1: NORMALIZAÇÃO DE COLUNAS E TIMES
    # ========================================================================
    if not df_l5.empty:
        df_l5.columns = [str(c).upper().strip() for c in df_l5.columns]

        # Detetive de Colunas
        col_team_found = None
        possible_cols = ['TEAM_ABBREVIATION', 'TEAM_CODE', 'TEAM', 'ABBREVIATION']
        for c in possible_cols:
            if c in df_l5.columns:
                col_team_found = c; break
                
        if col_team_found:
            df_l5['TEAM'] = df_l5[col_team_found]
        else:
            if 'MATCHUP' in df_l5.columns:
                df_l5['TEAM'] = df_l5['MATCHUP'].astype(str).str.split().str[0]
            else:
                df_l5['TEAM'] = 'UNK'

        # Padronização de Siglas (Usa a função global do Nexus se existir)
        try:
            formatter = normalize_team_signature if 'normalize_team_signature' in globals() else lambda x: str(x).upper()
            df_l5['TEAM'] = df_l5['TEAM'].apply(formatter)
        except: pass

        # Filtro: Quem joga hoje
        teams_playing_today = set()
        if not games.empty:
            if 'home' in games.columns and 'away' in games.columns:
                try:
                    formatter = normalize_team_signature if 'normalize_team_signature' in globals() else lambda x: str(x).upper()
                    raw_teams = games['home'].tolist() + games['away'].tolist()
                    teams_playing_today = set([formatter(x) for x in raw_teams])
                except: pass
        
        if not teams_playing_today:
            df_today = pd.DataFrame()
        else:
            df_today = df_l5[df_l5['TEAM'].isin(teams_playing_today)].copy()
    else:
        df_today = pd.DataFrame()

    # ========================================================================
    # 3. RENDERIZAÇÃO: DESTAQUES DO DIA (GOLDEN CARDS)
    # ========================================================================
    st.markdown('<div class="dash-title gold-text">⭐ DESTAQUES DO DIA (JOGOS DE HOJE)</div>', unsafe_allow_html=True)
    
    def truncate_name(name, limit=16):
        if not name: return ""
        name = str(name)
        if len(name) <= limit: return name
        parts = name.split()
        if len(parts) > 1: return f"{parts[0][0]}. {' '.join(parts[1:])}"[:limit]
        return name[:limit]

    if not df_today.empty:
        def get_top_n(df, col, n=3):
            if col not in df.columns: return pd.DataFrame()
            c_name = next((c for c in df.columns if c in ['PLAYER_NAME', 'PLAYER', 'NAME']), 'PLAYER')
            cols_to_fetch = [c_name, 'TEAM', col]
            if 'PLAYER_ID' in df.columns: cols_to_fetch.append('PLAYER_ID')
            elif 'ID' in df.columns: cols_to_fetch.append('ID')
            elif 'PERSON_ID' in df.columns: cols_to_fetch.append('PERSON_ID')
            final_cols = [c for c in cols_to_fetch if c in df.columns]
            return df.nlargest(n, col)[final_cols]

        for col in ['PTS', 'AST', 'REB']:
            target = f"{col}_AVG"
            if target not in df_today.columns:
                if col in df_today.columns: df_today[target] = df_today[col]
                else: df_today[target] = 0

        top_pts = get_top_n(df_today, 'PTS_AVG')
        top_ast = get_top_n(df_today, 'AST_AVG')
        top_reb = get_top_n(df_today, 'REB_AVG')

        def render_golden_card(title, df_top, color="#D4AF37", icon="👑"):
            if df_top.empty: return
            king = df_top.iloc[0]
            
            c_id = next((c for c in df_top.columns if c in ['PLAYER_ID', 'ID', 'PERSON_ID']), None)
            p_id = king[c_id] if c_id else 0
            try: p_id = int(float(p_id))
            except: p_id = 0
            
            photo = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{p_id}.png"
            c_name = next((c for c in df_top.columns if c in ['PLAYER_NAME', 'PLAYER', 'NAME']), 'PLAYER')
            p_name = king.get(c_name, 'UNK')
            
            stat_val = 0
            for c in df_top.columns:
                if "AVG" in c or c in ['PTS', 'AST', 'REB']: stat_val = king[c]; break
            
            def get_sub_row(idx, rank):
                if len(df_top) <= idx: return ""
                p = df_top.iloc[idx]
                nm = p.get(c_name, 'UNK')
                v = 0
                for c in df_top.columns:
                    if "AVG" in c or c in ['PTS', 'AST', 'REB']: v = p[c]; break
                return f"""<div style="display:flex; justify-content:space-between; font-size:11px; color:#cbd5e1; margin-bottom:3px; border-bottom:1px dashed #334155; font-family:'Oswald' !important;"><span>{rank}. {truncate_name(nm)}</span><span style="color:{color}">{v:.1f}</span></div>"""

            row2 = get_sub_row(1, 2)
            row3 = get_sub_row(2, 3)

            st.markdown(f"""
            <div style="background: #0f172a; border: 1px solid {color}; border-radius: 12px; overflow: hidden; height: 100%; box-shadow: 0 4px 15px rgba(0,0,0,0.5);">
                <div style="background: {color}20; padding: 6px; text-align: center; font-family: 'Oswald'; color: {color}; font-size: 13px; letter-spacing: 1px; border-bottom: 1px solid {color}40;">
                    {icon} {title}
                </div>
                <div style="padding: 12px; display: flex; align-items: center;">
                    <img src="{photo}" style="width: 55px; height: 55px; border-radius: 50%; border: 2px solid {color}; object-fit: cover; background: #000; margin-right: 12px;" onerror="this.src='https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png'">
                    <div style="overflow: hidden;">
                        <div style="color: #fff; font-weight: bold; font-size: 14px; line-height: 1.1; font-family: 'Oswald' !important; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">{truncate_name(p_name)}</div>
                        <div style="color: #94a3b8; font-size: 10px; font-family: 'Oswald' !important;">{king.get('TEAM', 'UNK')}</div>
                        <div style="color: {color}; font-size: 20px; font-family: 'Oswald' !important; font-weight: bold;">{stat_val:.1f}</div>
                    </div>
                </div>
                <div style="background: rgba(0,0,0,0.4); padding: 8px 12px;">
                    {row2} {row3}
                </div>
            </div>
            """, unsafe_allow_html=True)

        c1, c2, c3 = st.columns(3)
        with c1: render_golden_card("CESTINHAS", top_pts, "#FFD700", "🔥")
        with c2: render_golden_card("GARÇONS", top_ast, "#00E5FF", "🧠")
        with c3: render_golden_card("REBOTEIROS", top_reb, "#FF4F4F", "💪")
    
    st.markdown("<br>", unsafe_allow_html=True)

    # ========================================================================
    # 4. GAME GRID (JOGOS DE HOJE + ODDS) - AQUI ESTÃO OS CARDS!
    # ========================================================================
    st.markdown('<div class="dash-title" style="color:#E2E8F0;">🏀 JOGOS DE HOJE</div>', unsafe_allow_html=True)

    if games.empty:
        st.info("Nenhum jogo identificado na nuvem ou API.")
        if st.button("🔄 Forçar Atualização"):
            try: st.session_state['scoreboard'] = fetch_espn_scoreboard(False); st.rerun()
            except: pass
    else:
        # Usa o cache de odds (que foi carregado no início da função)
        safe_odds = odds_cache if isinstance(odds_cache, dict) else {}
        
        rows = st.columns(2)
        for i, (index, game) in enumerate(games.iterrows()):
            with rows[i % 2]:
                # Chama a função de renderização (assumindo que ela existe globalmente)
                render_game_card(
                    away_team=game.get('away', 'UNK'),
                    home_team=game.get('home', 'UNK'),
                    game_data=game,
                    odds_map=safe_odds 
                )
# ============================================================================
# EXECUÇÃO PRINCIPAL (V66.0 - FINAL STABLE)
# ============================================================================
def main():
    # CONFIGURAÇÃO DA PÁGINA
    # 'initial_sidebar_state="expanded"' força o menu a iniciar aberto
    st.set_page_config(
        page_title="DigiBets IA", 
        layout="wide", 
        page_icon="🏀",
        initial_sidebar_state="expanded"
    )
    
    # CSS GLOBAL CRÍTICO (DARK MODE PRO)
    st.markdown("""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;800&family=Oswald:wght@400;600&display=swap');

            /* 1. FORÇA FUNDO PRETO GLOBAL */
            .stApp { background-color: #000000 !important; }
            
            /* 2. REMOVE CABEÇALHO PADRÃO */
            header[data-testid="stHeader"] { visibility: hidden; height: 0px; }

            /* 3. MARGEM NEGATIVA NO TOPO (PUXA TUDO PRA CIMA) */
            .block-container {
                padding-top: 0rem !important;
                padding-bottom: 2rem !important;
                margin-top: -60px !important;
            }

            /* 4. AJUSTE SIDEBAR */
            section[data-testid="stSidebar"] .block-container {
                padding-top: 2rem !important; 
                margin-top: -20px !important;
            }
            section[data-testid="stSidebar"] {
                background-color: #000000 !important;
                border-right: 1px solid #1e293b;
            }

            /* 5. ESTILO MENU LATERAL (NUNITO) */
            div[role="radiogroup"] label {
                background: transparent !important;
                border: none !important;
                padding: 8px 12px !important;
                margin-bottom: 2px !important;
                font-family: 'Nunito', sans-serif !important;
                font-size: 0.95rem !important;
                color: #cbd5e1 !important;
                border-radius: 8px !important;
                transition: all 0.2s ease;
            }

            div[role="radiogroup"] label:hover {
                background: rgba(34, 211, 238, 0.1) !important;
                color: #ffffff !important;
                padding-left: 18px !important;
            }

            div[role="radiogroup"] label[data-checked="true"] {
                background: linear-gradient(90deg, rgba(34, 211, 238, 0.15) 0%, transparent 100%) !important;
                border-left: 4px solid #22d3ee !important;
                color: #22d3ee !important;
                font-weight: 700 !important;
            }

            div[role="radiogroup"] > label > div:first-child { display: none !important; }

            /* 6. SEPARADORES DE MENU (CSS INDEXING) */
            div[role="radiogroup"] > label:nth-of-type(4) { margin-top: 25px !important; }
            div[role="radiogroup"] > label:nth-of-type(4)::before { content: "INTELIGÊNCIA ARTIFICIAL"; display: block; font-size: 0.65rem; color: #64748b; font-weight: 700; margin-bottom: 5px; }

            div[role="radiogroup"] > label:nth-of-type(10) { margin-top: 25px !important; }
            div[role="radiogroup"] > label:nth-of-type(10)::before { content: "CAÇADORES & ESTRATÉGIA"; display: block; font-size: 0.65rem; color: #64748b; font-weight: 700; margin-bottom: 5px; }

            div[role="radiogroup"] > label:nth-of-type(13) { margin-top: 25px !important; }
            div[role="radiogroup"] > label:nth-of-type(13)::before { content: "ANÁLISE TÁTICA"; display: block; font-size: 0.65rem; color: #64748b; font-weight: 700; margin-bottom: 5px; }

            div[role="radiogroup"] > label:nth-of-type(18) { margin-top: 25px !important; border-top: 1px solid #1e293b; padding-top: 15px !important; }
            div[role="radiogroup"] > label:nth-of-type(18)::before { content: "SISTEMA"; display: block; font-size: 0.65rem; color: #64748b; font-weight: 700; margin-bottom: 5px; }
        </style>
    """, unsafe_allow_html=True)
    
    safe_load_initial_data()

    # --- MENU LATERAL (NAVEGAÇÃO PURA) ---
    with st.sidebar:
        st.markdown(f"""
            <div style="text-align: center; padding-bottom: 10px;">
                <img src="https://i.ibb.co/TxfVPy49/Sem-t-tulo.png" width="150" style="margin-bottom: 5px;">
                <div style="color: #94a3b8; font-family: 'Nunito'; font-size: 0.6rem; font-style: italic; opacity: 0.8;">
                    O Poder da IA nas suas Apostas Esportivas
                </div>
            </div>
        """, unsafe_allow_html=True)

        MENU_ITEMS = [
            "🏠 Dashboard", "📊 Ranking Teses", "📋 Auditoria",
            "🧬 Sinergia & Vácuo", "⚔️ Lab Narrativas", "⚡ Momentum", "🔥 Las Vegas Sync", "🌪️ Blowout Hunter", "🏆 Trinity Club",
            "🔥 Hot Streaks", "⛏️ Garimpo", "🧩 Desdobra Múltipla", "🔮 Oráculo",
            "🛡️ DvP Confrontos", "🏥 Depto Médico", "👥 Escalações",
            "⚙️ Config", "🔍 Testar Conexão Supabase"
        ]

        choice = st.radio("Navegação", MENU_ITEMS, label_visibility="collapsed")
        
        st.markdown("<br><div style='text-align: center; color: #334155; font-size: 0.6rem;'>● SYSTEM ONLINE v2.2</div>", unsafe_allow_html=True)

    # --- ROTEAMENTO ---
    if choice == "🏠 Dashboard": show_dashboard_page()
    elif choice == "📊 Ranking Teses": show_analytics_page()
    elif choice == "📋 Auditoria": show_audit_page()
    
    elif choice == "🧬 Sinergia & Vácuo": show_nexus_page()
    elif choice == "⚔️ Lab Narrativas": show_narrative_lab()
    elif choice == "⚡ Momentum": show_momentum_page()
    elif choice == "🔥 Las Vegas Sync": show_props_odds_page()
    elif choice == "🌪️ Blowout Hunter": show_blowout_hunter_page()
    elif choice == "🏆 Trinity Club": show_trinity_club_page()
    
    elif choice == "🔥 Hot Streaks": show_hit_prop_page()
    elif choice == "⛏️ Garimpo": show_garimpo_page()
    elif choice == "🧩 Desdobra Múltipla": show_desdobramentos_inteligentes()
    elif choice == "🔮 Oráculo": show_oracle_page()
    
    elif choice == "🛡️ DvP Confrontos": show_dvp_analysis()
    elif choice == "🏥 Depto Médico": show_depto_medico()
    elif choice == "👥 Escalações": show_escalacoes()
    
    elif choice == "⚙️ Config": show_config_page()
    elif choice == "🔍 Testar Conexão Supabase": show_cloud_diagnostics()

if __name__ == "__main__":
    main()
                





































































































































































