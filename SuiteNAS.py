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
from datetime import datetime, timedelta
from itertools import combinations

# --- Imports de Terceiros ---
import requests
import pandas as pd
import numpy as np
import streamlit as st
# import streamlit_authenticator as stauth # (Descomente se for usar)

# --- Configuração de Logger ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# Arquivos de Cache (Fallback Local)
L5_CACHE_FILE = os.path.join(CACHE_DIR, "l5_players.pkl")
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

# --- Importação do Gerenciador de Nuvem ---
try:
    from db_manager import db
except ImportError:
    db = None
    # st.warning("Aviso: db_manager.py não encontrado. Usando apenas modo local.")

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

def save_data_universal(key_db, data, file_path=None):
    """
    Salva no Supabase E no arquivo local (Backup).
    VERSÃO DEBUG: Mostra tamanho do payload e erros detalhados.
    """
    sucesso_nuvem = False
    
    # Prepara dados para análise de tamanho
    try:
        # Serialização de teste para ver tamanho
        payload_str = json.dumps(data, default=str)
        size_kb = len(payload_str) / 1024
        print(f"📦 [PREPARING] '{key_db}': Tamanho estimado {size_kb:.2f} KB")
    except Exception as e:
        print(f"⚠️ Erro ao serializar '{key_db}' para debug: {e}")
        size_kb = 0

    # 1. Salva na Nuvem (Supabase)
    if db:
        try:
            # Tenta salvar
            start_time = time.time()
            db.save_data(key_db, data)
            duration = time.time() - start_time
            
            msg = f"☁️ [UPLOAD] '{key_db}' salvo no Supabase! ({duration:.2f}s)"
            print(msg)
            
            # Avisa visualmente se for algo demorado ou crítico
            if size_kb > 10: 
                st.toast(f"Salvo na Nuvem: {key_db} ({size_kb:.0f}KB)", icon="☁️")
            
            sucesso_nuvem = True
        except Exception as e:
            err_msg = str(e)
            print(f"❌ [ERRO UPLOAD] '{key_db}': {err_msg}")
            if "413" in err_msg:
                st.error(f"Erro ao salvar '{key_db}': Arquivo muito grande para o Supabase!")
            else:
                st.warning(f"Falha ao salvar '{key_db}' na nuvem.")

    # 2. Salva Local (Backup obrigatório)
    if file_path:
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, default=str) # default=str ajuda com datas
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
# 7. SISTEMA DE AUTENTICAÇÃO (CORREÇÃO: PASSE MESTRE ANTI-BLOQUEIO)
# ============================================================================

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
def initialize_system_components():
    comps = {}
    if "archetype_engine" not in st.session_state and ArchetypeEngine:
        st.session_state.archetype_engine = ArchetypeEngine()
        comps["Archetype"] = "✅"
    if "rotation_analyzer" not in st.session_state and RotationAnalyzer:
        st.session_state.rotation_analyzer = RotationAnalyzer()
        comps["Rotation"] = "✅"
    return comps

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
# PÁGINA: DVP SNIPER (V34.0 - SHERLOCK DIAGNOSTIC ENGINE)
# ============================================================================
def show_dvp_analysis():
    import streamlit as st
    import pandas as pd
    import numpy as np
    import unicodedata
    import html

    # --- 1. FUNÇÕES AUXILIARES ---
    def normalize_str(text):
        if not text: return ""
        try:
            text = str(text)
            text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
            return text.upper().strip()
        except: return ""

    UNIVERSAL_MAP = {
        "GS": "GSW", "GOLDEN STATE": "GSW", "WARRIORS": "GSW",
        "NY": "NYK", "NEW YORK": "NYK", "KNICKS": "NYK",
        "NO": "NOP", "NEW ORLEANS": "NOP", "PELICANS": "NOP",
        "SA": "SAS", "SAN ANTONIO": "SAS", "SPURS": "SAS",
        "PHO": "PHX", "PHOENIX": "PHX", "SUNS": "PHX",
        "WSH": "WAS", "WASHINGTON": "WAS", "WIZARDS": "WAS",
        "UTAH": "UTA", "JAZZ": "UTA",
        "BRK": "BKN", "BROOKLYN": "BKN", "NETS": "BKN",
        "CHO": "CHA", "CHARLOTTE": "CHA", "HORNETS": "CHA",
        "LAL": "LAL", "LAKERS": "LAL",
        "LAC": "LAC", "CLIPPERS": "LAC",
        "DET": "DET", "DETROIT": "DET", "PISTONS": "DET",
        "OKC": "OKC", "OKLAHOMA": "OKC", "THUNDER": "OKC"
    }

    def get_standard_team_code(raw_team):
        raw = str(raw_team).upper().strip()
        # Remove caracteres estranhos se houver
        return UNIVERSAL_MAP.get(raw, raw)

    # --- 2. CSS VISUAL ---
    st.markdown("""
    <style>
        .dvp-title { font-family: 'Oswald'; font-size: 28px; color: #fff; margin-bottom: 5px; }
        .dvp-sub { font-family: 'Nunito'; font-size: 14px; color: #94a3b8; margin-bottom: 25px; }

        .sniper-card {
            background-color: #1e293b;
            border-radius: 10px;
            padding: 8px 12px;
            margin-bottom: 8px;
            border: 1px solid #334155;
            display: flex; align-items: center; justify-content: space-between;
            transition: transform 0.2s;
        }
        .sniper-card:hover { transform: translateX(3px); border-color: #64748b; }

        .player-box { display: flex; align-items: center; gap: 10px; width: 65%; }
        .s-img { width: 40px; height: 40px; border-radius: 50%; object-fit: cover; background: #0f172a; border: 2px solid #334155; }
        .s-info { display: flex; flex-direction: column; }
        .s-name { font-family: 'Oswald'; font-size: 14px; color: #fff; line-height: 1.1; }
        .s-meta { font-size: 10px; color: #94a3b8; font-weight: bold; text-transform: uppercase; margin-top: 2px; }
        
        .pos-badge { background: rgba(255,255,255,0.1); color: #cbd5e1; padding: 1px 4px; border-radius: 3px; font-size: 8px; margin-left: 4px; }
        .rank-box { text-align: right; min-width: 70px; }
        .rank-val { font-family: 'Oswald'; font-size: 18px; font-weight: bold; padding: 2px 8px; border-radius: 4px; display: inline-block; min-width: 40px; text-align: center; }
        
        .rank-elite { background: rgba(34, 197, 94, 0.2); color: #4ade80; border: 1px solid #22c55e; }
        .rank-good { background: rgba(34, 197, 94, 0.1); color: #86efac; border: 1px solid #4ade80; }
        .rank-avg { background: rgba(250, 204, 21, 0.1); color: #fde047; border: 1px solid #eab308; }
        .rank-bad { background: rgba(239, 68, 68, 0.2); color: #fca5a5; border: 1px solid #ef4444; }

        .matchup-header { font-family: 'Oswald'; font-size: 16px; color: #e2e8f0; background: linear-gradient(90deg, #0f172a 0%, transparent 100%); padding: 6px 10px; border-left: 4px solid #3b82f6; margin-top: 15px; margin-bottom: 8px; border-radius: 0 4px 4px 0; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="dvp-title">🎯 DvP RADAR</div>', unsafe_allow_html=True)
    st.markdown('<div class="dvp-sub">Análise híbrida (Oficial + Inferida). Se faltar dados, o diagnóstico aparecerá.</div>', unsafe_allow_html=True)

    # --- 3. VERIFICAÇÕES DE SISTEMA ---
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
        st.warning("⚠️ Scoreboard vazio. Atualize em Config.")
        return

    # Debug Trackers
    debug_counts = {"L5_Loaded": 0, "Logs_Loaded": 0, "Inferred": 0, "Total_Roster": 0}
    debug_teams_found = set()

    # --- 4. ENGINE HÍBRIDA ---
    df_l5 = st.session_state.get("df_l5", pd.DataFrame())
    real_logs = get_data_universal('real_game_logs') or {}
    
    # Lista de times jogando hoje para otimizar
    TEAMS_PLAYING_TODAY = set()
    for g in games:
        TEAMS_PLAYING_TODAY.add(get_standard_team_code(g['home']))
        TEAMS_PLAYING_TODAY.add(get_standard_team_code(g['away']))

    # --- A. FILTRO DE LESÕES ---
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
            p_name, status = "", ""
            if isinstance(item, dict):
                p_name = item.get('player') or item.get('name') or ""
                status = str(item.get('status', '')).upper()
            elif isinstance(item, str):
                p_name = item.split('-')[0]
                status = str(item).upper()
            if p_name and any(x in status for x in EXCLUSION):
                banned_players.add(normalize_str(p_name))
    except: pass

    # --- B. CONSTRUÇÃO DO ROSTER ---
    TEAM_ROSTER = {} # { "LAL": { "PG": [...], "SG": [...] } }
    POS_KEYS = ["PG", "SG", "SF", "PF", "C"]

    def infer_position(stats):
        pts = stats.get('PTS', 0)
        reb = stats.get('REB', 0)
        ast = stats.get('AST', 0)
        if ast >= 6.0: return ["PG", "SG"]
        if reb >= 9.0: return ["C", "PF"]
        if reb >= 6.0 and ast < 3.0: return ["PF", "C"]
        if pts >= 20.0: return ["SG", "SF"]
        return ["SF", "PF"]

    processed_players = set()

    # B1. Fonte Oficial
    if not df_l5.empty:
        try:
            cols = [c.upper() for c in df_l5.columns]
            df_l5.columns = cols
            col_name = next((c for c in cols if 'PLAYER' in c and 'NAME' in c), 'PLAYER')
            col_team = next((c for c in cols if 'TEAM' in c and 'ID' not in c), 'TEAM')
            col_pos = next((c for c in cols if 'POS' in c), None)
            col_min = next((c for c in cols if 'MIN' in c), 'MIN')
            col_id = next((c for c in cols if 'ID' in c and 'PLAYER' in c), 'PLAYER_ID')

            for _, row in df_l5.iterrows():
                p_name = normalize_str(row.get(col_name, ''))
                if not p_name or p_name in banned_players: continue
                
                raw_team = str(row.get(col_team, 'UNK'))
                team = get_standard_team_code(raw_team)
                
                if team not in TEAMS_PLAYING_TODAY and team != "UNK": continue
                
                if team not in TEAM_ROSTER: TEAM_ROSTER[team] = {k: [] for k in POS_KEYS}
                
                debug_teams_found.add(team)
                debug_counts["L5_Loaded"] += 1

                data = {
                    "name": p_name,
                    "id": row.get(col_id, 0),
                    "min": float(row.get(col_min, 0)),
                    "source": "OFFICIAL"
                }
                
                raw_pos = str(row.get(col_pos, 'UNK')).upper()
                assigned = False
                for k in POS_KEYS:
                    if k in raw_pos:
                        TEAM_ROSTER[team][k].append(data)
                        assigned = True
                
                if not assigned:
                    if "G" in raw_pos: TEAM_ROSTER[team]["PG"].append(data); TEAM_ROSTER[team]["SG"].append(data)
                    elif "C" in raw_pos: TEAM_ROSTER[team]["C"].append(data)
                    else: TEAM_ROSTER[team]["SF"].append(data)
                
                processed_players.add(p_name)
        except: pass

    # B2. Fonte Inferida (Logs)
    if real_logs:
        for p_name, p_data in real_logs.items():
            norm_name = normalize_str(p_name)
            
            if norm_name in processed_players or norm_name in banned_players: continue
            if not isinstance(p_data, dict): continue
            
            raw_team = str(p_data.get('team', 'UNK'))
            team = get_standard_team_code(raw_team)
            
            # Se o time for UNK ou não estiver jogando hoje, ignora
            if team not in TEAMS_PLAYING_TODAY: continue

            if team not in TEAM_ROSTER: TEAM_ROSTER[team] = {k: [] for k in POS_KEYS}
            debug_teams_found.add(team)

            logs = p_data.get('logs', {})
            stats_avg = {}
            valid_log = False
            for k in ['PTS', 'REB', 'AST', 'MIN']:
                vals = logs.get(k, [])
                clean = [float(x) for x in vals if x is not None]
                if clean:
                    stats_avg[k] = sum(clean)/len(clean)
                    valid_log = True
                else:
                    stats_avg[k] = 0
            
            if not valid_log: continue
            if stats_avg.get('MIN', 0) < 15: continue # Relaxei filtro para 15 min
            
            debug_counts["Inferred"] += 1
            
            inferred_pos_list = infer_position(stats_avg)
            data = {
                "name": p_name, "id": 0, 
                "min": stats_avg['MIN'], "source": "INFERRED"
            }
            
            for pos in inferred_pos_list:
                TEAM_ROSTER[team][pos].append(data)

    # Ordena Roster
    for tm in TEAM_ROSTER:
        for pos in TEAM_ROSTER[tm]:
            TEAM_ROSTER[tm][pos].sort(key=lambda x: x['min'], reverse=True)
            debug_counts["Total_Roster"] += len(TEAM_ROSTER[tm][pos])

    # --- 5. PROCESSAMENTO E EXIBIÇÃO ---
    matchups_data = []
    
    for g in games:
        home_code = get_standard_team_code(g['home'])
        away_code = get_standard_team_code(g['away'])
        
        game_analysis = {"game": f"{away_code} @ {home_code}", "targets": [], "avoids": []}
        
        # Verifica se temos dados dos times
        home_has_data = home_code in TEAM_ROSTER
        away_has_data = away_code in TEAM_ROSTER
        
        if not home_has_data and not away_has_data:
            # Marca jogo como vazio para aviso
            game_analysis["missing_data"] = True
            matchups_data.append(game_analysis)
            continue

        for side, attack_team, def_team_code in [('HOME', home_code, away_code), ('AWAY', away_code, home_code)]:
            if attack_team not in TEAM_ROSTER: continue

            for pos in POS_KEYS:
                rank = dvp_analyzer.get_position_rank(def_team_code, pos)
                if not rank: rank = 15
                
                players = TEAM_ROSTER[attack_team].get(pos, [])
                # Pega Top 1 da posição
                for p in players[:1]:
                    is_target = rank > 15
                    lst = game_analysis["targets"] if is_target else game_analysis["avoids"]
                    
                    if not any(x['name'] == p['name'] for x in lst):
                        lst.append({
                            "name": p['name'], "id": p['id'], "pos": pos,
                            "rank": rank, "min": p['min'], "opp": def_team_code,
                            "source": p.get('source', 'OFFICIAL')
                        })

        matchups_data.append(game_analysis)

    # --- 6. RENDERIZAÇÃO ---
    has_rendered_any = False
    
    def render_sniper_card(p, is_target):
        display_name = p['name'].title()
        photo = "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"
        if p['id'] != 0:
            photo = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{int(p['id'])}.png"
        
        rank = p['rank']
        if rank >= 25: rank_cls, rank_lbl = "rank-elite", "DEFESA PÉSSIMA"
        elif rank >= 16: rank_cls, rank_lbl = "rank-good", "DEFESA FRACA"
        elif rank >= 11: rank_cls, rank_lbl = "rank-avg", "DEFESA MÉDIA"
        else: rank_cls, rank_lbl = "rank-bad", "DEFESA ELITE"
        
        pos_icons = {"PG": "🏀", "SG": "🏹", "SF": "🗡️", "PF": "💪", "C": "🛡️"}
        icon = pos_icons.get(p['pos'], "👤")
        inferred_mark = "⚡" if p['source'] == "INFERRED" else ""

        return f"""
        <div class="sniper-card">
            <div class="player-box">
                <img src="{photo}" class="s-img" onerror="this.src='https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png';">
                <div class="s-info">
                    <div class="s-name">{display_name} {inferred_mark} <span class="pos-badge">{icon} {p['pos']}</span></div>
                    <div class="s-meta">{p['min']:.0f}M • vs {p['opp']}</div>
                </div>
            </div>
            <div class="rank-box">
                <div class="rank-val {rank_cls}">#{rank}</div>
            </div>
        </div>
        """

    for match in matchups_data:
        # Se faltar dados dos dois times, avisa
        if match.get("missing_data"):
            st.markdown(f'<div class="matchup-header" style="border-left-color:#64748b;">{match["game"]}</div>', unsafe_allow_html=True)
            st.caption(f"⚠️ Sem dados de jogadores para {match['game']}. Verifique os logs.")
            continue

        has_rendered_any = True
        st.markdown(f'<div class="matchup-header">{match["game"]}</div>', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        
        with c1:
            st.markdown("<div style='color:#4ade80; font-size:11px; font-weight:bold; margin-bottom:5px; text-align:center;'>🚀 ATAQUE FAVORÁVEL (Rank 16-30)</div>", unsafe_allow_html=True)
            if match['targets']:
                sorted_targets = sorted(match['targets'], key=lambda x: x['rank'], reverse=True)
                html_block = "".join([render_sniper_card(p, True) for p in sorted_targets])
                st.markdown(html_block, unsafe_allow_html=True)
            else: st.caption("Sem vantagens claras.")

        with c2:
            st.markdown("<div style='color:#f87171; font-size:11px; font-weight:bold; margin-bottom:5px; text-align:center;'>🛡️ MATCHUP DIFÍCIL (Rank 1-15)</div>", unsafe_allow_html=True)
            if match['avoids']:
                sorted_avoids = sorted(match['avoids'], key=lambda x: x['rank'])
                html_block = "".join([render_sniper_card(p, False) for p in sorted_avoids])
                st.markdown(html_block, unsafe_allow_html=True)
            else: st.caption("Sem bloqueios.")

    # --- 7. DIAGNÓSTICO SHERLOCK (SE VAZIO) ---
    if not has_rendered_any:
        st.error("⚠️ Nenhum matchup gerado. Iniciando Diagnóstico...")
        with st.expander("🕵️ Sherlock Holmes Debugger", expanded=True):
            st.write(f"**Jogos Hoje:** {len(games)}")
            st.write(f"**Times Jogando (Normalizado):** {sorted(list(TEAMS_PLAYING_TODAY))}")
            st.write(f"**Times Encontrados no DB+Logs:** {sorted(list(debug_teams_found))}")
            st.write("**Estatísticas de Carregamento:**")
            st.json(debug_counts)
            
            missing = TEAMS_PLAYING_TODAY - debug_teams_found
            if missing:
                st.error(f"❌ Times sem nenhum jogador detectado: {missing}")
                st.info("Dica: Os logs brutos podem estar com nomes de times diferentes (ex: 'Golden State' vs 'GSW'). O normalizador tentou corrigir, mas pode ter falhado.")
            else:
                st.success("✅ Todos os times foram encontrados. O problema pode ser o filtro de minutos (<15min) ou lesões.")
                
# ============================================================================
# PÁGINA: BLOWOUT RADAR (V27.0 - TARGETING V44 CACHE)
# ============================================================================
def show_blowout_hunter_page():
    import json
    import pandas as pd
    import re
    import time
    import numpy as np
    import unicodedata
    
    # --- 1. FUNÇÕES AUXILIARES ---
    def normalize_str(text):
        """Limpa texto para comparação (Remove acentos, uppercase)."""
        if not text: return ""
        try:
            text = str(text)
            text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
            return text.upper().strip()
        except: return ""

    # --- 2. ESTILO VISUAL ---
    st.markdown("""
    <style>
        .radar-title { font-family: 'Oswald'; font-size: 26px; color: #fff; margin-bottom: 5px; }
        .match-container { background-color: #1e293b; border-radius: 12px; margin-bottom: 20px; border: 1px solid #334155; }
        .risk-header { padding: 12px 16px; display: flex; justify-content: space-between; align-items: center; }
        .risk-high { background: linear-gradient(90deg, #7f1d1d 0%, #1e293b 80%); border-left: 5px solid #EF4444; }
        .risk-med { background: linear-gradient(90deg, #78350f 0%, #1e293b 80%); border-left: 5px solid #F59E0B; }
        .risk-low { background: linear-gradient(90deg, #064e3b 0%, #1e293b 80%); border-left: 5px solid #10B981; }
        .game-matchup { font-family: 'Oswald'; font-size: 18px; color: #fff; }
        .risk-label { font-size: 11px; font-weight: bold; color: #fff; text-transform: uppercase; }
        .spread-tag { font-size: 12px; color: #cbd5e1; background: rgba(0,0,0,0.4); padding: 2px 6px; border-radius: 4px; font-weight: bold; }
        .players-area { padding: 10px; background: rgba(0,0,0,0.2); }
        .team-label { color: #64748b; font-size: 10px; font-weight: bold; letter-spacing: 1px; margin-bottom: 8px; border-bottom: 1px solid #334155; }
        .vulture-row { display: flex; justify-content: space-between; align-items: center; padding: 8px; margin-bottom: 6px; background: rgba(255,255,255,0.03); border-radius: 6px; }
        .vulture-img { width: 42px; height: 42px; border-radius: 50%; border: 2px solid #a78bfa; margin-right: 12px; object-fit: cover; background: #0f172a; }
        .vulture-name { color: #e2e8f0; font-weight: 700; font-size: 13px; line-height: 1.2; }
        .vulture-role { font-size: 9px; color: #94a3b8; text-transform: uppercase; margin-top: 2px; }
        .stat-box { display: flex; gap: 12px; text-align: center; }
        .stat-val { font-family: 'Oswald'; font-size: 15px; font-weight: bold; }
        .stat-lbl { font-size: 7px; color: #64748B; font-weight: bold; }
        .c-pts { color: #4ade80; } .c-reb { color: #60a5fa; } .c-ast { color: #facc15; }
        .dna-badge { background: #6D28D9; color: #fff; padding: 1px 4px; border-radius: 3px; font-size: 8px; font-weight:bold; }
        .fallback-badge { background: #F59E0B; color: #000; padding: 1px 4px; border-radius: 3px; font-size: 8px; font-weight:bold; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="radar-title">&#127744; BLOWOUT RADAR</div>', unsafe_allow_html=True)

    # --- 3. CORE: LEITURA DO ARQUIVO V44 ---
    # Tenta carregar especificamente o arquivo v44
    data_source = "CACHE"
    try:
        # Tenta pelo nome exato do cache
        fresh_inj = get_data_universal('injuries_cache_v44')
        if not fresh_inj:
             # Tenta fallback para nome sem 'cache'
             fresh_inj = get_data_universal('injuries_v44')
        
        if fresh_inj: 
            st.session_state['injuries_data'] = fresh_inj
            data_source = "SUPABASE_V44"
        else:
            # Última tentativa: chave genérica
            generic = get_data_universal('injuries_data')
            if generic:
                st.session_state['injuries_data'] = generic
                data_source = "SUPABASE_GENERIC"
    except: pass

    banned_players = set()
    raw_inj_debug = []

    try:
        raw_inj_source = st.session_state.get('injuries_data', [])
        
        # Flattening
        flat_inj = []
        if isinstance(raw_inj_source, dict):
            for t in raw_inj_source.values(): 
                if isinstance(t, list): flat_inj.extend(t)
        elif isinstance(raw_inj_source, list):
            flat_inj = raw_inj_source
        
        raw_inj_debug = flat_inj[:3]

        EXCLUSION_KEYWORDS = ['OUT', 'DOUBT', 'SURG', 'INJUR', 'PROTOCOL', 'DAY', 'DTD', 'QUEST', 'GTD']
        
        for item in flat_inj:
            p_name = ""
            status = ""
            
            # Polimorfismo
            if isinstance(item, dict):
                p_name = item.get('player') or item.get('name') or item.get('athlete') or ""
                status = str(item.get('status', '')).upper()
            elif isinstance(item, str):
                parts = str(item).split('-')
                p_name = parts[0]
                status = str(item).upper()
            
            if p_name:
                norm_name = normalize_str(p_name)
                # Verifica status E se o nome não está vazio
                if norm_name and any(x in status for x in EXCLUSION_KEYWORDS):
                    banned_players.add(norm_name)
    except Exception as e:
        print(f"Erro processando v44: {e}")

    # --- 4. CONFIGURAÇÃO ---
    KEY_LOGS = "real_game_logs"
    KEY_DNA = "rotation_dna_v27" # Cache novo

    df_l5 = st.session_state.get('df_l5', pd.DataFrame())
    PLAYER_ID_MAP = {}
    PLAYER_TEAM_MAP = {}
    if not df_l5.empty:
        try:
            df_norm = df_l5.copy()
            df_norm['PLAYER_NORM'] = df_norm['PLAYER'].apply(normalize_str)
            PLAYER_ID_MAP = dict(zip(df_norm['PLAYER_NORM'], df_norm['PLAYER_ID']))
            PLAYER_TEAM_MAP = dict(zip(df_norm['PLAYER_NORM'], df_norm['TEAM']))
        except: pass

    # --- 5. ENGINE AUTO-RUN ---
    if 'dna_final_v27' not in st.session_state:
        with st.spinner("🤖 Processando rotações com V44 Injury Data..."):
            try:
                cloud_dna = get_data_universal(KEY_DNA)
                
                if not cloud_dna:
                    raw_data = get_data_universal(KEY_LOGS)
                    if raw_data:
                        new_dna = {}
                        temp_team_data = {}
                        
                        def clean_list(lst):
                            if not lst: return []
                            return [float(x) if x is not None else 0.0 for x in lst]

                        for p_name, p_data in raw_data.items():
                            if not isinstance(p_data, dict): continue
                            
                            norm_name = normalize_str(p_name)
                            p_id = PLAYER_ID_MAP.get(norm_name, 0)
                            
                            team = str(p_data.get('team', 'UNK')).upper().strip()
                            if team in ['UNK', 'NONE']: team = PLAYER_TEAM_MAP.get(norm_name, 'UNK')
                            if team == 'UNK': continue 

                            logs = p_data.get('logs', {})
                            if not logs: continue
                            
                            try:
                                pts_list = clean_list(logs.get('PTS', []))
                                min_list = clean_list(logs.get('MIN', []))
                                reb_list = clean_list(logs.get('REB', []))
                                ast_list = clean_list(logs.get('AST', []))
                                
                                if not pts_list: continue

                                avg_min = p_data.get('logs', {}).get('MIN_AVG', 0)
                                if avg_min == 0 and min_list: avg_min = sum(min_list) / len(min_list)
                                
                                if avg_min > 26: continue

                                is_qualified = False
                                b_pts, b_reb, b_ast, b_min = 0,0,0,0
                                logic_type = "REGULAR"

                                # Sniper
                                if min_list and len(min_list) == len(pts_list):
                                    arr_min = np.array(min_list)
                                    mask = arr_min >= max(12.0, avg_min * 2.0)
                                    if np.any(mask):
                                        is_qualified = True
                                        logic_type = "SNIPER"
                                        arr_pts = np.array(pts_list)
                                        arr_reb = np.array(reb_list)
                                        arr_ast = np.array(ast_list)
                                        b_pts = np.mean(arr_pts[mask])
                                        b_reb = np.mean(arr_reb[mask])
                                        b_ast = np.mean(arr_ast[mask])
                                        b_min = np.mean(arr_min[mask])

                                # Fallback
                                if not is_qualified and avg_min > 8.0:
                                    limit = len(pts_list)
                                    p = np.array(pts_list[:limit])
                                    r = np.array(reb_list[:limit]) if len(reb_list) >= limit else np.zeros(limit)
                                    a = np.array(ast_list[:limit]) if len(ast_list) >= limit else np.zeros(limit)
                                    scores = p + r*1.2 + a*1.5
                                    if len(scores) > 0:
                                        top_idx = np.argsort(scores)[-3:]
                                        if np.mean(scores[top_idx]) > 5.0:
                                            is_qualified = True
                                            logic_type = "CEILING"
                                            b_pts = np.mean(p[top_idx])
                                            b_reb = np.mean(r[top_idx])
                                            b_ast = np.mean(a[top_idx])
                                            b_min = avg_min

                                if is_qualified:
                                    impact = b_pts + b_reb + b_ast
                                    if team not in temp_team_data: temp_team_data[team] = []
                                    
                                    temp_team_data[team].append({
                                        "id": int(p_id),
                                        "name": p_name,
                                        "clean_name": norm_name,
                                        "avg_min": float(avg_min),
                                        "blowout_min": float(b_min),
                                        "pts": float(b_pts),
                                        "reb": float(b_reb),
                                        "ast": float(b_ast),
                                        "score": float(impact),
                                        "type": logic_type
                                    })
                            except: continue

                    for t, players in temp_team_data.items():
                        players.sort(key=lambda x: x['score'], reverse=True)
                        new_dna[t] = players[:10]
                    
                    save_data_universal(KEY_DNA, new_dna)
                    st.session_state['dna_final_v27'] = new_dna
                else:
                    st.session_state['dna_final_v27'] = cloud_dna if cloud_dna else {}
            except:
                 st.session_state['dna_final_v27'] = {}

    DNA_DB = st.session_state.get('dna_final_v27', {})

    # --- 6. EXIBIÇÃO ---
    games = st.session_state.get('scoreboard', [])
    if not games:
        st.warning("Aguardando jogos...")
        return

    st.markdown("---")
    c_sim, c_vazio = st.columns([1, 2])
    with c_sim:
        force_spread = st.slider("🎛️ Simular Cenário de Blowout (Spread):", 0, 30, 0)

    ALIAS_MAP = {
        "GS": "GSW", "GSW": "GSW", "NY": "NYK", "NYK": "NYK",
        "NO": "NOP", "NOP": "NOP", "SA": "SAS", "SAS": "SAS",
        "UTAH": "UTA", "UTA": "UTA", "PHO": "PHX", "PHX": "PHX",
        "WSH": "WAS", "WAS": "WAS", "BRK": "BKN", "BKN": "BKN",
        "CHO": "CHA", "CHA": "CHA", "LAL": "LAL", "LAC": "LAC",
        "DET": "DET", "OKC": "OKC"
    }

    def get_team_data(query):
        q = str(query).upper().strip()
        if q in ALIAS_MAP:
            target = ALIAS_MAP[q]
            if target in DNA_DB: return DNA_DB[target]
        if q in DNA_DB: return DNA_DB[q]
        for k in DNA_DB.keys():
            if q in k or k in q: return DNA_DB[k]
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
        
        st.markdown(f"""
        <div class="match-container">
            <div class="risk-header {risk_cls}">
                <div class="game-matchup">{g['away']} @ {g['home']}</div>
                <div class="game-meta">
                    <div class="risk-label">{risk_txt}</div>
                    <span class="spread-tag">SPREAD: {final_spread}</span>
                </div>
            </div>
        """, unsafe_allow_html=True)

        if show_players:
            c1, c2 = st.columns(2)
            
            def render_team_col(col, t_name):
                data = get_team_data(t_name)
                with col:
                    st.markdown(f"""
                    <div class="players-area">
                        <div class="team-label">{t_name} RESERVES</div>
                    """, unsafe_allow_html=True)
                    
                    if data:
                        active_players = []
                        for p in data:
                            p_clean = p.get('clean_name') or normalize_str(p['name'])
                            if p_clean not in banned_players:
                                active_players.append(p)
                        
                        if active_players:
                            for p in active_players[:3]:
                                photo = "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"
                                if p.get('id') and p['id'] != 0:
                                    photo = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{p['id']}.png"
                                
                                badge_cls = "dna-badge" if p.get('type') == 'SNIPER' else "fallback-badge"
                                badge_txt = "SNIPER" if p.get('type') == 'SNIPER' else "CEILING"
                                
                                st.markdown(f"""
                                <div class="vulture-row">
                                    <div style="display:flex; align-items:center;">
                                        <img src="{photo}" class="vulture-img" onerror="this.src='https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png';">
                                        <div class="vulture-info">
                                            <div class="vulture-name">{p['name']} <span class="{badge_cls}">{badge_txt}</span></div>
                                            <div class="vulture-role">
                                                {p['avg_min']:.0f}m <span style="color:#64748B;">➝</span> <span style="color:#4ade80;">{p['blowout_min']:.0f}m</span>
                                            </div>
                                        </div>
                                    </div>
                                    <div class="stat-box">
                                        <div><div class="stat-val c-pts">{p['pts']:.1f}</div><div class="stat-lbl">PTS</div></div>
                                        <div><div class="stat-val c-reb">{p['reb']:.1f}</div><div class="stat-lbl">REB</div></div>
                                        <div><div class="stat-val c-ast">{p['ast']:.1f}</div><div class="stat-lbl">AST</div></div>
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                        else:
                            st.markdown("<div style='text-align:center; padding:10px; font-size:11px; color:#e11d48;'>Reservas listados estão lesionados.</div>", unsafe_allow_html=True)
                    else:
                        st.markdown("<div style='text-align:center; padding:10px; font-size:11px; color:#64748b;'>Sem dados.</div>", unsafe_allow_html=True)
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

    # --- 7. DEBUG FINAL (PARA VOCÊ TESTAR) ---
    with st.expander(f"🔍 Debug V44 (Fonte: {data_source})", expanded=False):
        st.write(f"Banidos Carregados: {len(banned_players)}")
        st.write(f"Amostra de Lesão V44: {raw_inj_debug}")
        
        check = st.text_input("Testar:", "VALANCIUNAS")
        if check:
            nm = normalize_str(check)
            st.write(f"Status '{nm}': {'⛔ BANIDO' if nm in banned_players else '✅ JOGANDO'}")
# ============================================================================
# PÁGINA: MOMENTUM (V5.0 - BLINDADA & VISUAL)
# ============================================================================
def show_momentum_page():
    # --- 1. CSS SEGURO (Separado para evitar erro de decimal literal) ---
    # Usamos uma string normal (sem 'f') para que o Python ignore os números css
    MOMENTUM_CSS = """
    <style>
        .mom-header { 
            font-family: 'Oswald', sans-serif; 
            font-size: 26px; 
            color: #fff; 
            margin-bottom: 10px; 
            letter-spacing: 1px; 
        }
        
        /* Card Container */
        .mom-card {
            background: linear-gradient(90deg, #1e293b 0%, #0f172a 100%);
            border-radius: 12px;
            padding: 0; 
            margin-bottom: 15px;
            display: flex;
            align-items: center;
            border: 1px solid #334155;
            overflow: hidden; 
            transition: transform 0.2s;
        }
        .mom-card:hover { transform: scale(1.02); border-color: #64748B; }
        
        /* Bordas */
        .border-hot { border-left: 6px solid #10B981; }
        .border-cold { border-left: 6px solid #EF4444; }
        
        /* Área da Foto */
        .mom-img-box {
            width: 80px;
            height: 80px;
            background: #000;
            display: flex;
            justify-content: center;
            align-items: center;
            flex-shrink: 0;
        }
        .mom-img {
            width: 100%;
            height: 100%;
            object-fit: cover;
            opacity: 0.9;
        }
        
        /* Área de Texto */
        .mom-info { padding: 10px 15px; flex-grow: 1; }
        
        .mom-name { 
            font-family: 'Oswald', sans-serif; 
            font-size: 16px; 
            color: #fff; 
            line-height: 1.1; 
        }
        .mom-team { 
            font-size: 11px; 
            color: #94a3b8; 
            font-weight: bold; 
            margin-bottom: 4px; 
        }
        
        .mom-stat-row { display: flex; justify-content: space-between; align-items: end; }
        .mom-score { font-family: 'Oswald'; font-size: 22px; font-weight: bold; }
        .mom-avg { font-size: 10px; color: #64748B; text-align: right; }
    </style>
    """
    st.markdown(MOMENTUM_CSS, unsafe_allow_html=True)

    # HTML Entity para Raio: &#9889;
    st.markdown('<div class="mom-header">&#9889; MOMENTUM RADAR (Z-SCORE)</div>', unsafe_allow_html=True)
    st.info("Ranking baseado na Dominância Relativa. Jogadores com performance muito acima ou muito abaixo da média da liga nos últimos 5 jogos.")

    # --- 2. DADOS ---
    # Importante: .copy() para não quebrar outras páginas que usam df_l5
    original_df = st.session_state.get('df_l5', pd.DataFrame())
    
    if original_df.empty:
        st.warning("Dados insuficientes. Vá em Config > Atualizar L5.")
        return

    # --- 3. CÁLCULO ESTATÍSTICO (Z-SCORE) ---
    # Filtro mínimo de minutos para evitar ruído
    df_calc = original_df[original_df['MIN_AVG'] >= 15].copy()
    
    if df_calc.empty:
        st.warning("Nenhum jogador com mais de 15 minutos de média carregado.")
        return

    # Se PRA não existir, calcula na hora
    if 'PRA_AVG' not in df_calc.columns:
        df_calc['PRA_AVG'] = df_calc['PTS_AVG'] + df_calc['REB_AVG'] + df_calc['AST_AVG']

    # Estatísticas da Liga (Amostra Atual)
    mean_league = df_calc['PRA_AVG'].mean()
    std_league = df_calc['PRA_AVG'].std()
    
    # Evita divisão por zero
    if std_league == 0: std_league = 1

    # Z-Score: Quantos desvios padrão acima/abaixo da média o jogador está?
    df_calc['z_score'] = (df_calc['PRA_AVG'] - mean_league) / std_league

    # --- 4. SEPARAÇÃO RIGOROSA HOT / COLD ---
    
    # HOT: Apenas Z-Score Positivo (> 0)
    hot_candidates = df_calc[df_calc['z_score'] > 0].copy()
    top_hot = hot_candidates.sort_values('z_score', ascending=False).head(10)
    
    # COLD: Apenas Z-Score Negativo (< 0) E Titulares (Min > 24)
    cold_candidates = df_calc[
        (df_calc['z_score'] < 0) & 
        (df_calc['MIN_AVG'] >= 24)
    ].copy()
    top_cold = cold_candidates.sort_values('z_score', ascending=True).head(10)

    # --- 5. RENDERIZAÇÃO ---
    c1, c2 = st.columns(2)

    # Função Helper de Renderização (Local)
    def render_momentum_card(col, row, type_card):
        is_hot = type_card == "HOT"
        css_class = "border-hot" if is_hot else "border-cold"
        color = "#10B981" if is_hot else "#EF4444"
        # HTML Entities: Chart Up (&#128200;) / Chart Down (&#128201;)
        icon_html = "&#128200;" if is_hot else "&#128201;"
        
        pid = int(row['PLAYER_ID'])
        name = row['PLAYER']
        team = row['TEAM']
        pra = row['PRA_AVG']
        z = row['z_score']
        
        # Foto da NBA
        photo_url = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"
        
        # HTML do Card (F-String segura, sem CSS complexo dentro)
        col.markdown(f"""
        <div class="mom-card {css_class}">
            <div class="mom-img-box">
                <img src="{photo_url}" class="mom-img">
            </div>
            <div class="mom-info">
                <div class="mom-team">{team}</div>
                <div class="mom-name">{name}</div>
                <div class="mom-stat-row">
                    <div style="font-size:11px; color:{color}; font-weight:bold;">
                        {icon_html} {z:+.2f} <span style="color:#64748B;">(Z-Score)</span>
                    </div>
                    <div>
                        <div class="mom-score" style="color:#fff;">{pra:.1f}</div>
                        <div class="mom-avg">PRA (Média L5)</div>
                    </div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # --- COLUNA HOT ---
    with c1:
        # HTML Entity Fire: &#128293;
        st.markdown('<div style="color:#10B981; font-family:Oswald; font-size:20px; margin-bottom:15px; border-bottom:2px solid #10B981;">&#128293; ALTA PERFORMANCE (HOT)</div>', unsafe_allow_html=True)
        if top_hot.empty:
            st.info("Nenhum destaque positivo relevante (Z > 0).")
        else:
            for _, row in top_hot.iterrows():
                render_momentum_card(c1, row, "HOT")

    # --- COLUNA COLD ---
    with c2:
        # HTML Entity Snow: &#10052;
        st.markdown('<div style="color:#EF4444; font-family:Oswald; font-size:20px; margin-bottom:15px; border-bottom:2px solid #EF4444;">&#10052; BAIXA PRODUTIVIDADE (COLD)</div>', unsafe_allow_html=True)
        st.caption("Jogadores titulares (+24min) com performance abaixo da média.")
        if top_cold.empty:
            st.info("Nenhum titular com performance negativa crítica (Z < 0).")
        else:
            for _, row in top_cold.iterrows():
                render_momentum_card(c2, row, "COLD")
                

# ============================================================================
# TRINITY CLUB ENGINE v6 (MULTI-WINDOW SUPPORT)
# ============================================================================

class TrinityEngine:
    def __init__(self, logs_cache, games):
        self.logs = logs_cache
        self.games_map = self._map_games(games)
        
    def _normalize_team(self, team_code):
        mapping = {
            "NY": "NYK", "GS": "GSW", "PHO": "PHX", "NO": "NOP", "SA": "SAS",
            "WSH": "WAS", "UTAH": "UTA", "NOH": "NOP"
        }
        return mapping.get(team_code, team_code)

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
        """
        Escaneia o mercado com uma janela temporal específica (L5, L10, L15).
        """
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
            
            for stat in ['PTS', 'REB', 'AST']:
                values = logs.get(stat, [])
                if len(values) < window: continue 
                
                # --- LÓGICA DE JANELA TEMPORAL ---
                current_window_values = values[:window]
                
                # O Piso da Janela (Forma)
                floor_form = min(current_window_values)
                
                # Proxies para Venue e H2H baseados na Janela Atual
                # (Idealmente seriam filtrados, mas mantemos a heurística conservadora que funcionou)
                floor_venue = floor_form 
                floor_h2h = int(floor_form * 0.9)
                
                # Piso de Segurança Final
                safe_floor = min(floor_form, floor_venue, floor_h2h)
                
                # Filtros Mínimos de Relevância
                min_req = 10 if stat == 'PTS' else 4
                
                if safe_floor >= min_req:
                    candidates.append({
                        "player": player_name,
                        "team": raw_team,
                        "opp": ctx['opp'],
                        "stat": stat,
                        "line": safe_floor - 1, # Alvo Sugerido
                        "floors": {
                            "Form": floor_form,
                            "Venue": floor_venue,
                            "H2H": floor_h2h
                        },
                        "score": safe_floor,
                        "game_str": ctx['game_str'],
                        "game_id": ctx['game_id'],
                        "window": f"L{window}"
                    })
                        
        return sorted(candidates, key=lambda x: x['score'], reverse=True)


# ============================================================================
# CLASSE NEXUS ENGINE (v10.1 - SINTAXE CORRIGIDA & VARREDURA TOTAL)
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
        self.sinergy = None # Desligado para evitar alucinações de times

    # --- UTILITÁRIOS ---
    def _normalize_team(self, team_raw):
        """Converte qualquer nome para sigla de 3 letras"""
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
        return mapping.get(t, t[:3])

    def _strip_accents(self, text):
        try:
            text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")
            return str(text)
        except: return str(text)

    def _load_photo_map(self):
        # SINTAXE CORRIGIDA AQUI:
        if os.path.exists("nba_players_map.json"):
            try:
                with open("nba_players_map.json", "r", encoding="utf-8") as f:
                    return json.load(f)
            except:
                pass
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

    # --- MOTOR PRINCIPAL ---
    def run_nexus_scan(self):
        opportunities = []
        
        # 1. SGP (Estratégia Sinergia)
        opportunities.extend(self._scan_sgp_opportunities())

        # 2. Vácuo (Estratégia Lesão com Varredura Total)
        if self.injury_monitor:
            try: 
                opportunities.extend(self._scan_vacuum_opportunities())
            except Exception as e: 
                print(f"⚠️ Erro Vacuum: {e}") 
        
        return sorted(opportunities, key=lambda x: x['score'], reverse=True)

    def _scan_sgp_opportunities(self):
        found = []
        processed = set() 

        for p_name, data in self.logs.items():
            if p_name in processed: continue
            
            # Normaliza time
            my_team = self._normalize_team(data.get('team'))
            
            # Filtro 1: Motor (Assistências > 6.0)
            avg_ast = self._get_avg_stat(p_name, 'AST')
            if avg_ast < 6.0: continue

            # Filtro 2: Busca Parceiro (mesmo time estrito)
            partner_name = None
            best_pts = 0
            
            # Varre logs procurando cestinha do MESMO time
            for cand_name, cand_data in self.logs.items():
                if cand_name == p_name: continue
                if self._normalize_team(cand_data.get('team')) == my_team:
                    c_pts = self._get_avg_stat(cand_name, 'PTS')
                    if c_pts > best_pts:
                        best_pts = c_pts
                        partner_name = cand_name
            
            if not partner_name: continue
            
            # Monta Card SGP
            t_ast = f"{math.ceil(avg_ast - 0.5)}+"
            t_pts = f"{math.floor(best_pts)}+"
            
            score = 60
            badges = []
            
            if avg_ast > 9.0: score += 10
            if best_pts > 24: score += 10
            
            opp = self._get_opponent(my_team)
            if opp and self.pace_adjuster:
                pace = self.pace_adjuster.calculate_game_pace(my_team, opp)
                if pace >= 100: 
                    score += 10
                    badges.append(f"🏎️ Pace: {int(pace)}")
            
            if score >= 70:
                processed.add(p_name)
                found.append({
                    "type": "SGP",
                    "title": "ECOSSISTEMA SIMBIÓTICO",
                    "score": score,
                    "color": "#eab308",
                    "hero": {"name": p_name, "photo": self.get_photo(p_name), "role": "🧠 O MOTOR", "stat": "AST", "target": t_ast, "logo": self.get_team_logo(my_team)},
                    "partner": {"name": partner_name, "photo": self.get_photo(partner_name), "role": "🎯 O FINALIZADOR", "stat": "PTS", "target": t_pts, "logo": self.get_team_logo(my_team)},
                    "badges": badges + ["🔥 Sinergia Alta"]
                })
        return found

    def _scan_vacuum_opportunities(self):
        """
        LÓGICA VÁCUO 2.0 (VIP LIST):
        Garante que Jokic, Nurkic, etc sejam detectados como pivôs mesmo se a posição estiver errada.
        """
        found = []
        if not self.games: return []

        # 1. Mapa de Matchups
        matchups = {}
        for g in self.games:
            h = self._normalize_team(g.get('home'))
            a = self._normalize_team(g.get('away'))
            matchups[h] = a
            matchups[a] = h
        
        # 2. Varredura de Lesões
        all_injuries = self.injury_monitor.get_all_injuries()
        
        for team_raw, injuries in all_injuries.items():
            victim_team = self._normalize_team(team_raw)
            
            # Se o time machucado não joga hoje, ignora
            if victim_team not in matchups: continue
            
            predator_team = matchups[victim_team]
            
            for inj in injuries:
                status = str(inj.get('status', '')).upper()
                name = inj.get('name', '')
                pos_raw = str(inj.get('position', '')).upper()
                
                # CRITÉRIO 1: Status OUT
                if any(x in status for x in ['OUT', 'INJ', 'DOUBT']):
                    
                    # CRITÉRIO 2: É Pivô? (Verificação Tripla)
                    is_center = False
                    
                    # A) Pelo Injury Report
                    if 'C' in pos_raw or 'CENTER' in pos_raw: is_center = True
                    
                    # B) Pelo Cache de Logs
                    if not is_center and name in self.logs:
                        log_pos = str(self.logs[name].get('position', '')).upper()
                        if 'C' in log_pos or 'CENTER' in log_pos: is_center = True
                    
                    # C) Lista VIP de Pivôs (Garante que seu log seja respeitado)
                    vip_centers = [
                        "NIKOLA JOKIC", "DOMANTAS SABONIS", "JAKOB POELTL", "WALKER KESSLER", 
                        "JUSUF NURKIC", "ZACH EDEY", "ISAIAH HARTENSTEIN", "IVICA ZUBAC", 
                        "ALPEREN SENGUN", "JOEL EMBIID", "DEANDRE AYTON", "JALEN DUREN"
                    ]
                    if name.upper() in vip_centers: is_center = True

                    if is_center:
                        # ACHAMOS UM VÁCUO! Busca Predador
                        predator = self._find_best_rebounder(predator_team)
                        
                        if predator:
                            avg_reb = self._get_avg_stat(predator, 'REB')
                            
                            # Filtro Mínimo
                            if avg_reb >= 6.0:
                                # Boost
                                boost = 2.0 if avg_reb > 9 else 1.5
                                target = math.ceil(avg_reb + boost)
                                moon = math.ceil(avg_reb + boost + 3)
                                
                                score = 85 
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
                                        "name": victim_team, 
                                        "missing": name, 
                                        "status": "🚨 OUT", 
                                        "logo": self.get_team_logo(victim_team)
                                    },
                                    "ladder": [
                                        f"✅ Base: {int(avg_reb)}+", 
                                        f"💰 Alvo: {target}+", 
                                        f"🚀 Lua: {moon}+"
                                    ],
                                    "impact": f"Sem {name}, {victim_team} perde proteção de aro."
                                })
                                break 
        return found

    # --- AUXILIARES ---
    def _get_avg_stat(self, player, stat):
        vals = self.logs.get(player, {}).get('logs', {}).get(stat, [])
        return sum(vals[:10])/len(vals[:10]) if vals else 0

    def _get_opponent(self, team):
        target = self._normalize_team(team)
        for g in self.games:
            h = self._normalize_team(g.get('home'))
            a = self._normalize_team(g.get('away'))
            if h == target: return a
            if a == target: return h
        return None

    def _find_best_rebounder(self, team):
        best, max_reb = None, 0
        target = self._normalize_team(team)
        # Varre logs procurando o melhor reboteiro daquele time
        for name, data in self.logs.items():
            if self._normalize_team(data.get('team')) == target:
                val = self._get_avg_stat(name, 'REB')
                if val > max_reb: max_reb = val; best = name
        return best
        
def show_nexus_page():
    # Dados
    full_cache = get_data_universal("real_game_logs")
    scoreboard = get_data_universal("scoreboard")
    
    # Header
    st.markdown("""
    <div style="text-align: center; padding: 20px;">
        <h1 style="color: white; font-size: 3rem; margin:0; font-family:sans-serif;">🧠 NEXUS INTELLIGENCE</h1>
        <p style="color: #94a3b8; font-weight: bold; letter-spacing: 3px;">MODO PREDADOR • PRECISÃO CIRÚRGICA</p>
    </div>
    """, unsafe_allow_html=True)

    if not full_cache:
        st.error("❌ Logs vazios.")
        return

    # Engine
    nexus = NexusEngine(full_cache, scoreboard or [])
    min_score = st.sidebar.slider("🎚️ Score Mínimo", 50, 100, 60)

    try:
        all_ops = nexus.run_nexus_scan()
        opportunities = [op for op in all_ops if op['score'] >= min_score]
    except Exception as e:
        st.error(f"Erro no Scan: {e}")
        return

    if not opportunities:
        st.info("Nenhuma oportunidade encontrada.")
        return

    # Render
    for op in opportunities:
        is_sgp = (op['type'] == 'SGP')
        color = op['color']
        icon = "⚡" if is_sgp else "🌪️"
        
        with st.container():
            # Linha Topo
            st.markdown(f"""<div style="border-top: 4px solid {color}; margin-top: 15px; margin-bottom: 5px;"></div>""", unsafe_allow_html=True)
            
            # Cabeçalho
            c1, c2 = st.columns([3, 1])
            c1.markdown(f"### {icon} {op['title']}")
            c2.markdown(f"<div style='background:{color}; color:black; font-weight:bold; padding:5px; text-align:center; border-radius:5px;'>SCORE {op['score']}</div>", unsafe_allow_html=True)
            
            col_hero, col_mid, col_target = st.columns([1, 0.4, 1])
            
            # --- HEROI ---
            with col_hero:
                ci1, ci2 = st.columns([0.4, 1])
                with ci1: st.image(op['hero']['logo'], width=40)
                with ci2: st.image(op['hero']['photo'], width=70)
                
                st.markdown(f"**{op['hero']['name']}**")
                st.caption(f"{op['hero'].get('role', op['hero'].get('status'))}")
                
                t_val = op['hero'].get('target', '')
                t_stat = op['hero'].get('stat', '')
                st.markdown(f"<div style='border:1px solid {color}; padding:2px; text-align:center; border-radius:5px; background:#1e293b;'><b>{t_val}</b> {t_stat}</div>", unsafe_allow_html=True)

            # --- MEIO ---
            with col_mid:
                st.markdown("<br><br>", unsafe_allow_html=True)
                if is_sgp:
                    st.markdown(f"<div style='text-align:center; font-size:1.5rem;'>🔗</div>", unsafe_allow_html=True)
                    st.markdown(f"<div style='text-align:center; font-size:0.8rem; color:#94a3b8;'>{op.get('synergy_txt', '')}</div>", unsafe_allow_html=True)
                else:
                    st.markdown(f"<div style='text-align:center; font-size:1.5rem;'>⚔️</div>", unsafe_allow_html=True)
                    st.markdown(f"<div style='text-align:center; font-size:0.8rem; color:#f87171;'>VS DEFESA</div>", unsafe_allow_html=True)

            # --- ALVO ---
            with col_target:
                if is_sgp:
                    ci3, ci4 = st.columns([1, 0.4])
                    with ci3: st.image(op['partner']['photo'], width=70)
                    with ci4: st.image(op['partner']['logo'], width=40)
                    
                    st.markdown(f"**{op['partner']['name']}**")
                    st.caption(f"{op['partner']['role']}")
                    
                    p_val = op['partner']['target']
                    p_stat = op['partner']['stat']
                    st.markdown(f"<div style='border:1px solid white; padding:2px; text-align:center; border-radius:5px; background:#1e293b;'><b>{p_val}</b> {p_stat}</div>", unsafe_allow_html=True)
                else:
                    cv1, cv2 = st.columns([0.4, 1])
                    with cv1: st.image(op['villain']['logo'], width=40)
                    with cv2: 
                        st.markdown(f"**{op['villain']['name']}**")
                        st.caption("Adversário")
                    
                    st.markdown(f"🚨 <span style='color:#f87171; font-weight:bold'>{op['villain']['status']}</span>", unsafe_allow_html=True)
                    st.caption(f"Sem: {op['villain']['missing']}")

            # --- RODAPÉ ---
            st.divider()
            if is_sgp:
                st.caption(" | ".join([f"✅ {b}" for b in op['badges']]))
            else:
                l1, l2, l3 = st.columns(3)
                for i, s in enumerate(op['ladder']):
                    s = s.replace(":", "")
                    if i==0: l1.info(s)
                    if i==1: l2.success(s)
                    if i==2: l3.warning(s)
                st.caption(f"📉 {op['impact']}")
# ============================================================================
# FUNÇÃO DE RENDERIZAÇÃO (REUTILIZÁVEL & BLINDADA)
# ============================================================================
def render_trinity_table(members, label_suffix="L10"):
    """Renderiza a tabela visual v7 com CSS forçado para estabilidade."""
    if not members:
        st.info(f"Nenhum jogador encontrado para o critério {label_suffix}.")
        return

    # Agrupamento por Jogador
    grouped_members = {}
    for m in members:
        p_name = m['player']
        if p_name not in grouped_members:
            grouped_members[p_name] = { 'meta': m, 'stats': [] }
        grouped_members[p_name]['stats'].append(m)

    logo_base = "https://a.espncdn.com/i/teamlogos/nba/500"
    
    # Loop de Jogadores
    for p_name, data in grouped_members.items():
        meta = data['meta']
        stats_list = data['stats']
        
        team_code = meta['team'].lower()
        # Correções de Logo
        if team_code == "uta": team_code = "utah"
        if team_code == "nop": team_code = "no"
        if team_code == "phx": team_code = "pho"
        if team_code == "was": team_code = "wsh"
        logo_url = f"{logo_base}/{team_code}.png"

        # Colunas Mestre (Identidade vs Dados)
        col_id, col_content = st.columns([2.2, 7.8])
        
        # ESQUERDA: Identidade
        with col_id:
            st.markdown(f"""
            <div style="display:flex; flex-direction:column; align-items:center; justify-content:center; height:100%; text-align:center; padding-top:8px;">
                <img src="{logo_url}" style="width:32px; height:32px; object-fit:contain; opacity: 0.9; margin-bottom:5px;">
                <div class="trin-name">{p_name}</div>
                <div class="trin-matchup">{meta['team']} vs {meta['opp']}</div>
            </div>
            """, unsafe_allow_html=True)

        # DIREITA: Dados
        with col_content:
            for idx, stat_item in enumerate(stats_list):
                venue_txt = "CASA" if "vs" in stat_item['game_str'] else "FORA"
                venue_icon = "🏠" if "vs" in stat_item['game_str'] else "✈️"
                
                # Colunas Internas (Layout Fixo)
                # [Forma 2.2] [Local 2.2] [H2H 2.2] [ALVO 3.4]
                c1, c2, c3, c4 = st.columns([2.2, 2.2, 2.2, 3.4])
                
                with c1:
                    st.markdown(f"<div class='stat-group'><span class='stat-lbl'>FORMA {label_suffix}</span><span class='stat-val'>{int(stat_item['floors']['Form'])}</span></div>", unsafe_allow_html=True)
                with c2:
                    st.markdown(f"<div class='stat-group'><span class='stat-lbl'>{venue_icon} {venue_txt}</span><span class='stat-val'>{int(stat_item['floors']['Venue'])}</span></div>", unsafe_allow_html=True)
                with c3:
                    st.markdown(f"<div class='stat-group'><span class='stat-lbl'>H2H</span><span class='stat-val'>{int(stat_item['floors']['H2H'])}</span></div>", unsafe_allow_html=True)
                
                with c4:
                    st.markdown(f"""
                    <div style="text-align:center;">
                        <div class="target-pill">
                            <div class="target-val">{stat_item['line']}+</div>
                            <div class="target-sub">{stat_item['stat']}</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                # Separador sutil entre linhas do mesmo jogador
                if idx < len(stats_list) - 1:
                    st.markdown("<div class='thin-sep' style='opacity:0.3; margin: 5px 0;'></div>", unsafe_allow_html=True)

        # Separador entre jogadores (Dourado sutil)
        st.markdown("<div class='thin-sep' style='background: linear-gradient(90deg, transparent, #D4AF37, transparent); opacity:0.4;'></div>", unsafe_allow_html=True)


# ============================================================================
# PÁGINA PRINCIPAL (CSS CORRIGIDO COM !IMPORTANT)
# ============================================================================
def show_trinity_club_page():
    st.markdown("## 🏆 Trinity Club (Consistência Extrema)")
    
    # --- CARREGAMENTO VIA SUPABASE ---
    full_cache = get_data_universal("real_game_logs", os.path.join("cache", "real_game_logs.json"))
    scoreboard = get_data_universal("scoreboard", os.path.join("cache", "scoreboard_today.json"))

    if not full_cache:
        st.warning("Aguardando dados...")
        return

    engine = TrinityEngine(full_cache, st.session_state.scoreboard)

    st.header("🏆 Trinity Club")
    st.caption("Analise a consistência dos jogadores em 3 horizontes temporais diferentes.")

    # --- CSS GLOBAL (Blindado com !important) ---
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Oswald:wght@400;500;600&family=Inter:wght@400;600&display=swap');
        
        /* Glossário & Layout */
        .glossary-box {
            background: rgba(255, 255, 255, 0.03); border-radius: 6px; padding: 8px 15px; margin-bottom: 20px;
            font-family: 'Inter', sans-serif; font-size: 10px; color: #64748B; display: flex; justify-content: space-between; border-left: 3px solid #D4AF37;
        }
        .glossary-item { display: flex; align-items: center; gap: 5px; }
        .gloss-icon { color: #D4AF37; font-weight: 600; }
        .thin-sep { height: 1px; background: rgba(255, 255, 255, 0.08); margin: 10px 0; }
        
        /* Identidade */
        .trin-name { 
            font-family: 'Oswald', sans-serif; 
            font-size: 14px !important; /* Forçado */
            color: #F8FAFC; font-weight: 500; text-transform: uppercase; line-height: 1.2; letter-spacing: 0.5px; 
        }
        .trin-matchup { font-size: 10px !important; color: #64748B; margin-top: 2px; }
        
        /* ESTATÍSTICAS (Fonte Fixada) */
        .stat-group { display: flex; flex-direction: column; }
        .stat-lbl { 
            font-family: 'Inter', sans-serif; 
            font-size: 9px !important; 
            color: #64748B; text-transform: uppercase; margin-bottom: 2px; 
        }
        .stat-val { 
            font-family: 'Oswald', sans-serif; 
            font-size: 16px !important; /* AQUI ESTAVA O PROBLEMA - Agora travado em 16px */
            color: #10B981; 
            font-weight: 500; 
        }
        
        /* ALVO */
        .target-pill { 
            background: rgba(212, 175, 55, 0.1); border-radius: 6px; padding: 4px 12px; 
            display: inline-block; text-align: center; border: 1px solid rgba(212, 175, 55, 0.15); 
        }
        .target-val { 
            font-family: 'Oswald', sans-serif; 
            font-size: 18px !important; /* Forçado */
            color: #D4AF37; font-weight: 600; line-height: 1.1; 
        }
        .target-sub { 
            font-size: 9px !important; 
            color: #D4AF37; opacity: 0.8; font-weight: 600; 
        }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div class="glossary-box">
        <div class="glossary-item"><span class="gloss-icon">📊 FORMA</span> Piso da Janela</div>
        <div class="glossary-item"><span class="gloss-icon">🏠 LOCAL</span> Piso Casa/Fora</div>
        <div class="glossary-item"><span class="gloss-icon">⚔️ H2H</span> Piso Vs Opp</div>
        <div class="glossary-item"><span class="gloss-icon">🛡️ ALVO</span> Meta Segura</div>
    </div>
    """, unsafe_allow_html=True)

    # --- ABAS DE NAVEGAÇÃO ---
    tab_l5, tab_l10, tab_l15 = st.tabs(["🔥 L5 (Momentum)", "⚖️ L10 (Padrão)", "🏛️ L15 (Sólido)"])
    
    with tab_l5:
        # window=5 -> Engine calcula piso dos últimos 5
        members_l5 = engine.scan_market(window=5)
        render_trinity_table(members_l5, "L5")
        
    with tab_l10:
        # window=10 -> Engine calcula piso dos últimos 10
        members_l10 = engine.scan_market(window=10)
        render_trinity_table(members_l10, "L10")
        
    with tab_l15:
        # window=15 -> Engine calcula piso dos últimos 15
        members_l15 = engine.scan_market(window=15)
        render_trinity_table(members_l15, "L15")

        
# ============================================================================
# STRATEGY ENGINE: 5/7/10 (VERSÃO FINAL - CORRIGIDA)
# ============================================================================
import os
import json
import unicodedata

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
# PÁGINA: MATRIZ 5-7-10 (ESCADINHA) - DIRECT VIEW
# ============================================================================
# ============================================================================
# PÁGINA: MATRIZ 5-7-10 (V29.0 - LEGEND & POLISH)
# ============================================================================
def show_matriz_5_7_10_page():
    import json
    import pandas as pd
    import numpy as np
    import unicodedata
    import re
    
    # --- 1. FUNÇÕES AUXILIARES ---
    def normalize_str(text):
        if not text: return ""
        try:
            text = str(text)
            text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
            return text.upper().strip()
        except: return ""

    def get_step_color(prob):
        if prob >= 80: return "#22c55e" # Green
        if prob >= 50: return "#eab308" # Yellow
        if prob >= 20: return "#ef4444" # Red
        return "#334155" # Grey

    # --- 2. CSS (LAYOUT ESCADINHA + LEGENDA) ---
    st.markdown("""
    <style>
        .matriz-title { font-family: 'Oswald'; font-size: 32px; color: #fff; margin-bottom: 0px; letter-spacing: 1px; }
        .matriz-sub { font-family: 'Nunito'; font-size: 14px; color: #94a3b8; margin-bottom: 15px; }
        
        /* LEGENDA */
        .legend-container {
            display: flex; gap: 15px; align-items: center; flex-wrap: wrap;
            background: rgba(255,255,255,0.05); padding: 8px 15px; border-radius: 6px;
            font-size: 11px; color: #cbd5e1; margin-bottom: 25px; border: 1px solid #334155;
        }
        .legend-item { display: flex; align-items: center; gap: 5px; }
        
        .section-header { 
            font-family: 'Oswald'; font-size: 20px; color: #e2e8f0; 
            border-bottom: 2px solid #334155; padding-bottom: 8px; margin-top: 20px; margin-bottom: 15px; 
            display: flex; align-items: center; gap: 10px;
        }

        /* CARD PRINCIPAL */
        .ladder-card {
            background-color: #1e293b;
            border-radius: 10px;
            padding: 12px;
            margin-bottom: 12px;
            border: 1px solid #334155;
            display: flex;
            align-items: center;
            flex-wrap: wrap; 
            gap: 15px;
            transition: transform 0.2s;
        }
        .ladder-card:hover { border-color: #6366f1; transform: translateY(-2px); }

        .player-box { display: flex; align-items: center; min-width: 200px; flex: 1; }
        .p-img {
            width: 55px; height: 55px; border-radius: 50%;
            border: 3px solid #0f172a; margin-right: 12px;
            object-fit: cover; background: #000; box-shadow: 0 0 0 2px #6366f1;
        }
        .p-name { font-family: 'Oswald'; font-size: 16px; color: #fff; line-height: 1.1; margin-bottom: 2px; }
        .p-meta { font-size: 11px; color: #94a3b8; font-weight: bold; text-transform: uppercase; }
        
        .steps-container { display: flex; gap: 8px; flex: 2; justify-content: flex-end; align-items: center; }
        
        .step-item {
            display: flex; flex-direction: column; align-items: center; justify-content: center;
            background: rgba(0,0,0,0.3);
            border-radius: 6px;
            width: 70px; padding: 6px 0;
            border: 1px solid rgba(255,255,255,0.05);
        }
        .step-head { font-size: 10px; color: #cbd5e1; font-weight: bold; margin-bottom: 2px; }
        .step-val { font-family: 'Roboto Mono', monospace; font-size: 16px; font-weight: bold; }
        
        /* Badges */
        .badge-glue { background: #172554; color: #93c5fd; padding: 2px 6px; border-radius: 4px; font-size: 9px; font-weight:bold; border: 1px solid #3b82f6; }
        .badge-dyna { background: #450a0a; color: #fca5a5; padding: 2px 6px; border-radius: 4px; font-size: 9px; font-weight:bold; border: 1px solid #ef4444; }

    </style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="matriz-title">🏗️ MATRIZ 5-7-10</div>', unsafe_allow_html=True)
    st.markdown('<div class="matriz-sub">Análise de probabilidade em degraus ("Escadinha"). Base L25.</div>', unsafe_allow_html=True)

    # --- 3. LEGENDA EXPLICATIVA ---
    st.markdown("""
    <div class="legend-container">
        <div class="legend-item">📈 <b>Aquecendo:</b> Média Recente (L5) > Média Geral</div>
        <div class="legend-item">📉 <b>Esfriando:</b> Média Recente (L5) < Média Geral</div>
        <div class="legend-item">➡️ <b>Estável:</b> Performance Constante</div>
        <div style="flex-grow:1"></div>
        <div class="legend-item"><span class="badge-glue">🧪 GLUE</span> Seguro no 5+</div>
        <div class="legend-item"><span class="badge-dyna">🧨 BOOM</span> Explosivo no 10+</div>
    </div>
    """, unsafe_allow_html=True)

    # --- 4. PREPARAÇÃO DE DADOS ---
    
    # A. Scoreboard
    scoreboard = st.session_state.get('scoreboard', [])
    if not scoreboard:
        st.warning("⚠️ Scoreboard vazio. Atualize os jogos na aba Config.")
        return

    TEAMS_PLAYING_TODAY = set()
    for g in scoreboard:
        TEAMS_PLAYING_TODAY.add(g['home'].upper())
        TEAMS_PLAYING_TODAY.add(g['away'].upper())
        # Aliases
        aliases = {"GS": "GSW", "NY": "NYK", "NO": "NOP", "SA": "SAS", "PHO": "PHX", "WSH": "WAS", "CHO": "CHA", "UTAH": "UTA", "BRK": "BKN"}
        for k, v in aliases.items():
            if g['home'].upper() == k: TEAMS_PLAYING_TODAY.add(v)
            if g['away'].upper() == k: TEAMS_PLAYING_TODAY.add(v)

    # B. Mapa de IDs (L5)
    df_l5 = st.session_state.get('df_l5', pd.DataFrame())
    PLAYER_ID_MAP = {}
    PLAYER_TEAM_MAP = {}
    if not df_l5.empty:
        try:
            df_norm = df_l5.copy()
            df_norm['PLAYER_NORM'] = df_norm['PLAYER'].apply(normalize_str)
            PLAYER_ID_MAP = dict(zip(df_norm['PLAYER_NORM'], df_norm['PLAYER_ID']))
            PLAYER_TEAM_MAP = dict(zip(df_norm['PLAYER_NORM'], df_norm['TEAM']))
        except: pass

    # --- 5. ENGINE DE PROCESSAMENTO ---
    
    with st.spinner("Calculando probabilidades..."):
        # 1. Lesões (Shield V27)
        banned_players = set()
        try:
            fresh_inj = get_data_universal('injuries_cache_v44') or get_data_universal('injuries_data')
            if fresh_inj: st.session_state['injuries_data'] = fresh_inj
            
            raw_inj = st.session_state.get('injuries_data', [])
            flat_inj = []
            if isinstance(raw_inj, dict):
                for t in raw_inj.values(): 
                    if isinstance(t, list): flat_inj.extend(t)
            elif isinstance(raw_inj, list):
                flat_inj = raw_inj
            
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

        # 2. Logs
        raw_logs = get_data_universal('real_game_logs')
        if not raw_logs:
            st.error("Logs L25 não disponíveis.")
            return

        reb_ladder = []
        ast_ladder = []

        for p_name, p_data in raw_logs.items():
            if not isinstance(p_data, dict): continue
            
            norm_name = normalize_str(p_name)
            
            # Filtro Lesão
            if norm_name in banned_players: continue
            
            # Filtro Joga Hoje
            team = str(p_data.get('team', 'UNK')).upper().strip()
            if team in ['UNK', 'NONE']: team = PLAYER_TEAM_MAP.get(norm_name, 'UNK')
            
            is_playing = False
            if team in TEAMS_PLAYING_TODAY: is_playing = True
            else:
                for t in TEAMS_PLAYING_TODAY:
                    if t in team or team in t: 
                        is_playing = True; break
            
            if not is_playing: continue

            logs = p_data.get('logs', {})
            if not logs: continue
            
            # --- PROCESSA REBOTES ---
            vals_reb = logs.get('REB', [])
            if vals_reb:
                clean_reb = [float(x) for x in vals_reb if x is not None]
                if len(clean_reb) >= 10:
                    arr = np.array(clean_reb)
                    n = len(arr)
                    p5 = (np.sum(arr >= 5) / n) * 100
                    p7 = (np.sum(arr >= 7) / n) * 100
                    p10 = (np.sum(arr >= 10) / n) * 100
                    
                    if p5 >= 75 or p10 >= 20:
                        l5 = np.mean(arr[:5]) if n >= 5 else np.mean(arr)
                        l25 = np.mean(arr)
                        p_id = PLAYER_ID_MAP.get(norm_name, 0)
                        
                        item = {
                            "name": p_name, "id": int(p_id), "team": team,
                            "p5": p5, "p7": p7, "p10": p10,
                            "trend": l5 - l25,
                            "type": "GLUE" if p5 >= 80 else "DYNAMITE"
                        }
                        reb_ladder.append(item)

            # --- PROCESSA ASSISTÊNCIAS ---
            vals_ast = logs.get('AST', [])
            if vals_ast:
                clean_ast = [float(x) for x in vals_ast if x is not None]
                if len(clean_ast) >= 10:
                    arr = np.array(clean_ast)
                    n = len(arr)
                    p5 = (np.sum(arr >= 5) / n) * 100
                    p7 = (np.sum(arr >= 7) / n) * 100
                    p10 = (np.sum(arr >= 10) / n) * 100
                    
                    if p5 >= 75 or p10 >= 20:
                        l5 = np.mean(arr[:5]) if n >= 5 else np.mean(arr)
                        l25 = np.mean(arr)
                        p_id = PLAYER_ID_MAP.get(norm_name, 0)
                        
                        item = {
                            "name": p_name, "id": int(p_id), "team": team,
                            "p5": p5, "p7": p7, "p10": p10,
                            "trend": l5 - l25,
                            "type": "GLUE" if p5 >= 80 else "DYNAMITE"
                        }
                        ast_ladder.append(item)

    # --- 6. RENDERIZAÇÃO ---
    
    # Ordenação
    reb_ladder.sort(key=lambda x: x['p10'] if x['type'] == 'DYNAMITE' else x['p5'], reverse=True)
    ast_ladder.sort(key=lambda x: x['p10'] if x['type'] == 'DYNAMITE' else x['p5'], reverse=True)

    def render_ladder_card(p):
        photo = "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"
        if p['id'] != 0:
            photo = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{p['id']}.png"
            
        c5 = get_step_color(p['p5'])
        c7 = get_step_color(p['p7'])
        c10 = get_step_color(p['p10'])
        
        trend_icon = "➡️"
        if p['trend'] > 0.5: trend_icon = "📈" 
        elif p['trend'] < -0.5: trend_icon = "📉"
        
        badge_html = f'<span class="badge-glue">🧪 GLUE</span>' if p['type'] == 'GLUE' else f'<span class="badge-dyna">🧨 BOOM</span>'

        st.markdown(f"""
        <div class="ladder-card">
            <div class="player-box">
                <img src="{photo}" class="p-img" onerror="this.src='https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png';">
                <div>
                    <div class="p-name">{p['name']} {trend_icon}</div>
                    <div class="p-meta">{p['team']} {badge_html}</div>
                </div>
            </div>
            <div class="steps-container">
                <div class="step-item">
                    <div class="step-head">5+</div>
                    <div class="step-val" style="color:{c5}">{p['p5']:.0f}%</div>
                </div>
                <div class="step-item">
                    <div class="step-head">7+</div>
                    <div class="step-val" style="color:{c7}">{p['p7']:.0f}%</div>
                </div>
                <div class="step-item">
                    <div class="step-head">10+</div>
                    <div class="step-val" style="color:{c10}">{p['p10']:.0f}%</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # --- SEÇÃO REBOTES ---
    st.markdown('<div class="section-header">🛡️ REBOTES <span style="font-size:12px; color:#64748b; margin-left:10px;">(Escada 5 / 7 / 10)</span></div>', unsafe_allow_html=True)
    if reb_ladder:
        for p in reb_ladder: render_ladder_card(p)
    else:
        st.info("Nenhum jogador de Rebotes atingiu os critérios hoje.")

    # --- SEÇÃO ASSISTÊNCIAS ---
    st.markdown('<div class="section-header">🎨 ASSISTÊNCIAS <span style="font-size:12px; color:#64748b; margin-left:10px;">(Escada 5 / 7 / 10)</span></div>', unsafe_allow_html=True)
    if ast_ladder:
        for p in ast_ladder: render_ladder_card(p)
    else:
        st.info("Nenhum jogador de Assistências atingiu os critérios hoje.")
        
        
        
# ==============================================================================
# ☢️ HIT PROP HUNTER V47.3 - FINAL INTEGRAL FIX
# ==============================================================================

import os
import json
import time
import hashlib
import statistics
import concurrent.futures
import requests
import random
import uuid
from datetime import datetime, timedelta

# --- UI FALLBACK ---
try:
    import streamlit as st
    import pandas as pd
except ImportError:
    class _MockSt:
        def __getattr__(self, name): return lambda *a, **k: None
    st = _MockSt()
    pd = None

# --- PINNACLE CLIENT ---
try:
    from pinnacle_client import PinnacleClient
    PINNACLE_AVAILABLE = True
except ImportError:
    PINNACLE_AVAILABLE = False
    class PinnacleClient:
        def __init__(self, *args, **kwargs): pass
        def get_nba_games(self): return []
        def get_player_props(self, game_id): return []

# ==============================================================================
# 0. HELPERS
# ==============================================================================

def normalize_name(n: str) -> str:
    import re, unicodedata
    if not n: return ""
    n = str(n).lower().replace(".", " ").replace(",", " ").replace("-", " ")
    n = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", n)
    n = unicodedata.normalize("NFKD", n).encode("ascii", "ignore").decode("ascii")
    return " ".join(n.split())

def get_current_season_str(): return '2025-26'

def fix_team_abbr(abbr):
    if not abbr: return "UNK"
    abbr = abbr.upper().strip()
    mapping = {'GS': 'GSW', 'NO': 'NOP', 'NY': 'NYK', 'SA': 'SAS', 'PHO': 'PHX', 'WSH': 'WAS', 'BK': 'BKN', 'UTA': 'UTA'}
    return mapping.get(abbr, abbr)

def generate_stable_key(prefix, player, extra=""):
    return hashlib.md5(f"{prefix}_{player}_{extra}".encode()).hexdigest()

def resolve_game_info(game_id):
    """Busca informações do jogo na memória global para evitar UNK @ UNK."""
    if 'scoreboard' in st.session_state:
        for g in st.session_state['scoreboard']:
            if str(g.get('game_id')) == str(game_id):
                return g
    return {"home": "UNK", "away": "UNK", "game_str": "UNK @ UNK"}

# ==============================================================================
# 1. INTELLIGENCE MODULES
# ==============================================================================

class OddsManager:
    def __init__(self):
        base = os.path.dirname(__file__) if '__file__' in globals() else os.getcwd()
        self.cache_file = os.path.join(base, "cache", "pinnacle_cache.json")
        if not os.path.exists(os.path.dirname(self.cache_file)): os.makedirs(os.path.dirname(self.cache_file))
        self._memory_cache = {}

    def load_odds(self):
        if self._memory_cache: return self._memory_cache
        try:
            with open(self.cache_file, 'r') as f:
                data = json.load(f)
                # Cache válido por 4 horas
                if datetime.now() - datetime.fromisoformat(data.get('updated_at', '2000-01-01')) < timedelta(hours=4):
                    self._memory_cache = data.get('props', {})
                    return self._memory_cache
        except: pass
        return {}

    def force_update(self):
        try:
            client = RobustPinnacleClient()
            games = client.get_nba_games()
            if not games: return False, "Nenhum jogo encontrado na API."

            full = {}
            progress = st.progress(0); status = st.empty()
            total_props = 0
            
            for i, g in enumerate(games):
                status.text(f"📡 Varrendo Odds: {g['away_team']} @ {g['home_team']}")
                props = client.get_player_props(g['game_id'])
                
                for p in props:
                    name = normalize_name(p['player'])
                    mkt = p['market'].upper()
                    if name not in full: full[name] = {}
                    if mkt not in full[name]: full[name][mkt] = []
                    # Armazena como lista de dicionários
                    full[name][mkt].append({
                        "line": float(p['line']), "odds": float(p['odds']), "game_id": g['game_id']
                    })
                    total_props += 1
                progress.progress((i+1)/len(games))
            
            with open(self.cache_file, 'w') as f: 
                json.dump({"updated_at": datetime.now().isoformat(), "props": full}, f)
            
            self._memory_cache = full
            progress.empty(); status.empty()
            return True, f"Sucesso! {total_props} linhas atualizadas."
        except Exception as e: return False, str(e)

    # --- MÉTODO NOVO (USADO PELA MATRIX/TRIDENT) ---
    def match_odds(self, player_name, stat, target_line):
        props_db = self.load_odds()
        n_name = normalize_name(player_name)
        
        # Busca Fuzzy Inteligente
        player_data = props_db.get(n_name)
        if not player_data:
            for k in props_db.keys():
                if (len(n_name) > 3 and n_name in k) or (len(k) > 3 and k in n_name):
                    player_data = props_db[k]; break
        
        if not player_data: return 1.0, 0
        lines = player_data.get(stat, [])
        if not lines: return 1.0, 0
        
        best_match = None; min_diff = 999
        for item in lines:
            mkt_line = item['line']
            diff = abs(mkt_line - target_line)
            if diff < 0.1: return item['odds'], mkt_line
            if diff < min_diff and mkt_line <= target_line + 1.5:
                min_diff = diff; best_match = item
        
        if best_match: return best_match['odds'], best_match['line']
        return 1.0, 0

    # --- MÉTODO LEGADO (RESTAURADO PARA STAIRWAY/SNIPER) ---
    def get_market_data(self, player_name, stat):
        """Retorna o primeiro dado de mercado encontrado para compatibilidade."""
        props_db = self.load_odds()
        n_name = normalize_name(player_name)
        
        # Mesma lógica fuzzy
        player_data = props_db.get(n_name)
        if not player_data:
            for k in props_db.keys():
                if (len(n_name) > 3 and n_name in k) or (len(k) > 3 and k in n_name):
                    player_data = props_db[k]; break
        
        if not player_data: return None
        
        lines = player_data.get(stat, [])
        if not lines: return None
        
        # Retorna o primeiro objeto da lista para satisfazer códigos antigos que esperam um dict direto
        return lines[0]

# Instância Global Obrigatória
odds_manager = OddsManager()

class DvPIntelligence:
    def __init__(self):
        self.backup_data = { 
            "WAS": {"PG": 29, "SG": 30, "SF": 28, "PF": 30, "C": 30},
            "DET": {"PG": 26, "SG": 25, "SF": 22, "PF": 26, "C": 24},
            "ATL": {"PG": 27, "SG": 28, "SF": 25, "PF": 29, "C": 18},
            "ORL": {"PG": 1, "SG": 1, "SF": 1, "PF": 1, "C": 1},
            "UTA": {"PG": 28, "SG": 29, "SF": 30, "PF": 28, "C": 26}
        }
    def get_rank(self, team_abbr, pos):
        return self.backup_data.get(fix_team_abbr(team_abbr), {}).get(pos, 15)

dvp_intel = DvPIntelligence()

class SafeInjuryWrapper:
    def __init__(self):
        try: import injuries; self.real = injuries.InjuryMonitor(); self.active = True
        except: self.active = False
    def is_player_blocked(self, name, team):
        return self.real.is_player_blocked(name, team) if self.active else False

med_staff = SafeInjuryWrapper()

# ==============================================================================
# 2. AUDITORIA UNIVERSAL
# ==============================================================================

def safe_save_audit(ticket_data):
    try:
        base_dir = os.path.dirname(__file__) if '__file__' in globals() else os.getcwd()
        cache_dir = os.path.join(base_dir, "cache")
        if not os.path.exists(cache_dir): os.makedirs(cache_dir)
        path = os.path.join(cache_dir, "audit_trixies.json")
        
        current = []
        if os.path.exists(path):
            try: 
                with open(path, 'r', encoding='utf-8') as f: 
                    current = json.load(f)
            except: current = []
        
        raw_legs = ticket_data.get('legs', [])
        if not raw_legs: return False
        
        # 1. Resolução do Jogo
        game_id = "UNK"
        game_str = "UNK @ UNK"
        all_game_strs = set()
        
        for item in raw_legs:
            gid = str(item.get('game_id', 'UNK'))
            if gid != 'UNK': game_id = gid
            
            gstr = item.get('game_display') or item.get('game_str')
            if gstr and '@' in gstr: all_game_strs.add(gstr)
            elif gid != 'UNK':
                res = resolve_game_info(gid)
                if res['game_str'] != "UNK @ UNK": all_game_strs.add(res['game_str'])
        
        if all_game_strs: game_str = " | ".join(sorted(list(all_game_strs)))
        elif len(all_game_strs) == 1: game_str = list(all_game_strs)[0]

        # 2. Processamento das Pernas
        formatted_legs = []
        calculated_odd = 1.0
        portfolio = ticket_data.get('portfolio', 'CUSTOM')
        
        for leg in raw_legs:
            if 'props' in leg: # SGP
                for prop in leg['props']:
                    line = float(prop.get('line', 0))
                    if line <= 0: continue
                    formatted_legs.append({
                        "player_name": leg.get('player'), "team": leg.get('team'), "market_type": prop['stat'],
                        "market_display": f"{int(line)}+ {prop['stat']}", "line": line,
                        "odds": 1.40, "thesis": "SGP Lab", "status": "PENDING", "game_id": game_id
                    })
                    calculated_odd *= 1.40
            elif 'components' in leg: # Trident
                for stat, line_val in leg['components']:
                    line = float(line_val)
                    if line <= 0: continue
                    formatted_legs.append({
                        "player_name": leg.get('player'), "team": leg.get('team'), "market_type": stat,
                        "market_display": f"{int(line)}+ {stat}", "line": line,
                        "odds": 1.50, "thesis": "Trident", "status": "PENDING", "game_id": game_id
                    })
                    calculated_odd *= 1.50
            else: # Standard
                stat = leg.get('stat') or leg.get('market_type', 'UNK')
                line = float(leg.get('line', 0))
                if line <= 0: continue
                role = leg.get('role', 'Standard')
                leg_odd = float(leg.get('real_odd', 0))
                if leg_odd == 0: leg_odd = 1.40 if role == 'ANCHOR' else 1.50
                calculated_odd *= leg_odd
                
                formatted_legs.append({
                    "player_name": leg.get('player', 'Unknown'), "team": leg.get('team', 'UNK'),
                    "market_type": stat, "market_display": f"{int(line) if line.is_integer() else line}+ {stat}",
                    "line": line, "odds": round(leg_odd, 2), "thesis": f"{role} Pick",
                    "status": "PENDING", "game_id": str(leg.get('game_id', game_id))
                })

        # 3. Objeto Final
        ticket_id = f"TKT_{int(time.time())}_{str(uuid.uuid4())[:6]}"
        home, away = "UNK", "UNK"
        if " @ " in game_str:
            parts = game_str.split(" | ")[0].split(" @ ")
            if len(parts) == 2: away, home = parts[0], parts[1]

        new_ticket = {
            "id": ticket_id, "created_at": datetime.now().isoformat(), "date": datetime.now().strftime("%Y-%m-%d"),
            "category": portfolio, "total_odd": round(calculated_odd, 2), "status": "PENDING",
            "legs": formatted_legs, "game_info": {"game_id": game_id, "home": home, "away": away, "matchup_str": game_str}
        }

        current.append(new_ticket)
        with open(path, 'w', encoding='utf-8') as f: json.dump(current, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"Audit Error: {e}")
        return False

def repair_audit_file(): return True, "OK"

# ==============================================================================
# 3. DATA FETCHING (RESTAURADO)
# ==============================================================================

def get_games_safe():
    try:
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={datetime.now().strftime('%Y%m%d')}"
        data = requests.get(url, timeout=5).json()
        games = []
        for e in data['events']:
            c = e['competitions'][0]
            games.append({
                "home": fix_team_abbr(c['competitors'][0]['team']['abbreviation']),
                "away": fix_team_abbr(c['competitors'][1]['team']['abbreviation']),
                "game_id": str(e['id']),
                "game_str": f"{fix_team_abbr(c['competitors'][1]['team']['abbreviation'])} @ {fix_team_abbr(c['competitors'][0]['team']['abbreviation'])}"
            })
        return games
    except: return []

def get_player_logs(name):
    try:
        from nba_api.stats.static import players
        from nba_api.stats.endpoints import playergamelog
        p = players.find_players_by_full_name(name)
        if not p: return None
        df = playergamelog.PlayerGameLog(player_id=p[0]['id'], season=get_current_season_str()).get_data_frames()[0].head(30)
        mins = [float(x.split(':')[0]) + float(x.split(':')[1])/60 if ':' in str(x) else float(x) for x in df['MIN']]
        return {
            "PTS": df['PTS'].tolist(), "REB": df['REB'].tolist(), "AST": df['AST'].tolist(),
            "3PM": df['FG3M'].tolist(), "3PA": df['FG3A'].tolist(), "MIN_AVG": sum(mins)/len(mins) if mins else 0,
            "STL": df['STL'].tolist(), "BLK": df['BLK'].tolist()
        }
    except: return None

def update_batch_cache(games_list):
    """
    Atualiza o cache de logs (L25) buscando dados da NBA e SALVA NA NUVEM.
    """
    # Tenta carregar o cache atual (Nuvem -> Local)
    full_cache = get_data_universal(KEY_LOGS, LOGS_CACHE_FILE) or {}
    
    status = st.status("🔄 Conectando aos satélites da NBA...", expanded=True)
    
    players_queue = []
    # Identifica jogadores dos jogos de hoje
    for g in games_list:
        h = fix_team_abbr(g.get('home', 'UNK'))
        a = fix_team_abbr(g.get('away', 'UNK'))
        if h == "UNK": continue
        try:
            # Mock roster fetch para não quebrar se faltar libs
            from nba_api.stats.endpoints import commonteamroster
            from nba_api.stats.static import teams
            
            # Tenta pegar ID do time
            t_list = teams.get_teams()
            t_id = next((t['id'] for t in t_list if t['abbreviation'] == h), None)
            
            if t_id:
                roster = commonteamroster.CommonTeamRoster(team_id=t_id, season=SEASON).get_data_frames()[0]
                for _, r in roster.iterrows(): players_queue.append(r['PLAYER'])
            
            t_id_a = next((t['id'] for t in t_list if t['abbreviation'] == a), None)
            if t_id_a:
                roster_a = commonteamroster.CommonTeamRoster(team_id=t_id_a, season=SEASON).get_data_frames()[0]
                for _, r in roster_a.iterrows(): players_queue.append(r['PLAYER'])
        except: pass
    
    # Filtra quem precisa atualizar
    pending = [p for p in set(players_queue) if p not in full_cache]
    
    if not pending:
        status.update(label="✅ Cache já estava atualizado!", state="complete", expanded=False)
        time.sleep(1); return

    status.write(f"⚡ Baixando {len(pending)} jogadores novos...")
    bar = status.progress(0)
    
    def fetch_task(p_name):
        return (p_name, get_player_logs(p_name))

    # Executa o download em paralelo
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_task, p): p for p in pending}
        completed = 0
        for f in concurrent.futures.as_completed(futures):
            res = f.result()
            if res[1]:
                # Estrutura do log
                full_cache[res[0]] = {
                    "name": res[0], 
                    "team": "UNK", # Opcional: atualizar time real se possível
                    "logs": res[1], 
                    "updated": str(datetime.now())
                }
            completed += 1
            bar.progress(completed / len(pending))
            
    # --- SALVAMENTO CRÍTICO NA NUVEM ---
    # Aqui estava o problema: antes salvava só local. Agora salva universal.
    save_data_universal(KEY_LOGS, full_cache, LOGS_CACHE_FILE)
    
    status.update(label="✅ Nuvem Sincronizada!", state="complete", expanded=False)
    time.sleep(1)
# ==============================================================================
# 4. ENGINES & ANALYTICS
# ==============================================================================

class MatrixEngine:
    def __init__(self): 
        self.trident_priority = 1000  # Peso máximo para Tridents
        self.debug = True  # Modo diagnóstico
        
    def _calculate_smart_floor(self, logs, stat_type, span=25):
        """Calcula o piso seguro baseado no histórico, removendo outliers."""
        if not logs: return 0
        vals = logs.get(stat_type, [])
        if not vals or len(vals) < 5: return 0
        
        # Pegar apenas os últimos 'span' jogos e ordenar
        sorted_vals = sorted(vals[:span])
        
        # Lógica de corte (Trim) para remover outliers
        if len(sorted_vals) >= 15:
            core = sorted_vals[2:-2] # Remove 2 piores e 2 melhores
        elif len(sorted_vals) >= 8:
            core = sorted_vals[1:-1] # Remove 1 pior e 1 melhor
        else:
            core = sorted_vals # Amostra pequena
            
        return int(core[0]) if core else 0
    
    def analyze_market_pool(self, cache_data, games, tridents):
        """Analisa o pool e classifica em Anchors e Boosters."""
        import streamlit as st
        om = globals().get('odds_manager')
        game_map = get_game_info_map(games)
        
        # 1. MAPEAMENTO DE TRIDENTS (A ELITE)
        trident_map = {}
        for t in tridents:
            p_data = cache_data.get(t['player'])
            if not p_data: continue
            
            logs = p_data.get('logs', {})
            safe_pts = self._calculate_smart_floor(logs, 'PTS', 15)
            if safe_pts < 5: continue
            
            # Monta as pernas do Trident
            legs = []
            # Leg de Pontos
            real_odd, mkt_line = om.match_odds(t['player'], "PTS", safe_pts)
            legs.append({"stat": "PTS", "line": safe_pts, "real_odd": real_odd})
            
            # Legs dos componentes (REB/AST/3PM)
            for stat, min_line in t.get('components', []):
                stat_floor = self._calculate_smart_floor(logs, stat, 15)
                # Usa o maior entre o requisito do Trident e o piso calculado
                target = max(min_line, stat_floor)
                ro, ml = om.match_odds(t['player'], stat, target)
                legs.append({"stat": stat, "line": target, "real_odd": ro})
            
            # Garante formato padrão
            trident_map[t['player']] = {
                "name": t['player'], "team": t['team'],
                "game_id": t['game_id'], "game_str": t['game_display'],
                "role": "TRIDENT", "quality": 500 + safe_pts, # Prioridade alta
                "legs": legs[:2], "hit_rate": t.get('hit_rate', 0.8)
            }

        # 2. VARREDURA GERAL (O RESTO DO CACHE)
        anchors_raw = []
        boosters_raw = []
        
        for name, p_data in cache_data.items():
            if name in trident_map: continue # Já processado
            
            team = fix_team_abbr(p_data.get('team', 'UNK'))
            g_info = game_map.get(team)
            
            # Modo Simulação: Se não tem info de jogo, cria mock para não travar
            if not g_info: 
                if not games: g_info = {"game_id": f"MOCK_{team}", "game_str": f"OPP @ {team}"}
                else: continue
            
            logs = p_data.get('logs', {})
            safe_pts = self._calculate_smart_floor(logs, 'PTS', 20)
            if safe_pts < 6: continue
            
            # Classificação
            is_anchor = safe_pts >= 16
            is_booster = safe_pts >= 8
            
            if not (is_anchor or is_booster): continue
            
            # Monta Legs
            legs = []
            ro, _ = om.match_odds(name, "PTS", safe_pts)
            legs.append({"stat": "PTS", "line": safe_pts, "real_odd": ro})
            
            # Secundárias
            safe_reb = self._calculate_smart_floor(logs, 'REB', 20)
            safe_ast = self._calculate_smart_floor(logs, 'AST', 20)
            
            if safe_reb >= 4:
                ro, _ = om.match_odds(name, "REB", safe_reb)
                legs.append({"stat": "REB", "line": safe_reb, "real_odd": ro})
            elif safe_ast >= 3:
                ro, _ = om.match_odds(name, "AST", safe_ast)
                legs.append({"stat": "AST", "line": safe_ast, "real_odd": ro})
            
            player_obj = {
                "name": name, "team": team, 
                "game_id": g_info.get('game_id'), "game_str": g_info.get('game_str'),
                "legs": legs[:2]
            }
            
            if is_anchor and len(legs) >= 1:
                player_obj['role'] = "ANCHOR"
                player_obj['quality'] = safe_pts * 2
                anchors_raw.append(player_obj)
            
            if is_booster and len(legs) >= 1:
                player_obj['role'] = "BOOSTER"
                player_obj['quality'] = safe_pts
                boosters_raw.append(player_obj)

        # 3. FUSÃO (Tridents + Raw)
        final_anchors = []
        final_boosters = []
        
        # Tridents viram Anchors ou Boosters dependendo da força
        for t in trident_map.values():
            pts_leg = next((l for l in t['legs'] if l['stat'] == 'PTS'), None)
            if pts_leg and pts_leg['line'] >= 15:
                t['role'] = 'ANCHOR (TRIDENT)'
                final_anchors.append(t)
            else:
                t['role'] = 'BOOSTER (TRIDENT)'
                final_boosters.append(t)
                
        # Adiciona os Raw
        final_anchors.extend(anchors_raw)
        
        # Adiciona Boosters Raw (evitando duplicatas de nome se houver sobreposição)
        seen_boosters = {b['name'] for b in final_boosters}
        for b in boosters_raw:
            if b['name'] not in seen_boosters:
                final_boosters.append(b)
                seen_boosters.add(b['name'])
        
        # Ordenação por Qualidade
        final_anchors.sort(key=lambda x: x['quality'], reverse=True)
        final_boosters.sort(key=lambda x: x['quality'], reverse=True)
        
        if self.debug:
            st.sidebar.markdown(f"**Diagnóstico Matriz:**")
            st.sidebar.text(f"⚓ Anchors: {len(final_anchors)}")
            st.sidebar.text(f"🚀 Boosters: {len(final_boosters)}")
            
        return {"ANCHORS": final_anchors, "BOOSTERS": final_boosters}
    
    def _is_valid_combo(self, anchor, combo):
        # 1. Anti-Canibalismo (Mesmo Time)
        teams = {anchor['team']}
        for p in combo:
            if p['team'] in teams: return False
            teams.add(p['team'])
            
        # 2. Diversidade de Jogos (Pelo menos 2 jogos diferentes no bilhete)
        games = {anchor['game_id']}
        for p in combo: games.add(p['game_id'])
        if len(games) < 2: return False
        
        return True

    def _score_combo(self, anchor, combo):
        score = 0
        all_legs = anchor['legs'] + [l for p in combo for l in p['legs']]
        
        # Bônus por Trident
        if anchor.get('hit_rate'): score += 50
        score += sum(30 for p in combo if p.get('hit_rate'))
        
        # Bônus por Odds
        total_odd = 1.0
        for l in all_legs: total_odd *= l.get('real_odd', 1.0)
        
        if 2.5 <= total_odd <= 15.0: score += 40 # Sweet spot
        elif total_odd < 2.0: score -= 20 # Odd muito baixa
        
        return score

    def _get_best_combo(self, anchor, available_boosters):
        best_combo = None
        best_score = -float('inf')
        
        # Tenta combinacoes de 3 boosters
        # Otimização: Pega os top 15 boosters para não explodir combinações
        candidates = available_boosters[:15]
        
        # Importante: itertools para gerar combinações limpas
        from itertools import combinations
        
        # Tenta achar combos de 3
        for combo in combinations(candidates, 3):
            if self._is_valid_combo(anchor, combo):
                score = self._score_combo(anchor, combo)
                if score > best_score:
                    best_score = score
                    best_combo = list(combo)
        
        # Fallback: Se não achou de 3, tenta de 2 (para garantir ticket)
        if not best_combo:
            for combo in combinations(candidates, 2):
                if self._is_valid_combo(anchor, combo):
                    best_combo = list(combo)
                    best_score = 0 # Score baixo mas existe
                    break
                    
        return best_combo, best_score

    def generate_smart_matrix(self, pool):
        tickets = []
        anchors = pool["ANCHORS"]
        boosters = pool["BOOSTERS"]
        
        used_keys = set()
        
        # Gera tickets para os Top 10 Anchors
        for i, anchor in enumerate(anchors[:10]):
            # Filtra boosters válidos para este anchor (jogo diferente)
            valid_boosters = [b for b in boosters if b['game_id'] != anchor['game_id']]
            
            if len(valid_boosters) < 2: continue
            
            # Ticket Alpha (Melhor Combinação)
            combo, score = self._get_best_combo(anchor, valid_boosters)
            
            if combo:
                # Cria chave única para evitar tickets duplicados
                ticket_key = tuple(sorted([anchor['name']] + [p['name'] for p in combo]))
                
                if ticket_key not in used_keys:
                    tkt = self._build_ticket(f"MTX_{i}", anchor, combo, "ALPHA", score)
                    tickets.append(tkt)
                    used_keys.add(ticket_key)
                    
                    # Tenta gerar uma variação (BETA) removendo o primeiro booster usado
                    if len(valid_boosters) > 4:
                        remain_boosters = [b for b in valid_boosters if b['name'] != combo[0]['name']]
                        combo_b, score_b = self._get_best_combo(anchor, remain_boosters)
                        if combo_b:
                             tkt_b = self._build_ticket(f"MTX_{i}_B", anchor, combo_b, "BETA", score_b)
                             tickets.append(tkt_b)

        return tickets

    def _build_ticket(self, tid, anchor, boosters, type_str, score):
        legs = []
        # Anchor Leg (Pega a melhor)
        aleg = anchor['legs'][0]
        legs.append({
            "player": anchor['name'], "team": anchor['team'], "role": "ANCHOR",
            "stat": aleg['stat'], "line": aleg['line'], "real_odd": aleg.get('real_odd', 1.0)
        })
        
        # Booster Legs
        for b in boosters:
            bleg = b['legs'][0]
            legs.append({
                "player": b['name'], "team": b['team'], "role": "BOOSTER",
                "stat": bleg['stat'], "line": bleg['line'], "real_odd": bleg.get('real_odd', 1.0)
            })
            
        total_odd = 1.0
        for l in legs: total_odd *= l['real_odd']
        
        return {
            "id": tid,
            "title": f"🔥 {anchor['name']} Squad",
            "type": type_str,
            "total_odd": round(total_odd, 2),
            "legs": legs,
            "score": score
        }

matrix_engine = MatrixEngine()

# ==============================================================================
# 5. GENERATORS (MISSING LINK FIXED)
# ==============================================================================
def get_game_info_map(games):
    """Auxiliar: Cria mapa de jogos por time para busca rápida."""
    info_map = {}
    for g in games:
        h = fix_team_abbr(g.get('home', 'UNK')); a = fix_team_abbr(g.get('away', 'UNK'))
        gid = g.get('game_id', 'UNK')
        info = {"game_id": gid, "game_str": f"{a} @ {h}", "home": h, "away": a}
        info_map[h] = info; info_map[a] = info
    return info_map

def infer_position(stats):
    """Auxiliar: Infere posição baseada em stats (Simplificado)."""
    if stats.get('ast_avg', 0) >= 5: return "PG"
    if stats.get('reb_avg', 0) >= 8: return "C"
    return "SF"

class TridentEngine:
    def __init__(self):
        self.archetypes = {
            "THE_GLUE_GUY": {"name": "The Glue Guy", "stats": ["PTS", "REB", "AST"], "min_lines": [4, 2, 1], "min_hit": 0.80},
            "MINI_SNIPER": {"name": "Mini Sniper", "stats": ["PTS", "3PM"], "min_lines": [6, 1], "min_hit": 0.80}
        }
    def find_tridents(self, cache_data, games):
        tridents = []
        game_info_map = get_game_info_map(games)
        teams_active = {fix_team_abbr(g.get('home', 'UNK')) for g in games} | {fix_team_abbr(g.get('away', 'UNK')) for g in games}
        for p_key, data in cache_data.items():
            name = data.get('name'); team = fix_team_abbr(data.get('team', ''))
            if team not in teams_active or med_staff.is_player_blocked(name, team): continue
            g_info = game_info_map.get(team, {"game_id": "UNK", "game_str": "UNK"})
            logs = data.get('logs', {})
            if len(logs.get('PTS', [])) < 10: continue
            for arch_key, arch in self.archetypes.items():
                stats = arch['stats']; min_reqs = arch['min_lines']; hits = 0
                for i in range(10):
                    if all(logs[s][i] >= m for s, m in zip(stats, min_reqs)): hits += 1
                if (hits/10) >= arch['min_hit']:
                    comps = [(s, m) for s, m in zip(stats, min_reqs)]
                    tridents.append({
                        "player": name, "team": team, "game_id": g_info['game_id'], 
                        "game_info": g_info, "game_display": g_info['game_str'],
                        "archetype": arch['name'], "components": comps, "hit_rate": hits/10
                    })
                    break 
        return sorted(tridents, key=lambda x: x['hit_rate'], reverse=True)

def generate_atomic_props(cache_data, games):
    atomic_props = []
    game_info_map = get_game_info_map(games)
    teams_active = {fix_team_abbr(g.get('home', 'UNK')) for g in games} | {fix_team_abbr(g.get('away', 'UNK')) for g in games}
    min_thresholds = {"PTS": 8, "REB": 3, "AST": 2, "3PM": 1, "STL": 1, "BLK": 1}
    for p_key, data in cache_data.items():
        name = data.get('name'); team = fix_team_abbr(data.get('team', ''))
        if team not in teams_active: continue
        g_info = game_info_map.get(team, {"game_id": "UNK", "game_str": "UNK"})
        logs = data.get('logs', {})
        for stat, min_req in min_thresholds.items():
            vals = logs.get(stat, [])
            if not vals: continue
            for period in [10, 15, 20]:
                if len(vals) >= period:
                    cut = vals[:period]; floor = min(cut)
                    if floor < min_req: continue
                    atomic_props.append({
                        "player": name, "team": team, "stat": stat, "line": int(floor),
                        "record": f"{period}/{period}", "streak_val": period, "hit_rate": 1.0,
                        "game_info": g_info, "game_display": g_info.get('game_str'), "game_id": g_info.get('game_id')
                    })
    return sorted(atomic_props, key=lambda x: (x['streak_val'], x['line']), reverse=True)

def organize_sgp_lab(atomic_props):
    perfect_props = [p for p in atomic_props if p['hit_rate'] >= 1.0]
    unique_map = {}
    for p in perfect_props:
        key = f"{p['player']}_{p['stat']}"
        if key not in unique_map or p['streak_val'] > unique_map[key]['streak_val']: unique_map[key] = p
    deduplicated = list(unique_map.values())
    sgp_structure = {}
    for p in deduplicated:
        game_key = p.get('game_display', 'UNK'); player = p['player']
        if game_key not in sgp_structure: sgp_structure[game_key] = {}
        if player not in sgp_structure[game_key]: 
            sgp_structure[game_key][player] = {"team": p['team'], "game_id": p['game_id'], "game_info": p['game_info'], "props": []}
        sgp_structure[game_key][player]['props'].append({"stat": p['stat'], "line": p['line'], "record": p['record'], "streak": p['streak_val']})
    final_output = {}
    for game, players in sgp_structure.items():
        sorted_players = []
        for pname, pdata in players.items():
            pdata['props'].sort(key=lambda x: 0 if x['stat']=='PTS' else 1)
            pdata_copy = pdata.copy(); pdata_copy['player'] = pname
            sorted_players.append(pdata_copy)
        sorted_players.sort(key=lambda x: len(x['props']), reverse=True)
        final_output[game] = sorted_players
    return final_output

def generate_stairway_data(cache_data, games):
    stairs = []
    game_info_map = get_game_info_map(games)
    teams_active = {fix_team_abbr(g.get('home', 'UNK')) for g in games} | {fix_team_abbr(g.get('away', 'UNK')) for g in games}
    for p_key, data in cache_data.items():
        name = data.get('name'); team = fix_team_abbr(data.get('team', ''))
        if team not in teams_active: continue
        g_info = game_info_map.get(team, {})
        logs = data.get('logs', {})
        if not logs or len(logs.get('PTS', [])) < 20: continue
        avg_pts = sum(logs['PTS'][:15]) / 15; avg_ast = sum(logs['AST'][:15]) / 15; avg_reb = sum(logs['REB'][:15]) / 15
        if not (avg_pts >= 15 or avg_ast >= 6 or avg_reb >= 8): continue
        opp_team = g_info.get('away') if team == g_info.get('home') else g_info.get('home')
        dvp_rank = dvp_intel.get_rank(opp_team, infer_position({'pts_avg': avg_pts, 'ast_avg': avg_ast, 'reb_avg': avg_reb}))
        period = min(25, len(logs['PTS']))
        for stat in ['PTS', 'REB', 'AST', '3PM']:
            if stat == 'PTS' and avg_pts < 12: continue
            vals = logs[stat][:period]; sorted_vals = sorted(vals)
            trim_count = 3 if len(vals) >= 20 else 2
            if len(sorted_vals) <= (trim_count * 2): continue
            trimmed_vals = sorted_vals[trim_count : -trim_count]
            safe_val = trimmed_vals[0]
            target_val = int(statistics.median(trimmed_vals))
            sky_val = trimmed_vals[-1]
            if safe_val == 0 and stat != '3PM': safe_val = 1
            if target_val <= safe_val: target_val = safe_val + 1
            if sky_val <= target_val: sky_val = target_val + 1
            market_info = odds_manager.get_market_data(name, stat)
            stairs.append({
                "player": name, "team": team, "game_id": g_info.get('game_id'), 
                "game_info": g_info, "game_display": g_info.get('game_str'),
                "stat": stat, "matchup_score": 8.0, "is_volume": False,
                "steps": [{"label": "SAFE", "line": safe_val}, {"label": "TARGET", "line": target_val}, {"label": "SKY", "line": sky_val}],
                "score": 95, "market_ref": market_info
            })
    return sorted(stairs, key=lambda x: x['matchup_score'], reverse=True)

def generate_sniper_data(cache_data, games):
    snipers = []
    game_info_map = get_game_info_map(games)
    teams_active = {fix_team_abbr(g.get('home', 'UNK')) for g in games} | {fix_team_abbr(g.get('away', 'UNK')) for g in games}
    for p_key, data in cache_data.items():
        name = data.get('name'); team = fix_team_abbr(data.get('team', ''))
        if team not in teams_active: continue
        g_info = game_info_map.get(team, {})
        logs = data.get('logs', {})
        if not logs or len(logs.get('PTS', [])) < 15: continue
        if '3PA' in logs:
            avg_3pa = sum(logs['3PA'][:10]) / 10
            if avg_3pa >= 5.0:
                avg_pts = sum(logs['PTS'][:15]) / 15
                player_pos = infer_position({'pts_avg': avg_pts, 'ast_avg': 0, 'reb_avg': 0})
                dvp_rank = dvp_intel.get_rank(g_info.get('away', 'UNK'), player_pos)
                makes = logs['3PM'][:15]; floor_3pm = sorted(makes)[2]; 
                if floor_3pm < 2: floor_3pm = 2
                archetype = "💎 Sniper Gem" if avg_3pa >= 7.0 else "🎯 High Volume"
                market_info = odds_manager.get_market_data(name, "3PM")
                snipers.append({
                    "player": name, "team": team, "stat": "3PM", 
                    "line": floor_3pm, "archetype": archetype,
                    "reason": f"Def #{dvp_rank}", "avg_vol": f"🎯 {avg_3pa:.1f} Att",
                    "game_id": g_info.get('game_id'), "game_info": g_info, "game_display": g_info.get('game_str'),
                    "confidence": 90, "market_ref": market_info, "real_odd": market_info.get('odds') if market_info else None
                })
    return sorted(snipers, key=lambda x: x['confidence'], reverse=True)

# ==============================================================================
# 6. VISUAL RENDERING
# ==============================================================================

def render_obsidian_matrix_card(player, team, items):
    rows_html = ""
    for item in items:
        s_color = "#4ade80" if item['stat'] == 'PTS' else "#60a5fa"
        steps_html = ""
        for step in item['steps']:
            steps_html += f"""<div style='flex:1;text-align:center;background:#0f172a;border-top:2px solid #10b981;margin:0 2px;padding:6px 2px;'><div style='font-size:0.55rem;color:#94a3b8;font-weight:700;'>{step['label']}</div><div style='font-size:1.2rem;font-weight:900;color:#f8fafc;'>{step['line']}+</div></div>"""
        rows_html += f"""<div style='display:flex;align-items:center;margin-bottom:8px;padding:4px;'><div style='width:50px;text-align:center;margin-right:6px;'><div style='font-weight:900;color:{s_color};font-size:0.8rem;'>{item['stat']}</div></div><div style='flex:1;display:flex;'>{steps_html}</div></div>"""
    st.markdown(f"""<div style="background:#1e293b;border:1px solid #334155;border-radius:8px;padding:12px;margin-bottom:4px;"><div style="display:flex;justify-content:space-between;margin-bottom:10px;"><div><span style="color:#f1f5f9;font-weight:800;font-size:1.1rem;">{player}</span> <span style="color:#64748b;font-size:0.8rem;">{team}</span></div></div><div>{rows_html}</div></div>""", unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    def get_legs(lbl):
        return [{"player": player, "team": team, "stat": i['stat'], "line": [s['line'] for s in i['steps'] if s['label'] == lbl][0], "game_id": i['game_id'], "game_display": i['game_display']} for i in items]
    with c1: 
        if st.button("🛡️ SAFE", key=generate_stable_key("s", player)): safe_save_audit({'portfolio': 'STAIRWAY_COMBO', 'legs': get_legs('SAFE')}); st.toast("Salvo!")
    with c2: 
        if st.button("🎯 TGT", key=generate_stable_key("t", player)): safe_save_audit({'portfolio': 'STAIRWAY_COMBO', 'legs': get_legs('TARGET')}); st.toast("Salvo!")
    with c3: 
        if st.button("🚀 SKY", key=generate_stable_key("k", player)): safe_save_audit({'portfolio': 'STAIRWAY_COMBO', 'legs': get_legs('SKY')}); st.toast("Salvo!")

def render_sniper_card(item, btn_key):
    st.markdown(f"""<div style="background:#1e293b;border:1px solid #334155;border-left:4px solid #06b6d4;border-radius:8px;padding:12px;margin-bottom:12px;"><div style="display:flex;justify-content:space-between;"><div><div style="color:#f1f5f9;font-weight:800;">{item['player']}</div><div style="color:#94a3b8;font-size:0.75rem;">{item['team']} • {item['archetype']}</div></div><div style="color:#06b6d4;font-weight:900;font-size:1.4rem;">{item['line']}+ {item['stat']}</div></div></div>""", unsafe_allow_html=True)
    if st.button("Adicionar", key=btn_key, use_container_width=True): safe_save_audit({'portfolio': 'SNIPER_GEM', 'legs': [item]}); st.toast("Salvo!")

def render_tactical_card(title, subtitle, badge_text, items, btn_key, btn_label, payload, portfolio_type):
    stats_html = ""
    for s, l, e in items:
        stats_html += f"""<div style='display:inline-block;margin-right:6px;background:#0f172a;border-top:2px solid #facc15;padding:4px 8px;'><span style='color:#f8fafc;font-weight:900;'>{l}+</span><span style='color:#facc15;font-size:0.75rem;margin-left:4px;'>{s}</span></div>"""
    st.markdown(f"""<div style="background-color:#1e293b;border:1px solid #334155;border-left:4px solid #facc15;border-radius:8px;padding:12px;margin-bottom:12px;"><div style="display:flex;justify-content:space-between;margin-bottom:12px;"><div><div style="color:#F1F5F9;font-weight:800;">{title}</div><div style="color:#94A3B8;font-size:0.8rem;">{subtitle}</div></div></div><div style="display:flex;flex-wrap:wrap;">{stats_html}</div></div>""", unsafe_allow_html=True)
    if st.button(btn_label, key=btn_key, use_container_width=True): safe_save_audit({'portfolio': portfolio_type, 'legs': [payload]}); st.toast("Salvo!")

def render_matrix_card_html(ticket):
    legs_html = ""
    player_map = {}
    for leg in ticket['legs']:
        if leg['player'] not in player_map: player_map[leg['player']] = {"team": leg['team'], "role": leg['role'], "stats": []}
        player_map[leg['player']]["stats"].append(f"{leg['line']}+ {leg['stat']}")
    for player, data in player_map.items():
        role_style = "color:#f59e0b;border:1px solid #f59e0b;" if data['role'] == "ANCHOR" else "color:#22d3ee;border:1px solid #22d3ee;"
        stats_display = "".join([f"<span style='background:#020617;color:#f8fafc;padding:2px 6px;border-radius:4px;font-family:monospace;font-weight:bold;margin-right:4px;'>{s}</span>" for s in data['stats']])
        legs_html += f"""<div style="margin-bottom:8px;border-bottom:1px dashed #334155;padding-bottom:6px;"><div style="display:flex;justify-content:space-between;align-items:center;"><div><span style="color:#e2e8f0;font-weight:700;">{player}</span> <span style="color:#64748b;font-size:0.75rem;">{data['team']}</span></div><div style="font-size:0.6rem;padding:1px 4px;border-radius:3px;{role_style}">{data['role']}</div></div><div style="margin-top:4px;">{stats_display}</div></div>"""
    st.markdown(f"""<div style="background:{'#1e293b' if ticket['type'] == 'MAIN' else '#0f172a'};border:1px solid #334155;border-radius:8px;padding:12px;margin-bottom:12px;"><div style="font-size:1.1rem;font-weight:800;color:{'#fbbf24' if ticket['type'] == 'MAIN' else '#94a3b8'};margin-bottom:10px;border-bottom:2px solid #334155;padding-bottom:5px;">{ticket['title']}</div><div style="font-size:0.8rem;">{legs_html}</div></div>""", unsafe_allow_html=True)
    if st.button(f"💾 Salvar", key=ticket['id'], use_container_width=True): safe_save_audit({"portfolio": "MATRIX_GOLD", "total_odd": 15.0, "legs": ticket['legs']}); st.toast("Salvo!")

def show_hit_prop_page():
    st.markdown("""<style>.stTabs [data-baseweb="tab-list"] { gap: 8px; background: transparent; } .stTabs [data-baseweb="tab"] { background: #0F172A; border: 1px solid #334155; color: #94A3B8; border-radius: 4px; padding: 6px 16px; font-size: 0.85rem; } .stTabs [aria-selected="true"] { background-color: #1E293B !important; color: #F8FAFC !important; border-color: #475569 !important; font-weight: 600; } .block-container { padding-top: 2rem; } div[data-testid="column"] > div > div > div > div > div { margin-bottom: 0px !important; }</style>""", unsafe_allow_html=True)
    st.markdown(f'<h1 style="color:#F8FAFC; margin-bottom:0;">Hit Prop <span style="color:#EF4444;">Hunter</span></h1>', unsafe_allow_html=True)
    st.caption("v47.3 • Integral Version • All Engines Loaded • Audit Fixed")

    today = datetime.now().strftime('%Y-%m-%d')
    if 'last_update_date' not in st.session_state:
        st.session_state['last_update_date'] = '1900-01-01'
    
    if st.session_state['last_update_date'] != today:
        st.session_state['scoreboard'] = get_games_safe()
        st.session_state['last_update_date'] = today
        st.toast(f"📅 Jogos atualizados para: {today}")

    if 'scoreboard' not in st.session_state:
        st.session_state['scoreboard'] = get_games_safe()
    games = st.session_state['scoreboard']
    
    if not games:
        st.error("⚠️ Nenhum jogo encontrado para hoje.")
        # Criar abas mesmo sem dados
        tabs = st.tabs(["🧬 MÚLTIPLA", "💎 SNIPER GEM", "🪜 STAIRWAY", "🧪 SGP LAB", "🔱 PROPS", "⚙️ CONFIG"])
        with tabs[0]:
            st.info("Aguardando dados de jogos...")
        return

    # Carregar cache de dados
    cache_path = "cache/real_game_logs.json"
    try: 
        with open(cache_path, 'r') as f:
            cache_data = json.load(f)
    except: 
        cache_data = {}
    
    # Gerar todos os dados necessários
    atomic_props = generate_atomic_props(cache_data, games) if cache_data else []
    sgp_data = organize_sgp_lab(atomic_props) if atomic_props else {}
    
    trident_engine = TridentEngine()
    tridents = trident_engine.find_tridents(cache_data, games) if cache_data else []
    
    stairway_raw = generate_stairway_data(cache_data, games) if cache_data else []
    sniper_data = generate_sniper_data(cache_data, games) if cache_data else []

    tabs = st.tabs(["🧬 MÚLTIPLA", "💎 SNIPER GEM", "🪜 STAIRWAY", "🧪 SGP LAB", "🔱 PROPS", "⚙️ CONFIG"])

    # ============================================
    # ABA MÚLTIPLA (DESDOBRAMENTOS INTELIGENTES)
    # ============================================
    with tabs[0]:
        st.markdown("### 🧬 MÚLTIPLA (Desdobramentos Inteligentes)")
        
        # Verificar se temos dados suficientes
        if not cache_data:
            st.error("❌ Dados de jogadores não carregados.")
            st.markdown("""
            **Solução:** Vá para a aba **⚙️ CONFIG** e clique em:
            1. **🔄 ATUALIZAR STATS (NBA)** - Para baixar dados dos jogadores
            2. **🏛️ ATUALIZAR ODDS DE MERCADO** - Para obter odds atualizadas
            """)
        elif not games:
            st.error("❌ Nenhum jogo encontrado para hoje.")
        elif not tridents:
            st.warning("⚠️ Nenhum Trident encontrado.")
            st.markdown("""
            **Possíveis causas:**
            1. Thresholds muito altos nos Tridents
            2. Jogadores sem histórico suficiente
            3. Necessidade de atualizar dados
            """)
        else:
            # GERAR MATRIX TICKETS
            with st.spinner("🔄 Gerando múltiplas inteligentes..."):
                try:
                    pool = matrix_engine.analyze_market_pool(cache_data, games, tridents)
                    
                    # Diagnóstico
                    with st.expander("📊 Diagnóstico do Pool", expanded=False):
                        st.write(f"**Anchors encontrados**: {len(pool.get('ANCHORS', []))}")
                        st.write(f"**Boosters encontrados**: {len(pool.get('BOOSTERS', []))}")
                        
                        if pool.get('ANCHORS'):
                            st.write("**Top 3 Anchors**:")
                            for i, a in enumerate(pool['ANCHORS'][:3]):
                                legs_str = ", ".join([f"{l['stat']} {l['line']}+" for l in a.get('legs', [])])
                                st.write(f"{i+1}. {a['name']} ({a['role']}) - {legs_str}")
                    
                    matrix_tickets = matrix_engine.generate_smart_matrix(pool)
                    
                    if matrix_tickets:
                        st.success(f"🎯 {len(matrix_tickets)} múltiplas inteligentes geradas!")
                        
                        # Separar por tipo
                        alpha_tickets = [t for t in matrix_tickets if t['type'] == 'ALPHA']
                        beta_tickets = [t for t in matrix_tickets if t['type'] == 'BETA']
                        
                        if alpha_tickets:
                            st.markdown("#### 🔥 MÚLTIPLAS ALPHA (Premium)")
                            for ticket in alpha_tickets:
                                render_matrix_card_html(ticket)
                        
                        if beta_tickets:
                            st.markdown("#### ⚡ MÚLTIPLAS BETA (Alternativas)")
                            for ticket in beta_tickets:
                                render_matrix_card_html(ticket)
                    else:
                        st.info("📭 Nenhuma múltipla válida encontrada com os critérios atuais.")
                        st.markdown("""
                        **Soluções possíveis:**
                        1. Ajustar thresholds na classe MatrixEngine
                        2. Atualizar odds de mercado
                        3. Aguardar mais jogadores com histórico
                        """)
                        
                except Exception as e:
                    st.error(f"❌ Erro ao gerar múltiplas: {str(e)}")
                    import traceback
                    with st.expander("🔍 Ver detalhes do erro"):
                        st.code(traceback.format_exc())
    
    # ============================================
    # ABA SNIPER GEM
    # ============================================
    with tabs[1]:
        st.markdown("### 💎 SNIPER GEM (Volume Shooters)")
        if not sniper_data:
            st.info("Nenhuma oportunidade detectada.")
        else:
            grouped_sniper = {}
            for s in sniper_data:
                grouped_sniper.setdefault(s['game_display'], []).append(s)
            
            for game, items in grouped_sniper.items():
                st.markdown(f"#### {game}")
                cols = st.columns(3)
                for i, item in enumerate(items):
                    with cols[i % 3]:
                        k = generate_stable_key("sniper", item['player'], f"{item['stat']}_{i}")
                        render_sniper_card(item, k)
                st.divider()
    
    # ============================================
    # ABA STAIRWAY
    # ============================================
    with tabs[2]:
        st.markdown("### 🪜 STAIRWAY (Matrix View)")
        if not stairway_raw:
            st.info("Nada hoje.")
        else:
            grouped_stair = {}
            for s in stairway_raw:
                grouped_stair.setdefault(s['game_display'], []).append(s)
            
            for game, players in grouped_stair.items():
                st.markdown(f"#### {game}")
                grouped_p = {}
                for p in players:
                    grouped_p.setdefault(p['player'], []).append(p)
                
                cols = st.columns(2)
                for idx, (p_name, items) in enumerate(grouped_p.items()):
                    with cols[idx % 2]:
                        render_obsidian_matrix_card(p_name, items[0]['team'], items)
                st.divider()
    
    # ============================================
    # ABA SGP LAB
    # ============================================
    with tabs[3]:
        st.markdown("### 🧪 SGP LAB")
        if not sgp_data:
            st.info("Nada.")
        else:
            sgp_counter = 0
            for game, players in sgp_data.items():
                st.markdown(f"#### {game}")
                cols = st.columns(3)
                for i, p_data in enumerate(players):
                    sgp_counter += 1
                    with cols[i % 3]:
                        items = [(prop['stat'], prop['line'], "") for prop in p_data['props']]
                        btn_key = generate_stable_key("sgp", p_data['player'], f"{p_data['team']}_{sgp_counter}")
                        render_tactical_card(p_data['player'], p_data['team'], "100%", items, btn_key, "🧪 Adicionar", p_data, "SGP_LAB")
                st.markdown("---")
    
    # ============================================
    # ABA PROPS
    # ============================================
    with tabs[4]:
        st.markdown("### 🔱 PROPS")
        if not tridents:
            st.info("Nada.")
        else:
            grouped_tri = {}
            for t in tridents:
                grouped_tri.setdefault(t['game_display'], []).append(t)
            
            for game, items in grouped_tri.items():
                st.markdown(f"#### {game}")
                cols = st.columns(3)
                for i, item in enumerate(items):
                    with cols[i % 3]:
                        items_c = [(s, l, "") for s, l in item['components']]
                        btn_key = generate_stable_key("tri", item['player'], f"{item['team']}_{i}")
                        render_tactical_card(item['player'], f"{item['team']} • {item['archetype']}", f"{item['hit_rate']:.0%}", items_c, btn_key, "🔱 Salvar", item, "TRIDENT")
                st.markdown("---")
    
    # ============================================
    # ABA CONFIG
    # ============================================
    with tabs[5]:
        st.markdown("### 🔧 Configurações & Mercado")
        
        if st.button("🏛️ ATUALIZAR ODDS DE MERCADO (PINNACLE)", help="Consome cota de API."):
            with st.spinner("Conectando à Pinnacle..."):
                ok, msg = odds_manager.force_update()
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)
        
        st.caption("Última atualização: " + str(odds_manager.load_odds().get('updated_at', 'Nunca')))
        st.divider()
        
        c1, c2 = st.columns(2)
        if c1.button("🔄 ATUALIZAR STATS (NBA)"):
            st.session_state['scoreboard'] = get_games_safe()
            update_batch_cache(st.session_state['scoreboard'])
            st.rerun()
        
        if c2.button("🗑️ LIMPAR CACHE GERAL"):
            try:
                os.remove("cache/real_game_logs.json")
                st.success("Cache limpo!")
            except Exception as e:
                st.error(f"Erro: {e}")
            st.rerun()
        
        st.markdown("---")
        if st.button("CORRIGIR IDs DA AUDITORIA (NBA -> ESPN)"):
            ok, msg = repair_audit_file()
            if ok:
                st.success(msg)
            else:
                st.error(msg)
        
        st.divider()
        with st.expander("📂 Abrir Dados Brutos"):
            if pd is not None and atomic_props:
                st.dataframe(pd.DataFrame(atomic_props), use_container_width=True)
            else:
                st.write("Vazio.")


#============================================================================
# DEFINIÇÕES E NORMALIZAÇÕES
# ============================================================================
def normalize_name(n: str) -> str:
    if not n: return ""
    n = str(n).lower()
    n = n.replace(".", " ").replace(",", " ").replace("-", " ")
    n = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", n)
    n = unicodedata.normalize("NFKD", n).encode("ascii","ignore").decode("ascii")
    return " ".join(n.split())
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
# FUNÇÃO AUXILIAR: PROCESS ROSTER (COMPLETA COM STATS)
# ============================================================================
def process_roster(roster_list, team_abbr, is_home):
    """
    Processa o roster integrando L5, Stats Individuais e Archetypes.
    CORREÇÃO: Agora retorna PTS, REB, AST, STL, BLK, 3PM para evitar KeyError.
    """
    processed = []
    df_l5 = st.session_state.get("df_l5")
    
    for entry in roster_list:
        player = normalize_roster_entry(entry)
        player_name = player.get("PLAYER", "N/A")
        
        # Overrides de posição
        position_overrides = {
            "LeBron James": "SF", "Nikola Jokić": "C", "Luka Dončić": "PG",
            "Giannis Antetokounmpo": "PF", "Jimmy Butler": "SF", "Stephen Curry": "PG",
            "Joel Embiid": "C", "Jayson Tatum": "SF", "Kevin Durant": "SF", 
            "Anthony Davis": "PF", "Bam Adebayo": "C", "Domantas Sabonis": "C"
        }
        pos = position_overrides.get(player_name, player.get("POSITION", "").upper())
        starter = player.get("STARTER", False)
        
        # Status
        status_raw = player.get("STATUS", "").lower()
        badge_color = "#9CA3AF"
        status_display = "ACTIVE"
        if any(k in status_raw for k in ["out", "ir", "injur"]):
            badge_color = "#EF4444"; status_display = "OUT"
        elif "questionable" in status_raw or "doubt" in status_raw or "gtd" in status_raw:
            badge_color = "#F59E0B"; status_display = "QUEST"
        elif any(k in status_raw for k in ["active", "available", "probable"]):
            badge_color = "#10B981"; status_display = "ACTIVE"
            
        # Stats Iniciais (Zeros)
        stats = {
            "MIN_AVG": 0, "USG_PCT": 0, "PRA_AVG": 0,
            "PTS_AVG": 0, "REB_AVG": 0, "AST_AVG": 0,
            "STL_AVG": 0, "BLK_AVG": 0, "THREEPA_AVG": 0 # Usado como proxy de 3PM se não tiver
        }
        
        archetypes_clean_list = [] 
        
        if df_l5 is not None and not df_l5.empty:
            matches = df_l5[df_l5["PLAYER"].str.contains(player_name, case=False, na=False)]
            if not matches.empty:
                row = matches.iloc[0]
                
                # Extrair TODAS as stats necessárias
                stats["MIN_AVG"] = row.get("MIN_AVG", 0)
                stats["USG_PCT"] = row.get("USG_PCT", 0) if "USG_PCT" in df_l5.columns else 0
                stats["PRA_AVG"] = row.get("PRA_AVG", 0)
                stats["PTS_AVG"] = row.get("PTS_AVG", 0)
                stats["REB_AVG"] = row.get("REB_AVG", 0)
                stats["AST_AVG"] = row.get("AST_AVG", 0)
                stats["STL_AVG"] = row.get("STL_AVG", 0)
                stats["BLK_AVG"] = row.get("BLK_AVG", 0)
                stats["THREEPA_AVG"] = row.get("THREEPA_AVG", 0) if "THREEPA_AVG" in df_l5.columns else 0
                
                # --- INTEGRAÇÃO ARCHETYPE ENGINE ---
                if "archetype_engine" in st.session_state:
                    try:
                        # Monta payload para o engine
                        engine_stats = {
                            "REB_AVG": stats["REB_AVG"], "AST_AVG": stats["AST_AVG"],
                            "PTS_AVG": stats["PTS_AVG"], "USAGE_RATE": stats["USG_PCT"],
                            "OREB_PCT": row.get("OREB_PCT", 0) if "OREB_PCT" in df_l5.columns else 0,
                            "STL_AVG": stats["STL_AVG"], "BLK_AVG": stats["BLK_AVG"],
                            "THREEPA_AVG": stats["THREEPA_AVG"],
                            "AST_TO_RATIO": row.get("AST_TO_RATIO", 0) if "AST_TO_RATIO" in df_l5.columns else 0
                        }
                        
                        raw_result = st.session_state.archetype_engine.get_archetypes(player_stats=engine_stats)
                        
                        if raw_result:
                            for item in raw_result:
                                if isinstance(item, dict):
                                    archetypes_clean_list.append(str(item.get('name', 'Unknown')))
                                elif isinstance(item, str):
                                    archetypes_clean_list.append(item)
                    except Exception:
                        archetypes_clean_list = []

        # Role
        role = "deep_bench"
        if starter: role = "starter"
        elif stats["MIN_AVG"] >= 20: role = "rotation"
        elif stats["MIN_AVG"] >= 12: role = "bench"
        
        profile_str = ", ".join(archetypes_clean_list[:2]) if archetypes_clean_list else "-"

        # Retorno Completo (Incluindo PTS, REB, AST para evitar KeyError)
        processed.append({
            "PLAYER": player_name,
            "POSITION": pos,
            "ROLE": role,
            "STATUS": status_display,
            "STATUS_FULL": player.get("STATUS", ""),
            "STATUS_BADGE": badge_color,
            "PROFILE": profile_str,
            "ARCHETYPES": archetypes_clean_list,
            
            # Stats Essenciais
            "MIN_AVG": stats["MIN_AVG"],
            "USG_PCT": stats["USG_PCT"],
            "PRA_AVG": stats["PRA_AVG"],
            "PTS": stats["PTS_AVG"],
            "REB": stats["REB_AVG"],
            "AST": stats["AST_AVG"],
            "STL": stats["STL_AVG"],
            "BLK": stats["BLK_AVG"],
            "3PM": stats["THREEPA_AVG"] # Proxy
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

# ============================================================================
# FETCH ODDS (FONTE: ESPN)
# ============================================================================
def fetch_odds_for_today():
    """
    Gera o mapa de odds a partir dos dados da ESPN (Scoreboard).
    Substitui a API paga (The Odds API) por dados gratuitos da ESPN.
    """
    # 1. Tentar pegar do session_state
    games = st.session_state.get('scoreboard', [])
    
    # 2. Se vazio, tentar carregar do arquivo
    if not games:
        cached = load_json(SCOREBOARD_JSON_FILE)
        if cached:
            # Reconstrução rápida da lista de jogos caso venha do JSON bruto
            if "events" in cached:
                # Se for o JSON cru da ESPN, precisaria reprocessar, 
                # mas vamos assumir que fetch_espn_scoreboard já salvou processado ou re-chamar:
                pass
            else:
                games = cached # Assumindo formato lista já salvo
    
    # 3. Se ainda vazio, buscar agora
    if not games:
        games = fetch_espn_scoreboard(progress_ui=False)

    odds_map = {}
    
    for game in games:
        try:
            away = game.get("away")
            home = game.get("home")
            
            # Tentar extrair dados da ESPN que salvamos no fetch_espn_scoreboard
            # Formato esperado da string ESPN: "BOS -5.5" ou "EVEN"
            spread_str = game.get("odds_spread", "")
            total_val = game.get("odds_total")
            
            spread_val = 0.0
            
            # Parsear Spread
            if spread_str and spread_str != "N/A":
                try:
                    # Lógica: Se string for "BOS -5.5", o spread é 5.5
                    # Se for "EVEN", é 0
                    if "EVEN" in spread_str.upper():
                        spread_val = 0.0
                    else:
                        # Pega o último pedaço "-5.5"
                        parts = spread_str.split()
                        if parts:
                            spread_val = float(parts[-1])
                except:
                    spread_val = 0.0
            
            # Parsear Total
            try:
                total_float = float(total_val) if total_val else 0.0
            except:
                total_float = 0.0

            # Construir chaves para compatibilidade com o sistema antigo
            # O sistema espera chaves como "Lakers@Celtics"
            # Vamos usar os nomes completos que temos no dicionário
            away_full = get_full_team_name(away)
            home_full = get_full_team_name(home)
            
            entry = {
                "spread": spread_val,
                "total": total_float,
                "home_full": home_full,
                "away_full": away_full,
                "bookmaker": "ESPN (Free)"
            }
            
            # Salvar com várias chaves para garantir que o sistema encontre
            odds_map[f"{away}@{home}"] = entry
            odds_map[f"{away} @ {home}"] = entry
            odds_map[f"{away_full}@{home_full}"] = entry
            odds_map[f"{away_full} @ {home_full}"] = entry
            
        except Exception:
            continue
            
    # Salvar no cache de odds para manter compatibilidade
    save_json(ODDS_CACHE_FILE, odds_map)
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

def get_players_l5(progress_ui=True):
    """
    Baixa estatísticas L5 em PARALELO (Turbo Mode 🚀).
    Usa 8 threads simultâneas para reduzir o tempo de horas para minutos.
    """
    from nba_api.stats.static import players
    from nba_api.stats.endpoints import playergamelog
    import concurrent.futures
    import time
    import json
    import pandas as pd
    
    # --- CONFIGURAÇÕES TURBO ---
    MAX_WORKERS = 8       # Baixa 8 jogadores ao mesmo tempo (Seguro para NBA.com)
    BATCH_SAVE_SIZE = 20  # Salva no Supabase a cada 20 jogadores prontos
    
    # 1. Carrega o que já temos na Nuvem
    df_cached = pd.DataFrame()
    cloud_data = get_data_universal(KEY_L5) # Usa sua chave definida no inicio
    
    if cloud_data and "records" in cloud_data:
        try:
            df_cached = pd.DataFrame.from_records(cloud_data["records"])
            if not df_cached.empty and "PLAYER_ID" in df_cached.columns:
                df_cached["PLAYER_ID"] = df_cached["PLAYER_ID"].astype(int)
        except: pass

    # 2. Identifica Pendentes
    existing_ids = set()
    if not df_cached.empty:
        existing_ids = set(df_cached["PLAYER_ID"].unique())

    act_players = players.get_active_players()
    # Filtra quem falta
    pending_players = [p for p in act_players if p['id'] not in existing_ids]
    
    total_needed = len(pending_players)
    total_already = len(existing_ids)
    
    if total_needed == 0:
        if progress_ui: st.success(f"✅ Todos os {total_already} jogadores já estão na nuvem!")
        return df_cached

    # 3. UI
    if progress_ui:
        status_box = st.status(f"🚀 Iniciando Lote L5 TURBO (8x Rápido)...", expanded=True)
        p_bar = status_box.progress(0)
        metric_ph = status_box.empty()
    
    # Função auxiliar para ser rodada em paralelo
    def fetch_one_player(player_info):
        pid = player_info['id']
        pname = player_info['full_name']
        try:
            # Tenta baixar o log (Retry simples interno)
            time.sleep(0.1) # Pequena pausa para não tomar block
            log = playergamelog.PlayerGameLog(player_id=pid, season=SEASON, season_type_all_star="Regular Season", timeout=10)
            df = log.get_data_frames()[0]
            if not df.empty:
                # Pega só os últimos 5 jogos
                df_l5 = df.head(5).copy()
                # Adiciona metadados
                df_l5['PLAYER_NAME'] = pname
                df_l5['PLAYER_ID'] = pid
                return df_l5
        except Exception:
            return None
        return None

    # 4. O MOTOR PARALELO
    df_new_batch = pd.DataFrame()
    results_count = 0
    
    # Gerenciador de Threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Mapeia cada jogador para uma tarefa futura
        future_to_player = {executor.submit(fetch_one_player, p): p for p in pending_players}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_player)):
            player_data = future.result()
            
            if player_data is not None and not player_data.empty:
                df_new_batch = pd.concat([df_new_batch, player_data], ignore_index=True)
            
            results_count += 1
            
            # Atualiza UI
            if progress_ui:
                pct = (i + 1) / total_needed
                p_bar.progress(min(pct, 1.0))
                metric_ph.write(f"⚡ Processando: {results_count}/{total_needed} jogadores... (Coletados: {len(df_new_batch)})")

            # 5. CHECKPOINT DE SALVAMENTO (Incremental)
            if results_count % BATCH_SAVE_SIZE == 0:
                # Junta o antigo (df_cached) com o novo (df_new_batch)
                df_total_now = pd.concat([df_cached, df_new_batch], ignore_index=True)
                
                # Sanitização JSON (Aquela correção que fizemos antes)
                records_sanitized = json.loads(df_total_now.to_json(orient="records", date_format="iso"))
                
                json_payload = {
                    "records": records_sanitized,
                    "timestamp": datetime.now().isoformat(),
                    "count": len(df_total_now)
                }
                
                # Salva sem bloquear a UI
                if save_data_universal(KEY_L5, json_payload):
                     if progress_ui: status_box.write(f"💾 Checkpoint: {len(df_total_now)} salvos na nuvem.")

    # 6. Salvamento Final
    df_final = pd.concat([df_cached, df_new_batch], ignore_index=True)
    records_sanitized = json.loads(df_final.to_json(orient="records", date_format="iso"))
    
    json_payload = {
        "records": records_sanitized,
        "timestamp": datetime.now().isoformat(),
        "count": len(df_final)
    }
    save_data_universal(KEY_L5, json_payload)
    
    if progress_ui:
        status_box.update(label=f"✅ Turbo Finalizado! Total: {len(df_final)} jogadores.", state="complete", expanded=False)
            
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

   
# ============================================================================
# PÁGINA: CONFIGURAÇÕES 
# ============================================================================
def show_config_page():
    # --- ENFORCE: Forçar tudo ligado ---
    st.session_state.use_advanced_features = True
    
    # IMPORTA O CAMINHO DEFINIDO NO INJURIES.PY
    try:
        from injuries import INJURIES_CACHE_FILE
    except ImportError:
        INJURIES_CACHE_FILE = os.path.join(os.getcwd(), "cache", "injuries_cache_v44.json")

    st.header("⚙️ PAINEL DE CONTROLE")
    
    # ==============================================================================
    # 1. STATUS DO SISTEMA
    # ==============================================================================
    st.markdown("### 📡 Status dos Motores")
    c1, c2, c3, c4 = st.columns(4)
    
    l5_ok = not st.session_state.get('df_l5', pd.DataFrame()).empty
    odds_ok = len(st.session_state.get('odds', {}) or []) > 0
    dvp_ok = st.session_state.get('dvp_analyzer') is not None
    audit_ok = st.session_state.get('audit_system') is not None
    
    def render_mini_status(col, label, is_ok):
        color = "#00FF9C" if is_ok else "#FF4F4F"
        icon = "🟢 ONLINE" if is_ok else "🔴 OFFLINE"
        col.markdown(f"""<div style="border:1px solid {color}40; background:rgba(0,0,0,0.2); padding:10px; border-radius:8px; text-align:center;"><div style="font-weight:bold; color:#E2E8F0; font-size:14px;">{label}</div><div style="color:{color}; font-size:11px; font-weight:bold; margin-top:5px;">{icon}</div></div>""", unsafe_allow_html=True)

    render_mini_status(c1, "Database L5", l5_ok)
    render_mini_status(c2, "Odds Feed", odds_ok)
    render_mini_status(c3, "DvP Radar", dvp_ok)
    render_mini_status(c4, "Auditoria", audit_ok)
    st.markdown("---")

    # ==============================================================================
    # 2. AÇÕES DE DADOS
    # ==============================================================================
    st.subheader("🔄 Sincronização de Dados")
    col_act1, col_act2 = st.columns(2)
    
    with col_act1:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown("**1. Estatísticas & Jogadores**")
        
        if st.button("🔁 ATUALIZAR L5 COMPLETO", type="primary", use_container_width=True):
            with st.spinner("Baixando dados..."):
                try:
                    # Supondo que get_players_l5 esteja definida globalmente
                    st.session_state.df_l5 = get_players_l5(progress_ui=True)
                    st.success("✅ Atualizado!")
                    time.sleep(1); st.rerun()
                except Exception as e: st.error(f"Erro L5: {e}")
        
# --- BOTÃO DE LESÕES (CLOUD NATIVE ☁️) ---
        st.markdown("<div style='height: 5px;'></div>", unsafe_allow_html=True)
        if st.button("🚑 ATUALIZAR LESÕES (30 TIMES)", use_container_width=True):
            with st.spinner("Conectando ao Depto. Médico (ESPN API)..."):
                try:
                    # 1. Instancia o Monitor
                    from injuries import InjuryMonitor
                    monitor = InjuryMonitor(cache_file=INJURIES_CACHE_FILE)
                    
                    ALL_TEAMS = [
                        "ATL","BOS","BKN","CHA","CHI","CLE","DAL","DEN","DET","GSW",
                        "HOU","IND","LAC","LAL","MEM","MIA","MIL","MIN","NOP","NYK",
                        "OKC","ORL","PHI","PHX","POR","SAC","SAS","TOR","UTA","WAS"
                    ]
                    
                    # 2. Varredura (Scraping)
                    p = st.progress(0)
                    for i, team in enumerate(ALL_TEAMS):
                        # O Scraper roda e guarda na memória interna dele
                        monitor.fetch_injuries_for_team(team)
                        p.progress((i+1)/len(ALL_TEAMS))
                    
                    p.empty()
                    
                    # 3. EXTRAÇÃO E UPLOAD (AQUI É O PULO DO GATO 🐱)
                    # Pegamos os dados da memória do monitor
                    fresh_data = monitor.get_all_injuries()
                    
                    if fresh_data:
                        # Salva no Supabase usando a função universal do SuiteNAS
                        # (Certifique-se que KEY_INJURIES está definido no topo do SuiteNAS)
                        save_data_universal("injuries", {"teams": fresh_data, "updated_at": datetime.now().isoformat()})
                        
                        # Também salva local para backup (o monitor já faz isso, mas garantimos)
                        monitor.save_cache()
                        
                        st.success(f"✅ Sincronizado com Supabase! {len(fresh_data)} times atualizados.")
                        
                        # Atualiza a sessão para ver o resultado na hora
                        st.session_state.injuries_data = fresh_data 
                        if 'injuries' in st.session_state: del st.session_state['injuries'] # Força reload visual
                        
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.warning("⚠️ O scraper rodou mas não retornou dados. A ESPN pode ter bloqueado ou mudado o layout.")
                        
                except Exception as e:
                    st.error(f"Erro crítico no processo: {e}")
                    

    with col_act2:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown("**2. Contexto de Jogo & Pace**")
        c_a, c_b = st.columns(2)
        with c_a:
            if st.button("🎯 ATUALIZAR ODDS", use_container_width=True):
                try: 
                    st.session_state.odds = fetch_odds_for_today()
                    st.success("✅ Odds Atualizadas!")
                except: st.error("Erro ao buscar Odds.")
        with c_b:
            if st.button("🛡️ ATUALIZAR DVP", use_container_width=True):
                try: 
                    from modules.new_modules.dvp_analyzer import DvPAnalyzer
                    st.session_state.dvp_analyzer = DvPAnalyzer()
                    st.success("✅ DvP Atualizado!")
                except: st.error("Erro ao carregar DvP.")
        
        st.markdown("<div style='height: 5px;'></div>", unsafe_allow_html=True)
        
        if st.button("🔄 Sincronizar Pace (Temporada 2025-26)"):
            with st.spinner("Conectando à API da NBA..."):
                new_stats = fetch_real_time_team_stats()
                
                if new_stats:
                    try:
                        import importlib
                        current_dir = os.getcwd() 
                        target_dir = os.path.join(current_dir, "modules", "new_modules")
                        
                        if target_dir not in sys.path:
                            sys.path.append(target_dir)
                        
                        import pace_adjuster
                        importlib.reload(pace_adjuster)
                        
                        st.session_state.pace_adjuster = pace_adjuster.PaceAdjuster(new_stats)
                        st.success(f"✅ Sucesso! PaceAdjuster carregado.")
                        time.sleep(1)
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Erro técnico: {e}")

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
# FUNÇÃO DE RENDERIZAÇÃO DO CARD DE JOGO (ATUALIZADA v2.0 - TIME & REAL DATA)
# ============================================================================
def render_game_card(away_team, home_team, game_data, odds_map=None):
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
            # Parseia UTC e converte para SP
            dt_utc = dateutil.parser.parse(raw_time)
            dt_br = dt_utc.astimezone(pytz.timezone('America/Sao_Paulo'))
            game_time = dt_br.strftime("%H:%M")
    except: pass

    # Odds (Spread & Total)
    spread_display = game_data.get("odds_spread", "N/A")
    total_display = game_data.get("odds_total", "N/A")
    spread_val = 0.0
    try:
        if spread_display and spread_display not in ["N/A", "EVEN"]:
            spread_val = float(spread_display.split()[-1])
    except: pass

    status = game_data.get('status', 'Agendado')
    if "Final" in status: status = "FIM"

    # --- 2. PACE REAL ---
    adv_stats = st.session_state.get('team_advanced', {})
    def get_team_pace(abbr):
        t_data = adv_stats.get(abbr, {})
        if not t_data: return 100.0
        return float(t_data.get('PACE') or t_data.get('pace') or 100.0)

    pace_home = get_team_pace(home_team)
    pace_away = get_team_pace(away_team)
    avg_pace = (pace_home + pace_away) / 2
    
    # Classificação Visual do Pace
    if avg_pace >= 101.5:
        pace_color, pace_icon = "#00FF9C", "⚡"
    elif avg_pace <= 98.5:
        pace_color, pace_icon = "#FFA500", "🐌"
    else:
        pace_color, pace_icon = "#9CA3AF", "⚖️"

    blowout = calculate_blowout_risk(spread_val)

    # --- 3. HTML DO CARD (AJUSTADO) ---
    # Mudanças: Fontes menores (-20%), Logo menor, Spread Dourado (#FFD700)
    card_html = f"""
    <div style="
        background: linear-gradient(135deg, #0F172A 0%, #1E293B 100%);
        border-radius: 10px;
        padding: 12px;
        margin-bottom: 12px;
        border: 1px solid #334155;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.3);
        font-family: 'Inter', sans-serif;
        position: relative;
    ">
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; border-bottom: 1px solid rgba(255,255,255,0.05); padding-bottom: 6px;">
        <div style="background: rgba(30, 144, 255, 0.1); color: #60A5FA; padding: 2px 6px; border-radius: 4px; font-size: 9px; font-weight: 700; text-transform: uppercase;">
            {status}
        </div>
        <div style="font-family: 'Oswald', sans-serif; font-size: 12px; color: #E2E8F0;">
            🕒 {game_time} (BR)
        </div>
      </div>

      <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px;">
          <div style="text-align: center; width: 35%;">
            <img src="{get_logo(away_team)}" style="width: 36px; height: 36px; margin-bottom: 4px; filter: drop-shadow(0 0 5px rgba(0,0,0,0.5));">
            <div style="font-weight: 800; font-size: 13px; color: #F1F5F9; font-family: 'Oswald';">{away_team}</div>
            <div style="font-size: 9px; color: #94A3B8;">PACE: {int(pace_away)}</div>
          </div>
          
          <div style="text-align: center; width: 30%;">
            <div style="font-size: 9px; color: #64748B; font-weight: 600;">SPREAD</div>
            <div style="font-size: 13px; color: #FFD700; font-weight: bold; font-family: 'Oswald'; margin-bottom: 4px; text-shadow: 0 0 5px rgba(255, 215, 0, 0.3);">{spread_display}</div>
            <div style="font-size: 9px; color: #64748B; font-weight: 600;">TOTAL</div>
            <div style="font-size: 13px; color: #E2E8F0; font-weight: bold; font-family: 'Oswald';">{total_display}</div>
          </div>
          
          <div style="text-align: center; width: 35%;">
            <img src="{get_logo(home_team)}" style="width: 36px; height: 36px; margin-bottom: 4px; filter: drop-shadow(0 0 5px rgba(0,0,0,0.5));">
            <div style="font-weight: 800; font-size: 13px; color: #F1F5F9; font-family: 'Oswald';">{home_team}</div>
            <div style="font-size: 9px; color: #94A3B8;">PACE: {int(pace_home)}</div>
          </div>
      </div>

      <div style="background: rgba(0, 0, 0, 0.3); border-radius: 6px; padding: 6px; display: flex; justify-content: space-around; align-items: center; border: 1px solid rgba(255,255,255,0.05);">
        <div style="text-align: center;">
            <div style="font-size: 8px; color: #94A3B8; font-weight: 600;">RITMO ({int(avg_pace)})</div>
            <div style="color: {pace_color}; font-weight: bold; font-size: 10px;">{pace_icon}</div>
        </div>
        <div style="width: 1px; height: 15px; background: rgba(255,255,255,0.1);"></div>
        <div style="text-align: center;">
            <div style="font-size: 8px; color: #94A3B8; font-weight: 600;">RISCO</div>
            <div style="color: {blowout['color']}; font-weight: bold; font-size: 10px;" title="{blowout['desc']}">{blowout['icon']} {blowout['nivel']}</div>
        </div>
      </div>
    </div>
    """
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
# PÁGINA: DESDOBRAMENTOS INTELIGENTES
# ============================================================================
def show_desdobramentos_inteligentes():
    """
    Página de Desdobramentos Inteligentes - OTIMIZADA PARA BACKEND v3.4
    Fix 1: Captura game_id numérico para validação de boxscore.
    Fix 2: Captura a 'thesis' (motivo) real gerada pelo motor.
    Fix 3: Ajuste de filtros visuais para não esconder resultados válidos.
    """
    import streamlit as st
    
    st.header("🎯 Desdobramentos Estratégicos (v3.0)")
    st.info("""
    **Sistema de Roadmap Estratégico:**
    Gera trixies focando em diversificação de confrontos e narrativas (Vacuum, Pace, Matchup).
    - **PISO:** Linhas de segurança (~90% da média).
    - **MÉDIO:** Linhas justas (~100% da média).
    - **TETO:** Linhas de alavancagem (>110% da média).
    *Nota: As teses agora são salvas na auditoria para análise de causa.*
    """)
    
    # Verificar dados
    if 'scoreboard' not in st.session_state or not st.session_state.scoreboard:
        st.error("Scoreboard vazio. Vá em 'Config' e clique em Atualizar Dados.")
        return
    
    games = st.session_state.scoreboard
    game_options = [f"{g.get('away')} @ {g.get('home')}" for g in games]
    
    if not game_options:
        st.warning("Nenhum jogo disponível hoje.")
        return
    
    # --- CONFIGURAÇÕES ---
    col1, col2, col3 = st.columns(3)
    
    with col1:
        perfil = st.selectbox(
            "Perfil de Risco",
            ["CONSERVADOR", "BALANCEADO", "AGRESSIVO"],
            index=1,
            help="Conservador: Prioriza Piso. Balanceado: Mix. Agressivo: Busca Teto."
        )
    
    with col2:
        max_combinacoes = st.slider("Qtd. Combinações", 5, 50, 20)
    
    with col3:
        selected_games = st.multiselect(
            "Jogos para Analisar",
            game_options,
            default=game_options
        )
    
    # --- GERAR CHAVE DE CACHE ---
    config_key = f"desdob_{perfil}_{max_combinacoes}_{'_'.join(sorted(selected_games))}"
    
    if 'desdob_cache' not in st.session_state:
        st.session_state.desdob_cache = {}
    
    has_cached_results = config_key in st.session_state.desdob_cache
    cache_info = ""
    
    if has_cached_results:
        cache_info = f"📊 {len(st.session_state.desdob_cache[config_key]['desdobramentos'])} combinações cacheadas"
    
    # --- BOTÃO DE AÇÃO ---
    col1, col2 = st.columns([3, 1])
    
    with col1:
        generate_button = st.button(
            "🎲 Gerar Estratégia" if not has_cached_results else "🔄 Regenerar Estratégia",
            type="primary" if not has_cached_results else "secondary",
            use_container_width=True
        )
    
    with col2:
        if has_cached_results:
            st.info(cache_info)
    
    # --- LIMPAR CACHE ---
    with st.expander("⚙️ Opções Avançadas"):
        if st.button("🧹 Limpar Cache de Desdobramentos", type="secondary"):
            st.session_state.desdob_cache = {}
            if 'desdobramentos_gerados' in st.session_state:
                del st.session_state.desdobramentos_gerados
            st.success("Cache limpo!")
            st.rerun()
    
    # --- LÓGICA DE GERAÇÃO/CACHE ---
    desdobramentos = []
    
    if has_cached_results and not generate_button:
        cached = st.session_state.desdob_cache[config_key]
        desdobramentos = cached['desdobramentos']
        st.session_state.desdobramentos_gerados = desdobramentos
        st.success(f"✅ Usando combinações cacheadas ({len(desdobramentos)} encontradas)")
    
    elif generate_button or (not has_cached_results and not desdobramentos):
        with st.spinner("Aplicando Roadmap Estratégico v3.0..."):
            try:
                # Imports Seguros
                try:
                    from modules.new_modules.desdobrador_inteligente import DesdobradorInteligente
                    from modules.new_modules.strategy_engine import StrategyEngine
                except ImportError:
                    st.error("Módulos não encontrados. Verifique a instalação.")
                    return

                # Inicializa Engine
                if 'strategy_engine' in st.session_state:
                    strat_engine = st.session_state.strategy_engine
                else:
                    strat_engine = StrategyEngine()
                    st.session_state.strategy_engine = strat_engine
                
                # --- PREPARAÇÃO DE DADOS (COM GAME ID) ---
                all_players_ctx = {}
                game_objects = []
                
                # 1. Cria Mapa de Game IDs (CRUCIAL PARA VALIDAÇÃO)
                game_id_map = {}
                if st.session_state.scoreboard:
                    for g in st.session_state.scoreboard:
                        k = f"{g.get('away')} @ {g.get('home')}"
                        gid = g.get('gameId') or g.get('game_id')
                        if gid:
                            game_id_map[k] = gid

                # Funções helpers locais
                def fetch_team_roster_safe(team):
                    if 'fetch_team_roster' in globals(): return fetch_team_roster(team, False)
                    return [] 
                
                def process_roster_safe(roster, team, is_home):
                    if 'process_roster' in globals() and 'extract_list' in globals():
                        return process_roster(extract_list(roster), team, is_home)
                    return [] 

                for game_str in selected_games:
                    away, home = game_str.split(" @ ")
                    gid = game_id_map.get(game_str, "UNK")
                    
                    r_away = fetch_team_roster_safe(away)
                    r_home = fetch_team_roster_safe(home)
                    p_away = process_roster_safe(r_away, away, False)
                    p_home = process_roster_safe(r_home, home, True)
                    
                    if not p_away or not p_home: continue
                        
                    def prepare_for_backend(p_list, team_name):
                        clean = []
                        for p in p_list:
                            name = p.get('PLAYER') or p.get('name') or p.get('player_name')
                            if not name: continue
                            p_obj = p.copy()
                            p_obj['name'] = name
                            p_obj['team'] = team_name
                            clean.append(p_obj)
                        return clean

                    all_players_ctx[away] = prepare_for_backend(p_away, away)
                    all_players_ctx[home] = prepare_for_backend(p_home, home)
                    
                    # Passa o game_id para o motor
                    game_objects.append({"away": away, "home": home, "game_id": gid})
                
                # --- EXECUÇÃO ---
                desdobrador = DesdobradorInteligente(strat_engine)
                
                desdobramentos = desdobrador.gerar_desdobramentos(
                    players_ctx=all_players_ctx,
                    games_ctx=game_objects,
                    perfil=perfil,
                    max_combinacoes=max_combinacoes
                )
                
                # Compatibilidade de Score
                for d in desdobramentos:
                    d['score_qualidade'] = d.get('score_final', d.get('score_ajustado', 0))
                
                # Salvar no Cache
                import time
                st.session_state.desdob_cache[config_key] = {
                    'desdobramentos': desdobramentos,
                    'timestamp': time.time(),
                    'config': {'perfil': perfil, 'max_combinacoes': max_combinacoes, 'num_games': len(selected_games)}
                }
                
                st.session_state.desdobramentos_gerados = desdobramentos
                
                if desdobramentos:
                    st.success(f"✅ Sucesso! {len(desdobramentos)} combinações geradas.")
                else:
                    st.warning("O algoritmo não encontrou combinações válidas.")

            except Exception as e:
                st.error(f"Erro Crítico no Motor: {str(e)}")
    
    # --- EXIBIÇÃO DOS RESULTADOS ---
    if 'desdobramentos_gerados' in st.session_state and st.session_state.desdobramentos_gerados:
        st.markdown("---")
        desds = st.session_state.desdobramentos_gerados
        
        c1, c2 = st.columns(2)
        # --- CORREÇÃO AQUI: Baixei o padrão de 3.0 para 1.5 e 5.0 para 4.0 ---
        min_odd = c1.slider("Odd Mínima", 1.0, 10.0, 1.5) 
        min_qualidade = c2.slider("Qualidade Mínima (Score)", 4.0, 10.0, 4.0)
        
        filtered = [
            d for d in desds 
            if d['total_odd'] >= min_odd 
            and d.get('score_qualidade', 0) >= min_qualidade
        ]
        
        if filtered:
            st.caption(f"📈 Mostrando {len(filtered)} de {len(desds)} combinações.")
        else:
            st.info(f"Nenhuma combinação atende aos filtros visuais (Odd > {min_odd} e Score > {min_qualidade}). Tente diminuir os filtros.")
            return

        filtered.sort(key=lambda x: (-x.get('score_qualidade', 0), x['total_odd']))
        
        for i, d in enumerate(filtered):
            color_map = {"CONSERVADOR": "#00C853", "BALANCEADO": "#FFD600", "AGRESSIVO": "#FF1744"}
            b_color = color_map.get(d['perfil'], "#FFF")
            score_valor = d.get('score_qualidade', 0)
            
            composicao = d.get('composicao', {})
            mix_riscos = composicao.get('mix_riscos', {})
            
            with st.container():
                st.markdown(f"""
                <div style="border-left: 5px solid {b_color}; background-color: rgba(255,255,255,0.05); padding: 15px; border-radius: 5px; margin-bottom: 15px;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <span style="font-size: 1.1em; font-weight: bold;">Combinação #{i+1}</span>
                            <span style="background: {b_color}33; color: {b_color}; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; margin-left: 10px;">{d['perfil']}</span>
                        </div>
                        <div style="text-align: right;">
                            <div style="font-size: 1.2em; font-weight: bold; color: #4FC3F7;">@{d['total_odd']:.2f}</div>
                            <div style="font-size: 0.8em; color: #AAA;">Score: {score_valor:.2f}</div>
                        </div>
                    </div>
                    <div style="font-size: 0.8em; color: #888; margin-top: 5px;">
                        🎲 {composicao.get('jogos_distintos', 0)} Jogos | 👥 {composicao.get('unique_players', 0)} Players
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                cols = st.columns(len(d['legs']))
                for idx, leg in enumerate(d['legs']):
                    risco = leg.get('risco', 'MEDIO')
                    icon = "🟢" if risco == 'PISO' else "🟡" if risco == 'MEDIO' else "🔴"
                    ratio_pct = int((leg['line'] / leg['avg']) * 100) if leg.get('avg') else 0
                    
                    with cols[idx]:
                        st.markdown(f"""
                        <div style="background: rgba(0,0,0,0.3); padding: 10px; border-radius: 5px; text-align: center; height: 100%;">
                            <div style="font-size: 0.8em; color: #CCC;">{leg['team']}</div>
                            <div style="font-weight: bold; color: #FFF;">{leg['player_name'].split(' ')[0]}</div>
                            <div style="font-size: 1.1em; color: #4FC3F7; font-weight: bold;">{leg['market_display']}</div>
                            <div style="font-size: 0.75em; color: #AAA; border-top: 1px solid #444; margin-top: 5px;">
                                {icon} {risco} ({ratio_pct}%)
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                
                # --- BOTÃO SALVAR (AGORA CAPTURA A TESE REAL) ---
                col_save, col_info = st.columns([1, 3])
                with col_save:
                    if st.button(f"💾 Salvar #{i+1}", key=f"save_desd_{i}"):
                        if "audit_system" in st.session_state:
                            audit_legs = []
                            for l in d['legs']:
                                # Game ID da perna (CRUCIAL)
                                lid = l.get('game_id') or l.get('game_info', {}).get('game_id', 'UNK')
                                
                                # Market Type
                                m_type = l.get('market_type')
                                if not m_type and 'market_display' in l:
                                    try: m_type = l['market_display'].split(' ')[-1]
                                    except: m_type = "UNK"
                                
                                # Tese (AQUI ESTÁ A CORREÇÃO PRINCIPAL)
                                # Tenta pegar a tese rica gerada pelo motor. Se não tiver, usa fallback.
                                final_thesis = l.get('thesis')
                                if not final_thesis:
                                    final_thesis = f"{l.get('risco','')} | Avg:{l.get('avg',0)}"

                                audit_legs.append({
                                    "player_name": l['player_name'],
                                    "team": l['team'],
                                    "market_type": m_type,
                                    "market_display": l['market_display'],
                                    "line": float(l.get('line', 0)),
                                    "odds": float(l.get('odds', 1.0)),
                                    "game_id": lid,
                                    "thesis": final_thesis # <--- AGORA SALVA CORRETAMENTE
                                })
                            
                            game_info_mix = {"home": "MIX", "away": "MIX", "game_id": "MULTI"}
                            
                            st.session_state.audit_system.log_trixie(
                                trixie_data={
                                    "players": audit_legs,
                                    "total_odd": d['total_odd'],
                                    "category": "DESDOBRADOR",
                                    "sub_category": d['perfil'],
                                    "score": score_valor
                                },
                                game_info=game_info_mix,
                                category="DESDOBRADOR",
                                source="DesdobradorInteligente"
                            )
                            st.toast(f"Combinação #{i+1} salva na auditoria!", icon="✅")
                        else:
                            st.toast("Sistema de auditoria não carregado.", icon="⚠️")
                
                with col_info:
                    if mix_riscos:
                        risk_str = " | ".join([f"{k}:{v}" for k, v in mix_riscos.items()])
                        st.caption(f"📊 Mix: {risk_str}")
                
                st.markdown("---")
# ============================================================================
# FUNÇÃO AUXILIAR: RENDERIZAÇÃO DO BANCO (ESCALAÇÕES)
# ============================================================================
def render_player_list_bench(players, injured_names):
    """
    Renderiza a lista visual de jogadores do banco de reservas.
    Usa um design compacto para não poluir a tela.
    """
    if not players:
        st.caption("🔍 Dados de banco indisponíveis.")
        return

    # Container scrollável para o banco (opcional, mas fica bonito)
    with st.container():
        for p in players:
            # Normalização de nomes de chaves (Blinda contra variações da API)
            name = p.get('PLAYER', p.get('name', 'Unknown'))
            pos = p.get('POSITION', p.get('position', '-'))
            
            # Tenta pegar média de pontos para dar contexto
            try:
                pts = float(p.get('PTS_AVG', p.get('pts_L5', 0)))
            except:
                pts = 0.0

            # Verifica se está machucado
            is_injured = False
            if injured_names and name in injured_names:
                is_injured = True

            # Definição de Estilo (Visual Dark/Neon)
            bg_color = "rgba(255, 79, 79, 0.15)" if is_injured else "rgba(255, 255, 255, 0.03)"
            border_color = "#FF4F4F" if is_injured else "#475569"
            text_color = "#E2E8F0"
            status_icon = "🚑" if is_injured else "⚡"
            
            # HTML do Card Compacto
            st.markdown(f"""
            <div style="
                display: flex; 
                align-items: center; 
                justify-content: space-between;
                background-color: {bg_color}; 
                border-left: 3px solid {border_color}; 
                padding: 6px 10px; 
                margin-bottom: 4px; 
                border-radius: 4px;">
                
                <div style="display: flex; align-items: center; gap: 8px;">
                    <span style="font-size: 12px;">{status_icon}</span>
                    <div style="line-height: 1.2;">
                        <div style="font-size: 13px; font-weight: 500; color: {text_color};">{name}</div>
                        <div style="font-size: 10px; color: #94A3B8;">{pos}</div>
                    </div>
                </div>
                
                <div style="text-align: right;">
                    <span style="font-size: 11px; font-weight: bold; color: #64748B;">{pts:.1f} <span style="font-size: 9px;">PPG</span></span>
                </div>
            </div>
            """, unsafe_allow_html=True)

# ============================================================================
# MATCHUP CENTER 
# ============================================================================
def show_escalacoes():
    import streamlit as st
    import html
    import textwrap

    # --- 1. CSS ---
    st.markdown(textwrap.dedent("""
    <style>
        .match-card { background: rgba(30, 41, 59, 0.5); border-radius: 8px; padding: 10px; margin-bottom: 8px; border-left: 4px solid #64748B; transition: transform 0.2s; }
        .match-card:hover { transform: translateX(3px); background: rgba(255,255,255,0.05); }
        .border-home { border-left-color: #00E5FF; }
        .border-away { border-left-color: #FF4F4F; }
        .match-header { display: flex; justify-content: space-between; align-items: center; }
        .match-name { font-weight: bold; color: #F8FAFC; font-size: 14px; font-family: sans-serif; }
        .match-pos { font-size: 10px; color: #94A3B8; background: rgba(255,255,255,0.1); padding: 1px 5px; border-radius: 3px; margin-left: 5px; }
        .match-stats { font-family: 'Courier New', monospace; font-weight: bold; font-size: 13px; color: #CBD5E1; }
        .match-sub { font-size: 10px; color: #64748B; margin-top: 4px; display: flex; justify-content: space-between; border-top: 1px solid rgba(255,255,255,0.05); padding-top: 4px; }
        .status-badge { font-size: 9px; font-weight: bold; padding: 1px 4px; border-radius: 2px; }
        .bg-official { background: #10B981; color: #fff; }
        .bg-proj { background: #F59E0B; color: #000; }
    </style>
    """), unsafe_allow_html=True)

    st.header("👥 MATCHUP CENTER")

    # --- 2. SELEÇÃO ---
    if 'scoreboard' not in st.session_state or not st.session_state.scoreboard:
        st.warning("⚠️ Scoreboard vazio. Atualize na aba Config.")
        return

    games = st.session_state.scoreboard
    opts = [f"{g.get('away')} @ {g.get('home')}" for g in games]
    
    idx = 0
    if 'last_match_sel' in st.session_state and st.session_state.last_match_sel in opts:
        idx = opts.index(st.session_state.last_match_sel)
        
    sel_game = st.selectbox("Confronto:", opts, index=idx)
    st.session_state.last_match_sel = sel_game
    if not sel_game: return
    away_abbr, home_abbr = sel_game.split(" @ ")

    # --- 3. CARREGAMENTO ---
    with st.spinner(f"Analisando {away_abbr} vs {home_abbr}..."):
        try:
            if 'fetch_team_roster' not in globals():
                st.error("Erro: fetch_team_roster não encontrada.")
                return

            roster_away = fetch_team_roster(away_abbr, progress_ui=False)
            roster_home = fetch_team_roster(home_abbr, progress_ui=False)
            
            # Garante lista
            def ensure_list(r): return r if isinstance(r, list) else (extract_list(r) if 'extract_list' in globals() else [])
            l_away = ensure_list(roster_away)
            l_home = ensure_list(roster_home)
            
            if 'process_roster' in globals():
                p_home = process_roster(l_home, home_abbr, True)
                p_away = process_roster(l_away, away_abbr, False)
            else:
                p_home, p_away = l_home, l_away
            
        except Exception as e:
            st.error(f"Erro ao carregar elencos: {e}")
            return

    # --- 4. SEPARAÇÃO ---
    def split_roster(players):
        # Detecta titulares
        starters = [p for p in players if str(p.get("ROLE",'')).lower() == 'starter' or p.get('STARTER') is True]
        is_projected = False
        
        # Helper de minutos
        def get_mins(p):
            for k in ["MIN_AVG", "min_L5", "MIN", "mpg"]:
                if p.get(k): return float(p[k])
            return 0.0

        # Se não tem oficial, projeta (excluindo os OUT)
        if len(starters) < 5:
            is_projected = True
            valid = []
            for p in players:
                # Checagem simples de status para não projetar lesionado
                stat = str(p.get("STATUS",'') or '').lower()
                if "out" not in stat and "inj" not in stat:
                    valid.append(p)
            starters = sorted(valid, key=get_mins, reverse=True)[:5]
            
        s_names = [p.get("PLAYER") or p.get("name") for p in starters]
        bench = [p for p in players if (p.get("PLAYER") or p.get("name")) not in s_names]
        bench = sorted(bench, key=get_mins, reverse=True)
        return starters, bench, is_projected

    h_starters, h_bench, h_proj = split_roster(p_home)
    a_starters, a_bench, a_proj = split_roster(p_away)

    # --- 5. RENDERIZADOR ---
    def render_player_card(player, team_type):
        raw_name = str(player.get("PLAYER") or player.get("name") or "Unknown")
        safe_name = html.escape(raw_name)
        pos = html.escape(str(player.get("POSITION") or player.get("pos") or "-"))
        
        def get_stat(p, keys):
            for k in keys:
                if p.get(k) is not None: return float(p[k])
            return 0.0

        ppg = get_stat(player, ["PTS_AVG", "pts_L5", "PTS", "ppg"])
        rpg = get_stat(player, ["REB_AVG", "reb_L5", "REB", "rpg"])
        apg = get_stat(player, ["AST_AVG", "ast_L5", "AST", "apg"])
        mins = get_stat(player, ["MIN_AVG", "min_L5", "MIN", "mpg"])
        
        css = "border-home" if team_type == "home" else "border-away"
        col = "#00E5FF" if team_type == "home" else "#FF4F4F"
        
        st.markdown(f"""
        <div class="match-card {css}">
            <div class="match-header">
                <div><span class="match-name">{safe_name}</span><span class="match-pos">{pos}</span></div>
                <div class="match-stats" style="color:{col}">{ppg:.1f} <span style="font-size:10px; color:#64748B">PTS</span></div>
            </div>
            <div class="match-sub">
                <span>Minutos: <b>{mins:.1f}</b></span>
                <span>REB: {rpg:.1f} • AST: {apg:.1f}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # --- 6. LAYOUT ---
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"<div style='font-family:Oswald; font-size:20px; color:#00E5FF; border-bottom:2px solid #00E5FF; margin-bottom:10px;'>🏠 {home_abbr}</div>", unsafe_allow_html=True)
        lbl, bg = ("OFICIAL", "bg-official") if not h_proj else ("PROJETADO", "bg-proj")
        st.markdown(f"<div style='margin-bottom:8px;'><span class='status-badge {bg}'>{lbl}</span> <span style='font-size:12px; font-weight:bold; color:#E2E8F0;'>TITULARES</span></div>", unsafe_allow_html=True)
        for p in h_starters: render_player_card(p, "home")
        with st.expander(f"🔄 Banco ({len(h_bench)})"):
            for p in h_bench[:8]: render_player_card(p, "home")

    with col2:
        st.markdown(f"<div style='font-family:Oswald; font-size:20px; color:#FF4F4F; border-bottom:2px solid #FF4F4F; margin-bottom:10px;'>✈️ {away_abbr}</div>", unsafe_allow_html=True)
        lbl, bg = ("OFICIAL", "bg-official") if not a_proj else ("PROJETADO", "bg-proj")
        st.markdown(f"<div style='margin-bottom:8px;'><span class='status-badge {bg}'>{lbl}</span> <span style='font-size:12px; font-weight:bold; color:#E2E8F0;'>TITULARES</span></div>", unsafe_allow_html=True)
        for p in a_starters: render_player_card(p, "away")
        with st.expander(f"🔄 Banco ({len(a_bench)})"):
            for p in a_bench[:8]: render_player_card(p, "away")

    # --- 7. NOTAS TÁTICAS (INTEGRADA COM DEPT MÉDICO) ---
    st.markdown("---")
    if "rotation_analyzer" in st.session_state:
        ra = st.session_state.rotation_analyzer
        if ra:
            # Prepara dados para análise em tempo real
            def prep_data(roster):
                mins, inj = {}, []
                for p in roster:
                    name = p.get("PLAYER") or p.get("name")
                    if not name: continue
                    # Minutos
                    m = 0.0
                    for k in ["MIN_AVG", "min_L5", "MIN"]:
                        if p.get(k): m = float(p[k]); break
                    mins[name] = m
                    # Lesões (Roster Scan)
                    stat = str(p.get("STATUS") or "").lower()
                    if "out" in stat or "inj" in stat or "gtd" in stat: 
                        inj.append(name)
                return mins, inj

            # 1. Analisa Home
            hm, hi = prep_data(p_home)
            ra.analyze_team_rotation(home_abbr, p_home, hi, hm)
            
            # 2. Analisa Away
            am, ai = prep_data(p_away)
            ra.analyze_team_rotation(away_abbr, p_away, ai, am)

            # 3. Exibe
            ctx = {"away_team": away_abbr, "home_team": home_abbr}
            try:
                info_h = str(ra.get_lineup_insights(home_abbr, ctx)).replace(home_abbr, "").strip()
                info_a = str(ra.get_lineup_insights(away_abbr, ctx)).replace(away_abbr, "").strip()
                
                c1, c2 = st.columns(2)
                with c1: 
                    st.info(f"**Coach {home_abbr}:**\n\n{info_h}")
                with c2: 
                    st.warning(f"**Coach {away_abbr}:**\n\n{info_a}")
            except Exception as e: 
                st.error(f"Erro ao gerar insights: {e}")

# ============================================================================
# PÁGINA: DEPTO MÉDICO (BIO-MONITOR V23.2 - FILE VERSION FIX)
# ============================================================================
def show_depto_medico():
    import streamlit as st
    import pandas as pd
    import os
    import json
    from datetime import datetime

    # --- 1. IMPORTAÇÕES E CAMINHOS ---
    BASE_DIR = os.path.dirname(__file__) if '__file__' in globals() else os.getcwd()
    
    # CORREÇÃO CRÍTICA: Apontar para o mesmo arquivo que o injuries.py v45 gera
    INJURIES_CACHE_FILE = os.path.join(BASE_DIR, "cache", "injuries_cache_v44.json")

    # --- 2. CSS PREMIUM ---
    st.markdown("""
    <style>
        .team-header {
            font-size: 22px; font-weight: bold; color: #F8FAFC;
            margin: 32px 0 16px 0; padding-bottom: 8px;
            border-bottom: 3px solid rgba(255,255,255,0.2);
            font-family: 'Orbitron', sans-serif;
        }
        .injury-card {
            background: rgba(15, 23, 42, 0.95); border-radius: 16px;
            padding: 18px; margin-bottom: 16px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
            transition: all 0.3s ease; border: 1px solid #334155;
        }
        .injury-card:hover { transform: translateY(-4px); box-shadow: 0 8px 20px rgba(0, 0, 0, 0.6); border-color: #64748b; }
        .injury-out { border-left: 6px solid #FF4F4F; }
        .injury-quest { border-left: 6px solid #F59E0B; }
        .injury-prob { border-left: 6px solid #10B981; }
        .injury-name { font-size: 18px; font-weight: bold; color: #F8FAFC; margin-bottom: 8px; }
        .injury-team { font-size: 14px; color: #94A3B8; background: rgba(255,255,255,0.1); padding: 4px 10px; border-radius: 8px; display: inline-block; margin-left: 10px; }
        .injury-desc { font-size: 14px; color: #CBD5E1; margin: 10px 0; line-height: 1.5; }
        .injury-meta { font-size: 13px; color: #94A3B8; display: flex; justify-content: space-between; margin-top: 12px; padding-top: 10px; border-top: 1px dashed rgba(255,255,255,0.1); }
        .star-badge { background: #F59E0B; color: #000; padding: 4px 10px; border-radius: 8px; font-weight: bold; font-size: 12px; margin-left: 10px; }
        .rot-badge { background: #3B82F6; color: #fff; padding: 4px 10px; border-radius: 8px; font-weight: bold; font-size: 12px; margin-left: 10px; }
        .stButton button { width: 100%; border-radius: 8px; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

    c_head, c_btn = st.columns([3, 1])
    with c_head:
        st.header("🚑 BIO-MONITOR (INJURY REPORT)")
    with c_btn:
        # Botão de Reload Limpa a Sessão para forçar leitura do arquivo novo
        if st.button("🔄 Recarregar Dados"):
            if "injuries" in st.session_state:
                del st.session_state["injuries"]
            st.rerun()

    # --- 3. CARREGAMENTO E PARSING ---
    if "injuries" not in st.session_state or not st.session_state["injuries"]:
        if os.path.exists(INJURIES_CACHE_FILE):
            try:
                with open(INJURIES_CACHE_FILE, 'r', encoding='utf-8') as f:
                    raw_data = json.load(f)
                
                final_list = []
                teams_data = raw_data.get('teams', raw_data)
                
                if isinstance(teams_data, dict):
                    for team_abbr, players in teams_data.items():
                        if not isinstance(players, list): continue
                        for p in players:
                            status_lower = str(p.get('status','')).lower()
                            if 'active' in status_lower and 'day' not in status_lower: continue
                            p['team'] = team_abbr
                            final_list.append(p)
                elif isinstance(teams_data, list):
                    final_list = [p for p in teams_data if 'active' not in str(p.get('status','')).lower()]

                st.session_state["injuries"] = final_list
            except Exception as e:
                st.error(f"Erro ao ler cache v44: {e}")
                return
        else:
            # Se o arquivo v45 não existe, avisa para rodar o update
            st.warning(f"⚠️ Cache v44 não encontrado. Vá em CONFIG e clique em 'ATUALIZAR TUDO'.")
            return
    
    injuries_list = st.session_state.get("injuries", [])
    df_l5 = st.session_state.get('df_l5', pd.DataFrame())

    # --- 4. PROCESSAMENTO ---
    teams_injured = {}
    critical_losses = []

    for p in injuries_list:
        name = str(p.get('player') or p.get('name') or 'Unknown')
        team = str(p.get('team') or 'NB')
        
        p['impact'] = 0
        try:
            if not df_l5.empty:
                match = df_l5[df_l5['PLAYER'].str.contains(name, case=False, na=False)]
                if not match.empty:
                    min_avg = match['MIN_AVG'].mean()
                    if min_avg >= 28: p['impact'] = 2
                    elif min_avg >= 18: p['impact'] = 1
        except: pass

        if team not in teams_injured: teams_injured[team] = []
        teams_injured[team].append(p)

        if p['impact'] == 2:
            critical_losses.append(f"{name} ({team})")

    # --- 5. DASHBOARD METRICS ---
    st.markdown("""
    <style>
        .audit-card {
            background: linear-gradient(145deg, #1e1e24, #2d2d35);
            border: 1px solid #444; border-radius: 8px; padding: 10px;
            text-align: center; height: 100%;
        }
        .audit-val { font-size: 20px; font-weight: 800; color: #f8fafc; }
        .audit-lbl { font-size: 10px; text-transform: uppercase; color: #94a3b8; font-weight: 600; }
        .bd-blue { border-top: 3px solid #3b82f6; }
        .bd-cyan { border-top: 3px solid #06b6d4; }
        .bd-red { border-top: 3px solid #ef4444; }
    </style>
    """, unsafe_allow_html=True)

    ac1, ac2, ac3 = st.columns(3)
    ac1.markdown(f'<div class="audit-card bd-blue"><div style="font-size:16px;">🏥</div><div class="audit-val">{len(injuries_list)}</div><div class="audit-lbl">Reportes</div></div>', unsafe_allow_html=True)
    ac2.markdown(f'<div class="audit-card bd-cyan"><div style="font-size:16px;">👥</div><div class="audit-val">{len(teams_injured)}</div><div class="audit-lbl">Times</div></div>', unsafe_allow_html=True)
    ac3.markdown(f'<div class="audit-card bd-red"><div style="font-size:16px;">⭐</div><div class="audit-val">{len(critical_losses)}</div><div class="audit-lbl">Estrelas Off</div></div>', unsafe_allow_html=True)

    if critical_losses:
        with st.expander(f"🚨 {len(critical_losses)} Jogadores de Impacto Fora/Dúvida", expanded=False):
            st.write(", ".join(critical_losses))

    # --- 6. FILTROS E EXIBIÇÃO ---
    st.markdown("---")
    c_filter, c_search = st.columns([1, 2])
    
    team_options = sorted(list(teams_injured.keys()))
    sel_team = c_filter.selectbox("Filtrar Time:", ["TODOS"] + team_options)
    sel_player = c_search.text_input("Buscar Jogador:", placeholder="Ex: LeBron...")

    st.markdown("### 📋 Lista de Lesões")
    
    sorted_teams = sorted(teams_injured.items(), key=lambda x: len(x[1]), reverse=True)
    found_any = False
    
    for team, players in sorted_teams:
        if sel_team != "TODOS" and team != sel_team: continue
        
        filtered_players = players
        if sel_player:
            filtered_players = [p for p in players if sel_player.lower() in str(p.get('name','')).lower()]
        
        if not filtered_players: continue
        found_any = True
        
        st.markdown(f'<div class="team-header">🏀 {team} <span style="font-size:14px; color:#94a3b8; font-weight:normal">({len(filtered_players)})</span></div>', unsafe_allow_html=True)
        
        filtered_players.sort(key=lambda x: (x.get('impact',0), 1 if 'out' in str(x.get('status','')).lower() else 0), reverse=True)
        
        for p in filtered_players:
            name = p.get('name', 'Unknown')
            status = str(p.get('status', '')).upper()
            desc = p.get('details', '')
            date = str(p.get('date', ''))[:10]
            
            card_class = ""
            icon = "ℹ️"
            if 'OUT' in status: 
                card_class = "injury-out"; icon = "❌"
            elif any(x in status for x in ['QUEST', 'DOUBT', 'DAY']): 
                card_class = "injury-quest"; icon = "⚠️"
            elif 'PROB' in status:
                card_class = "injury-prob"; icon = "✅"
            
            badge = ""
            if p.get('impact') == 2: badge = '<span class="star-badge">⭐ STAR</span>'
            elif p.get('impact') == 1: badge = '<span class="rot-badge">🔄 ROTATION</span>'
            
            st.markdown(f"""
            <div class="injury-card {card_class}">
                <div class="injury-name">{icon} {name} {badge}</div>
                <div style="color: #facc15; font-size: 13px; font-weight:bold; margin-bottom:4px;">{status}</div>
                <div class="injury-desc">{desc}</div>
                <div class="injury-meta">
                    <span>📅 {date}</span>
                    <span>{team}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)

    if not found_any:
        st.info("Nenhum jogador encontrado com os filtros atuais.")

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
# PÁGINA: LAB DE NARRATIVAS (WAR ROOM V3.0 - GRID & THREATS)
# ============================================================================
def show_narrative_lab():
    import time
    
    # CSS Específico para o Layout "War Room"
    st.markdown("""
    <style>
        /* Header */
        .war-room-title { font-family: 'Oswald'; font-size: 28px; color: #fff; letter-spacing: 2px; margin-bottom: 5px; }
        .war-room-sub { font-family: 'Nunito'; font-size: 14px; color: #94a3b8; margin-bottom: 25px; }
        
        /* Top Threats Cards */
        .threat-card {
            background: linear-gradient(145deg, #1e293b 0%, #0f172a 100%);
            border-radius: 12px;
            padding: 15px;
            text-align: center;
            border: 1px solid #334155;
            box-shadow: 0 4px 20px rgba(0,0,0,0.4);
            transition: transform 0.2s;
        }
        .threat-card:hover { transform: translateY(-5px); }
        .threat-val { font-family: 'Oswald'; font-size: 32px; font-weight: bold; margin: 5px 0; }
        .threat-lbl { font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #94a3b8; }
        
        /* Battle Grid */
        .battle-header { 
            background: #1e293b; color: #e2e8f0; 
            padding: 8px 15px; border-radius: 6px; 
            font-family: 'Oswald'; font-size: 16px; 
            display: flex; justify-content: space-between; align-items: center;
            border-bottom: 2px solid #334155;
            margin-top: 20px; margin-bottom: 10px;
        }
        
        /* Player Mini Cards */
        .p-card {
            background: rgba(15, 23, 42, 0.6);
            border-radius: 6px;
            padding: 10px;
            margin-bottom: 8px;
            border-left-width: 4px;
            border-left-style: solid;
            display: flex; justify-content: space-between; align-items: center;
        }
        .p-card-killer { border-left-color: #FF4F4F; background: linear-gradient(90deg, rgba(255, 79, 79, 0.1) 0%, transparent 100%); }
        .p-card-cold { border-left-color: #00E5FF; background: linear-gradient(90deg, rgba(0, 229, 255, 0.1) 0%, transparent 100%); }
    </style>
    """, unsafe_allow_html=True)

    # 1. SETUP & DADOS
    try:
        from modules.new_modules.narrative_intelligence import NarrativeIntelligence
        if "narrative_engine" not in st.session_state:
            st.session_state.narrative_engine = NarrativeIntelligence()
        engine = st.session_state.narrative_engine
    except ImportError:
        st.error("⚠️ Engine não encontrada.")
        return

    games = st.session_state.get("scoreboard", [])
    df_l5 = st.session_state.get('df_l5', pd.DataFrame())

    if not games or df_l5.empty:
        st.warning("⚠️ Dados insuficientes. Atualize na aba Config.")
        return

    # 2. AUTO-SCAN LÓGICO (Processamento)
    if "narrative_cache_v3" not in st.session_state:
        st.session_state.narrative_cache_v3 = {}

    cache_key = f"war_room_{len(games)}_{pd.Timestamp.now().strftime('%Y%m%d')}"
    scan_results = st.session_state.narrative_cache_v3.get(cache_key)

    if not scan_results:
        with st.status("📡 Rastreando Anomalias H2H...", expanded=True) as status:
            scan_results = []
            ESPN_MAP = {"SA": "SAS", "NY": "NYK", "NO": "NOP", "UTAH": "UTA", "GS": "GSW", "WSH": "WAS", "PHO": "PHX", "BRK": "BKN", "NOR": "NOP"}
            
            prog = st.progress(0)
            for i, game in enumerate(games):
                try:
                    away_raw, home_raw = game['away'], game['home']
                    away_n = ESPN_MAP.get(away_raw, away_raw)
                    home_n = ESPN_MAP.get(home_raw, home_raw)
                    
                    # Analisa os Top 8 de cada time (aumentei o range)
                    r_away = df_l5[df_l5['TEAM'] == away_n].sort_values('PTS_AVG', ascending=False).head(8)
                    r_home = df_l5[df_l5['TEAM'] == home_n].sort_values('PTS_AVG', ascending=False).head(8)

                    def analyze_player(row, opp_team, my_team):
                        pid, pname = row['PLAYER_ID'], row['PLAYER']
                        data = engine.get_player_matchup_history(pid, pname, opp_team)
                        
                        if data and 'comparison' in data:
                            diff = data['comparison'].get('diff_pct', 0)
                            
                            # LÓGICA DE CLASSIFICAÇÃO RIGOROSA
                            n_type = "NEUTRAL"
                            if diff >= 15: n_type = "KILLER"      # +15% melhor que a média
                            elif diff <= -15: n_type = "COLD"     # -15% pior que a média
                            
                            if n_type != "NEUTRAL":
                                scan_results.append({
                                    "game_id": f"{away_raw} @ {home_raw}",
                                    "player": pname,
                                    "team": my_team,
                                    "opponent": opp_team,
                                    "diff": diff,
                                    "avg": data['avg_stats'].get('PTS', 0),
                                    "type": n_type,
                                    "pid": pid,
                                    "badge": data.get('badge', '')
                                })

                    for _, p in r_away.iterrows(): analyze_player(p, home_n, away_raw)
                    for _, p in r_home.iterrows(): analyze_player(p, away_n, home_raw)
                    
                    prog.progress((i+1)/len(games))
                except: continue
            
            st.session_state.narrative_cache_v3[cache_key] = scan_results
            status.update(label="✅ Scan Completo!", state="complete", expanded=False)
            time.sleep(0.5)
            st.rerun()

    # 3. RENDERIZAÇÃO DA UI (LAYOUT SOLICITADO)
    
    st.markdown('<div class="war-room-title">&#9876; NARRATIVE WAR ROOM</div>', unsafe_allow_html=True)
    st.markdown('<div class="war-room-sub">Monitoramento de anomalias estatísticas históricas.</div>', unsafe_allow_html=True)

    if not scan_results:
        st.info("Nenhuma anomalia crítica detectada hoje (Jogos equilibrados).")
        return

    # --- A. TICKER DE ALERTA (TOP 3 THREATS) ---
    killers = [x for x in scan_results if x['type'] == "KILLER"]
    top_threats = sorted(killers, key=lambda x: x['diff'], reverse=True)[:3]
    
    if top_threats:
        cols = st.columns(3)
        for idx, t in enumerate(top_threats):
            photo = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{int(t['pid'])}.png"
            color = "#FF4F4F"
            
            with cols[idx]:
                st.markdown(f"""
                <div class="threat-card" style="border-top: 4px solid {color};">
                    <div style="display:flex; justify-content:center; margin-bottom:10px;">
                        <img src="{photo}" style="width:60px; height:60px; border-radius:50%; border:2px solid {color}; object-fit:cover;">
                    </div>
                    <div style="font-family:'Oswald'; font-size:16px; color:#fff;">{t['player']}</div>
                    <div style="font-size:11px; color:#94a3b8;">vs {t['opponent']}</div>
                    <div class="threat-val" style="color:{color};">+{t['diff']:.0f}%</div>
                    <div class="threat-lbl">Performance Histórica</div>
                </div>
                """, unsafe_allow_html=True)
    
    # --- B. GRID DE BATALHA (MATCHUPS) ---
    # Agrupa por Jogo
    games_dict = {}
    for item in scan_results:
        gid = item['game_id']
        if gid not in games_dict: games_dict[gid] = {"home": [], "away": []}
        
        # Identifica lado (Home ou Away) baseado no nome do time no ID do jogo
        # ID: "AWAY @ HOME"
        away_name, home_name = gid.split(" @ ")
        if item['team'] == home_name:
            games_dict[gid]["home"].append(item)
        else:
            games_dict[gid]["away"].append(item)

    for game_name, rosters in games_dict.items():
        away_team, home_team = game_name.split(" @ ")
        
        # Renderiza Header do Jogo
        st.markdown(f"""
        <div class="battle-header">
            <div>✈️ {away_team}</div>
            <div style="font-size:12px; color:#64748B;">VS</div>
            <div>🏠 {home_team}</div>
        </div>
        """, unsafe_allow_html=True)
        
        c1, c2 = st.columns(2)
        
        # Função interna para renderizar lista
        def render_list(col, items, align="left"):
            with col:
                if not items:
                    st.markdown(f"<div style='text-align:{align}; color:#475569; font-size:12px; padding:10px;'><i>Neutro</i></div>", unsafe_allow_html=True)
                else:
                    for p in items:
                        is_killer = p['type'] == "KILLER"
                        css_class = "p-card-killer" if is_killer else "p-card-cold"
                        color = "#FF4F4F" if is_killer else "#00E5FF"
                        icon = "&#128293;" if is_killer else "&#10052;" # Fire / Snowflake
                        sign = "+" if p['diff'] > 0 else ""
                        
                        st.markdown(f"""
                        <div class="p-card {css_class}">
                            <div>
                                <div style="font-weight:bold; font-size:13px; color:#fff;">{p['player']}</div>
                                <div style="font-size:10px; color:{color}; font-weight:bold;">{icon} {p['badge']}</div>
                            </div>
                            <div style="text-align:right;">
                                <div style="font-family:'Oswald'; font-size:16px; color:{color};">{sign}{p['diff']:.0f}%</div>
                                <div style="font-size:9px; color:#94a3b8;">Avg: {p['avg']:.1f}</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

        render_list(c1, rosters["away"], "left")
        render_list(c2, rosters["home"], "right")
# ============================================================================
# FUNÇÃO PARA RENDERIZAR CARD DE JOGO (ATUALIZADA)
# ============================================================================
import streamlit as st
import streamlit.components.v1 as components
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

# ============================================================================
# DASHBOARD (VISUAL ARENA V7.0 - STABLE & CLEAN)
# ============================================================================
def show_dashboard_page():
    # Helper de Fontes e Cores
    st.markdown("""
    <style>
        .dash-title { font-family: 'Oswald'; font-size: 20px; color: #E2E8F0; margin-bottom: 10px; letter-spacing: 1px; text-transform: uppercase; }
        .gold-text { color: #D4AF37; }
    </style>
    """, unsafe_allow_html=True)

    # 1. Carrega Dados
    df_l5 = st.session_state.get('df_l5', pd.DataFrame())
    games = get_scoreboard_data()
    
    if df_l5.empty:
        st.warning("⚠️ Base de dados L5 vazia.")
        return

    # --- FILTRO: APENAS QUEM JOGA HOJE ---
    teams_playing_today = []
    if not games.empty:
        teams_playing_today = set(games['home'].tolist() + games['away'].tolist())
    
    if not teams_playing_today:
        st.info("Nenhum jogo identificado para hoje.")
        df_today = pd.DataFrame()
    else:
        df_today = df_l5[df_l5['TEAM'].isin(teams_playing_today)]

    # ========================================================================
    # 2. DESTAQUES DO DIA
    # ========================================================================
    st.markdown('<div class="dash-title gold-text">⭐ DESTAQUES DO DIA (JOGOS DE HOJE)</div>', unsafe_allow_html=True)
    
    def truncate_name(name, limit=16):
        if not name: return ""
        if len(name) <= limit: return name
        parts = name.split()
        if len(parts) > 1: return f"{parts[0][0]}. {' '.join(parts[1:])}"[:limit]
        return name[:limit]

    if df_today.empty:
        st.warning("Nenhum jogador da base L5 joga hoje.")
    else:
        def get_top_n(df, col, n=3):
            return df.nlargest(n, col)[['PLAYER', 'TEAM', col, 'PLAYER_ID']]

        top_pts = get_top_n(df_today, 'PTS_AVG')
        top_ast = get_top_n(df_today, 'AST_AVG')
        top_reb = get_top_n(df_today, 'REB_AVG')

        def render_golden_card(title, df_top, color="#D4AF37", icon="👑"):
            if df_top.empty: return
            king = df_top.iloc[0]
            p_id = king['PLAYER_ID']
            photo = f"https://cdn.nba.com/headshots/nba/latest/1040x760/{int(p_id)}.png"
            val = king[df_top.columns[2]] 
            
            row2_html = ""
            if len(df_top) > 1:
                p2 = df_top.iloc[1]
                row2_html = f"""<div style="display:flex; justify-content:space-between; font-size:11px; color:#cbd5e1; margin-bottom:3px; border-bottom:1px dashed #334155; font-family:'Oswald' !important;"><span>2. {truncate_name(p2['PLAYER'])}</span><span style="color:{color}">{p2[df_top.columns[2]]:.1f}</span></div>"""
            
            row3_html = ""
            if len(df_top) > 2:
                p3 = df_top.iloc[2]
                row3_html = f"""<div style="display:flex; justify-content:space-between; font-size:11px; color:#cbd5e1; font-family:'Oswald' !important;"><span>3. {truncate_name(p3['PLAYER'])}</span><span style="color:{color}">{p3[df_top.columns[2]]:.1f}</span></div>"""

            st.markdown(f"""
            <div style="background: #0f172a; border: 1px solid {color}; border-radius: 12px; overflow: hidden; height: 100%; box-shadow: 0 4px 15px rgba(0,0,0,0.5);">
                <div style="background: {color}20; padding: 6px; text-align: center; font-family: 'Oswald'; color: {color}; font-size: 13px; letter-spacing: 1px; border-bottom: 1px solid {color}40;">
                    {icon} {title}
                </div>
                <div style="padding: 12px; display: flex; align-items: center;">
                    <img src="{photo}" style="width: 55px; height: 55px; border-radius: 50%; border: 2px solid {color}; object-fit: cover; background: #000; margin-right: 12px;">
                    <div style="overflow: hidden;">
                        <div style="color: #fff; font-weight: bold; font-size: 14px; line-height: 1.1; font-family: 'Oswald' !important; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">{truncate_name(king['PLAYER'])}</div>
                        <div style="color: #94a3b8; font-size: 10px; font-family: 'Oswald' !important;">{king['TEAM']}</div>
                        <div style="color: {color}; font-size: 20px; font-family: 'Oswald' !important; font-weight: bold;">{val:.1f}</div>
                    </div>
                </div>
                <div style="background: rgba(0,0,0,0.4); padding: 8px 12px;">
                    {row2_html}
                    {row3_html}
                </div>
            </div>
            """, unsafe_allow_html=True)

        c1, c2, c3 = st.columns(3)
        with c1: render_golden_card("CESTINHAS", top_pts, "#FFD700", "🔥")
        with c2: render_golden_card("GARÇONS", top_ast, "#00E5FF", "🧠")
        with c3: render_golden_card("REBOTEIROS", top_reb, "#FF4F4F", "💪")

    st.markdown("<br>", unsafe_allow_html=True)

    # ========================================================================
    # 3. GAME GRID (MANTIDO)
    # ========================================================================
    st.markdown('<div class="dash-title" style="color:#E2E8F0;">🏀 JOGOS DE HOJE</div>', unsafe_allow_html=True)

    if games.empty:
        st.info("Nenhum jogo encontrado para hoje.")
    else:
        odds_cache = st.session_state.get("odds", {})
        rows = st.columns(2)
        for i, (index, game) in enumerate(games.iterrows()):
            with rows[i % 2]:
                render_game_card(
                    away_team=game['away'],
                    home_team=game['home'],
                    game_data=game,
                    odds_map=odds_cache
                )
# ============================================================================
# EXECUÇÃO PRINCIPAL (CORRIGIDA E CONSOLIDADA)
# ============================================================================
def main():
    st.set_page_config(page_title="DigiBets IA", layout="wide", page_icon="🏀")
    
    # CSS GLOBAL CRÍTICO (FUNDO PRETO & FONTE NUNITO & REMOÇÃO DE ESPAÇOS)
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

    # --- MENU LATERAL (DENTRO DO MAIN PARA FUNCIONAR) ---
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
            "🔥 Hot Streaks", "📊 Matriz 5-7-10", "🧩 Desdobra Múltipla",
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
    elif choice == "📊 Matriz 5-7-10": show_matriz_5_7_10_page()
    elif choice == "🧩 Desdobra Múltipla": show_desdobramentos_inteligentes()
    
    elif choice == "🛡️ DvP Confrontos": show_dvp_analysis()
    elif choice == "🏥 Depto Médico": show_depto_medico()
    elif choice == "👥 Escalações": show_escalacoes()
    
    elif choice == "⚙️ Config": show_config_page()
    elif choice == "🔍 Testar Conexão Supabase": show_cloud_diagnostics()

if __name__ == "__main__":
    main()































































