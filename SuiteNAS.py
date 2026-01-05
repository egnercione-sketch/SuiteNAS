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
# DVP SNIPER
# ============================================================================
def show_dvp_analysis():
    import streamlit as st
    import pandas as pd
    import textwrap
    import html

    # --- 1. CSS VISUAL ---
    st.markdown(textwrap.dedent("""
    <style>
        .sniper-card {
            background: rgba(15, 23, 42, 0.8);
            border: 1px solid #334155;
            border-radius: 8px;
            padding: 10px;
            margin-bottom: 8px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-left: 4px solid #64748B;
        }
        .target-green { border-left-color: #00FF9C; background: linear-gradient(90deg, rgba(0,255,156,0.05) 0%, rgba(15,23,42,0.8) 100%); }
        .avoid-red { border-left-color: #FF4F4F; opacity: 0.6; }
        
        .snip-name { font-weight: bold; color: #F8FAFC; font-size: 14px; font-family: sans-serif; }
        .snip-team { font-size: 10px; color: #94A3B8; background: rgba(255,255,255,0.1); padding: 2px 6px; border-radius: 4px; margin-left: 6px; }
        .snip-reason { font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 3px; }
        .snip-rank { font-family: 'Courier New', monospace; font-weight: bold; font-size: 16px; }
        
        .gold-player-box {
            background: linear-gradient(135deg, rgba(6, 78, 59, 0.8) 0%, rgba(0, 0, 0, 0.6) 100%);
            border: 1px solid #00FF9C;
            border-radius: 8px;
            padding: 15px;
            text-align: center;
        }
        .gold-rank { color: #00FF9C; font-weight: bold; font-size: 12px; margin-bottom: 5px; }
        .gold-name { color: #fff; font-weight: bold; font-size: 16px; margin-bottom: 2px; }
        .gold-vs { color: #94A3B8; font-size: 11px; }
    </style>
    """), unsafe_allow_html=True)

    st.header("🎯 DvP SNIPER")
    st.caption("Alvos baseados em vulnerabilidade defensiva (Posições Inferidas).")

    # --- 2. DADOS ---
    dvp_analyzer = st.session_state.get("dvp_analyzer")
    if not dvp_analyzer or not hasattr(dvp_analyzer, 'defense_data'):
        try:
            from modules.new_modules.dvp_analyzer import DvPAnalyzer
            st.session_state.dvp_analyzer = DvPAnalyzer()
            dvp_analyzer = st.session_state.dvp_analyzer
        except:
            st.error("Erro no DvP. Atualize em Config.")
            return

    games = st.session_state.get("scoreboard", [])
    df_l5 = st.session_state.get("df_l5", pd.DataFrame())

    if not games or df_l5.empty:
        st.warning("⚠️ Dados insuficientes. Atualize Scoreboard e L5.")
        return

    TEAM_MAP = {"GS": "GSW", "NO": "NOP", "NY": "NYK", "SA": "SAS", "UTAH": "UTA", "WSH": "WAS", "PHO": "PHX", "BRK": "BKN"}
    POSITIONS = ["PG", "SG", "SF", "PF", "C"]
    
    # --- 3. LÓGICA DE INFERÊNCIA V34 (FILTRO DE EXCLUSÃO) ---
    def infer_if_player_fits_pos(row, target_pos):
        ast = float(row.get('AST_AVG', 0))
        reb = float(row.get('REB_AVG', 0))
        pts = float(row.get('PTS_AVG', 0))
        blk = float(row.get('BLK_AVG', 0))
        fg3 = float(row.get('FG3M_AVG', 0))
        
        # 1. PIVÔ (C)
        if target_pos == "C":
            # Tem que pegar rebote E (dar toco OU não chutar muito de 3)
            # Exclui alas que só pegam rebote
            return (reb >= 8.0) or (reb >= 6.0 and blk >= 0.8)
            
        # 2. ARMADOR (PG)
        elif target_pos == "PG":
            # Tem que dar assistência E NÃO pode pegar muito rebote (pra excluir Jokic/Sabonis)
            return (ast >= 4.5) and (reb < 7.5)
            
        # 3. ALA-PIVÔ (PF)
        elif target_pos == "PF":
            # Rebote sólido, mas menos assistências que um PG e menos rebote que um C puro
            return (reb >= 5.0) and (reb < 10.0) and (ast < 4.5)
            
        # 4. ALA-ARMADOR (SG)
        elif target_pos == "SG":
            # Chuta de 3, mas NÃO é o armador principal (pouca ast) e NÃO é pivô (pouco rebote/toco)
            return (fg3 >= 1.5) and (ast < 5.5) and (reb < 6.0) and (blk < 0.8)
            
        # 5. ALA (SF)
        elif target_pos == "SF":
            # O "Resto": Pontua, pega rebote médio, mas não domina nenhuma estatística extrema
            return (reb >= 2.5) and (reb < 8.0) and (ast < 5.0) and (blk < 1.0)
            
        return False

    def find_best_player_for_pos(team_abbr, pos_code, df_source):
        norm_team = TEAM_MAP.get(team_abbr, team_abbr)
        
        # Identificação de Colunas
        cols = df_source.columns
        team_col = next((c for c in cols if c.upper() in ['TEAM', 'TEAM_ABBREVIATION']), None)
        if not team_col: return None, 0
        
        name_col = None
        for cand in ['PLAYER_NAME', 'PLAYER', 'NAME', 'Player']:
            if cand in cols: name_col = cand; break
        if not name_col:
            name_col = next((c for c in cols if 'PLAYER' in c.upper() and 'ID' not in c.upper()), None)
        if not name_col: return None, 0

        pos_col = next((c for c in cols if c.upper() in ['POSITION', 'POS', 'PLAYER_POSITION']), None)
        min_col = next((c for c in cols if 'MIN' in c.upper()), 'MIN_AVG')

        # Filtra Time
        team_players = df_source[df_source[team_col] == norm_team].copy()
        if team_players.empty: return None, 0
        
        candidates = pd.DataFrame()
        
        # Tenta Coluna Oficial
        if pos_col:
            try:
                team_players['TEMP_POS'] = team_players[pos_col].astype(str).str.upper()
                if pos_code == "PG": candidates = team_players[team_players['TEMP_POS'].str.contains("PG|POINT")]
                elif pos_code == "SG": candidates = team_players[team_players['TEMP_POS'].str.contains("SG|SHOOTING")]
                elif pos_code == "SF": candidates = team_players[team_players['TEMP_POS'].str.contains("SF|SMALL")]
                elif pos_code == "PF": candidates = team_players[team_players['TEMP_POS'].str.contains("PF|POWER")]
                elif pos_code == "C": candidates = team_players[team_players['TEMP_POS'].str.contains("C|CENTER")]
            except: pass
        
        # Fallback: Inferência V34
        if candidates.empty:
            mask = team_players.apply(lambda r: infer_if_player_fits_pos(r, pos_code), axis=1)
            candidates = team_players[mask]

        if candidates.empty: return None, 0
            
        # Pega o Titular (Mais Minutos)
        try:
            candidates[min_col] = pd.to_numeric(candidates[min_col], errors='coerce').fillna(0)
            best = candidates.sort_values(min_col, ascending=False).iloc[0]
            return str(best[name_col]), best[min_col]
        except:
            return None, 0

    # --- 4. PROCESSAMENTO ---
    targets = []
    matchups = []

    for g in games:
        home, away = g['home'], g['away']
        home_key, away_key = TEAM_MAP.get(home, home), TEAM_MAP.get(away, away)
        
        g_targets, g_avoids = [], []
        
        for side, my_team, opp_team, opp_key in [('HOME', home, away, away_key), ('AWAY', away, home, home_key)]:
            for pos in POSITIONS:
                rank = dvp_analyzer.get_position_rank(opp_key, pos)
                
                if rank >= 25 or rank <= 5:
                    p_name, p_min = find_best_player_for_pos(my_team, pos, df_l5)
                    
                    if p_name and p_min > 20:
                        # Evita duplicatas do mesmo jogador no mesmo jogo
                        # (Ex: Impedir que o mesmo cara seja listado como SG e SF)
                        exists = any(t['player'] == p_name for t in g_targets + g_avoids)
                        if exists: continue

                        item = {
                            'player': p_name, 'team': my_team, 'vs': opp_team, 'pos': pos, 
                            'rank': rank, 'side': side, 'min': p_min
                        }
                        if rank >= 25: 
                            targets.append(item); g_targets.append(item)
                        else: 
                            g_avoids.append(item)
        
        if g_targets or g_avoids:
            matchups.append({'game': f"{away} @ {home}", 'targets': g_targets, 'avoids': g_avoids})

    # --- 5. RENDERIZAÇÃO ---
    targets.sort(key=lambda x: (x['rank'], x['min']), reverse=True)
    st.subheader("🏆 Top 3 Melhores Matchups")
    if targets:
        c1, c2, c3 = st.columns(3)
        for i, t in enumerate(targets[:3]):
            with [c1, c2, c3][i]:
                st.markdown(f"""
                <div class="gold-player-box">
                    <div class="gold-rank">🎯 ALVO RANK {t['rank']}</div>
                    <div class="gold-name">{html.escape(t['player'])}</div>
                    <div class="gold-vs">{t['team']} vs {t['vs']} ({t['pos']}*)</div>
                </div>
                """, unsafe_allow_html=True)
        st.caption("* Posição estimada por stats. Verifique escalações se jogadores aparecerem em times trocados.")
    else: st.info("Sem alvos perfeitos detectados.")

    st.markdown("---")
    st.subheader("⚔️ Análise por Jogo")

    def render_card(item):
        is_target = item['rank'] >= 25
        css = "target-green" if is_target else "avoid-red"
        color = "#00FF9C" if is_target else "#FF4F4F"
        icon = "🔥" if is_target else "❄️"
        txt = f"Defesa Fraca vs {item['pos']}" if is_target else f"Defesa Forte vs {item['pos']}"
        
        return f"""
        <div class="sniper-card {css}">
            <div>
                <div style="display:flex; align-items:center;">
                    <span style="font-size:16px; margin-right:8px;">{icon}</span>
                    <span class="snip-name">{html.escape(item['player'])}</span>
                    <span class="snip-team">{item['team']}</span>
                </div>
                <div class="snip-reason" style="color:{color}">{txt}</div>
            </div>
            <div class="snip-rank" style="color:{color}">#{item['rank']}</div>
        </div>
        """

    for m in matchups:
        st.markdown(f"<div style='text-align:center; font-family:Oswald; margin:20px 0; border-bottom:1px solid #334155;'>{m['game']}</div>", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("<div style='color:#00FF9C; font-weight:bold; font-size:12px;'>🚀 OVER (ALVOS)</div>", unsafe_allow_html=True)
            if m['targets']:
                h = "".join([render_card(t) for t in sorted(m['targets'], key=lambda x: x['rank'], reverse=True)])
                st.markdown(h, unsafe_allow_html=True)
            else: st.caption("---")
        with c2:
            st.markdown("<div style='color:#FF4F4F; font-weight:bold; font-size:12px;'>🛑 UNDER (EVITAR)</div>", unsafe_allow_html=True)
            if m['avoids']:
                h = "".join([render_card(a) for a in sorted(m['avoids'], key=lambda x: x['rank'])])
                st.markdown(h, unsafe_allow_html=True)
            else: st.caption("---")
                
# ============================================================================
# PÁGINA: BLOWOUT COMMANDER (V12.4 - ROBUST SPREAD & UTAH FIX)
# ============================================================================
def show_blowout_hunter_page():
    import streamlit as st
    import pandas as pd
    import os
    import json
    import re
    import html

    # --- 1. CONFIGURAÇÃO VISUAL ---
    st.markdown("""<style>
        .blowout-container { background: #0f172a; border: 1px solid #1e293b; border-radius: 8px; padding: 15px; margin-bottom: 20px; }
        .vulture-card { background: #172554; border-left: 4px solid #3b82f6; padding: 10px; margin-bottom: 8px; border-radius: 4px; display: flex; justify-content: space-between; align-items: center; }
        .v-name { color: #f8fafc; font-weight: bold; font-size: 14px; }
        .v-meta { color: #94a3b8; font-size: 11px; }
        .v-proj { color: #4ade80; font-weight: bold; font-size: 16px; text-align: right; }
        .tag-dna { background: #7c3aed; color: #fff; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: bold; }
        .tag-math { background: #334155; color: #cbd5e1; padding: 2px 6px; border-radius: 4px; font-size: 10px; }
    </style>""", unsafe_allow_html=True)

    st.header("🌪️ BLOWOUT HUNTER PRO (V13.8)")
    show_debug = st.checkbox("🔍 Modo Auditoria (Filtro Universal de Injuries)", value=True)

    # --- 2. CARREGAMENTO DE MAPAS E DNA ---
    TEAM_VARS = globals().get('TEAM_NAME_VARIATIONS', {})
    ABBR_MAP = globals().get('TEAM_ABBR_TO_ODDS', {})
    normalize_func = globals().get('normalize_name')
    
    dna_path = os.path.join("cache", "rotation_dna.json")
    ROTATION_DNA = st.session_state.get('rotation_dna_cache', {})
    if not ROTATION_DNA and os.path.exists(dna_path):
        try:
            with open(dna_path, 'r') as f: ROTATION_DNA = json.load(f)
        except: ROTATION_DNA = {}

    # --- 3. CARREGAMENTO DE LESÕES (PROTOCOLO FONTE ÚNICA) ---
    banned_players = set()
    try:
        from injuries import InjuryMonitor, INJURIES_CACHE_FILE
        monitor = st.session_state.get('injuries_manager') or InjuryMonitor(cache_file=INJURIES_CACHE_FILE)
        raw_injuries = monitor.get_all_injuries()
        
        # Achatamento da lista (Flattening)
        global_list = []
        source = raw_injuries.get('teams', raw_injuries) if isinstance(raw_injuries, dict) else raw_injuries
        if isinstance(source, dict):
            for team_players in source.values():
                if isinstance(team_players, list): global_list.extend(team_players)
        elif isinstance(source, list):
            global_list = source

        # Identificar quem está fora
        for inj in global_list:
            status = str(inj.get('status') or inj.get('Status') or "").lower()
            # Bloqueia se tiver status de lesão e NÃO estiver marcado como Ativo/Disponível
            if any(x in status for x in ['out', 'doubt', 'quest', 'day', 'injur', 'ir']):
                if not any(x in status for x in ['available', 'active']):
                    p_name = inj.get('name') or inj.get('player')
                    if p_name and normalize_func:
                        banned_players.add(normalize_func(p_name))
    except Exception as e:
        st.error(f"Erro ao carregar sistema de lesões: {e}")

    # --- 4. NORMALIZAÇÃO DE SIGLAS ---
    def get_official_abbr(raw_name):
        if not raw_name: return "UNK"
        name_up = str(raw_name).upper().strip()
        # 1. Busca via variações
        for official_name, variants in TEAM_VARS.items():
            if name_up == official_name.upper() or name_up in [v.upper() for v in variants]:
                for abbr, full in ABBR_MAP.items():
                    if full.upper() == official_name.upper(): return abbr
        # 2. Correções manuais
        manual = {"OKL": "OKC", "BRK": "BKN", "PHX": "PHO", "GOL": "GSW", "WSH": "WAS", "UTAH": "UTA", "NY": "NYK", "SA": "SAS"}
        return manual.get(name_up, name_up[:3])

    # --- 5. PROCESSAMENTO DE JOGOS ---
    source_games = st.session_state.get('pinnacle_games') or st.session_state.get('scoreboard') or []
    
    for g in source_games:
        raw_spread = g.get('spread') or g.get('handicap') or 0
        try:
            spread_val = abs(float(re.findall(r"[-+]?\d*\.\d+|\d+", str(raw_spread))[-1])) if raw_spread else 0
        except: spread_val = 0

        if spread_val >= 9.0:
            h_raw = g.get('home_team') or g.get('home')
            a_raw = g.get('away_team') or g.get('away')
            h = get_official_abbr(h_raw)
            a = get_official_abbr(a_raw)
            
            st.markdown(f'<div class="blowout-container"><h4>{a} @ {h} <span style="color:#fca5a5;">(SPREAD {spread_val})</span></h4>', unsafe_allow_html=True)
            cols = st.columns(2)
            
            for idx, team in enumerate([h, a]):
                with cols[idx]:
                    st.write(f"**Analisando {team}...**")
                    candidates, logs = [], []

                    if 'df_l5' in st.session_state and not st.session_state.df_l5.empty:
                        df_t = st.session_state.df_l5[st.session_state.df_l5['TEAM'] == team]
                        
                        for _, p in df_t.iterrows():
                            name = p.get('PLAYER') or p.get('PLAYER_NAME')
                            norm_pname = normalize_func(name) if normalize_func else str(name).lower()
                            avg_min = float(p.get('MIN_AVG') or p.get('MIN') or 0)
                            avg_pts = float(p.get('PTS_AVG') or p.get('PTS') or 0)

                            # --- 🚑 FILTRO DE INJURIES ATIVO ---
                            if norm_pname in banned_players:
                                logs.append(f"🚑 {name}: REMOVIDO (Injury Report)")
                                continue

                            # --- FILTROS DE ROTAÇÃO ---
                            if not (2.0 <= avg_min <= 30.5):
                                logs.append(f"❌ {name}: {avg_min}m (Fora da faixa)")
                                continue

                            # --- PROJEÇÃO DNA VS MATH ---
                            team_dna = ROTATION_DNA.get(team, [])
                            dna_hit = next((d for d in team_dna if norm_pname in (normalize_func(d['name']) if normalize_func else d['name'].lower())), None)
                            
                            if dna_hit:
                                proj = dna_hit.get('avg_pts_blowout', 0)
                                src = f"🧬 DNA ({dna_hit.get('frequency')})"
                                if proj < avg_pts: proj = (proj + avg_pts) / 2
                            else:
                                boost = 2.2 if avg_min < 12 else 1.6
                                proj = (avg_pts / max(avg_min, 1)) * (avg_min * boost)
                                src = "📐 MATH"

                            if proj >= 2.5:
                                candidates.append({'name': name, 'proj': proj, 'min': avg_min, 'src': src})

                    if not candidates:
                        st.caption("Sem alvos.")
                    else:
                        for c in sorted(candidates, key=lambda x: x['proj'], reverse=True)[:5]:
                            tag_cls = "tag-dna" if "DNA" in c['src'] else "tag-math"
                            st.markdown(f"""<div class="vulture-card">
                                <div><div class="v-name">{c['name']}</div>
                                <div class="v-meta">{c['min']:.1f}m <span class="{tag_cls}">{c['src']}</span></div></div>
                                <div class="v-proj">{c['proj']:.1f}</div></div>""", unsafe_allow_html=True)
                    
                    if show_debug and logs:
                        with st.expander(f"🔍 Auditoria {team}"):
                            for l in logs: st.text(l)
            st.markdown("</div>", unsafe_allow_html=True)
            

# ============================================================================
# ENGINE MOMENTUM (CÁLCULO E CACHE - V2.0 BLINDADA)
# ============================================================================
def get_momentum_data():
    """
    Processa df_l5 para gerar dados de Momentum e usa cache universal.
    """
    # 1. Tenta recuperar do Cache Universal
    cached_data = get_data_universal("momentum_cache")
    
    if cached_data and isinstance(cached_data, list) and len(cached_data) > 0:
        return pd.DataFrame(cached_data)

    # 2. Se não tem cache, calcula do zero
    df_l5 = st.session_state.get('df_l5', pd.DataFrame())
    
    if df_l5.empty:
        return pd.DataFrame()

    try:
        # LIMPEZA CRÍTICA: Remove linhas onde Jogador ou Time são nulos
        df = df_l5.copy()
        df = df.dropna(subset=['PLAYER', 'TEAM']) 
        
        # --- ALGORITMO DE MOMENTUM ---
        df['PRA'] = df['PTS_AVG'] + df['REB_AVG'] + df['AST_AVG']
        
        # Evita divisão por zero nos minutos
        df['EFF_PER_MIN'] = df['PRA'] / df['MIN_AVG'].replace(0, 1)
        
        min_eff = df['EFF_PER_MIN'].min()
        max_eff = df['EFF_PER_MIN'].max()
        
        # Normalização segura
        if max_eff == min_eff:
            df['SCORE_RAW'] = 50
        else:
            df['SCORE_RAW'] = ((df['EFF_PER_MIN'] - min_eff) / (max_eff - min_eff)) * 100
        
        df['MOMENTUM_SCORE'] = df['SCORE_RAW'].apply(lambda x: min(100, max(0, x)))
        
        def get_status(score):
            if score >= 75: return "🔥 BULLISH"
            if score <= 35: return "🧊 BEARISH"
            return "⚖️ NEUTRAL"
            
        df['STATUS'] = df['MOMENTUM_SCORE'].apply(get_status)
        
        # CORREÇÃO DA NUVEM: Substitui NaN por 0 ou vazio antes de salvar (JSON não aceita NaN)
        df_clean = df.fillna(0)
        
        data_to_save = df_clean.to_dict('records')
        save_data_universal("momentum_cache", data_to_save)
        
        return df_clean

    except Exception as e:
        st.error(f"Erro ao calcular momentum: {e}")
        return pd.DataFrame()

# ============================================================================
# PÁGINA MOMENTUM (VISUAL GLOBAL DIRETO - SEM FILTROS)
# ============================================================================
def show_momentum_page():
    # CSS Específico
    st.markdown("""
    <style>
        .market-header { font-family: 'Oswald'; font-size: 24px; color: #fff; letter-spacing: 2px; }
        .ticker-box {
            background: #0f172a; 
            border: 1px solid #334155; 
            padding: 10px; 
            border-radius: 4px; 
            text-align: center;
            font-family: 'Oswald';
            color: #e2e8f0;
        }
        .ticker-val { font-size: 18px; font-weight: bold; }
        .ticker-lbl { font-size: 10px; color: #94a3b8; text-transform: uppercase; }
        .section-title {
            font-family: 'Oswald'; 
            font-size: 18px; 
            margin-bottom: 15px; 
            padding-bottom: 5px;
            letter-spacing: 1px;
        }
    </style>
    """, unsafe_allow_html=True)

    # 1. Dados
    df = get_momentum_data()
    
    if df.empty:
        st.warning("⚠️ Sem dados de Momentum. Por favor, vá em Configurações e Atualize a Base L5.")
        return

    # --- TOP HEADER: MARKET SNAPSHOT ---
    c1, c2, c3, c4 = st.columns(4)
    
    avg_score = df['MOMENTUM_SCORE'].mean()
    bulls_count = len(df[df['STATUS'] == "🔥 BULLISH"])
    bears_count = len(df[df['STATUS'] == "🧊 BEARISH"])
    vol = df['MIN_AVG'].mean()

    with c1: st.markdown(f"""<div class="ticker-box"><div class="ticker-val" style="color:#22d3ee">{avg_score:.1f}</div><div class="ticker-lbl">ÍNDICE GERAL</div></div>""", unsafe_allow_html=True)
    with c2: st.markdown(f"""<div class="ticker-box"><div class="ticker-val" style="color:#00ff9c">{bulls_count}</div><div class="ticker-lbl">MERCADO EM ALTA</div></div>""", unsafe_allow_html=True)
    with c3: st.markdown(f"""<div class="ticker-box"><div class="ticker-val" style="color:#f87171">{bears_count}</div><div class="ticker-lbl">MERCADO EM BAIXA</div></div>""", unsafe_allow_html=True)
    with c4: st.markdown(f"""<div class="ticker-box"><div class="ticker-val
# ============================================================================
# PROPS ODDS - LAS VEGAS
# ============================================================================
def show_props_odds_page():
    st.header("🔥 Props & Odds Reais (Pinnacle)")

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
        
# ============================================================================
# PÁGINA: NEXUS INTELLIGENCE (VISUAL FINAL V4.0 - ASCII SAFE MODE)
# ============================================================================
def show_nexus_page():
    # 1. Dados
    full_cache = get_data_universal("real_game_logs")
    scoreboard = get_data_universal("scoreboard")
    
    # HEADER (SEM CARACTERES ESPECIAIS NO CODIGO PYTHON)
    # Substituimos o ponto (bullet) por &bull; e o cerebro por codigo HTML
    st.markdown("""
    <div style="padding: 20px; text-align: center;">
        <h1 style="font-family: 'Oswald', sans-serif; font-size: 48px; color: #fff; margin: 0;">&#129504; NEXUS INTELLIGENCE</h1>
        <p style="color: #94a3b8; font-weight: bold; letter-spacing: 3px; font-size: 14px; margin-top: 5px;">MODO PREDADOR &bull; PRECISAO CIRURGICA</p>
    </div>
    """, unsafe_allow_html=True)

    if not full_cache:
        st.error("Logs de jogos vazios. Atualize a base de dados.")
        return

    # 2. Filtros (REMOVIDOS EMOJIS DOS LABELS DO STREAMLIT)
    st.markdown("<div style='background: #1e293b; padding: 15px; border-radius: 8px; margin-bottom: 20px; border: 1px solid #334155;'>", unsafe_allow_html=True)
    c_slider, c_type = st.columns([2, 1])
    with c_slider:
        # Removido emoji do label
        min_score = st.slider("Score Minimo (Qualidade)", 50, 100, 65)
    with c_type:
        filter_type = st.selectbox("Tipo de Oportunidade", ["TODAS", "SGP (Duplas)", "DEF (vs Defesa)"])
    st.markdown("</div>", unsafe_allow_html=True)

    # 3. Engine & Execução
    try:
        # Tenta importar caso não esteja no escopo global
        if 'NexusEngine' not in globals():
            from modules.new_modules.nexus_engine import NexusEngine 
            
        nexus = NexusEngine(full_cache, scoreboard or [])
        all_ops = nexus.run_nexus_scan()
        
        # Filtragem
        opportunities = [op for op in all_ops if op['score'] >= min_score]
        
        if filter_type == "SGP (Duplas)":
            opportunities = [op for op in opportunities if op['type'] == 'SGP']
        elif filter_type == "DEF (vs Defesa)":
            opportunities = [op for op in opportunities if op['type'] != 'SGP']
            
    except Exception as e:
        # Msg simples sem caracteres especiais
        st.info("Aguardando sincronizacao da Engine Nexus...")
        return

    if not opportunities:
        st.info(f"Nenhuma oportunidade encontrada com Score acima de {min_score}.")
        return

    # Icone de Raio via HTML: &#9889;
    st.markdown(f"**&#9889; {len(opportunities)} Oportunidades Encontradas**", unsafe_allow_html=True)
    st.markdown("---")

    # 4. Renderização (CARD BLINDADO - TABELA HTML)
    for op in opportunities:
        color = op['color']
        score = op['score']
        
        # Conversão Segura de Strings e tratativa de acentos
        title = str(op['title'])
        
        # Hero Data
        h_name = str(op['hero']['name'])
        if len(h_name) > 18: h_name = h_name[:16] + "..."
        h_photo = op['hero']['photo']
        h_info = f"{op['hero']['target']} {op['hero']['stat']}"
        
        # Partner/Villain Data
        p_obj = op.get('partner', op.get('villain'))
        p_name = str(p_obj['name'])
        if len(p_name) > 18: p_name = p_name[:16] + "..."
        p_photo = p_obj.get('photo', p_obj.get('logo'))
        
        # Ícones HTML
        if 'partner' in op:
            p_info = f"{op['partner']['target']} {op['partner']['stat']}"
            mid_icon = "&#128279;" # Link Icon
        else:
            p_info = f"Alvo: {op['villain']['status']}"
            mid_icon = "&#9876;" # Swords Icon
            
        impact = op.get('impact', 'Alta Sinergia Detectada')

        # Card HTML
        card_html = f"""
        <div style="border: 1px solid {color}; border-left: 5px solid {color}; border-radius: 12px; background-color: #0f172a; overflow: hidden; margin-bottom: 20px; box-shadow: 0 4px 10px rgba(0,0,0,0.5);">
            <div style="background-color: {color}20; padding: 8px 15px; border-bottom: 1px solid {color}40; display: flex; justify-content: space-between; align-items: center;">
                <span style="font-family: 'Oswald', sans-serif; color: #ffffff; font-size: 14px; letter-spacing: 1px;">{title}</span>
                <span style="background-color: {color}; color: #000000; font-weight: bold; font-family: 'Oswald', sans-serif; font-size: 11px; padding: 2px 6px; border-radius: 4px;">SCORE {score}</span>
            </div>

            <table style="width: 100%; table-layout: fixed; border-collapse: collapse; border: none; margin: 0;">
                <tr>
                    <td style="width: 40%; text-align: center; vertical-align: top; padding: 15px 5px; border: none;">
                        <img src="{h_photo}" style="width: 55px; height: 55px; border-radius: 50%; border: 2px solid {color}; object-fit: cover; margin: 0 auto; display: block;">
                        <div style="color: #ffffff; font-family: 'Oswald', sans-serif; font-size: 13px; margin-top: 5px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">{h_name}</div>
                        <div style="color: {color}; font-family: sans-serif; font-size: 10px; font-weight: bold;">{h_info}</div>
                    </td>

                    <td style="width: 20%; text-align: center; vertical-align: middle; border: none;">
                        <div style="font-size: 20px; color: #64748b; opacity: 0.7;">{mid_icon}</div>
                    </td>

                    <td style="width: 40%; text-align: center; vertical-align: top; padding: 15px 5px; border: none;">
                        <img src="{p_photo}" style="width: 55px; height: 55px; border-radius: 50%; border: 2px solid #ffffff; object-fit: cover; margin: 0 auto; display: block;">
                        <div style="color: #ffffff; font-family: 'Oswald', sans-serif; font-size: 13px; margin-top: 5px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">{p_name}</div>
                        <div style="color: #cbd5e1; font-family: sans-serif; font-size: 10px;">{p_info}</div>
                    </td>
                </tr>
            </table>

            <div style="background-color: rgba(0,0,0,0.3); padding: 6px; text-align: center; font-family: sans-serif; font-size: 10px; color: #94a3b8; border-top: 1px solid rgba(255,255,255,0.05);">
                ANALISTA: {impact}
            </div>
        </div>
        """
        st.markdown(card_html, unsafe_allow_html=True)
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
# PÁGINA: TRINITY CLUB (VERSÃO FINAL BLINDADA - NO-FORMAT ERROR)
# ============================================================================
def show_trinity_club_page():
    # Helper para renderizar a tabela
    def render_trinity_table(members, label):
        if not members:
            st.info(f"Nenhum jogador atingiu o critério de consistência {label} hoje.")
            return

        # Estilos Inline (Strings simples para não confundir o Python)
        style_card = "background: #0f172a; border-left: 4px solid #D4AF37; border-radius: 8px; padding: 15px; margin-bottom: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.3);"
        style_name = "font-family: 'Oswald', sans-serif; font-size: 16px; color: #F8FAFC; font-weight: 500; text-transform: uppercase; margin: 0;"
        style_match = "font-size: 11px; color: #94a3b8; margin-bottom: 8px;"
        
        style_stat_lbl = "font-family: sans-serif; font-size: 10px; color: #64748B; text-transform: uppercase;"
        style_stat_val = "font-family: 'Oswald', sans-serif; font-size: 18px; color: #10B981; font-weight: bold;"
        
        style_target_box = "background: rgba(212, 175, 55, 0.1); border-radius: 6px; padding: 5px 10px; border: 1px solid rgba(212, 175, 55, 0.15); text-align: center;"
        style_target_val = "font-family: 'Oswald', sans-serif; font-size: 20px; color: #D4AF37; font-weight: bold; line-height: 1;"
        style_target_sub = "font-size: 9px; color: #D4AF37; opacity: 0.8;"

        for p in members:
            # 1. Extração Segura de Dados
            p_name = str(p.get('player', 'Desconhecido'))
            p_team = str(p.get('team', 'N/A'))
            p_opp = str(p.get('opponent', 'N/A'))
            
            # 2. Valores Numéricos
            raw_floor = p.get('floor_l5', 0) if label == 'L5' else (p.get('floor_l10', 0) if label == 'L10' else p.get('floor_l15', 0))
            raw_avg = p.get('pts_avg', 0)
            raw_target = p.get('safe_target', 0)
            
            # 3. Formatação PRÉVIA (AQUI ESTAVA O ERRO ANTES)
            # Formatamos fora da f-string do HTML para evitar SyntaxError
            val_floor = f"{raw_floor:.1f}"
            val_avg = f"{raw_avg:.1f}"
            val_target = f"{raw_target:.1f}"

            # 4. HTML Simplificado (Só insere as strings já formatadas)
            html = f"""
            <div style="{style_card}">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    
                    <div style="flex: 2;">
                        <div style="{style_name}">{p_name}</div>
                        <div style="{style_match}">{p_team} vs {p_opp}</div>
                        <div style="display: flex; gap: 15px;">
                            <div>
                                <div style="{style_stat_lbl}">PISO {label}</div>
                                <div style="{style_stat_val}">{val_floor}</div>
                            </div>
                            <div>
                                <div style="{style_stat_lbl}">MEDIA</div>
                                <div style="{style_stat_val}">{val_avg}</div>
                            </div>
                        </div>
                    </div>

                    <div style="flex: 1; display: flex; justify-content: flex-end;">
                        <div style="{style_target_box}">
                            <div style="{style_target_val}">{val_target}</div>
                            <div style="{style_target_sub}">ALVO SEGURO</div>
                        </div>
                    </div>
                </div>
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)

    # --- INÍCIO DA PÁGINA ---
    # Usamos HTML entity para o troféu para evitar erro de encoding
    st.markdown("## &#127942; Trinity Club (Consistencia Extrema)")
    
    # 1. Carregamento
    full_cache = get_data_universal("real_game_logs")
    scoreboard = get_data_universal("scoreboard")

    if not full_cache:
        st.warning("Aguardando dados de logs...")
        return

    # Tenta carregar Engine
    try:
        # Verifica se precisa importar
        if 'TrinityEngine' not in globals():
            from modules.new_modules.trinity_engine import TrinityEngine
        engine = TrinityEngine(full_cache, scoreboard or [])
    except Exception as e:
        # Fallback silencioso ou msg simples
        st.info("Carregando motor Trinity...")
        return

    st.caption("Analise a consistencia dos jogadores em 3 horizontes temporais diferentes.")

    # 2. Glossário (Estilo Inline Seguro)
    st.markdown("""
    <div style="background: rgba(255, 255, 255, 0.05); border-radius: 6px; padding: 10px 15px; margin-bottom: 20px; font-family: sans-serif; font-size: 11px; color: #94a3b8; display: flex; flex-wrap: wrap; gap: 15px; border-left: 3px solid #D4AF37;">
        <div style="display: flex; align-items: center; gap: 5px;"><span style="color: #D4AF37;">&#128202; FORMA:</span> Piso da Janela</div>
        <div style="display: flex; align-items: center; gap: 5px;"><span style="color: #D4AF37;">&#127968; LOCAL:</span> Piso Casa/Fora</div>
        <div style="display: flex; align-items: center; gap: 5px;"><span style="color: #D4AF37;">&#9876; H2H:</span> Piso Vs Oponente</div>
        <div style="display: flex; align-items: center; gap: 5px;"><span style="color: #D4AF37;">&#128737; ALVO:</span> Meta Segura</div>
    </div>
    """, unsafe_allow_html=True)

    # 3. Abas
    tab_l5, tab_l10, tab_l15 = st.tabs(["L5 (Momentum)", "L10 (Padrao)", "L15 (Solido)"])
    
    with tab_l5:
        try:
            members_l5 = engine.scan_market(window=5)
            render_trinity_table(members_l5, "L5")
        except: st.info("Sem dados L5 no momento.")
        
    with tab_l10:
        try:
            members_l10 = engine.scan_market(window=10)
            render_trinity_table(members_l10, "L10")
        except: st.info("Sem dados L10 no momento.")
        
    with tab_l15:
        try:
            members_l15 = engine.scan_market(window=15)
            render_trinity_table(members_l15, "L15")
        except: st.info("Sem dados L15 no momento.")

        
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
# PAGINA: STRATEGY 5/7/10 (VERSAO BLINDADA V3.1 - ASCII SAFE)
# ============================================================================
def show_5_7_10_page():
    import json
    import os

    # --- 1. CONFIGURACAO E CARREGAMENTO ---
    def local_load_json(filepath):
        if os.path.exists(filepath):
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    return json.load(f)
            except: return {}
        return {}

    cache_file = os.path.join("cache", "real_game_logs.json")
    full_cache = local_load_json(cache_file) or {}
    
    # Fallback
    if not full_cache:
        full_cache = local_load_json("real_game_logs.json") or {}

    # Executa a Engine
    try:
        # Verifica importacao
        if 'FiveSevenTenEngine' not in globals():
            from modules.new_modules.five_seven_ten import FiveSevenTenEngine
        
        engine = FiveSevenTenEngine(full_cache, st.session_state.get('scoreboard', []))
        opportunities, diag = engine.analyze_market()
    except Exception as e:
        st.error(f"Erro ao inicializar Engine 5-7-10: {e}")
        return

    # --- 2. CABECALHO ---
    # Icone de Alvo via HTML: &#127919;
    st.markdown("## &#127919; Strategy 5 / 7 / 10")
    st.caption("Scanner de Glue Guys & Estrelas: Da seguranca (5+) a explosao (10+). Base L25.")

    # --- 3. DIAGNOSTICO ---
    with st.expander("Diagnostico do Sistema"):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Jogadores DB", diag.get("total_players", 0))
        c2.metric("Jogando Hoje", diag.get("playing_today", 0))
        c3.metric("Dados Insuf.", diag.get("insufficient_data", 0))
        c4.metric("Reprovados", diag.get("failed_criteria", 0))
        
        if diag.get("total_players", 0) == 0:
            st.error(f"DB vazio. Verifique: {cache_file}")

    if not opportunities:
        if diag.get("playing_today", 0) > 0:
            st.info("Nenhum jogador atingiu os criterios (50% Safe / 8% Explosao) hoje.")
        return

    # --- 4. RENDERIZACAO (ESTILOS INLINE PARA EVITAR ERROS DE SINTAXE) ---
    
    # Filtros
    filter_stat = st.radio("Filtrar:", ["TODOS", "AST", "REB"], horizontal=True)
    f_opps = opportunities
    if filter_stat == "AST": f_opps = [x for x in opportunities if x['stat'] == 'AST']
    if filter_stat == "REB": f_opps = [x for x in opportunities if x['stat'] == 'REB']

    # Definicao de Estilos (Strings Python Puras - ASCII Only)
    s_card = "background: linear-gradient(90deg, #1e293b 0%, #0f172a 100%); border-radius: 8px; padding: 10px; margin-bottom: 12px; display: flex; align-items: center; border: 1px solid rgba(255,255,255,0.05); box-shadow: 0 4px 6px rgba(0,0,0,0.3);"
    s_img = "width: 55px; height: 55px; border-radius: 50%; object-fit: cover; background: #000;"
    s_info = "margin-left: 15px; width: 140px;"
    s_name = "font-family: 'Oswald', sans-serif; font-size: 15px; color: #fff; line-height: 1.1;"
    s_team = "font-size: 10px; color: #94a3b8; font-weight: bold;"
    s_arch = "font-size: 9px; background: rgba(255,255,255,0.1); padding: 2px 6px; border-radius: 4px; display: inline-block; margin-top: 4px; color: #cbd5e1;"
    
    s_ladder = "flex-grow: 1; display: flex; gap: 8px; justify-content: space-around; align-items: center;"
    s_step_box = "text-align: center; width: 32%;"
    s_step_lbl = "font-size: 8px; color: #64748B; font-weight: bold; margin-bottom: 4px;"
    s_bar_bg = "width: 100%; height: 5px; background: #334155; border-radius: 3px; overflow: hidden;"
    s_bar_fill = "height: 100%; border-radius: 3px;"
    s_val = "font-family: sans-serif; font-size: 13px; font-weight: bold; margin-top: 2px;"

    for item in f_opps:
        # Logica de Cor
        if "DYNAMITE" in item['archetype']:
            border_c = "#f87171"
        else:
            border_c = "#3b82f6"
            
        # Cores das Barras
        safe_pct = item['metrics']['Safe_5']
        target_pct = item['metrics']['Target_7']
        ceil_pct = item['metrics']['Ceiling_10']
        
        # HTML Montado
        # SUBSTITUICAO CRITICA: O simbolo de ponto foi trocado por &bull;
        html = f"""
        <div style="{s_card} border-left: 5px solid {border_c};">
            <img src="{item['photo']}" style="{s_img} border: 2px solid {border_c};">
            
            <div style="{s_info}">
                <div style="{s_name}">{item['player']}</div>
                <div style="{s_team}">{item['team']} vs {item['opp']} &bull; {item['stat']}</div>
                <div style="{s_arch}">{item['archetype']}</div>
            </div>
            
            <div style="{s_ladder}">
                <div style="{s_step_box}">
                    <div style="{s_step_lbl}">SAFE (5+)</div>
                    <div style="{s_bar_bg}">
                        <div style="{s_bar_fill} width: {safe_pct}%; background: #4ade80;"></div>
                    </div>
                    <div style="{s_val} color: #4ade80;">{safe_pct}%</div>
                </div>

                <div style="{s_step_box}">
                    <div style="{s_step_lbl}">TARGET (7+)</div>
                    <div style="{s_bar_bg}">
                        <div style="{s_bar_fill} width: {target_pct}%; background: #facc15;"></div>
                    </div>
                    <div style="{s_val} color: #facc15;">{target_pct}%</div>
                </div>

                <div style="{s_step_box}">
                    <div style="{s_step_lbl}">EXPLOSAO (10+)</div>
                    <div style="{s_bar_bg}">
                        <div style="{s_bar_fill} width: {ceil_pct}%; background: #f87171;"></div>
                    </div>
                    <div style="{s_val} color: #f87171;">{ceil_pct}%</div>
                </div>
            </div>
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)
        
        
        
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
# 6. VISUAL RENDERING (VERSAO SANITIZADA - SEM EMOJIS/UNICODE)
# ==============================================================================

def render_obsidian_matrix_card(player, team, items):
    rows_html = ""
    for item in items:
        s_color = "#4ade80" if item['stat'] == 'PTS' else "#60a5fa"
        steps_html = ""
        for step in item['steps']:
            # Uso de HTML puro sem caracteres especiais
            steps_html += f"""<div style='flex:1;text-align:center;background:#0f172a;border-top:2px solid #10b981;margin:0 2px;padding:6px 2px;'><div style='font-size:0.55rem;color:#94a3b8;font-weight:700;'>{step['label']}</div><div style='font-size:1.2rem;font-weight:900;color:#f8fafc;'>{step['line']}+</div></div>"""
        
        rows_html += f"""<div style='display:flex;align-items:center;margin-bottom:8px;padding:4px;'><div style='width:50px;text-align:center;margin-right:6px;'><div style='font-weight:900;color:{s_color};font-size:0.8rem;'>{item['stat']}</div></div><div style='flex:1;display:flex;'>{steps_html}</div></div>"""
    
    # Renderiza card principal
    st.markdown(f"""<div style="background:#1e293b;border:1px solid #334155;border-radius:8px;padding:12px;margin-bottom:4px;"><div style="display:flex;justify-content:space-between;margin-bottom:10px;"><div><span style="color:#f1f5f9;font-weight:800;font-size:1.1rem;">{player}</span> <span style="color:#64748b;font-size:0.8rem;">{team}</span></div></div><div>{rows_html}</div></div>""", unsafe_allow_html=True)
    
    c1, c2, c3 = st.columns(3)
    def get_legs(lbl):
        return [{"player": player, "team": team, "stat": i['stat'], "line": [s['line'] for s in i['steps'] if s['label'] == lbl][0], "game_id": i['game_id'], "game_display": i['game_display']} for i in items]
    
    # Botoes limpos (Sem emojis para evitar erro de encoding)
    with c1: 
        if st.button("SAFE", key=generate_stable_key("s", player)): safe_save_audit({'portfolio': 'STAIRWAY_COMBO', 'legs': get_legs('SAFE')}); st.toast("Salvo!")
    with c2: 
        if st.button("TGT", key=generate_stable_key("t", player)): safe_save_audit({'portfolio': 'STAIRWAY_COMBO', 'legs': get_legs('TARGET')}); st.toast("Salvo!")
    with c3: 
        if st.button("SKY", key=generate_stable_key("k", player)): safe_save_audit({'portfolio': 'STAIRWAY_COMBO', 'legs': get_legs('SKY')}); st.toast("Salvo!")

def render_sniper_card(item, btn_key):
    # Substituido caractere bullet por codigo HTML &bull;
    st.markdown(f"""<div style="background:#1e293b;border:1px solid #334155;border-left:4px solid #06b6d4;border-radius:8px;padding:12px;margin-bottom:12px;"><div style="display:flex;justify-content:space-between;"><div><div style="color:#f1f5f9;font-weight:800;">{item['player']}</div><div style="color:#94a3b8;font-size:0.75rem;">{item['team']} &bull; {item['archetype']}</div></div><div style="color:#06b6d4;font-weight:900;font-size:1.4rem;">{item['line']}+ {item['stat']}</div></div></div>""", unsafe_allow_html=True)
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
    
    # Cores definidas via hex direto
    bg_color = '#1e293b' if ticket['type'] == 'MAIN' else '#0f172a'
    title_color = '#fbbf24' if ticket['type'] == 'MAIN' else '#94a3b8'
    
    st.markdown(f"""<div style="background:{bg_color};border:1px solid #334155;border-radius:8px;padding:12px;margin-bottom:12px;"><div style="font-size:1.1rem;font-weight:800;color:{title_color};margin-bottom:10px;border-bottom:2px solid #334155;padding-bottom:5px;">{ticket['title']}</div><div style="font-size:0.8rem;">{legs_html}</div></div>""", unsafe_allow_html=True)
    
    # Botao limpo
    if st.button(f"Salvar", key=ticket['id'], use_container_width=True): safe_save_audit({"portfolio": "MATRIX_GOLD", "total_odd": 15.0, "legs": ticket['legs']}); st.toast("Salvo!")

def show_hit_prop_page():
    # Removido bloco CSS complexo para evitar SyntaxError de decimal literal
    
    st.markdown(f'<h1 style="color:#F8FAFC; margin-bottom:0;">Hit Prop <span style="color:#EF4444;">Hunter</span></h1>', unsafe_allow_html=True)
    st.caption("v47.4 - Integral Version - Audit Fixed")

    today = datetime.now().strftime('%Y-%m-%d')
    if 'last_update_date' not in st.session_state:
        st.session_state['last_update_date'] = '1900-01-01'
    
    if st.session_state['last_update_date'] != today:
        st.session_state['scoreboard'] = get_games_safe()
        st.session_state['last_update_date'] = today
        st.toast(f"Jogos atualizados para: {today}")

    if 'scoreboard' not in st.session_state:
        st.session_state['scoreboard'] = get_games_safe()
    games = st.session_state['scoreboard']
    
    # Nomes das abas simplificados
    tab_labels = ["MULTIPLA", "SNIPER", "STAIRWAY", "SGP LAB", "PROPS", "CONFIG"]
    
    if not games:
        st.error("Nenhum jogo encontrado para hoje.")
        tabs = st.tabs(tab_labels)
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
    
    # Inicializa Engines e Dados
    trident_engine = TridentEngine()
    tridents = trident_engine.find_tridents(cache_data, games) if cache_data else []
    
    stairway_raw = generate_stairway_data(cache_data, games) if cache_data else []
    sniper_data = generate_sniper_data(cache_data, games) if cache_data else []

    # Renderiza Abas
    tabs = st.tabs(tab_labels)
    
    # Lógica de Abas (Simplificada para caber no bloco, adicione o conteúdo das abas conforme seu código original)
    # Exemplo para Sniper:
    with tabs[1]: # SNIPER
        if sniper_data:
            for item in sniper_data[:10]: # Top 10
                 render_sniper_card(item, generate_stable_key("sn", item['player']))
        else:
            st.info("Nenhuma oportunidade Sniper hoje.")
            
    # Certifique-se de preencher as outras abas com sua lógica original.

# ============================================
    # DEFINICAO DAS ABAS (ASCII PURO)
    # ============================================
    # Nomes das abas sem emojis e sem acentos para evitar erro de encoding
    tabs = st.tabs(["MULTIPLA", "SNIPER", "STAIRWAY", "SGP LAB", "PROPS", "CONFIG"])

    # ============================================
    # ABA 0: MULTIPLA (DESDOBRAMENTOS)
    # ============================================
    with tabs[0]:
        # HTML Entity para DNA: &#129468;
        st.markdown("### &#129468; MULTIPLA (Desdobramentos Inteligentes)", unsafe_allow_html=True)
        
        # Verificar se temos dados suficientes
        if not cache_data:
            st.error("ERRO: Dados de jogadores nao carregados.")
            # HTML Entity para Engrenagem: &#9881;
            st.markdown("""
            **Solucao:** Va para a aba **&#9881; CONFIG** e clique em:
            1. **ATUALIZAR STATS (NBA)** - Para baixar dados dos jogadores
            2. **ATUALIZAR ODDS DE MERCADO** - Para obter odds atualizadas
            """, unsafe_allow_html=True)
        elif not games:
            st.error("ERRO: Nenhum jogo encontrado para hoje.")
        elif not tridents:
            st.warning("ALERTA: Nenhum Trident encontrado.")
            st.markdown("""
            **Possiveis causas:**
            1. Thresholds muito altos nos Tridents
            2. Jogadores sem historico suficiente
            3. Necessidade de atualizar dados
            """)
        else:
            # GERAR MATRIX TICKETS
            with st.spinner("Gerando multiplas inteligentes..."):
                try:
                    pool = matrix_engine.analyze_market_pool(cache_data, games, tridents)
                    
                    # Diagnostico
                    with st.expander("Diagnostico do Pool", expanded=False):
                        st.write(f"**Anchors encontrados**: {len(pool.get('ANCHORS', []))}")
                        st.write(f"**Boosters encontrados**: {len(pool.get('BOOSTERS', []))}")
                        
                        if pool.get('ANCHORS'):
                            st.write("**Top 3 Anchors**:")
                            for i, a in enumerate(pool['ANCHORS'][:3]):
                                legs_str = ", ".join([f"{l['stat']} {l['line']}+" for l in a.get('legs', [])])
                                st.write(f"{i+1}. {a['name']} ({a['role']}) - {legs_str}")
                    
                    matrix_tickets = matrix_engine.generate_smart_matrix(pool)
                    
                    if matrix_tickets:
                        st.success(f"Sucesso: {len(matrix_tickets)} multiplas inteligentes geradas!")
                        
                        # Separar por tipo
                        alpha_tickets = [t for t in matrix_tickets if t['type'] == 'ALPHA']
                        beta_tickets = [t for t in matrix_tickets if t['type'] == 'BETA']
                        
                        if alpha_tickets:
                            st.markdown("#### ALPHA (Premium)")
                            for ticket in alpha_tickets:
                                render_matrix_card_html(ticket)
                        
                        if beta_tickets:
                            st.markdown("#### BETA (Alternativas)")
                            for ticket in beta_tickets:
                                render_matrix_card_html(ticket)
                    else:
                        st.info("Nenhuma multipla valida encontrada com os criterios atuais.")
                        st.markdown("""
                        **Solucoes possiveis:**
                        1. Ajustar thresholds na classe MatrixEngine
                        2. Atualizar odds de mercado
                        3. Aguardar mais jogadores com historico
                        """)
                        
                except Exception as e:
                    st.error(f"Erro ao gerar multiplas: {str(e)}")
                    # Removemos traceback complexo para evitar erros de string
    
    # ============================================
    # ABA 1: SNIPER GEM
    # ============================================
    with tabs[1]:
        # HTML Entity para Diamante: &#128142;
        st.markdown("### &#128142; SNIPER GEM (Volume Shooters)", unsafe_allow_html=True)
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
    # ABA 2: STAIRWAY
    # ============================================
    with tabs[2]:
        # HTML Entity para Escada: &#129692;
        st.markdown("### &#129692; STAIRWAY (Matrix View)", unsafe_allow_html=True)
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
    # ABA 3: SGP LAB
    # ============================================
    with tabs[3]:
        # HTML Entity para Tubo de Ensaio: &#12951ea;
        st.markdown("### &#129514; SGP LAB", unsafe_allow_html=True)
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
                        # Botao sem emoji
                        render_tactical_card(p_data['player'], p_data['team'], "100%", items, btn_key, "Adicionar", p_data, "SGP_LAB")
                st.markdown("---")
    
    # ============================================
    # ABA 4: PROPS
    # ============================================
    with tabs[4]:
        # HTML Entity para Tridente: &#128305;
        st.markdown("### &#128305; PROPS", unsafe_allow_html=True)
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
                        # Substituicao critica: Hifen no lugar de bullet point
                        display_text = f"{item['team']} - {item['archetype']}"
                        render_tactical_card(item['player'], display_text, f"{item['hit_rate']:.0%}", items_c, btn_key, "Salvar", item, "TRIDENT")
                st.markdown("---")
    
    # ============================================
    # ABA 5: CONFIG
    # ============================================
    with tabs[5]:
        st.markdown("### Configuracoes & Mercado")
        
        # Botoes sem emojis
        if st.button("ATUALIZAR ODDS DE MERCADO (PINNACLE)", help="Consome cota de API."):
            with st.spinner("Conectando a Pinnacle..."):
                ok, msg = odds_manager.force_update()
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)
        
        st.caption("Ultima atualizacao: " + str(odds_manager.load_odds().get('updated_at', 'Nunca')))
        st.divider()
        
        c1, c2 = st.columns(2)
        if c1.button("ATUALIZAR STATS (NBA)"):
            st.session_state['scoreboard'] = get_games_safe()
            update_batch_cache(st.session_state['scoreboard'])
            st.rerun()
        
        if c2.button("LIMPAR CACHE GERAL"):
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
        with st.expander("Abrir Dados Brutos"):
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
# FUNCAO AUXILIAR: PROCESS ROSTER (VERSAO SANITIZADA)
# ============================================================================
def process_roster(roster_list, team_abbr, is_home):
    # Processa o roster integrando L5, Stats Individuais e Archetypes.
    # Retorna chaves essenciais para evitar KeyError.
    processed = []
    df_l5 = st.session_state.get("df_l5")
    
    for entry in roster_list:
        player = normalize_roster_entry(entry)
        player_name = player.get("PLAYER", "N/A")
        
        # Overrides de posicao (Nomes sem acento para seguranca)
        position_overrides = {
            "LeBron James": "SF", "Nikola Jokic": "C", "Luka Doncic": "PG",
            "Giannis Antetokounmpo": "PF", "Jimmy Butler": "SF", "Stephen Curry": "PG",
            "Joel Embiid": "C", "Jayson Tatum": "SF", "Kevin Durant": "SF", 
            "Anthony Davis": "PF", "Bam Adebayo": "C", "Domantas Sabonis": "C"
        }
        # Tenta pegar override, senao pega do player, senao vazio
        pos_raw = player.get("POSITION", "")
        # Remove caracteres nao-ascii do nome antes de buscar no dict
        p_name_clean = player_name.encode('ascii', 'ignore').decode('ascii')
        pos = position_overrides.get(p_name_clean, pos_raw.upper())
        
        starter = player.get("STARTER", False)
        
        # Status
        status_raw = str(player.get("STATUS", "")).lower()
        badge_color = "#9CA3AF"
        status_display = "ACTIVE"
        
        if any(k in status_raw for k in ["out", "ir", "injur"]):
            badge_color = "#EF4444"
            status_display = "OUT"
        elif "questionable" in status_raw or "doubt" in status_raw or "gtd" in status_raw:
            badge_color = "#F59E0B"
            status_display = "QUEST"
        elif any(k in status_raw for k in ["active", "available", "probable"]):
            badge_color = "#10B981"
            status_display = "ACTIVE"
            
        # Stats Iniciais (Zeros)
        stats = {
            "MIN_AVG": 0, "USG_PCT": 0, "PRA_AVG": 0,
            "PTS_AVG": 0, "REB_AVG": 0, "AST_AVG": 0,
            "STL_AVG": 0, "BLK_AVG": 0, "THREEPA_AVG": 0
        }
        
        archetypes_clean_list = [] 
        
        if df_l5 is not None and not df_l5.empty:
            # Busca insensivel a caixa
            matches = df_l5[df_l5["PLAYER"].str.contains(player_name, case=False, na=False)]
            if not matches.empty:
                row = matches.iloc[0]
                
                # Extrair TODAS as stats necessarias com seguranca (.get)
                stats["MIN_AVG"] = row.get("MIN_AVG", 0)
                stats["USG_PCT"] = row.get("USG_PCT", 0) if "USG_PCT" in df_l5.columns else 0
                stats["PRA_AVG"] = row.get("PRA_AVG", 0)
                stats["PTS_AVG"] = row.get("PTS_AVG", 0)
                stats["REB_AVG"] = row.get("REB_AVG", 0)
                stats["AST_AVG"] = row.get("AST_AVG", 0)
                stats["STL_AVG"] = row.get("STL_AVG", 0)
                stats["BLK_AVG"] = row.get("BLK_AVG", 0)
                stats["THREEPA_AVG"] = row.get("THREEPA_AVG", 0) if "THREEPA_AVG" in df_l5.columns else 0
                
                # --- INTEGRACAO ARCHETYPE ENGINE ---
                if "archetype_engine" in st.session_state:
                    try:
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

        # Role Logic
        role = "deep_bench"
        if starter: role = "starter"
        elif stats["MIN_AVG"] >= 20: role = "rotation"
        elif stats["MIN_AVG"] >= 12: role = "bench"
        
        profile_str = ", ".join(archetypes_clean_list[:2]) if archetypes_clean_list else "-"

        # Retorno Completo (Mapeamento Explicito)
        processed.append({
            "PLAYER": player_name,
            "POSITION": pos,
            "ROLE": role,
            "STATUS": status_display,
            "STATUS_FULL": str(player.get("STATUS", "")),
            "STATUS_BADGE": badge_color,
            "PROFILE": profile_str,
            "ARCHETYPES": archetypes_clean_list,
            
            # Stats Essenciais
            "MIN_AVG": float(stats["MIN_AVG"]),
            "USG_PCT": float(stats["USG_PCT"]),
            "PRA_AVG": float(stats["PRA_AVG"]),
            "PTS": float(stats["PTS_AVG"]),
            "REB": float(stats["REB_AVG"]),
            "AST": float(stats["AST_AVG"]),
            "STL": float(stats["STL_AVG"]),
            "BLK": float(stats["BLK_AVG"]),
            "3PM": float(stats["THREEPA_AVG"])
        })
    
    return processed

def validate_pipeline_integrity(required_components=None):
    # Valida se os dados necessarios para o pipeline estao disponiveis.
    if required_components is None:
        required_components = ['l5', 'scoreboard']
    
    checks = {
        'l5': {'name': 'Dados L5', 'critical': True, 'status': False, 'message': ''},
        'scoreboard': {'name': 'Scoreboard', 'critical': True, 'status': False, 'message': ''},
        'odds': {'name': 'Odds', 'critical': False, 'status': False, 'message': ''},
        'dvp': {'name': 'Dados DvP', 'critical': False, 'status': False, 'message': ''},
        'injuries': {'name': 'Lesoes', 'critical': False, 'status': False, 'message': ''},
        'advanced_system': {'name': 'Sistema Avancado', 'critical': False, 'status': False, 'message': ''}
    }
    
    # Validar L5
    if 'l5' in required_components:
        df_l5 = st.session_state.get('df_l5')
        if df_l5 is not None and hasattr(df_l5, 'shape') and not df_l5.empty:
            checks['l5']['status'] = True
            checks['l5']['message'] = f'OK ({len(df_l5)} players)'
        else:
            checks['l5']['message'] = 'Indisponivel'
    
    # Validar scoreboard
    if 'scoreboard' in required_components:
        scoreboard = st.session_state.get('scoreboard')
        if scoreboard and len(scoreboard) > 0:
            checks['scoreboard']['status'] = True
            checks['scoreboard']['message'] = f'OK ({len(scoreboard)} games)'
        else:
            checks['scoreboard']['message'] = 'Vazio'
    
    # Validar odds
    if 'odds' in required_components:
        odds = st.session_state.get('odds')
        if odds and len(odds) > 0:
            checks['odds']['status'] = True
            checks['odds']['message'] = 'OK'
        else:
            checks['odds']['message'] = 'Indisponivel'
    
    # Validar DvP
    if 'dvp' in required_components:
        dvp = st.session_state.get('dvp_analyzer')
        if dvp and hasattr(dvp, 'defense_data') and dvp.defense_data:
            checks['dvp']['status'] = True
            checks['dvp']['message'] = 'OK'
        else:
            checks['dvp']['message'] = 'Indisponivel'
    
    # Validar lesoes
    if 'injuries' in required_components:
        inj = st.session_state.get('injuries_data')
        if inj and len(inj) > 0:
            checks['injuries']['status'] = True
            checks['injuries']['message'] = 'OK'
        else:
            checks['injuries']['message'] = 'Indisponivel'
    
    # Validar sistema avancado
    if 'advanced_system' in required_components:
        if st.session_state.get("use_advanced_features", False):
            checks['advanced_system']['status'] = True
            checks['advanced_system']['message'] = 'Ativo'
        else:
            checks['advanced_system']['message'] = 'Inativo'
    
    all_critical_ok = all(
        check['status'] for key, check in checks.items() 
        if key in required_components and check['critical']
    )
    
    return all_critical_ok, checks

# ============================================================================
# DATA FETCHERS (SANITIZED & ROBUST)
# ============================================================================
def get_scoreboard_data():
    from datetime import datetime, timedelta
    import pandas as pd
    import requests
    
    # Logica de Data Manual (Sem dependencia de pytz para evitar erros)
    # UTC-3 para Brasil (Simples e eficaz)
    now_utc = datetime.utcnow()
    now_br = now_utc - timedelta(hours=3)

    # Se for madrugada (antes das 4AM), conta como dia anterior
    if now_br.hour < 4:
        target_date = now_br - timedelta(days=1)
    else:
        target_date = now_br
    
    date_str = target_date.strftime("%Y%m%d")
    
    # 1. TENTA LER DO SUPABASE/CACHE UNIVERSAL
    try:
        cached = get_data_universal("scoreboard")
        if cached and isinstance(cached, list) and len(cached) > 0:
            first_game_date = str(cached[0].get('date_str', ''))
            if first_game_date == date_str:
                return pd.DataFrame(cached)
    except: pass

    # 2. BAIXA DA ESPN API
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
            
            # Busca segura por Home/Away
            home = {}
            away = {}
            for t in comp["competitors"]:
                if t["homeAway"] == "home": home = t
                elif t["homeAway"] == "away": away = t
            
            # Status e Odds
            status_text = evt["status"]["type"]["shortDetail"]
            
            odds_txt = ""
            odds_total = ""
            if "odds" in comp and comp["odds"]:
                odds_txt = comp["odds"][0].get("details", "")
                odds_total = comp["odds"][0].get("overUnder", "")

            game_dict = {
                "gameId": evt["id"],
                "date_str": date_str,
                "startTimeUTC": comp.get("date"),
                "home": home.get("team", {}).get("abbreviation", "UNK"),
                "away": away.get("team", {}).get("abbreviation", "UNK"),
                "status": status_text,
                "odds_spread": odds_txt,
                "odds_total": odds_total,
                "home_logo": home.get("team", {}).get("logo", ""),
                "away_logo": away.get("team", {}).get("logo", "")
            }
            games_list.append(game_dict)

        if games_list:
            save_data_universal("scoreboard", games_list)
            
        return pd.DataFrame(games_list)

    except Exception as e:
        # Erro silencioso ou log simples
        return pd.DataFrame()
        
def fetch_team_roster(team_abbr_or_id, progress_ui=True):
    # Sanitizacao do caminho
    safe_team_code = str(team_abbr_or_id).strip().upper()
    cache_path = os.path.join(CACHE_DIR, f"roster_{safe_team_code}.json")
    
    cached = load_json(cache_path)
    if cached:
        return cached
    
    # Codigos ESPN fixos para garantir compatibilidade
    espn_code = ESPN_TEAM_CODES.get(safe_team_code, safe_team_code.lower())
    url = ESPN_TEAM_ROSTER_TEMPLATE.format(team=espn_code)
    
    try:
        if progress_ui:
            st.info(f"Buscando roster: {safe_team_code}...")
        
        r = requests.get(url, timeout=10, headers=HEADERS)
        r.raise_for_status()
        jr = r.json()
        
        save_json(cache_path, jr)
        return jr
    except Exception:
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
# EXECUCAO PRINCIPAL (MAIN - SANITIZADA)
# ============================================================================
def main():
    # Config da pagina (Icone como string simples)
    st.set_page_config(page_title="DigiBets IA", layout="wide", page_icon=":basketball:")
    
    # CSS Global Seguro (Sem imports complexos ou caracteres estranhos)
    st.markdown("""
    <style>
        .stApp { background-color: #000000 !important; }
        header[data-testid="stHeader"] { visibility: hidden; height: 0px; }
        .block-container { padding-top: 1rem !important; }
        
        /* Menu Lateral */
        section[data-testid="stSidebar"] { background-color: #050505 !important; border-right: 1px solid #333; }
        div[role="radiogroup"] label {
            background: transparent !important;
            color: #ccc !important;
            padding: 8px !important;
            border-radius: 5px !important;
        }
        div[role="radiogroup"] label:hover {
            background: #222 !important;
            color: #fff !important;
        }
        div[role="radiogroup"] label[data-checked="true"] {
            background: #1e3a8a !important;
            color: #fff !important;
            border-left: 3px solid #3b82f6 !important;
        }
    </style>
    """, unsafe_allow_html=True)
    
    safe_load_initial_data()

    # Menu Lateral (Strings Puras)
    with st.sidebar:
        st.markdown("<h2 style='text-align: center; color: #fff;'>DIGIBETS IA</h2>", unsafe_allow_html=True)
        
        # Opcoes de Menu sem Emojis literais (Usa texto ou codigos se suportado pelo radio)
        # O radio do streamlit suporta emojis, mas vamos garantir que o arquivo python nao tenha.
        # Vamos usar strings simples e mapear depois.
        
        OPT_DASH = "Dashboard"
        OPT_RANK = "Ranking Teses"
        OPT_AUDIT = "Auditoria"
        OPT_NEXUS = "Sinergia & Vacuo"
        OPT_NARRATIVE = "Lab Narrativas"
        OPT_MOMENTUM = "Momentum"
        OPT_LASVEGAS = "Las Vegas Sync"
        OPT_BLOWOUT = "Blowout Hunter"
        OPT_TRINITY = "Trinity Club"
        OPT_HOT = "Hot Streaks"
        OPT_5710 = "Matriz 5-7-10"
        OPT_DESDOBRA = "Desdobra Multipla"
        OPT_DVP = "DvP Confrontos"
        OPT_MEDICO = "Depto Medico"
        OPT_ROTACOES = "Mapa de Rotacoes"
        OPT_ESCALA = "Escalacoes"
        OPT_CONFIG = "Config"
        OPT_TESTE = "Diagnostico Cloud"

        menu_options = [
            OPT_DASH, OPT_RANK, OPT_AUDIT, 
            OPT_NEXUS, OPT_NARRATIVE, OPT_MOMENTUM, OPT_LASVEGAS, OPT_BLOWOUT, OPT_TRINITY,
            OPT_HOT, OPT_5710, OPT_DESDOBRA,
            OPT_DVP, OPT_MEDICO, OPT_ROTACOES, OPT_ESCALA,
            OPT_CONFIG, OPT_TESTE
        ]
        
        choice = st.radio("Menu", menu_options, label_visibility="collapsed")
        st.markdown("<div style='text-align:center; color:#555; font-size:10px;'>v2.2 STABLE</div>", unsafe_allow_html=True)

    # Roteamento
    if choice == OPT_DASH: show_dashboard_page()
    elif choice == OPT_RANK: show_analytics_page()
    elif choice == OPT_AUDIT: show_audit_page()
    elif choice == OPT_NEXUS: show_nexus_page()
    elif choice == OPT_NARRATIVE: show_narrative_lab()
    elif choice == OPT_MOMENTUM: show_momentum_page()
    elif choice == OPT_LASVEGAS: show_props_odds_page()
    elif choice == OPT_BLOWOUT: show_blowout_hunter_page()
    elif choice == OPT_TRINITY: show_trinity_club_page()
    elif choice == OPT_HOT: show_hit_prop_hunter()
    elif choice == OPT_5710: show_5_7_10_page()
    elif choice == OPT_DESDOBRA: show_desdobramentos_inteligentes()
    elif choice == OPT_DVP: show_dvp_analysis()
    elif choice == OPT_MEDICO: show_depto_medico()
    elif choice == OPT_ROTACOES: show_mapa_rotacoes()
    elif choice == OPT_ESCALA: show_escalacoes()
    elif choice == OPT_CONFIG: show_config_page()
    elif choice == OPT_TESTE: show_cloud_diagnostics()

if __name__ == "__main__":
    main()

