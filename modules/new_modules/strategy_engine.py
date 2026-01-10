# modules/new_modules/strategy_engine.py
# VERSÃO V80.0 - SUPABASE NATIVE & TURBO FLOW
# - Remoção completa de cache local de lesões (JSON).
# - Integração nativa com Supabase para blacklist.
# - Fluxo: Lesão -> Vacuum -> DvP -> Tese.

import logging
import random
import uuid
import sys
import os
import hashlib
import json
import math
import unicodedata
import statistics
from datetime import datetime

# Tenta importar gerenciador de DB (Supabase)
try:
    from db_manager import db
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

logger = logging.getLogger("StrategyEngine_V80_0")

# =========================================================================
# IMPORTAÇÃO DOS MÓDULOS DE INTELIGÊNCIA
# =========================================================================
MODULES = {}
try:
    from modules.new_modules.monte_carlo import MonteCarloEngine
    MODULES['monte_carlo'] = MonteCarloEngine
except: pass

try:
    from modules.new_modules.pace_adjuster import PaceAdjuster
    MODULES['pace'] = PaceAdjuster
except: pass

try:
    from modules.new_modules.vacuum_matrix import VacuumMatrixAnalyzer
    MODULES['vacuum'] = VacuumMatrixAnalyzer
except: pass

try:
    from modules.new_modules.thesis_engine import ThesisEngine
    MODULES['thesis'] = ThesisEngine
except: pass

try:
    from modules.new_modules.dvp_analyzer import DvPAnalyzer
    MODULES['dvp'] = DvPAnalyzer
except: pass

class StrategyEngine:
    def __init__(self, external_blacklist=None):
        self.version = "80.0_SUPABASE"
        
        # Inicializa Módulos
        self.dvp = MODULES['dvp']() if 'dvp' in MODULES else None
        self.vacuum = MODULES['vacuum']() if 'vacuum' in MODULES else None
        self.thesis_eng = MODULES['thesis']() if 'thesis' in MODULES else None
        self.monte_carlo = MODULES['monte_carlo']() if 'monte_carlo' in MODULES else None
        self.pace = MODULES['pace']() if 'pace' in MODULES else None
        
        # Carrega Lesões (Supabase ou Externo)
        self.injuries_banned = self._load_injuries(external_blacklist)
        
    def _normalize_name(self, text):
        if not text: return ""
        text = str(text).lower().strip()
        try:
            text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
        except: pass
        return " ".join(text.replace(".", "").replace(",", "").replace("'", "").split())

    def _load_injuries(self, external_list):
        """Carrega lista de lesionados do Supabase ou usa lista externa"""
        banned = set()
        
        # 1. Se veio lista externa (do Frontend), usa ela (mais rápida/fresca)
        if external_list:
            for name in external_list:
                banned.add(self._normalize_name(name))
            logger.info(f"Injuries: {len(banned)} carregados via lista externa.")
            return banned

        # 2. Se não, busca no Supabase
        if DB_AVAILABLE and db:
            try:
                # Busca na tabela 'injuries' do Supabase
                # Assume que o db_manager tem um método get_data ou query direta
                raw_data = db.get_data("injuries") 
                
                # Adapta conforme o formato que você guarda no Supabase
                # Se for lista de dicts: [{'player': 'Curry', 'status': 'Out'}, ...]
                if isinstance(raw_data, list):
                    for item in raw_data:
                        status = str(item.get('status', '')).lower()
                        if any(x in status for x in ['out', 'inj', 'gtd', 'doubt']):
                            p_name = item.get('player') or item.get('name')
                            if p_name:
                                banned.add(self._normalize_name(p_name))
                
                # Se for dict agrupado por time
                elif isinstance(raw_data, dict):
                    for team, players in raw_data.items():
                        for p in players:
                            status = str(p.get('status', '')).lower()
                            if any(x in status for x in ['out', 'inj', 'gtd']):
                                banned.add(self._normalize_name(p.get('name')))
                                
                logger.info(f"Injuries: {len(banned)} carregados do Supabase.")
                return banned
            except Exception as e:
                logger.error(f"Erro ao carregar injuries do Supabase: {e}")
        
        return banned

    def refresh_context(self, external_blacklist=None):
        """Atualiza o contexto de lesões manualmente"""
        self.injuries_banned = self._load_injuries(external_blacklist)

    # =========================================================================
    # CORE: GERAÇÃO DE TRIXIES (FLUXO TURBO)
    # =========================================================================
    def generate_basic_trixies_by_category(self, players_ctx, game_ctx, category):
        """
        Gera sugestões aplicando o fluxo: Vacuum -> DvP -> Tese.
        """
        candidates = []
        
        # 1. ANALISA VACUUM (Oportunidades por lesão no time)
        vacuum_report = {}
        if self.vacuum:
            # Reconstrói estrutura de roster para o VacuumAnalyzer
            # Precisamos passar uma lista plana de todos os jogadores do time
            for team, p_list in players_ctx.items():
                roster_for_vacuum = []
                for p in p_list:
                    # Marca status baseado na blacklist
                    p_norm = self._normalize_name(p.get('name'))
                    status = "Out" if p_norm in self.injuries_banned else "Active"
                    
                    roster_for_vacuum.append({
                        'name': p.get('name'),
                        'status': status,
                        'position': p.get('position', 'F'),
                        'min_L5': float(p.get('min_L5', 0)),
                        'is_starter': p.get('is_starter', False)
                    })
                
                # Roda análise
                team_vacuum = self.vacuum.analyze_team_vacuum(roster_for_vacuum, team)
                if team_vacuum:
                    vacuum_report.update(team_vacuum)

        # 2. PROCESSA JOGADORES
        for team, p_list in players_ctx.items():
            for p in p_list:
                # A. Filtro Nuclear de Lesão
                p_norm = self._normalize_name(p.get('name'))
                if p_norm in self.injuries_banned:
                    continue

                # B. Prepara Dados Numéricos
                stats = {}
                for k in ['pts', 'reb', 'ast', '3pm', 'stl', 'blk', 'min']:
                    stats[k] = float(p.get(f'{k}_L5', 0))
                
                # C. Aplica Vacuum Boost (Se houver)
                is_vacuum_boosted = False
                vacuum_reason = ""
                if p.get('name') in vacuum_report:
                    vac_info = vacuum_report[p.get('name')]
                    boost = vac_info['boost'] # ex: 1.25
                    vacuum_reason = f"Beneficiado por {vac_info['source']} ({vac_info['type']})"
                    
                    # Inflaciona as projeções
                    for k in stats:
                        stats[k] = stats[k] * boost
                    is_vacuum_boosted = True

                # D. Aplica DvP (Defesa vs Posição)
                matchup_score = 50 # Neutro
                matchup_reason = ""
                if self.dvp:
                    opp = p.get('opponent', 'UNK') # Precisa vir do context
                    pos = self._estimate_position(p)
                    rank = self.dvp.get_position_rank(opp, pos) # 1=Forte, 30=Fraca
                    
                    # Ajuste fino no score
                    if rank >= 25: 
                        matchup_score = 80
                        matchup_reason = f"Matchup Top (Defesa #{rank})"
                    elif rank <= 5: 
                        matchup_score = 20
                        matchup_reason = f"Matchup Ruim (Defesa #{rank})"

                # E. Gera Tese (Narrativa)
                thesis_txt = "Análise Técnica"
                win_rate = 0.5
                
                if self.thesis_eng:
                    # Monta objeto enriquecido para o Thesis
                    p_enriched = p.copy()
                    p_enriched.update({
                        'pts_L5': stats['pts'], # Passa os stats já boostados!
                        'reb_L5': stats['reb'],
                        'ast_L5': stats['ast'],
                        'min_L5': stats['min'],
                        'matchup_rank': rank if self.dvp else 15,
                        'is_vacuum': is_vacuum_boosted
                    })
                    
                    # Gera
                    theses = self.thesis_eng.generate_theses(p_enriched, {})
                    if theses:
                        best_t = theses[0]
                        thesis_txt = best_t['reason']
                        win_rate = best_t['win_rate']
                        
                        # Se tiver vacuum, força a tese de vacuum no topo
                        if is_vacuum_boosted:
                            thesis_txt = f"💎 {vacuum_reason} | {thesis_txt}"
                        elif matchup_score > 70:
                            thesis_txt = f"🔥 {matchup_reason} | {thesis_txt}"

                # F. Cria Candidato
                # Define mercado alvo baseado nos stats projetados
                targets = []
                if stats['pts'] >= 15: targets.append(('PTS', stats['pts']))
                if stats['reb'] >= 6: targets.append(('REB', stats['reb']))
                if stats['ast'] >= 4: targets.append(('AST', stats['ast']))
                
                for mkt, val in targets:
                    # Linha Segura (90% da projeção)
                    safe_line = max(1, int(val * 0.9))
                    
                    # Score Final (0-100)
                    # Base (Win Rate) + Contexto (Matchup/Vacuum)
                    final_score = (win_rate * 100) 
                    if is_vacuum_boosted: final_score += 15
                    if matchup_score > 60: final_score += 10
                    if matchup_score < 40: final_score -= 10
                    
                    candidates.append({
                        "player_name": p.get('name'),
                        "team": p.get('team'),
                        "market_type": mkt,
                        "market_display": f"{safe_line}+ {mkt}",
                        "line": safe_line,
                        "odds": 1.0, # Sem odds financeiras
                        "thesis": thesis_txt,
                        "score": final_score
                    })

        # Ordena e Retorna
        candidates.sort(key=lambda x: x['score'], reverse=True)
        return self._pack_trixie(candidates[:3], category, "AUTO", game_ctx)

    def _pack_trixie(self, legs, cat, sub, ctx):
        if len(legs) < 2: return None
        return {
            "id": uuid.uuid4().hex[:6],
            "category": cat,
            "sub_category": sub,
            "game_info": ctx,
            "players": legs,
            "score": int(sum(l['score'] for l in legs)/len(legs)),
            "estimated_total_odd": 1.0
        }

    def _estimate_position(self, p):
        pos = p.get('position', 'F')
        if 'G' in pos: return 'PG'
        if 'C' in pos: return 'C'
        return 'SF'
