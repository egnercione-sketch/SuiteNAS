# modules/new_modules/strategy_engine.py
# VERSÃO V80.0 - ANALYST CORE (SEM ODDS, SUPABASE NATIVE)

import logging
import random
import uuid
import sys
import os
import unicodedata
import statistics
from datetime import datetime

# Tenta importar gerenciador de DB (Supabase)
try:
    from db_manager import db
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

logger = logging.getLogger("StrategyEngine_V80")

# Importação Dinâmica dos Módulos
MODULES = {}
try: from modules.new_modules.vacuum_matrix import VacuumMatrixAnalyzer; MODULES['vacuum'] = VacuumMatrixAnalyzer
except: pass
try: from modules.new_modules.thesis_engine import ThesisEngine; MODULES['thesis'] = ThesisEngine
except: pass
try: from modules.new_modules.dvp_analyzer import DvPAnalyzer; MODULES['dvp'] = DvPAnalyzer
except: pass

class StrategyEngine:
    def __init__(self, external_blacklist=None):
        self.version = "80.0_ANALYST"
        
        # Inicializa Módulos
        self.dvp = MODULES['dvp']() if 'dvp' in MODULES else None
        self.vacuum = MODULES['vacuum']() if 'vacuum' in MODULES else None
        self.thesis_eng = MODULES['thesis']() if 'thesis' in MODULES else None
        
        # Carrega Blacklist (Lesões)
        self.injuries_banned = self._load_injuries(external_blacklist)
        
    def _normalize_name(self, text):
        if not text: return ""
        text = str(text).lower().strip()
        try: text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
        except: pass
        return " ".join(text.replace(".", "").replace(",", "").replace("'", "").split())

    def _load_injuries(self, external_list):
        """Carrega lesionados (Prioridade: Lista Externa > Supabase)"""
        banned = set()
        
        # 1. Lista Externa (Passada pela UI - Mais fresca)
        if external_list:
            for name in external_list: banned.add(self._normalize_name(name))
            return banned

        # 2. Supabase (Backup)
        if DB_AVAILABLE and db:
            try:
                raw = db.get_data("injuries") 
                if isinstance(raw, dict) and 'teams' in raw:
                    for team, players in raw['teams'].items():
                        for p in players:
                            if any(x in str(p.get('status','')).lower() for x in ['out', 'inj', 'doubt']):
                                banned.add(self._normalize_name(p.get('name')))
            except: pass
        return banned

    # =========================================================================
    # CORE: GERAÇÃO DE TRIXIES
    # =========================================================================
    def generate_basic_trixies_by_category(self, players_ctx, game_ctx, category):
        """Fluxo: Vacuum -> Filtro Lesão -> DvP -> Tese -> Score"""
        candidates = []
        
        # 1. VACUUM (Quem ganha bônus?)
        vacuum_boosts = {}
        if self.vacuum:
            for team, p_list in players_ctx.items():
                roster_vac = []
                for p in p_list:
                    # Marca status para o Vacuum saber quem está fora
                    status = "Out" if self._normalize_name(p.get('name')) in self.injuries_banned else "Active"
                    roster_vac.append({
                        'name': p.get('name'), 'status': status, 
                        'position': p.get('position','F'), 
                        'min_L5': float(p.get('min_L5',0)), 'is_starter': p.get('is_starter', False)
                    })
                rep = self.vacuum.analyze_team_vacuum(roster_vac, team)
                if rep:
                    for name, info in rep.items(): vacuum_boosts[self._normalize_name(name)] = info

        # 2. VARREDURA
        for team, p_list in players_ctx.items():
            for p in p_list:
                norm_name = self._normalize_name(p.get('name'))
                
                # A. Filtro Lesão
                if norm_name in self.injuries_banned: continue

                # B. Stats & Boosts
                stats = {k: float(p.get(f'{k}_L5', 0)) for k in ['pts','reb','ast','min']}
                
                is_vacuum = False
                vac_info = vacuum_boosts.get(norm_name)
                narrative_parts = []
                
                if vac_info:
                    boost = vac_info['boost']
                    for k in stats: stats[k] *= boost
                    is_vacuum = True
                    narrative_parts.append(f"Beneficiado: {vac_info['source']} OUT")

                # C. Matchup (DvP)
                matchup_rank = 15
                if self.dvp:
                    opp = p.get('opponent', 'UNK')
                    pos = self._estimate_position(p)
                    matchup_rank = self.dvp.get_position_rank(opp, pos)
                    
                    if matchup_rank >= 25: narrative_parts.append(f"Defesa Fraca (#{matchup_rank})")
                    elif matchup_rank <= 5: narrative_parts.append(f"Defesa Elite (#{matchup_rank})")

                # D. Tese
                thesis_txt = "Análise Técnica"
                win_rate = 0.5
                if self.thesis_eng:
                    p_enriched = {**p, **stats, 'matchup_rank': matchup_rank, 'is_vacuum': is_vacuum}
                    theses = self.thesis_eng.generate_theses(p_enriched, {})
                    if theses:
                        thesis_txt = theses[0]['reason']
                        win_rate = theses[0]['win_rate']
                        if is_vacuum: thesis_txt = f"💎 {thesis_txt}" # Destaque visual

                # E. Score & Candidatura
                # Define alvos
                targets = []
                if stats['pts'] >= 12: targets.append(('PTS', stats['pts']))
                if stats['reb'] >= 6: targets.append(('REB', stats['reb']))
                if stats['ast'] >= 4: targets.append(('AST', stats['ast']))
                
                for mkt, val in targets:
                    # Linha Segura (Piso)
                    safe_line = max(1, int(val * 0.85)) # 85% da média projetada
                    
                    # Score (0-100)
                    base_score = win_rate * 100
                    if is_vacuum: base_score += 15
                    if matchup_rank >= 25: base_score += 10
                    elif matchup_rank <= 5: base_score -= 15
                    
                    candidates.append({
                        "player_name": p.get('name'), "team": team, 
                        "market_type": mkt, "market_display": f"{safe_line}+ {mkt}",
                        "line": safe_line, "odds": 1.0, 
                        "thesis": thesis_txt, "score": int(base_score)
                    })

        candidates.sort(key=lambda x: x['score'], reverse=True)
        return self._pack_trixie(candidates[:3], category, "AUTO", game_ctx)

    def _pack_trixie(self, legs, cat, sub, ctx):
        if len(legs) < 2: return None
        return {
            "id": uuid.uuid4().hex[:6], "category": cat, "sub_category": sub,
            "game_info": ctx, "players": legs, 
            "score": int(sum(l['score'] for l in legs)/len(legs)), "estimated_total_odd": 1.0
        }

    def _estimate_position(self, p):
        pos = p.get('position', 'F')
        if 'G' in pos: return 'PG'
        if 'C' in pos: return 'C'
        return 'SF'
