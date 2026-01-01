# modules/new_modules/multipla_do_dia.py
# SGP PRO v8.2 - THESIS INTELLIGENCE OPTIMIZED
# Otimização baseada em win rate real: HighCeiling (75%), MinutesSafe (67%), PlaymakerEdge (50%)

import logging
import random

logger = logging.getLogger("SGP_Pro_V8_2")

class StreakHunterMultipla:
    def __init__(self, strategy_engine=None):
        self.strategy = strategy_engine
        
        # Configuração Otimizada Baseada em Win Rate
        self.ROLES = {
            "HIGH_CEILING": {"min_pts": 15.0, "min_min": 30.0, "win_rate": 0.75, "priority": 100},
            "MINUTES_SAFE": {"min_pts": 12.0, "min_min": 28.0, "win_rate": 0.667, "priority": 90},
            "PLAYMAKER_EDGE": {"min_ast": 7.0, "min_min": 26.0, "win_rate": 0.50, "priority": 80},
            "SCORER_LINE": {"min_pts": 12.0, "min_min": 24.0, "win_rate": 0.333, "priority": 70},
            "ASSIST_MATCHUP": {"min_ast": 6.0, "min_min": 28.0, "win_rate": 0.111, "priority": 30},
            "BIG_REBOUND": {"min_reb": 8.0, "min_min": 26.0, "win_rate": 0.20, "priority": 40}
        }

        # Cache de injuries
        self._injured_cache = set()
        if self.strategy and hasattr(self.strategy, 'injuries_banned'):
            self._injured_cache = self.strategy.injuries_banned.copy()
            logger.info(f"Cache de injuries carregado: {len(self._injured_cache)} jogadores")

    def _is_player_injured(self, player_name):
        if not player_name or not self.strategy: return False
        try:
            norm_name = self.strategy._normalize_name(player_name)
            return norm_name in self._injured_cache
        except: return False

    def _safe_float(self, val):
        if val is None: return 0.0
        try: return float(val)
        except: return 0.0

    def _get_stat(self, p, stat_type):
        maps = {
            "PTS": ["pts_L5", "pts_avg", "PTS", "ppg", "pts"],
            "AST": ["ast_L5", "ast_avg", "AST", "apg", "ast"],
            "REB": ["reb_L5", "reb_avg", "REB", "rpg", "reb"],
            "MIN": ["min_L5", "min_avg", "MIN", "mpg", "min"],
            "3PM": ["3pm_L5", "3pm_avg", "3PM"],
            "BLK": ["blk_L5", "blk_avg", "BLK"],
            "STL": ["stl_L5", "stl_avg", "STL"]
        }
        for k in maps.get(stat_type, []):
            if k in p and p[k] is not None:
                val = self._safe_float(p[k])
                if val > 0: return val
        return 0.0

    def _get_name(self, p):
        return p.get('name') or p.get('PLAYER') or p.get('player_name') or "Unknown"

    # --- GERADOR DE TESE INTELIGENTE OTIMIZADO ---
    def _get_dynamic_thesis(self, player, market, role_tag):
        """Consulta o ThesisEngine para obter justificativa baseada em win rate real"""
        try:
            if self.strategy and hasattr(self.strategy, 'thesis_engine') and self.strategy.thesis_engine:
                # Contexto enriquecido
                ctx = {
                    'pace_factor': player.get('pace_factor', 1.0),
                    'game_spread': player.get('game_spread', 0),
                    'market': market,
                    'opponent': player.get('opponent', '')
                }
                
                # Gera teses ordenadas por win rate
                theses = self.strategy.thesis_engine.generate_theses(player, ctx)
                
                if theses:
                    # 1. Prioridade: teses do mercado específico com win rate > 0
                    valid = [t for t in theses if t['market'] == market and t.get('win_rate', 0) > 0]
                    if valid:
                        best = max(valid, key=lambda x: x.get('win_rate', 0))
                        return best['reason']
                    
                    # 2. Fallback: melhor tese disponível (win rate > 0)
                    safe_theses = [t for t in theses if t.get('win_rate', 0) > 0]
                    if safe_theses:
                        best_safe = max(safe_theses, key=lambda x: x.get('win_rate', 0))
                        return f"{best_safe['reason']} | Adaptado para {market}"
        
        except Exception as e:
            logger.debug(f"Erro em _get_dynamic_thesis: {e}")
        
        # Fallback seguro baseado em win rate real
        safe_fallback_map = {
            "ScorerLine": "HighCeiling (75% WR) - Adaptação Segura",
            "AssistMatchup": "PlaymakerEdge (50% WR) - Adaptação Segura", 
            "BigRebound": "MinutesSafe (67% WR) - Adaptação Segura",
            "GlassCleaner": "MinutesSafe (67% WR) - Adaptação Segura",
            "FloorGeneral": "PlaymakerEdge (50% WR) - Adaptação Segura",
            "VolumeShooter": "HighCeiling (75% WR) - Adaptação Segura"
        }
        return safe_fallback_map.get(role_tag, "Análise Técnica Otimizada")

    def generate_multipla(self, all_players_ctx, game_objects):
        sgp_list = []
        
        # Garante Engine
        if not self.strategy:
            try:
                from modules.new_modules.strategy_engine import StrategyEngine
                self.strategy = StrategyEngine()
                if hasattr(self.strategy, 'injuries_banned'):
                    self._injured_cache = self.strategy.injuries_banned.copy()
            except Exception as e:
                return {"sgp_list": [], "error": "StrategyEngine Error"}

        for game in game_objects:
            try:
                home_team = game['home']
                away_team = game['away']
                game_spread = game.get('spread', 0)
                
                home_roster = self._process_roster(all_players_ctx.get(home_team, []), game_spread)
                away_roster = self._process_roster(all_players_ctx.get(away_team, []), game_spread)
                
                if not home_roster and not away_roster: 
                    continue

                # Draft baseado em win rate real
                draft_home = self._draft_roles_optimized(home_roster)
                draft_away = self._draft_roles_optimized(away_roster)
                
                # Opção 1: Foco em HighCeiling e MinutesSafe (win rate alto)
                legs_opt1, map_opt1 = self._build_high_winrate_ecosystem(draft_home, draft_away)
                strat_1 = "High Win Rate Focus"
                
                # Fallback se não conseguir montar
                used_legs = set()
                if not legs_opt1 or len(legs_opt1) < 3:
                    legs_opt1 = self._build_fallback_optimized(home_roster, away_roster, used_legs)
                    strat_1 = "Safety Net Optimized"

                if legs_opt1 and len(legs_opt1) >= 3:
                    sgp_list.append({
                        "game": f"{away_team} @ {home_team}",
                        "game_id": game.get('game_id'),
                        "option_label": "Opção 1 (Win Rate Alto)",
                        "legs": legs_opt1,
                        "strategy_name": strat_1
                    })
                    for l in legs_opt1: 
                        used_legs.add((l['player_name'], l['market_type']))
                        if not map_opt1: 
                            map_opt1 = {l['player_name']: {"player": l.get('player_data'), "mkt": l['market_type']}}

                # Opção 2: Variação com PlaymakerEdge
                legs_opt2 = self._build_variation_with_playmaker(draft_home, draft_away, map_opt1)
                strat_2 = "Playmaker Edge Mix"
                
                if not legs_opt2 or len(legs_opt2) < 3:
                    legs_opt2 = self._build_fallback_optimized(home_roster, away_roster, used_legs, skip_high_ceiling=True)
                    strat_2 = "Balanced Rotation"

                if legs_opt2 and len(legs_opt2) >= 3:
                    sgp_list.append({
                        "game": f"{away_team} @ {home_team}",
                        "game_id": game.get('game_id'),
                        "option_label": "Opção 2 (Variação)",
                        "legs": legs_opt2,
                        "strategy_name": strat_2
                    })
                    
            except Exception as e:
                logger.error(f"Erro processando jogo {game}: {e}")
                continue

        # Filtrar múltiplas que contenham teses ruins (0% win rate)
        filtered_sgp_list = []
        for sgp in sgp_list:
            bad_theses = ['FloorGeneral', 'GlassCleaner', 'VolumeShooter']
            has_bad_thesis = any(
                any(bad in leg.get('thesis', '') for bad in bad_theses)
                for leg in sgp['legs']
            )
            if not has_bad_thesis:
                filtered_sgp_list.append(sgp)

        return {"sgp_list": filtered_sgp_list}

    def _process_roster(self, raw_roster, game_spread=0):
        """Processa roster com critérios otimizados baseados em win rate"""
        clean = []
        for p in raw_roster:
            p_name = self._get_name(p)
            status = str(p.get('STATUS', '')).lower()
            if 'out' in status or 'inj' in status: 
                continue
            if self._is_player_injured(p_name): 
                continue

            # Calcula estatísticas
            p['pts_L5'] = self._safe_float(p.get('pts_L5', p.get('pts_avg', 0)))
            p['ast_L5'] = self._safe_float(p.get('ast_L5', p.get('ast_avg', 0)))
            p['reb_L5'] = self._safe_float(p.get('reb_L5', p.get('reb_avg', 0)))
            p['min_L5'] = self._safe_float(p.get('min_L5', p.get('min_avg', 0)))
            p['3pm_L5'] = self._safe_float(p.get('3pm_L5', p.get('3pm_avg', 0)))
            p['blk_L5'] = self._safe_float(p.get('blk_L5', p.get('blk_avg', 0)))
            p['stl_L5'] = self._safe_float(p.get('stl_L5', p.get('stl_avg', 0)))
            p['name'] = p_name
            p['game_spread'] = game_spread
            
            # Filtro Otimizado Baseado em Win Rate:
            # 1. HighCeiling: PTS >= 15, MIN >= 30 (75% WR)
            # 2. MinutesSafe: MIN >= 28, PTS >= 12 (67% WR)
            # 3. PlaymakerEdge: AST >= 7, MIN >= 26 (50% WR)
            # 4. ScorerLine: PTS >= 12, MIN >= 24 (33% WR)
            
            qualifies = False
            
            # HighCeiling (máxima prioridade)
            if p['pts_L5'] >= 15 and p['min_L5'] >= 30:
                qualifies = True
                p['role_priority'] = 100
                p['best_thesis'] = 'HighCeiling'
                p['best_market'] = 'PTS'
            
            # MinutesSafe (alta prioridade)
            elif p['min_L5'] >= 28 and p['pts_L5'] >= 12:
                qualifies = True
                p['role_priority'] = 90
                p['best_thesis'] = 'MinutesSafe'
                p['best_market'] = 'PTS'
                if p['reb_L5'] >= 8:
                    p['best_market'] = 'REB'
                elif p['ast_L5'] >= 6:
                    p['best_market'] = 'AST'
            
            # PlaymakerEdge (boa prioridade)
            elif p['ast_L5'] >= 7 and p['min_L5'] >= 26:
                qualifies = True
                p['role_priority'] = 80
                p['best_thesis'] = 'PlaymakerEdge'
                p['best_market'] = 'AST'
            
            # ScorerLine (prioridade média)
            elif p['pts_L5'] >= 12 and p['min_L5'] >= 24:
                qualifies = True
                p['role_priority'] = 70
                p['best_thesis'] = 'ScorerLine'
                p['best_market'] = 'PTS'
            
            # AssistMatchup (baixa prioridade - só se for muito bom)
            elif p['ast_L5'] >= 6 and p['min_L5'] >= 28 and p.get('matchup_score', 0) >= 70:
                qualifies = True
                p['role_priority'] = 30
                p['best_thesis'] = 'AssistMatchup'
                p['best_market'] = 'AST'
            
            # BigRebound (baixa prioridade - só se for muito bom)
            elif p['reb_L5'] >= 8 and p['min_L5'] >= 26 and p.get('matchup_score', 0) >= 70:
                qualifies = True
                p['role_priority'] = 40
                p['best_thesis'] = 'BigRebound'
                p['best_market'] = 'REB'
            
            if qualifies:
                clean.append(p)
            
        # Ordenar por prioridade
        clean.sort(key=lambda x: x.get('role_priority', 0), reverse=True)
        return clean

    def _draft_roles_optimized(self, roster):
        """Draft otimizado baseado em win rate real"""
        roles = {
            "HIGH_CEILING": [],
            "MINUTES_SAFE": [],
            "PLAYMAKER_EDGE": [],
            "SCORER_LINE": [],
            "ASSIST_MATCHUP": [],
            "BIG_REBOUND": []
        }
        
        for p in roster:
            # Classificar em múltiplas categorias se atender
            if p.get('best_thesis') == 'HighCeiling':
                roles["HIGH_CEILING"].append(p)
            if p.get('best_thesis') == 'MinutesSafe':
                roles["MINUTES_SAFE"].append(p)
            if p.get('best_thesis') == 'PlaymakerEdge':
                roles["PLAYMAKER_EDGE"].append(p)
            if p.get('best_thesis') == 'ScorerLine':
                roles["SCORER_LINE"].append(p)
            if p.get('best_thesis') == 'AssistMatchup':
                roles["ASSIST_MATCHUP"].append(p)
            if p.get('best_thesis') == 'BigRebound':
                roles["BIG_REBOUND"].append(p)
        
        # Ordenar cada categoria por prioridade
        for role in roles.values():
            role.sort(key=lambda x: x.get('role_priority', 0), reverse=True)
        
        return roles

    def _build_high_winrate_ecosystem(self, home_draft, away_draft):
        """Constrói múltipla focando em teses com win rate alto"""
        legs = []
        used_players = set()
        player_map = {}

        def pick_high_winrate(role_list, market, role_key, max_per_market=2):
            if len(legs) >= 6: 
                return None
            
            # Limita jogadores do mesmo mercado
            current_market_count = sum(1 for l in legs if l['market_type'] == market)
            if current_market_count >= max_per_market: 
                return None
            
            for p in role_list:
                if p['name'] not in used_players:
                    # Gera tese dinâmica
                    real_thesis = self._get_dynamic_thesis(p, market, role_key)
                    
                    # Usa categoria "conservadora" para alta win rate
                    leg = self.strategy.get_specific_leg(p, market, "conservadora", thesis_override=real_thesis)
                    if leg:
                        leg['player_data'] = p  # Guarda dados do jogador
                        used_players.add(p['name'])
                        player_map[p['name']] = {"player": p, "mkt": market}
                        return leg
            return None

        # Ordem de prioridade baseada em win rate
        priority_order = [
            (home_draft["HIGH_CEILING"], "PTS", "HighCeiling"),
            (away_draft["HIGH_CEILING"], "PTS", "HighCeiling"),
            (home_draft["MINUTES_SAFE"], "PTS", "MinutesSafe"),
            (away_draft["MINUTES_SAFE"], "PTS", "MinutesSafe"),
            (home_draft["PLAYMAKER_EDGE"], "AST", "PlaymakerEdge"),
            (away_draft["PLAYMAKER_EDGE"], "AST", "PlaymakerEdge"),
            (home_draft["SCORER_LINE"], "PTS", "ScorerLine"),
            (away_draft["SCORER_LINE"], "PTS", "ScorerLine"),
        ]
        
        for role_list, market, role_key in priority_order:
            leg = pick_high_winrate(role_list, market, role_key)
            if leg: 
                legs.append(leg)
                if len(legs) >= 4:  # Limitar a 4 legs para manter odd razoável
                    break
        
        # Se tiver menos de 3 legs, tentar adicionar rebounds de MinutesSafe
        if 2 <= len(legs) < 4:
            reb_attempts = [
                (home_draft["MINUTES_SAFE"], "REB", "MinutesSafe"),
                (away_draft["MINUTES_SAFE"], "REB", "MinutesSafe"),
            ]
            for role_list, market, role_key in reb_attempts:
                if len(legs) >= 4: break
                leg = pick_high_winrate(role_list, market, role_key, max_per_market=1)
                if leg: legs.append(leg)
        
        if len(legs) >= 3: 
            return legs, player_map
        return None, {}

    def _build_variation_with_playmaker(self, home_draft, away_draft, prim_map):
        """Constrói variação focando em PlaymakerEdge e versatilidade"""
        legs = []
        used_players = set()

        def pick_variation(role_list, market, role_key):
            tag = role_key
            
            # 1. Tenta jogador já usado em outro mercado (versatilidade)
            for p_name, data in prim_map.items():
                p = data.get('player')
                if p and data.get('mkt') != market and p_name not in used_players:
                    stat_val = p.get(f"{market.lower()}_L5", 0)
                    if stat_val >= self.ROLES.get(role_key, {}).get(f"min_{market.lower()}", 0) * 0.8:
                        dyn_thesis = self._get_dynamic_thesis(p, market, tag)
                        leg = self.strategy.get_specific_leg(p, market, "balanceada", 
                                                           thesis_override=f"{dyn_thesis} (Versátil)")
                        if leg:
                            used_players.add(p_name)
                            return leg
            
            # 2. Tenta novo jogador da mesma categoria
            for p in role_list:
                name = p['name']
                is_prim_same = (name in prim_map and prim_map[name].get('mkt') == market)
                if name not in used_players and not is_prim_same:
                    dyn_thesis = self._get_dynamic_thesis(p, market, tag)
                    leg = self.strategy.get_specific_leg(p, market, "balanceada", 
                                                       thesis_override=f"{dyn_thesis}")
                    if leg:
                        used_players.add(name)
                        return leg
            return None

        # Foco em PlaymakerEdge e variações
        attempts = [
            (home_draft["PLAYMAKER_EDGE"], "AST", "PlaymakerEdge"),
            (away_draft["PLAYMAKER_EDGE"], "AST", "PlaymakerEdge"),
            (home_draft["MINUTES_SAFE"], "REB", "MinutesSafe"),
            (away_draft["MINUTES_SAFE"], "REB", "MinutesSafe"),
            (home_draft["HIGH_CEILING"], "PTS", "HighCeiling"),
            (away_draft["HIGH_CEILING"], "PTS", "HighCeiling"),
        ]
        
        for role_list, market, role_key in attempts:
            if len(legs) >= 4: break
            leg = pick_variation(role_list, market, role_key)
            if leg: legs.append(leg)

        if len(legs) >= 3: return legs
        return None

    def _build_fallback_optimized(self, home_roster, away_roster, used_set, skip_high_ceiling=False):
        """Fallback otimizado com foco em consistência"""
        all_p = home_roster + away_roster
        if not all_p: return None
        
        # Ordenar por consistência (minutagem + prioridade)
        all_p.sort(key=lambda x: (x.get('min_L5', 0) * 1.5 + x.get('role_priority', 0)), reverse=True)
        
        # Pular HighCeiling se solicitado
        candidates = all_p
        if skip_high_ceiling:
            candidates = [p for p in all_p if p.get('best_thesis') != 'HighCeiling']
        
        if not candidates:
            candidates = all_p
            
        legs = []
        for p in candidates:
            if len(legs) >= 4: break
            p_name = p['name']
            
            # Determinar melhor mercado baseado na tese
            market = p.get('best_market', 'PTS')
            thesis_type = p.get('best_thesis', 'ScorerLine')
            
            if (p_name, market) in used_set:
                continue
            
            # Verificar se atende critérios mínimos
            min_threshold = {
                'PTS': 10, 'REB': 6, 'AST': 5, '3PM': 1.5, 'BLK': 0.8, 'STL': 0.9
            }.get(market, 1)
            
            stat_val = p.get(f"{market.lower()}_L5", 0)
            if stat_val < min_threshold:
                continue
            
            # Gerar leg
            t = self._get_dynamic_thesis(p, market, thesis_type)
            l = self.strategy.get_specific_leg(p, market, "conservadora", thesis_override=t)
            
            if l:
                legs.append(l)
                used_set.add((p_name, market))
        
        return legs if len(legs) >= 3 else None