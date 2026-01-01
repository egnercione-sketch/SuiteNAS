# modules/new_modules/desdobrador_inteligente.py
"""
DESDOBRADOR INTELIGENTE v3.3 - DINÂMICO ORGÂNICO (CORRIGIDO + AUDIT FIX + DETERMINISTIC)
Diferencial: Mix de perfis dentro da mesma combinação, consideração de matchups,
rotação inteligente baseada em contexto do jogo (blowout, pace, rest days).
Fix: Propagação correta de game_id numérico para validação de auditoria.
Fix Determinismo: Implementação de Seed baseada nos jogos para resultados estáticos.
"""

import logging
import math
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from collections import defaultdict, Counter
import itertools
import random
import hashlib # Adicionado para garantir determinismo

logger = logging.getLogger("Desdobrador_Inteligente_v3.3_Fixed")

class DesdobradorInteligente:
    def __init__(self, strategy_engine):
        self.engine = strategy_engine
        self.config = {
            'min_minutes': 18.0,
            'max_same_game': 2,
            'target_diversity': 3,
            'weights': {'diversity': 3.0, 'minutes': 0.1, 'edge': 2.0, 'matchup': 2.5},
            # Perfis de risco dinâmicos (com nomes em português)
            'risk_profiles': {
                'AGRESSIVO': {
                    'pts_multiplier': {'PISO': 0.85, 'MEDIO': 1.0, 'TETO': 1.15},
                    'ast_reb_multiplier': {'PISO': 0.90, 'MEDIO': 1.05, 'TETO': 1.25},
                    'risk_distribution': [('TETO', 0.5), ('MEDIO', 0.3), ('PISO', 0.2)]
                },
                'BALANCEADO': {
                    'pts_multiplier': {'PISO': 0.80, 'MEDIO': 0.95, 'TETO': 1.05},
                    'ast_reb_multiplier': {'PISO': 0.85, 'MEDIO': 1.0, 'TETO': 1.15},
                    'risk_distribution': [('MEDIO', 0.5), ('PISO', 0.35), ('TETO', 0.15)]
                },
                'CONSERVADOR': {
                    'pts_multiplier': {'PISO': 0.75, 'MEDIO': 0.85, 'TETO': 0.95},
                    'ast_reb_multiplier': {'PISO': 0.80, 'MEDIO': 0.90, 'TETO': 1.05},
                    'risk_distribution': [('PISO', 0.7), ('MEDIO', 0.25), ('TETO', 0.05)]
                }
            },
            # Thresholds por perfil (também em português)
            'thresholds': {
                'AGRESSIVO': {'PTS': 10, 'AST': 3, 'REB': 4},
                'BALANCEADO': {'PTS': 12, 'AST': 4, 'REB': 5},
                'CONSERVADOR': {'PTS': 14, 'AST': 5, 'REB': 6}
            },
            # Penalidades avançadas
            'advanced_penalties': {
                'player_reuse': 0.25,
                'team_concentration': 0.15,
                'market_monotony': 0.20
            }
        }
        
        # Cache para análise de matchup
        self.matchup_cache = {}

    # --- NOVO MÉTODO: TRAVA DE DETERMINISMO ---
    def _set_deterministic_seed(self, games_ctx: List[Dict]):
        """
        Gera uma seed única baseada nos IDs dos jogos do dia.
        Isso garante que random.sample e random.choice retornem sempre
        os mesmos resultados para o mesmo conjunto de jogos.
        """
        try:
            # Cria string única com IDs dos jogos + Data de hoje
            # Ordena para garantir que a ordem de seleção não afete o hash
            ids = sorted([str(g.get('game_id') or f"{g.get('away')}@{g.get('home')}") for g in games_ctx])
            unique_str = "".join(ids) + datetime.now().strftime("%Y%m%d")
            
            # Gera hash numérico
            hash_val = int(hashlib.md5(unique_str.encode()).hexdigest(), 16) % (2**32)
            
            # Trava as seeds
            random.seed(hash_val)
            np.random.seed(hash_val)
            logger.info(f"Seed determinística aplicada: {hash_val}")
        except Exception as e:
            logger.warning(f"Erro ao aplicar seed: {e}")

    def gerar_desdobramentos(self, players_ctx: Dict, games_ctx: List[Dict], 
                            perfil: str = 'BALANCEADO', max_combinacoes: int = 20) -> List[Dict]:
        logger.info(f"Iniciando v3.3 (Audit Fix) - Perfil: {perfil}")
        
        # 0. APLICAR DETERMINISMO (FIX: Isso impede que os resultados mudem a cada clique)
        self._set_deterministic_seed(games_ctx)

        # Garantir que o perfil está em maiúsculas
        perfil = perfil.upper()
        
        # 1. ANÁLISE CONTEXTUAL DOS JOGOS
        game_analysis = self._analisar_contexto_jogos(games_ctx)
        
        # 2. CLUSTERIZAÇÃO COM CONTEXTO
        pools = self._criar_pools_contextuais(players_ctx, games_ctx, perfil, game_analysis)
        
        total_legs = sum(len(p) for p in pools.values())
        if total_legs < 8:
            logger.warning(f"Pools pequenos: {total_legs} legs total")
            return []
            
        # 3. SELEÇÃO DINÂMICA
        pools_dinamicos = self._selecao_dinamica_pools(pools, perfil)
        
        # 4. GERAÇÃO ORGÂNICA DE COMBINAÇÕES
        todas_combs = self._gerar_combinacoes_organicas(pools_dinamicos, perfil, game_analysis)
        
        if not todas_combs:
            logger.warning("Nenhuma combinação válida gerada")
            return []
        
        # 5. APLICAÇÃO DE PENALIDADES CONTEXTUAIS
        todas_combs_com_penalidade = []
        for comb in todas_combs:
            score_ajustado = self._aplicar_penalidades_contextuais(comb, perfil, game_analysis)
            comb['score_ajustado'] = score_ajustado
            todas_combs_com_penalidade.append(comb)
        
        # FIX: Ordenação estável antes da seleção final para garantir determinismo
        todas_combs_com_penalidade.sort(key=lambda x: x['score_ajustado'], reverse=True)

        # 6. SELEÇÃO FINAL COM ROTAÇÃO INTELIGENTE
        comb_finais = self._selecionar_com_rotacao_inteligente(
            todas_combs_com_penalidade, 
            max_combinacoes,
            perfil
        )
        
        return comb_finais
    
    def _analisar_contexto_jogos(self, games_ctx: List[Dict]) -> Dict:
        """Analisa contexto dos jogos para tomada de decisão"""
        analysis = {}
        
        for game in games_ctx:
            # FIX: Prioriza game_id numérico se disponível
            game_id = game.get('game_id')
            if not game_id or game_id == "UNK":
                game_id = f"{game.get('away')} @ {game.get('home')}"
            
            # Análise de blowout potential (baseado em spreads)
            spread = 0
            try:
                spread = abs(float(game.get('spread', 0)))
            except:
                spread = 0
                
            blowout_risk = 'ALTO' if spread > 12 else 'MEDIO' if spread > 8 else 'BAIXO'
            
            analysis[game_id] = {
                'spread': spread,
                'blowout_risk': blowout_risk,
                'total_line': game.get('total', 0),
                'teams': [game.get('away'), game.get('home')]
            }
            
        return analysis
    
    def _obter_pace_jogo(self, away_team: str, home_team: str) -> float:
        """Obtém o pace estimado do jogo"""
        try:
            if hasattr(self.engine, 'pace_adjuster') and self.engine.pace_adjuster:
                # Tenta obter pace de ambas as equipes
                away_pace = 1.0
                home_pace = 1.0
                
                try:
                    away_pace = self.engine.pace_adjuster.get_team_pace_factor(away_team)
                except:
                    pass
                    
                try:
                    home_pace = self.engine.pace_adjuster.get_team_pace_factor(home_team)
                except:
                    pass
                    
                return (away_pace + home_pace) / 2
        except Exception as e:
            logger.debug(f"Erro ao obter pace: {e}")
            
        return 1.0  # Neutro

    # --- NOVO MÉTODO: GERAÇÃO DE NARRATIVA ---
    def _obter_tese_narrativa(self, player: Dict, market: str, game_ctx: Dict) -> str:
        """Consulta o ThesisEngine para obter a narrativa real da aposta"""
        try:
            if hasattr(self.engine, 'thesis_engine') and self.engine.thesis_engine:
                # Prepara contexto para o ThesisEngine
                t_ctx = {
                    'pace_factor': player.get('pace_factor', 1.0),
                    'spread': game_ctx.get('spread', 0),
                    'vacuum_boost': False
                }
                
                # Adapta chaves para o padrão L5 esperado pelo ThesisEngine
                # Usamos stats ajustados (pace) para melhor precisão
                player_compat = player.copy()
                adj = player.get('adjusted_stats', {})
                if adj:
                    player_compat['pts_L5'] = adj.get('pts', 0)
                    player_compat['ast_L5'] = adj.get('ast', 0)
                    player_compat['reb_L5'] = adj.get('reb', 0)
                    player_compat['min_L5'] = adj.get('min', 0)
                
                # Gera teses
                theses = self.engine.thesis_engine.generate_theses(player_compat, t_ctx)
                
                # Busca tese específica para o mercado
                for t in theses:
                    if t['market'] == market:
                        return t['reason']
                        
        except Exception as e:
            logger.debug(f"Erro ao gerar tese: {e}")
            
        # Fallback se não conseguir gerar tese avançada
        avg = player.get('avg', 0)
        if avg == 0 and 'adjusted_stats' in player:
             avg = player['adjusted_stats'].get(market.lower(), 0)
        return f"Análise Técnica (Avg {avg:.1f})"
    
    def _criar_pools_contextuais(self, players_ctx: Dict, games_ctx: List[Dict], 
                                perfil: str, game_analysis: Dict) -> Dict[str, List[Dict]]:
        pools = {'PTS': [], 'AST': [], 'REB': [], 'COMBO': []}
        
        game_map = {}
        for game in games_ctx:
            # FIX: Prioriza game_id numérico
            game_id = game.get('game_id')
            if not game_id or game_id == "UNK":
                game_id = f"{game.get('away')} @ {game.get('home')}"
                
            game_map[game.get('away')] = {'game_id': game_id, 'opponent': game.get('home')}
            game_map[game.get('home')] = {'game_id': game_id, 'opponent': game.get('away')}
        
        for team, players in players_ctx.items():
            if team not in game_map:
                continue
                
            game_info = game_map[team]
            game_id = game_info['game_id']
            opponent = game_info['opponent']
            
            # Contexto do jogo atual
            game_ctx = game_analysis.get(game_id, {})
            blowout_risk = game_ctx.get('blowout_risk', 'BAIXO')
            pace_factor = self._obter_pace_jogo(team, opponent)
            
            for p in players:
                p_name = p.get('name', 'Unknown')
                
                # Verificar lesão
                if self.engine._normalize_name(p_name) in self.engine.injuries_banned:
                    continue
                
                minutes = self._safe_float(p, ['MIN_AVG', 'min_L5', 'min', 'minutes'])
                if minutes < self.config['min_minutes']:
                    continue
                
                # Aplicar ajuste de pace
                pts_val = self._safe_float(p, ['pts_L5', 'pts', 'PTS', 'ppg']) * pace_factor
                ast_val = self._safe_float(p, ['ast_L5', 'ast', 'AST', 'apg']) * pace_factor
                reb_val = self._safe_float(p, ['reb_L5', 'reb', 'REB', 'rpg']) * pace_factor
                
                # Análise de matchup
                matchup_score = self._analisar_matchup_player(p, opponent, team)
                
                # Análise de ceiling com contexto
                ceiling_ratio = 1.0
                if self.engine.ceiling_engine:
                    try:
                        game_context = {
                            'opponent': opponent,
                            'blowout_risk': blowout_risk,
                            'pace': pace_factor,
                            'spread': game_ctx.get('spread', 0)
                        }
                        ceil_data = self.engine._get_ceiling_analysis(p, game_context)
                        if ceil_data:
                            ceiling_ratio = ceil_data.get('ceiling_ratio', 1.0)
                    except Exception as e:
                        logger.debug(f"Erro ceiling analysis {p_name}: {e}")
                
                # Dados enriquecidos
                p_enriched = p.copy()
                p_enriched.update({
                    'opponent': opponent,
                    'game_id': game_id, # ID correto propagado
                    'team': team,
                    'ceiling_ratio': ceiling_ratio,
                    'matchup_score': matchup_score,
                    'blowout_risk': blowout_risk,
                    'pace_factor': pace_factor,
                    'adjusted_stats': {
                        'pts': pts_val,
                        'ast': ast_val,
                        'reb': reb_val,
                        'min': minutes
                    }
                })
                
                # Criar legs para diferentes mercados
                for market in ['PTS', 'AST', 'REB']:
                    leg = self._criar_leg_contextual(p_enriched, market, perfil, game_ctx)
                    if leg:
                        pools[market].append(leg)
                
                # Considerar combos para jogadores versáteis
                if ast_val >= 5 and reb_val >= 5 and pts_val >= 12:
                    leg_combo = self._criar_leg_combo(p_enriched, perfil, game_ctx)
                    if leg_combo:
                        pools['COMBO'].append(leg_combo)
        
        return pools
    
    def _analisar_matchup_player(self, player: Dict, opponent: str, team: str) -> float:
        """Analisa matchup específico do jogador"""
        cache_key = f"{player.get('name')}_{opponent}"
        
        if cache_key in self.matchup_cache:
            return self.matchup_cache[cache_key]
        
        base_score = 50.0  # Neutro
        
        try:
            # Usar DvP se disponível
            if hasattr(self.engine, 'dvp_analyzer') and self.engine.dvp_analyzer:
                position = self.engine._estimate_position(player)
                dvp_rank = self.engine.dvp_analyzer.get_position_rank(opponent, position)
                # Rank 1 = melhor defesa, Rank 30 = pior defesa
                dvp_score = (30 - dvp_rank) * 3.33  # Converter para escala 0-100
                base_score = dvp_score
        except Exception as e:
            logger.debug(f"Erro análise DvP: {e}")
        
        self.matchup_cache[cache_key] = base_score
        return base_score
    
    def _criar_leg_contextual(self, player: Dict, market: str, perfil: str, game_ctx: Dict) -> Optional[Dict]:
        """Cria leg considerando contexto do jogo e perfil"""
        stats_key = market.lower()
        avg = player['adjusted_stats'].get(stats_key, 0)
        
        if avg == 0:
            return None
        
        # Verificar thresholds
        thresholds = self.config['thresholds'][perfil]
        if avg < thresholds[market]:
            return None
        
        # Determinar perfil de risco dinamicamente
        risk_profile = self._determinar_risco_dinamico(player, market, perfil, game_ctx)
        
        # Obter multiplicador baseado no perfil de risco
        risk_config = self.config['risk_profiles'][perfil]
        if market == 'PTS':
            multiplier = risk_config['pts_multiplier'][risk_profile]
        else:
            multiplier = risk_config['ast_reb_multiplier'][risk_profile]
        
        # Ajustes contextuais
        matchup_bonus = max(-0.3, min(0.3, (player['matchup_score'] - 50) / 100))
        multiplier *= (1 + matchup_bonus)
        
        # Ajuste por blowout risk
        if player['blowout_risk'] == 'ALTO' and market in ['PTS', 'REB']:
            multiplier *= 0.9
        
        # Calcular linha
        smart_line = max(1, int(avg * multiplier))
        
        # Calcular odds baseado no risco
        odds = self._calcular_odds_por_risco(risk_profile, multiplier, player['ceiling_ratio'])
        
        # Score de qualidade
        quality_score = self._calcular_quality_score(player, market, avg, smart_line, risk_profile)
        
        # --- GERAÇÃO DA TESE ---
        narrative = self._obter_tese_narrativa(player, market, game_ctx)
        
        return {
            'player_name': player.get('name', 'Unknown'),
            'team': player['team'],
            'game_id': player['game_id'], # ID numérico
            'market': market,
            'market_display': f"{smart_line}+ {market}",
            'line': smart_line,
            'risco': risk_profile,
            'odds': round(odds, 2),
            'avg': round(avg, 1),
            'minutes': player['adjusted_stats']['min'],
            'quality_score': quality_score,
            'matchup_score': player['matchup_score'],
            'ceiling_ratio': player['ceiling_ratio'],
            'blowout_risk': player['blowout_risk'],
            'thesis': narrative,  # TESE PREENCHIDA
            'game_info': {
                'opponent': player['opponent'],
                'context': game_ctx,
                'game_id': player['game_id'] # FIX: Injeta ID para redundância de auditoria
            }
        }
    
    def _determinar_risco_dinamico(self, player: Dict, market: str, perfil: str, game_ctx: Dict) -> str:
        """Determina o perfil de risco de forma dinâmica"""
        # Baseado nas distribuições configuradas
        risk_config = self.config['risk_profiles'][perfil]
        risk_dist = risk_config['risk_distribution']
        
        if not risk_dist:
            return 'MEDIO'
            
        risks, probs = zip(*risk_dist)
        
        # Ajustar probabilidades baseado no contexto
        adjusted_probs = list(probs)
        
        # Ajustar por matchup
        if player['matchup_score'] > 70:  # Matchup favorável
            if 'TETO' in risks:
                idx = risks.index('TETO')
                adjusted_probs[idx] *= 1.3
            if 'PISO' in risks:
                idx = risks.index('PISO')
                adjusted_probs[idx] *= 0.7
        elif player['matchup_score'] < 30:  # Matchup desfavorável
            if 'PISO' in risks:
                idx = risks.index('PISO')
                adjusted_probs[idx] *= 1.4
            if 'TETO' in risks:
                idx = risks.index('TETO')
                adjusted_probs[idx] *= 0.6
        
        # Ajustar por blowout risk
        if player['blowout_risk'] == 'ALTO' and 'PISO' in risks:
            idx = risks.index('PISO')
            adjusted_probs[idx] *= 1.2
        
        # Ajustar por ceiling ratio
        if player['ceiling_ratio'] > 1.3 and 'TETO' in risks:
            idx = risks.index('TETO')
            adjusted_probs[idx] *= 1.5
        elif player['ceiling_ratio'] < 0.9 and 'PISO' in risks:
            idx = risks.index('PISO')
            adjusted_probs[idx] *= 1.3
        
        # Normalizar probabilidades
        total = sum(adjusted_probs)
        if total == 0:
            return 'MEDIO'
            
        adjusted_probs = [p/total for p in adjusted_probs]
        
        # Selecionar risco (AGORA DETERMINÍSTICO GRAÇAS AO SEED NO INIT)
        try:
            return np.random.choice(risks, p=adjusted_probs)
        except:
            return 'MEDIO'
    
    def _calcular_odds_por_risco(self, risco: str, multiplier: float, ceiling_ratio: float) -> float:
        """Calcula odds baseado no perfil de risco e contexto"""
        base_odds = {
            'PISO': 1.40,
            'MEDIO': 1.80,
            'TETO': 2.20
        }
        
        odds = base_odds.get(risco, 1.80)
        
        # Ajustar por agressividade da linha
        line_aggressiveness = abs(multiplier - 1.0)
        odds += line_aggressiveness * 0.5
        
        # Ajustar por ceiling
        if ceiling_ratio > 1.2:
            odds -= 0.1
        elif ceiling_ratio < 0.9:
            odds += 0.15
        
        # Limites
        return min(3.50, max(1.20, odds))
    
    def _calcular_quality_score(self, player: Dict, market: str, avg: float, 
                               line: int, risco: str) -> float:
        """Calcula score de qualidade considerando múltiplos fatores"""
        score = 0
        
        # Base: performance vs linha
        if avg > 0 and line > 0:
            score += (avg * 2) / line
        
        # Bônus por minutos
        score += player['adjusted_stats']['min'] * 0.05
        
        # Bônus por matchup
        score += player['matchup_score'] * 0.02
        
        # Bônus por ceiling
        score += player['ceiling_ratio'] * 2
        
        # Bônus/penalidade por risco
        risk_bonus = {'PISO': -0.5, 'MEDIO': 0, 'TETO': 1.0}
        score += risk_bonus.get(risco, 0)
        
        # Penalidade por blowout risk alto
        if player['blowout_risk'] == 'ALTO':
            score -= 1.0
        
        return round(max(0, score), 2)
    
    def _criar_leg_combo(self, player: Dict, perfil: str, game_ctx: Dict) -> Optional[Dict]:
        """Cria leg de combo para jogadores versáteis"""
        pts = player['adjusted_stats']['pts']
        ast = player['adjusted_stats']['ast']
        reb = player['adjusted_stats']['reb']
        
        # Determinar combo baseado no perfil
        combo_type = None
        line_pts = 0
        line_ast = 0
        line_reb = 0
        
        if perfil == 'AGRESSIVO':
            if pts >= 15 and ast >= 5:
                combo_type = 'PTS+AST'
                line_pts = max(1, int(pts * 0.9))
                line_ast = max(1, int(ast * 0.9))
            elif pts >= 15 and reb >= 6:
                combo_type = 'PTS+REB'
                line_pts = max(1, int(pts * 0.9))
                line_reb = max(1, int(reb * 0.9))
        elif perfil == 'BALANCEADO':
            if pts >= 18 and ast >= 6:
                combo_type = 'PTS+AST'
                line_pts = max(1, int(pts * 0.85))
                line_ast = max(1, int(ast * 0.85))
        
        if not combo_type:
            return None
        
        # Calcular odds para combo
        base_odd = 2.50
        if perfil == 'AGRESSIVO':
            base_odd = 2.80
        elif perfil == 'CONSERVADOR':
            base_odd = 2.20
        
        # Ajustar por matchup
        matchup_factor = player['matchup_score'] / 50  # 0.6 a 1.4
        final_odd = min(4.0, max(1.8, base_odd * matchup_factor))
        
        # Criar display
        if combo_type == 'PTS+AST':
            market_display = f"{line_pts}+ PTS & {line_ast}+ AST"
        else:
            market_display = f"{line_pts}+ PTS & {line_reb}+ REB"
        
        return {
            'player_name': player.get('name', 'Unknown'),
            'team': player['team'],
            'game_id': player['game_id'], # ID numérico
            'market': 'COMBO',
            'market_display': market_display,
            'combo_type': combo_type,
            'odds': round(final_odd, 2),
            'quality_score': (pts + ast + reb) * 0.3,
            'matchup_score': player['matchup_score'],
            'thesis': f"Combo Versátil ({combo_type})", # TESE FIXA PARA COMBOS
            'game_info': {
                'opponent': player['opponent'],
                'context': game_ctx,
                'game_id': player['game_id'] # FIX: Injeta ID
            }
        }
    
    def _selecao_dinamica_pools(self, pools: Dict[str, List[Dict]], perfil: str) -> Dict[str, List[Dict]]:
        """Seleção dinâmica do pool para aumentar variedade"""
        pools_dinamicos = {}
        
        for market, legs in pools.items():
            if not legs:
                continue
            
            # FIX: Ordenar por score E nome para garantir estabilidade no determinismo
            legs_sorted = sorted(legs, key=lambda x: (x.get('quality_score', 0), x.get('player_name', '')), reverse=True)
            
            # Tamanho do pool dinâmico
            base_size = 15
            if perfil == 'AGRESSIVO':
                base_size = 20
            elif perfil == 'CONSERVADOR':
                base_size = 12
            
            # Adicionar variedade
            selected = []
            seen_players = set()
            
            # Primeiro, garantir os top
            top_count = 8 if perfil == 'AGRESSIVO' else 6
            for leg in legs_sorted[:top_count]:
                player_name = leg.get('player_name', '')
                if player_name and player_name not in seen_players:
                    selected.append(leg)
                    seen_players.add(player_name)
            
            # Depois, adicionar variedade de riscos e times
            remaining = [l for l in legs_sorted if l.get('player_name', '') not in seen_players]
            
            # Agrupar por risco para garantir representação
            by_risk = defaultdict(list)
            for leg in remaining:
                risk = leg.get('risco', 'MEDIO')
                by_risk[risk].append(leg)
            
            # Selecionar de cada grupo de risco
            risk_quota = {'PISO': 2, 'MEDIO': 3, 'TETO': 4}
            for risk, quota in risk_quota.items():
                risk_legs = by_risk.get(risk, [])
                selected_from_risk = min(quota, len(risk_legs))
                for leg in risk_legs[:selected_from_risk]:
                    player_name = leg.get('player_name', '')
                    if player_name and player_name not in seen_players and len(selected) < base_size:
                        selected.append(leg)
                        seen_players.add(player_name)
            
            pools_dinamicos[market] = selected
        
        return pools_dinamicos
    
    def _gerar_combinacoes_organicas(self, pools: Dict[str, List[Dict]], 
                                    perfil: str, game_analysis: Dict) -> List[Dict]:
        """Gera combinações de forma orgânica"""
        todas_combs = []
        
        # Estratégias diferentes baseadas no perfil
        if perfil == 'AGRESSIVO':
            strategies = ['balanced_mix', 'high_ceiling']
        elif perfil == 'BALANCEADO':
            strategies = ['balanced_mix', 'safe_mix']
        else:  # CONSERVADOR
            strategies = ['safe_mix']
        
        # Gerar combinações para cada estratégia
        for strategy in strategies:
            strategy_combs = self._gerar_por_estrategia(strategy, pools, perfil, game_analysis)
            todas_combs.extend(strategy_combs)
        
        # Limitar número total
        return todas_combs[:100]
    
    def _gerar_por_estrategia(self, strategy: str, pools: Dict[str, List[Dict]], 
                             perfil: str, game_analysis: Dict) -> List[Dict]:
        """Gera combinações baseadas em uma estratégia específica"""
        combs = []
        
        if strategy == 'balanced_mix':
            combs = self._gerar_combinacoes_balanceadas(pools, perfil)
        elif strategy == 'high_ceiling':
            combs = self._gerar_combinacoes_high_ceiling(pools, perfil)
        elif strategy == 'safe_mix':
            combs = self._gerar_combinacoes_safe(pools, perfil)
        
        return combs
    
    def _gerar_combinacoes_balanceadas(self, pools: Dict[str, List[Dict]], perfil: str) -> List[Dict]:
        """Combinações com mix de riscos e mercados"""
        combs = []
        
        # Garantir que temos samples
        pts_pool = pools.get('PTS', [])
        ast_pool = pools.get('AST', [])
        reb_pool = pools.get('REB', [])
        
        if not pts_pool or not ast_pool or not reb_pool:
            return combs
        
        # Amostrar legs de cada pool (AGORA DETERMINÍSTICO DEVIDO AO SEED)
        pts_sample = random.sample(pts_pool, min(5, len(pts_pool)))
        ast_sample = random.sample(ast_pool, min(5, len(ast_pool)))
        reb_sample = random.sample(reb_pool, min(5, len(reb_pool)))
        
        # Gerar combinações aleatórias (MAS ESTÁTICAS PELO SEED)
        for _ in range(20):
            pts_leg = random.choice(pts_sample) if pts_sample else None
            ast_leg = random.choice(ast_sample) if ast_sample else None
            reb_leg = random.choice(reb_sample) if reb_sample else None
            
            if not all([pts_leg, ast_leg, reb_leg]):
                continue
            
            legs = [pts_leg, ast_leg, reb_leg]
            
            # Verificar validade
            if self._validar_combinacao_organica(legs, perfil):
                trixie = self._empacotar_combinacao(legs, perfil)
                if trixie:
                    combs.append(trixie)
        
        return combs
    
    def _gerar_combinacoes_high_ceiling(self, pools: Dict[str, List[Dict]], perfil: str) -> List[Dict]:
        """Combinações focadas em alto ceiling"""
        combs = []
        
        # Filtrar legs com alto ceiling
        high_ceiling_legs = []
        for market in ['PTS', 'AST', 'REB']:
            for leg in pools.get(market, []):
                if leg.get('ceiling_ratio', 1.0) > 1.3:
                    high_ceiling_legs.append(leg)
        
        if len(high_ceiling_legs) < 3:
            return combs
        
        # Gerar combinações (DETERMINÍSTICO)
        for _ in range(15):
            sample = random.sample(high_ceiling_legs, 3)
            
            # Verificar se temos pelo menos um de cada mercado
            markets = [leg.get('market') for leg in sample]
            if len(set(markets)) < 2:
                continue
            
            if self._validar_combinacao_organica(sample, perfil):
                trixie = self._empacotar_combinacao(sample, perfil)
                if trixie:
                    combs.append(trixie)
        
        return combs
    
    def _gerar_combinacoes_safe(self, pools: Dict[str, List[Dict]], perfil: str) -> List[Dict]:
        """Combinações conservadoras"""
        combs = []
        
        # Filtrar legs seguras (PISO ou MEDIO)
        safe_legs = []
        for market in ['PTS', 'AST', 'REB']:
            for leg in pools.get(market, []):
                risco = leg.get('risco', 'MEDIO')
                if risco in ['PISO', 'MEDIO']:
                    safe_legs.append(leg)
        
        if len(safe_legs) < 3:
            return combs
        
        # Gerar combinações (DETERMINÍSTICO)
        for _ in range(15):
            sample = random.sample(safe_legs, 3)
            
            if self._validar_combinacao_organica(sample, perfil):
                trixie = self._empacotar_combinacao(sample, perfil)
                if trixie:
                    combs.append(trixie)
        
        return combs
    
    def _validar_combinacao_organica(self, legs: List[Dict], perfil: str) -> bool:
        """Validação para combinações orgânicas"""
        if len(legs) < 3:
            return False
        
        # Verificar jogadores únicos
        players = set()
        for l in legs:
            player_name = l.get('player_name')
            if not player_name:
                return False
            players.add(player_name)
        
        if len(players) < len(legs):
            return False
        
        # Verificar diversidade de jogos (não mais que 2 do mesmo jogo)
        game_counts = Counter()
        for l in legs:
            game_id = l.get('game_id')
            if game_id:
                game_counts[game_id] += 1
        
        if any(count > 2 for count in game_counts.values()):
            return False
        
        # Verificar diversidade de times (não mais que 2 do mesmo time)
        team_counts = Counter()
        for l in legs:
            team = l.get('team')
            if team:
                team_counts[team] += 1
        
        if any(count > 2 for count in team_counts.values()):
            return False
        
        # Mix de riscos baseado no perfil
        riscos = []
        for l in legs:
            risco = l.get('risco')
            if risco:
                riscos.append(risco)
        
        if riscos:
            if perfil == 'CONSERVADOR' and riscos.count('PISO') < 2:
                return False
            elif perfil == 'AGRESSIVO' and riscos.count('TETO') < 1:
                return False
        
        return True
    
    def _empacotar_combinacao(self, legs: List[Dict], perfil: str) -> Optional[Dict]:
        """Empacota uma combinação em formato de trixie"""
        if not legs:
            return None
        
        total_odd = 1.0
        players = set()
        
        for l in legs:
            odds = l.get('odds', 1.0)
            total_odd *= odds
            player_name = l.get('player_name')
            if player_name:
                players.add(player_name)
        
        # Calcular score base
        quality_scores = [l.get('quality_score', 0) for l in legs]
        score_base = sum(quality_scores) / len(legs) if legs else 0
        
        # Bônus por diversidade
        unique_games = len(set(l.get('game_id', '') for l in legs))
        diversity_bonus = unique_games * 0.5
        
        # Bônus por mix de riscos
        riscos = [l.get('risco', 'MEDIO') for l in legs if l.get('risco')]
        risk_bonus = len(set(riscos)) * 0.3
        
        score_final = score_base + diversity_bonus + risk_bonus
        
        return {
            'legs': legs,
            'total_odd': round(total_odd, 2),
            'score_original': round(score_base, 2),
            'score_ajustado': round(score_final, 2),
            'perfil': perfil,
            'player_set': players,
            'composicao': {
                'jogos_distintos': unique_games,
                'mix_riscos': dict(Counter(riscos)),
                'unique_players': len(players)
            }
        }
    
    def _aplicar_penalidades_contextuais(self, comb: Dict, perfil: str, game_analysis: Dict) -> float:
        """Aplica penalidades baseadas em contexto"""
        score = comb.get('score_ajustado', comb.get('score_original', 0))
        
        for leg in comb.get('legs', []):
            game_id = leg.get('game_id')
            game_ctx = game_analysis.get(game_id, {}) if game_id else {}
            
            # Penalidade por blowout risk alto
            if leg.get('blowout_risk') == 'ALTO':
                score -= 1.0
            
            # Penalidade por matchup ruim
            matchup_score = leg.get('matchup_score', 50)
            if matchup_score < 40:
                score -= 0.5
            
            # Bônus por matchup excelente
            if matchup_score > 70:
                score += 0.8
        
        return max(0, score)
    
    def _selecionar_com_rotacao_inteligente(self, combs: List[Dict], 
                                            max_combinacoes: int, perfil: str) -> List[Dict]:
        """Seleção final com rotação inteligente"""
        if not combs:
            return []
        
        # Ordenar por score ajustado
        combs_sorted = sorted(combs, key=lambda x: x.get('score_ajustado', 0), reverse=True)
        
        selecionadas = []
        player_usage = Counter()
        team_usage = Counter()
        
        penalty_config = self.config['advanced_penalties']
        
        for _ in range(max_combinacoes):
            if not combs_sorted:
                break
            
            best_idx = -1
            best_score = -float('inf')
            
            for idx, comb in enumerate(combs_sorted):
                # Calcular penalidades acumuladas
                penalty = 1.0
                
                # Penalidade por reuso de jogadores
                for player in comb.get('player_set', set()):
                    usage = player_usage.get(player, 0)
                    penalty *= (1 - penalty_config['player_reuse']) ** usage
                
                # Penalidade por concentração de time
                for leg in comb.get('legs', []):
                    team = leg.get('team')
                    if team:
                        usage = team_usage.get(team, 0)
                        penalty *= (1 - penalty_config['team_concentration']) ** (usage / 2)
                
                score_atual = comb.get('score_ajustado', 0) * penalty
                
                if score_atual > best_score:
                    best_score = score_atual
                    best_idx = idx
            
            if best_idx == -1:
                break
            
            # Selecionar combinação
            selected = combs_sorted.pop(best_idx)
            selected['score_final'] = round(best_score, 2)
            selecionadas.append(selected)
            
            # Atualizar contadores de uso
            for player in selected.get('player_set', set()):
                player_usage[player] += 1
            
            for leg in selected.get('legs', []):
                team = leg.get('team')
                if team:
                    team_usage[team] += 1
        
        return selecionadas[:max_combinacoes]
    
    def _safe_float(self, data: Dict, keys: List[str]) -> float:
        for k in keys:
            try:
                val = data.get(k)
                if val is None:
                    continue
                val_float = float(val)
                if val_float > 0:
                    return val_float
            except (ValueError, TypeError):
                continue
        return 0.0
    
    def gerar_relatorio_desdobramento(self, desdobramentos: List[Dict]) -> str:
        if not desdobramentos:
            return "Nenhum desdobramento gerado."
        
        txt = [f"📊 RELATÓRIO v3.3 - DINÂMICO ORGÂNICO ({len(desdobramentos)} combinações)\n"]
        
        for i, d in enumerate(desdobramentos[:10]):
            legs = d.get('legs', [])
            perfil = d.get('perfil', 'BALANCEADO')
            
            txt.append(f"\n#{i+1} [{perfil}] Score: {d.get('score_final', d.get('score_ajustado', 0)):.2f}")
            txt.append(f"   Odd Total: {d.get('total_odd', 0):.2f} | Jogos Distintos: {d.get('composicao', {}).get('jogos_distintos', 0)}")
            
            risk_mix = d.get('composicao', {}).get('mix_riscos', {})
            if risk_mix:
                risk_str = " | ".join([f"{k}:{v}" for k, v in risk_mix.items()])
                txt.append(f"   Mix de Riscos: {risk_str}")
            
            for leg in legs:
                player = leg.get('player_name', 'Unknown')
                market = leg.get('market_display', '')
                risco = leg.get('risco', 'MEDIO')
                matchup = leg.get('matchup_score', 50)
                ceiling = leg.get('ceiling_ratio', 1.0)
                game_info = leg.get('game_info', {})
                opponent = game_info.get('opponent', 'Unknown')
                thesis = leg.get('thesis', 'N/A')
                
                risco_icon = "🟢" if risco == 'PISO' else "🟡" if risco == 'MEDIO' else "🔴"
                matchup_icon = "🔥" if matchup > 70 else "❄️" if matchup < 40 else "⚪"
                
                txt.append(f"   {risco_icon}{matchup_icon} {player} vs {opponent}: {market}")
                txt.append(f"        (Matchup: {matchup:.0f} | Ceiling: {ceiling:.2f}x | Tese: {thesis})")
        
        return "\n".join(txt)