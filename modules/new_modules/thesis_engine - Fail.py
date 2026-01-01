# modules/new_modules/thesis_engine_fixed.py

import logging
import numpy as np

logger = logging.getLogger("ThesisEngine_Fixed")

class ThesisEngine:
    """
    Motor de Teses V2.2 (Corrigido e Melhorado)
    Gera teses de investimento com cálculo de confiança robusto.
    CORREÇÕES: Garante que NUNCA retorne tese "Unknown"
    """

    def __init__(self):
        self.tesis_templates = {
            "VolumeScorer": {
                "description": "Scorer consistente com volume garantido",
                "min_pts": 10.0,
                "market": "PTS"
            },
            "GlassCleaner": {
                "description": "Dominante nos rebotes com matchup favorável",
                "min_reb": 6.0,
                "market": "REB"
            },
            "Playmaker": {
                "description": "Criador de jogadas com ritmo acelerado",
                "min_ast": 5.0,
                "market": "AST"
            },
            "DefensiveAnchor": {
                "description": "Especialista defensivo com atividade alta",
                "min_stocks": 1.5,
                "market": ["STL", "BLK"]
            },
            "Sniper": {
                "description": "Especialista em bolas de 3 com volume",
                "min_3pm": 2.5,
                "market": "3PM"
            },
            "SafeMinutes": {
                "description": "Minutagem segura e produção consistente",
                "min_minutes": 26.0,
                "market": "PTS"
            },
            "MatchupExploit": {
                "description": "Exploração de matchup favorável específico",
                "requires_matchup": True,
                "market": ["PTS", "REB", "AST"]
            },
            "VacuumBoost": {
                "description": "Oportunidade aumentada por lesões no time",
                "requires_vacuum": True,
                "market": ["PTS", "REB", "AST"]
            },
            "CeilingUpside": {
                "description": "Alto teto estatístico em contexto favorável",
                "requires_ceiling": True,
                "market": ["PTS", "PRA"]
            }
        }

    def generate_theses(self, player, context_data):
        """
        Gera TODAS as teses possíveis para um jogador.
        GARANTE: Sempre retorna pelo menos uma tese válida.
        """
        theses = []
        
        # 1. EXTRAÇÃO SEGURA DE DADOS
        try:
            stats = player.get('stats', {})
            if not stats:
                # Fallback: usar estrutura raw
                stats = {
                    'min_L5': player.get('min_l5', player.get('minutes', 0)),
                    'pts_L5': player.get('pts_l5', player.get('points', 0)),
                    'reb_L5': player.get('reb_l5', player.get('rebounds', 0)),
                    'ast_L5': player.get('ast_l5', player.get('assists', 0)),
                    'stl_L5': player.get('stl_l5', player.get('steals', 0)),
                    'blk_L5': player.get('blk_l5', player.get('blocks', 0)),
                    '3pm_L5': player.get('tpm_l5', player.get('threes_made', 0))
                }
            
            # Valores com fallback seguro
            min_l5 = float(stats.get('min_L5', 0)) or float(stats.get('minutes', 0)) or 0
            pts_l5 = float(stats.get('pts_L5', 0)) or float(stats.get('points', 0)) or 0
            reb_l5 = float(stats.get('reb_L5', 0)) or float(stats.get('rebounds', 0)) or 0
            ast_l5 = float(stats.get('ast_L5', 0)) or float(stats.get('assists', 0)) or 0
            stl_l5 = float(stats.get('stl_L5', 0)) or float(stats.get('steals', 0)) or 0
            blk_l5 = float(stats.get('blk_L5', 0)) or float(stats.get('blocks', 0)) or 0
            tpm_l5 = float(stats.get('3pm_L5', 0)) or float(stats.get('threes_made', 0)) or 0
            
            # Fatores de contexto
            pace_factor = context_data.get('pace_factor', 1.0)
            dvp_score = context_data.get('dvp_score', 1.0)
            vacuum_active = context_data.get('vacuum_boost', False)
            is_high_pace = context_data.get('is_high_pace', False)
            
        except Exception as e:
            logger.error(f"Erro extraindo dados do jogador: {e}")
            # Dados mínimos de fallback
            min_l5, pts_l5, reb_l5, ast_l5, stl_l5, blk_l5, tpm_l5 = 0, 0, 0, 0, 0, 0, 0
            pace_factor, dvp_score, vacuum_active, is_high_pace = 1.0, 1.0, False, False
        
        # ==============================================================================
        # 2. GERAÇÃO DE TESES ESPECÍFICAS
        # ==============================================================================
        
        # --- TESES PARA CATEGORIA CONSERVADORA ---
        
        # Tese 1: Safe Minutes (Conservadora)
        if min_l5 >= 26.0 and pts_l5 >= 8.0:
            conf = 0.60
            if min_l5 >= 30: conf += 0.10
            if dvp_score > 1.05: conf += 0.08
            if vacuum_active: conf += 0.05
            
            theses.append({
                "type": "SafeMinutes",
                "market": "PTS",
                "category": "conservadora",
                "confidence": min(0.95, conf),
                "reason": f"Minutagem segura ({min_l5:.0f} min/jogo) com produção consistente",
                "line": max(0.5, pts_l5 * 0.85)
            })
        
        # Tese 2: Volume Scorer (Conservadora/Ousada)
        if pts_l5 >= 12.0:
            conf = 0.55
            if min_l5 >= 28: conf += 0.10
            if dvp_score > 1.08: conf += 0.12
            if pace_factor > 1.03: conf += 0.07
            if vacuum_active: conf += 0.08
            
            category = "ousada"
            if conf >= 0.70: category = "conservadora"
            
            theses.append({
                "type": "VolumeScorer",
                "market": "PTS",
                "category": category,
                "confidence": min(0.95, conf),
                "reason": f"Scorer volume ({pts_l5:.1f} PTS L5) em contexto favorável",
                "line": max(0.5, pts_l5 * 0.9)
            })
        
        # Tese 3: Glass Cleaner (Conservadora - AGORA PERMITIDO)
        if reb_l5 >= 7.0:
            conf = 0.58
            if reb_l5 >= 9.0: conf += 0.10
            if dvp_score > 1.05: conf += 0.08
            if min_l5 >= 25: conf += 0.05
            
            # Rebotes altos podem ser conservadores
            category = "conservadora" if reb_l5 >= 8.0 and conf >= 0.65 else "ousada"
            
            theses.append({
                "type": "GlassCleaner",
                "market": "REB",
                "category": category,
                "confidence": min(0.95, conf),
                "reason": f"Dominante no garrafão ({reb_l5:.1f} REB L5)",
                "line": max(0.5, reb_l5 * 0.9)
            })
        
        # Tese 4: Playmaker (Conservadora - AGORA PERMITIDO)
        if ast_l5 >= 5.0:
            conf = 0.56
            if ast_l5 >= 7.0: conf += 0.12
            if pace_factor > 1.05: conf += 0.10
            if is_high_pace: conf += 0.05
            
            category = "conservadora" if ast_l5 >= 6.5 and conf >= 0.68 else "ousada"
            
            theses.append({
                "type": "Playmaker",
                "market": "AST",
                "category": category,
                "confidence": min(0.95, conf),
                "reason": f"Facilitador primário ({ast_l5:.1f} AST L5) em ritmo {('acelerado' if pace_factor > 1.05 else 'normal')}",
                "line": max(0.5, ast_l5 * 0.9)
            })
        
        # --- TESES PARA CATEGORIA OUSADA ---
        
        # Tese 5: Matchup Exploit (Ousada)
        if dvp_score > 1.10 and (pts_l5 >= 15 or reb_l5 >= 8 or ast_l5 >= 6):
            conf = 0.65
            best_market = "PTS"
            best_value = pts_l5
            
            if reb_l5 * 1.3 > pts_l5:  # Rebotes tem multiplier
                best_market = "REB"
                best_value = reb_l5
                conf += 0.05
            
            if ast_l5 * 1.4 > best_value:
                best_market = "AST"
                best_value = ast_l5
                conf += 0.05
            
            theses.append({
                "type": "MatchupExploit",
                "market": best_market,
                "category": "ousada",
                "confidence": min(0.92, conf),
                "reason": f"Exploração de matchup favorável (DvP: {dvp_score:.2f}x)",
                "line": max(0.5, best_value * 0.95)
            })
        
        # Tese 6: Ceiling Upside (Ousada/Explosão)
        if (pts_l5 >= 18 or reb_l5 >= 10 or ast_l5 >= 8) and pace_factor > 1.05:
            conf = 0.62
            if vacuum_active: conf += 0.08
            
            theses.append({
                "type": "CeilingUpside",
                "market": "PTS" if pts_l5 >= 18 else ("REB" if reb_l5 >= 10 else "AST"),
                "category": "ousada",
                "confidence": min(0.90, conf),
                "reason": f"Alto teto em jogo de ritmo acelerado (Pace: {pace_factor:.2f}x)",
                "line": max(0.5, max(pts_l5, reb_l5, ast_l5) * 0.98)
            })
        
        # --- TESES PARA CATEGORIA VERSÁTIL ---
        
        # Tese 7: Sniper (Versátil)
        if tpm_l5 >= 2.5:
            conf = 0.58
            if tpm_l5 >= 3.5: conf += 0.10
            if dvp_score > 1.08: conf += 0.07
            
            theses.append({
                "type": "Sniper",
                "market": "3PM",
                "category": "versatil",
                "confidence": min(0.88, conf),
                "reason": f"Volume consistente de 3 pontos ({tpm_l5:.1f} 3PM L5)",
                "line": max(0.5, tpm_l5 * 0.85)
            })
        
        # Tese 8: Defensive Anchor (Versátil)
        stocks = stl_l5 + blk_l5
        if stocks >= 1.8:
            conf = 0.55 + (stocks * 0.08)
            target_market = "STL" if stl_l5 > blk_l5 else "BLK"
            
            theses.append({
                "type": "DefensiveAnchor",
                "market": target_market,
                "category": "versatil",
                "confidence": min(0.85, conf),
                "reason": f"Atividade defensiva alta ({stocks:.1f} stocks L5)",
                "line": 1.5 if stocks >= 2.0 else 1.0
            })
        
        # Tese 9: Vacuum Boost (Qualquer categoria)
        if vacuum_active:
            conf = 0.63
            # Determinar melhor mercado baseado em stats
            if pts_l5 >= reb_l5 and pts_l5 >= ast_l5:
                market = "PTS"
                value = pts_l5
            elif reb_l5 >= pts_l5 and reb_l5 >= ast_l5:
                market = "REB"
                value = reb_l5
            else:
                market = "AST"
                value = ast_l5
            
            theses.append({
                "type": "VacuumBoost",
                "market": market,
                "category": "ousada",  # Vacuum é mais ousado por natureza
                "confidence": min(0.90, conf),
                "reason": "Oportunidade aumentada por ausência de jogadores-chave",
                "line": max(0.5, value * 0.92)
            })
        
        # ==============================================================================
        # 3. REDE DE SEGURANÇA ABSOLUTA (GARANTE PELO MENOS UMA TESES)
        # ==============================================================================
        
        if not theses:
            logger.warning(f"Nenhuma tese gerada para jogador. Criando fallback básico.")
            
            # Fallback 1: Baseado no melhor stat
            best_stat = max(pts_l5, reb_l5, ast_l5)
            if best_stat == pts_l5 and pts_l5 > 0:
                market = "PTS"
                reason = f"Produção básica de pontos ({pts_l5:.1f} PTS L5)"
            elif best_stat == reb_l5 and reb_l5 > 0:
                market = "REB"
                reason = f"Contribuição em rebotes ({reb_l5:.1f} REB L5)"
            elif best_stat == ast_l5 and ast_l5 > 0:
                market = "AST"
                reason = f"Distribuição básica ({ast_l5:.1f} AST L5)"
            else:
                market = "PTS"
                reason = "Análise técnica padrão"
                best_stat = 8.0  # Valor mínimo
            
            theses.append({
                "type": "SafeMinutes" if min_l5 >= 20 else "VolumeScorer",
                "market": market,
                "category": "conservadora" if min_l5 >= 22 else "ousada",
                "confidence": 0.55,  # Confiança mínima
                "reason": reason,
                "line": max(0.5, best_stat * 0.8)
            })
        
        # ==============================================================================
        # 4. SANITIZAÇÃO FINAL (CORREÇÃO DE BUGS CRÍTICOS)
        # ==============================================================================
        
        final_theses = []
        for t in theses:
            # 1. Garantir tipo não vazio
            if not t.get('type') or t['type'] == 'Unknown':
                t['type'] = 'AnaliseTecnica'
            
            # 2. Garantir confiança válida
            t['confidence'] = max(0.3, min(0.95, t.get('confidence', 0.55)))
            
            # 3. Garantir razão não vazia
            if not t.get('reason'):
                t['reason'] = f"Tendência favorável em {t.get('market', 'PTS')}"
            
            # 4. Garantir linha válida
            t['line'] = max(0.5, t.get('line', 1.0))
            
            # 5. Garantir categoria válida
            valid_categories = ['conservadora', 'ousada', 'banco', 'explosao', 'versatil']
            if t.get('category') not in valid_categories:
                # Determinar categoria baseado em confiança
                t['category'] = 'conservadora' if t['confidence'] >= 0.65 else 'ousada'
            
            final_theses.append(t)
        
        # Ordenar por confiança e garantir unicidade por tipo
        seen_types = set()
        unique_theses = []
        for t in sorted(final_theses, key=lambda x: x['confidence'], reverse=True):
            if t['type'] not in seen_types:
                seen_types.add(t['type'])
                unique_theses.append(t)
        
        logger.debug(f"✅ Geradas {len(unique_theses)} teses para jogador")
        return unique_theses

    def get_thesis_for_category(self, theses_list, target_category):
        """Filtra teses por categoria específica"""
        if not theses_list:
            return None
        
        # Primeiro: teses exatas da categoria
        category_theses = [t for t in theses_list if t['category'] == target_category]
        if category_theses:
            return max(category_theses, key=lambda x: x['confidence'])
        
        # Fallback: teses de categoria similar
        category_map = {
            'conservadora': ['ousada', 'banco'],
            'ousada': ['explosao', 'conservadora'],
            'explosao': ['ousada', 'versatil'],
            'versatil': ['explosao', 'ousada'],
            'banco': ['conservadora', 'ousada']
        }
        
        for similar_cat in category_map.get(target_category, []):
            similar_theses = [t for t in theses_list if t['category'] == similar_cat]
            if similar_theses:
                return max(similar_theses, key=lambda x: x['confidence'])
        
        # Último fallback: qualquer tese
        return max(theses_list, key=lambda x: x['confidence'])

    def format_thesis_for_display(self, thesis):
        """Formata tese para exibição amigável"""
        if not thesis:
            return "Análise Técnica Favorável"
        
        type_map = {
            "VolumeScorer": "Scorer Volume",
            "GlassCleaner": "Dominante Rebotes",
            "Playmaker": "Playmaker Criativo",
            "DefensiveAnchor": "Ancora Defensivo",
            "Sniper": "Especialista 3 Pontos",
            "SafeMinutes": "Minutos Seguros",
            "MatchupExploit": "Matchup Favorável",
            "VacuumBoost": "Oportunidade Vacuum",
            "CeilingUpside": "Alto Teto",
            "AnaliseTecnica": "Análise Técnica"
        }
        
        display_type = type_map.get(thesis['type'], thesis['type'])
        confidence_str = f"{thesis['confidence']:.0%}"
        
        return f"{display_type} ({confidence_str}): {thesis['reason']}"