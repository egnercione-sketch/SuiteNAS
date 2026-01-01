# modules/new_modules/narrative_formatter.py
"""
NarrativeFormatter - Formatação de narrativas estratégicas para trixies
Gera relatórios legíveis e insights para as recomendações do sistema
"""
from typing import Dict, List, Any, Optional
import json
import numpy as np
import pandas as pd
from datetime import datetime

class NarrativeFormatter:
    """Formatador de narrativas estratégicas para o sistema NAS"""
    
    def __init__(self):
        self.category_descriptions = self._get_category_descriptions()
        self.market_names = self._get_market_names()
    
    def _get_category_descriptions(self) -> Dict[str, str]:
        """Descrições das categorias estratégicas"""
        return {
            "conservadora": "Jogadores consistentes com baixa volatilidade e alta confiança. Foco em segurança e previsibilidade.",
            "ousada": "Jogadores com alto potencial explosivo e ceiling elevado. Maior risco, maior retorno potencial.",
            "banco": "Jogadores do banco com oportunidades aumentadas devido a lesões ou rotação. Valor escondido.",
            "explosao": "Foco em estatísticas explosivas (roubos, tocos, 3pts) com potencial para grandes performances pontuais."
        }
    
    def _get_market_names(self) -> Dict[str, str]:
        """Nomes amigáveis para os mercados"""
        return {
            "PTS": "Pontos",
            "REB": "Rebotes", 
            "AST": "Assistências",
            "PRA": "Pontos + Rebotes + Assistências",
            "STL": "Roubos de Bola",
            "BLK": "Tocos",
            "3PM": "Cestas de 3 Pontos",
            "MIN": "Minutos"
        }
    
    def format_recommendations(self, recommendations: Dict[str, Any], game_ctx: Dict[str, Any]) -> Dict[str, Any]:
        """Formata recomendações completas para exibição"""
        formatted = {}
        
        for category, data in recommendations.items():
            if not data.get("players"):
                continue
            
            formatted[category] = {
                "overview": self._generate_category_overview(category, data, game_ctx),
                "players": self._format_player_recommendations(data["players"], category, game_ctx),
                "recommendation_count": len(data["players"]),
                "confidence_score": data.get("confidence_score", 0),
                "risk_level": self._calculate_category_risk(category, data["players"])
            }
        
        return formatted
    
    def _generate_category_overview(self, category: str, data: Dict[str, Any], game_ctx: Dict[str, Any]) -> Dict[str, Any]:
        """Gera visão geral para uma categoria"""
        player_count = len(data.get("players", []))
        
        # Calcular confiança média
        confidences = []
        for p in data.get("players", []):
            conf = p.get("confidence")
            if conf is not None:
                confidences.append(conf)
        
        avg_confidence = np.mean(confidences) if confidences else 0
        
        # Contexto do jogo
        home_team = game_ctx.get("home_abbr", "HOME")
        away_team = game_ctx.get("away_abbr", "AWAY")
        total = game_ctx.get("total", 225)
        spread = game_ctx.get("spread", 0)
        
        # Gerar texto baseado na categoria
        if category == "conservadora":
            text = f"🎯 **ESTRATÉGIA CONSERVADORA**\n\n"
            text += f"Analisando {player_count} jogadores com perfil **seguro e consistente** para {away_team} @ {home_team}.\n\n"
            text += f"**Contexto do Jogo:** Total de {total} pontos, spread de {spread:+.1f} pontos.\n"
            text += f"**Confiança Média:** {avg_confidence:.1%}\n"
            text += f"**Foco:** Minimizar risco com jogadores de baixa volatilidade e alta disponibilidade."
        elif category == "ousada":
            text = f"🚀 **ESTRATÉGIA OUSADA**\n\n"
            text += f"Identificando {player_count} jogadores com **alto potencial explosivo** para {away_team} @ {home_team}.\n\n"
            text += f"**Contexto do Jogo:** Total de {total} pontos {'(ALTO)' if total >= 235 else ''}, spread de {spread:+.1f} pontos.\n"
            text += f"**Confiança Média:** {avg_confidence:.1%}\n"
            text += f"**Foco:** Maximizar retorno com jogadores que podem ter performances acima da média."
        elif category == "banco":
            text = f"🪑 **ESTRATÉGIA BANCO**\n\n"
            text += f"Descobrindo {player_count} jogadores do **banco com oportunidades** em {away_team} @ {home_team}.\n\n"
            text += f"**Contexto do Jogo:** Análise de rotação e oportunidades por lesões.\n"
            text += f"**Confiança Média:** {avg_confidence:.1%}\n"
            text += f"**Foco:** Valor escondido em jogadores subestimados com minutos aumentados."
        elif category == "explosao":
            text = f"💥 **ESTRATÉGIA EXPLOSÃO**\n\n"
            text += f"Focando em {player_count} jogadores com potencial para **stats explosivas** em {away_team} @ {home_team}.\n\n"
            text += f"**Contexto do Jogo:** Buscando jogos com ritmo acelerado e matchup favoráveis.\n"
            text += f"**Confiança Média:** {avg_confidence:.1%}\n"
            text += f"**Foco:** Roubos, tocos e cestas de 3 pontos com alto ceiling estatístico."
        else:
            text = f"📊 **ESTRATÉGIA {category.upper()}**\n\n"
            text += f"Analisando {player_count} jogadores para {away_team} @ {home_team}"
        
        return {
            "text": text,
            "category": category,
            "player_count": player_count,
            "avg_confidence": avg_confidence,
            "game_context": {
                "teams": f"{away_team} @ {home_team}",
                "total": total,
                "spread": spread
            }
        }
    
    def _format_player_recommendations(self, players: List[Dict[str, Any]], category: str, game_ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Formata recomendações individuais de jogadores"""
        formatted_players = []
        
        for player in players:
            formatted = {
                "name": player.get("name", "Unknown"),
                "team": player.get("team", "Unknown"),
                "position": player.get("position", "Unknown"),
                "confidence": player.get("confidence", 0) * 100,  # Converter para porcentagem
                "narrative": self._generate_player_narrative(player, category, game_ctx),
                "stats": self._format_player_stats(player),
                "primary_thesis": player.get("primary_thesis", "unknown"),
                "raw_data": player
            }
            formatted_players.append(formatted)
        
        return formatted_players
    
    def _generate_player_narrative(self, player: Dict[str, Any], category: str, game_ctx: Dict[str, Any]) -> str:
        """Gera narrativa individual para um jogador"""
        name = player.get("name", "Jogador")
        team = player.get("team", "Time")
        position = player.get("position", "Pos")
        confidence = player.get("confidence", 0)
        primary_thesis = player.get("primary_thesis", "unknown")
        
        # Mercado(s)
        market = player.get("market", "PRA")
        if isinstance(market, list):
            market_str = ", ".join(market)
        else:
            market_str = market
        
        # Estatísticas relevantes (adaptado para mercados)
        pts = player.get("pts_L5", player.get("pts_avg", 0))
        reb = player.get("reb_L5", player.get("reb_avg", 0))
        ast = player.get("ast_L5", player.get("ast_avg", 0))
        min_avg = player.get("min_L5", player.get("min_avg", 0))
        
        # Contexto do jogador
        is_starter = player.get("is_starter", False)
        team_injuries = player.get("team_injuries", 0)
        is_b2b = player.get("is_b2b", False)
        
        # Gerar narrativa baseada na categoria
        if category == "conservadora":
            narrative = f"✅ **{name} ({position} - {team})** é uma escolha **segura** com {confidence:.0%} de confiança.\n\n"
            narrative += f"**Perfil:** {'Titular' if is_starter else 'Reserva'} com {min_avg:.1f} minutos em média nos últimos 5 jogos.\n"
            narrative += f"**Estatísticas L5:** {pts:.1f} PTS, {reb:.1f} REB, {ast:.1f} AST\n"
            narrative += f"**Mercado:** {market_str}\n"
            narrative += f"**Tese Principal:** {self._format_thesis_name(primary_thesis)}\n"
            
            if player.get("min_cv", 1.0) < 0.3:
                narrative += f"📊 **Consistência:** Baixa variação de minutos ({player.get('min_cv', 0):.2f}), sinal de rotação estável.\n"
            
            if team_injuries > 0:
                narrative += f"📈 **Oportunidade:** {team_injuries} jogador(es) lesionado(s) no time, potencial para minutos seguros.\n"
        
        elif category == "ousada":
            narrative = f"🔥 **{name} ({position} - {team})** tem **ALTO POTENCIAL** com {confidence:.0%} de confiança.\n\n"
            narrative += f"**Perfil:** {'Titular' if is_starter else 'Reserva'} buscando performance acima da média.\n"
            narrative += f"**Estatísticas L5:** {pts:.1f} PTS, {reb:.1f} REB, {ast:.1f} AST\n"
            narrative += f"**Mercado:** {market_str}\n"
            narrative += f"**Tese Principal:** {self._format_thesis_name(primary_thesis)}\n"
            
            # Ceiling potencial
            if "proj_ceil_95_pra" in player:
                ceiling = player["proj_ceil_95_pra"]
                upside = (ceiling / (pts + reb + ast)) if (pts + reb + ast) > 0 else 0
                narrative += f"🚀 **Ceiling 95%:** {ceiling:.1f} ({upside:.1f}x a média L5)\n"
            
            if player.get("usage_spike", False):
                narrative += f"📈 **Uso Aumentado:** Detectado aumento significativo no uso ofensivo recentemente.\n"
        
        elif category == "banco":
            narrative = f"🔍 **{name} ({position} - {team})** é um **TESOURO ESCONDIDO** com {confidence:.0%} de confiança.\n\n"
            narrative += f"**Perfil:** {'Reserva' if not is_starter else 'Titular'} com oportunidade incomum.\n"
            narrative += f"**Estatísticas L5:** {pts:.1f} PTS, {reb:.1f} REB, {ast:.1f} AST\n"
            narrative += f"**Mercado:** {market_str}\n"
            narrative += f"**Tese Principal:** {self._format_thesis_name(primary_thesis)}\n"
            
            if team_injuries >= 1:
                narrative += f"⚡ **Oportunidade por Lesões:** {team_injuries} titular(es) lesionado(s), minutos aumentados esperados.\n"
            
            if not is_starter and min_avg >= 20:
                narrative += f"💡 **Surpresa:** Reserva jogando como titular com {min_avg:.1f} minutos recentes.\n"
        
        elif category == "explosao":
            narrative = f"💣 **{name} ({position} - {team})** tem potencial para **EXPLOSÃO** com {confidence:.0%} de confiança.\n\n"
            narrative += f"**Perfil:** Especialista em estatísticas raras e de alto impacto.\n"
            narrative += f"**Estatísticas L5:** {pts:.1f} PTS, {reb:.1f} REB, {ast:.1f} AST\n"
            narrative += f"**Mercado:** {market_str}\n"
            narrative += f"**Tese Principal:** {self._format_thesis_name(primary_thesis)}\n"
            
            # Stats explosivas
            stl = player.get("stl_L5", 0)
            blk = player.get("blk_L5", 0)
            threes = player.get("3pm_L5", 0)
            
            explosive_stats = []
            if stl >= 1.0:
                explosive_stats.append(f"{stl:.1f} STL")
            if blk >= 1.0:
                explosive_stats.append(f"{blk:.1f} BLK")
            if threes >= 2.0:
                explosive_stats.append(f"{threes:.1f} 3PM")
            
            if explosive_stats:
                narrative += f"✨ **Stats Explosivas:** {', '.join(explosive_stats)}\n"
            
            if game_ctx.get("is_high_pace", False):
                narrative += f"⚡ **Contexto Favorável:** Jogo de ritmo acelerado ideal para stats explosivas.\n"
        
        else:
            narrative = f"⭐ **{name} ({position} - {team})** - Confiança: {confidence:.0%}\n"
            narrative += f"Estatísticas: {pts:.1f} PTS, {reb:.1f} REB, {ast:.1f} AST\n"
            narrative += f"Tese: {self._format_thesis_name(primary_thesis)}"
        
        # Adicionar fatores de contexto universais
        if is_b2b:
            narrative += f"\n⚠️ **Atenção:** Jogo back-to-back, pode afetar performance de veteranos.\n"
        
        if player.get("is_underdog", False):
            narrative += f"\n🎯 **Motivação Extra:** Jogando como azarão, pode ter performance motivada.\n"
        
        return narrative
    
    def _format_thesis_name(self, thesis_name: str) -> str:
        """Formata nomes de teses para exibição amigável"""
        thesis_map = {
            "MinutesSafe": "Minutos Seguros",
            "LowVariance": "Baixa Variação",
            "SynergyRebAst": "Sinergia Rebotes/Assistências",
            "TripleThreat": "Ameaça Tripla (PTS+REB+AST)",
            "ReboundMatchup": "Matchup Favorável para Rebotes",
            "AssistMatchup": "Matchup Favorável para Assistências",
            "DVPPointsMatchup": "Defesa Fraca para Pontos",
            "DVPReboundMatchup": "Defesa Fraca para Rebotes",
            "DVPAssistMatchup": "Defesa Fraca para Assistências",
            "UsageSpike": "Aumento de Uso Ofensivo",
            "BenchMonster": "Monstro do Banco",
            "RookieBlindado": "Rookie Blindado",
            "PivoSobrevivente": "Pivô Sobrevivente",
            "HiddenReboundValue": "Valor Escondido em Rebotes",
            "HiddenAssistValue": "Valor Escondido em Assistências",
            "StealPotential": "Potencial para Roubos",
            "BlockPotential": "Potencial para Tocos",
            "ThreePointPotential": "Potencial para 3 Pontos",
            "CeilingExplosion": "Teto Estatístico Explosivo"
        }
        return thesis_map.get(thesis_name, thesis_name.replace("_", " ").title())
    
    def _format_player_stats(self, player: Dict[str, Any]) -> Dict[str, float]:
        """Formata estatísticas do jogador para exibição"""
        return {
            "pts_avg": round(player.get("pts_L5", player.get("pts_avg", 0)), 1),
            "reb_avg": round(player.get("reb_L5", player.get("reb_avg", 0)), 1), 
            "ast_avg": round(player.get("ast_L5", player.get("ast_avg", 0)), 1),
            "pra_avg": round(player.get("pra_L5", player.get("pra_avg", 0)), 1),
            "min_avg": round(player.get("min_L5", player.get("min_avg", 0)), 1),
            "stl_avg": round(player.get("stl_L5", player.get("stl_avg", 0)), 1),
            "blk_avg": round(player.get("blk_L5", player.get("blk_avg", 0)), 1),
            "3pm_avg": round(player.get("3pm_L5", player.get("3pm_avg", 0)), 1)
        }
    
    def _calculate_category_risk(self, category: str, players: List[Dict[str, Any]]) -> str:
        """Calcula nível de risco da categoria"""
        if category == "conservadora":
            return "low"
        elif category == "ousada":
            return "high"
        elif category == "banco":
            # Risco médio, mas pode variar
            confidences = [p.get("confidence", 0) for p in players if p.get("confidence") is not None]
            avg_confidence = np.mean(confidences) if confidences else 0
            return "medium-low" if avg_confidence >= 0.7 else "medium-high"
        elif category == "explosao":
            return "very-high"
        return "medium"
    
    def generate_compact_table(self, category: str, players: List[Dict[str, Any]]) -> pd.DataFrame:
        """Gera tabela compacta para exibição"""
        if not players:
            return pd.DataFrame()
        
        table_data = []
        for player in players:
            # Garantir que confidence está em formato decimal (0-1)
            confidence = player.get("confidence", 0)
            if confidence > 1:  # Se estiver em porcentagem (0-100), converter para decimal
                confidence = confidence / 100
            
            row = {
                "Jogador": player.get("name", ""),
                "Time": player.get("team", ""),
                "Pos": player.get("position", ""),
                "Conf": f"{confidence:.1%}",
                "PRA L5": f"{player.get('pra_L5', 0):.1f}",
                "MIN L5": f"{player.get('min_L5', 0):.1f}",
                "Tese": self._format_thesis_name(player.get("primary_thesis", ""))
            }
            table_data.append(row)
        
        return pd.DataFrame(table_data)
    
    def format_multipla_narrative(self, multipla_data: Dict[str, Any], game_ctx: Dict[str, Any]) -> Dict[str, str]:
        """Formata narrativa para a Múltipla do Dia"""
        conservadora = multipla_data.get("conservadora", [])
        ousada = multipla_data.get("ousada", [])
        
        narrative = {}
        
        if conservadora:
            narrative["conservadora"] = self._format_multipla_category(conservadora, "conservadora", game_ctx)
        if ousada:
            narrative["ousada"] = self._format_multipla_category(ousada, "ousada", game_ctx)
        
        return narrative
    
    def _format_multipla_category(self, players: List[Dict[str, Any]], category: str, game_ctx: Dict[str, Any]) -> str:
        """Formata narrativa para uma categoria da Múltipla do Dia"""
        if not players:
            return ""
        
        header = "🎯 **MÚLTIPLA DO DIA - VERSÃO CONSERVADORA**" if category == "conservadora" else "🚀 **MÚLTIPLA DO DIA - VERSÃO OUSADA**"
        
        text = f"{header}\n\n"
        text += f"**Estratégia:** {'Segurança e consistência' if category == 'conservadora' else 'Alto potencial e ceiling'}\n"
        text += f"**Número de Jogadores:** {len(players)}\n\n"
        
        for i, player in enumerate(players, 1):
            name = player.get("name", "Jogador")
            team = player.get("team", "Time")
            position = player.get("position", "Pos")
            confidence = player.get("confidence", 0)
            primary_thesis = player.get("primary_thesis", "unknown")
            
            text += f"{i}. **{name}** ({position} - {team})\n"
            text += f"   • Confiança: {confidence:.0%}\n"
            text += f"   • Tese Principal: {self._format_thesis_name(primary_thesis)}\n"
            
            stats = player.get("stats", {})
            if stats:
                pra = stats.get("pra_avg", stats.get("pra_L5", 0))
                text += f"   • PRA Esperado: {pra:.1f}\n"
            
            text += "\n"
        
        # Resumo estratégico
        text += "**🎯 Estratégia Resumida:**\n"
        if category == "conservadora":
            text += "• Foco em jogadores com alta disponibilidade e baixa volatilidade\n"
            text += "• Prioriza minutos consistentes e produção estável\n"
            text += "• Ideal para apostas de baixo risco e alta confiança"
        else:
            text += "• Busca jogadores com alto ceiling estatístico\n"
            text += "• Aproveita matchups favoráveis e oportunidades por lesões\n"
            text += "• Ideal para apostas de alto retorno com maior risco"
        
        return text