"""
Gerador de Narrativas Inteligentes para Trixies
"""
import random
from datetime import datetime
import os
import json

class NarrativeGenerator:
    def __init__(self, cache_dir="cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        # Templates de narrativas
        self.narrative_templates = {
            "THE_BATTERY": [
                "A dupla dinâmica {player1} (criador) e {player2} (finalizador) deve explorar a defesa fraca do {opponent_team}.",
                "A química entre {player1} e {player2} será crucial contra a defesa permissiva do {opponent_team}.",
                "A conexão {player1} → {player2} tem histórico de sucesso contra defesas como a do {opponent_team}."
            ],
            "SHOOTOUT_PAIR": [
                "Jogo de ritmo alto ({pace}) favorece os scorers {player1} e {player2} em um duelo ofensivo.",
                "Com total projetado de {total}, {player1} e {player2} devem se beneficiar do volume de posses.",
                "Ambiente de shootout perfeito para {player1} e {player2} mostrarem seu volume ofensivo."
            ],
            "GLASS_BANGERS_TRIO": [
                "Domínio no garrafão: {player1}, {player2} e {player3} contra time que permite rebotes.",
                "A física no garrafão será decisiva com essa triade de reboteadores contra {opponent_team}.",
                "Segundas chances garantidas com essa combinação de reboteadores contra defesa frágil no garrafão."
            ],
            "BLOWOUT_SPECIAL": [
                "Spread alto ({spread}) indica garbage time para {players} mostrarem valor.",
                "Potencial blowout abre espaço para reservas como {players} terem minutos extras.",
                "Com jogo tendendo a definir cedo, {players} terão oportunidade no garbage time."
            ],
            "DVPA_MATCHUP": [
                "Matchup favorável: {player} contra defesa rank #{rank} em {stat_category}.",
                "{player} explora defesa fraca do {opponent_team} que permite {stat_value} {stat_category}.",
                "Hole na defesa do {opponent_team} será explorado por {player} no {stat_category}."
            ],
            "GENERIC": [
                "Combinação sólida baseada em matchups favoráveis e consistência recente.",
                "Trixie construída sobre fundamentos sólidos de minutos e matchups.",
                "Seleção balanceada considerando ritmo, defesas e volume individual."
            ]
        }
        
        # Insights contextuais
        self.context_insights = {
            "PACE_ALTO": ["Ritmo acelerado", "Muitas posses", "Volume ofensivo garantido"],
            "PACE_BAIXO": ["Jogo travado", "Rebotes importantes", "Eficiência valorizada"],
            "TOTAL_ALTO": ["Shootout esperado", "Volume de pontos", "Ambiente ofensivo"],
            "TOTAL_BAIXO": ["Defesa prevalente", "Jogo baixo", "Rebotes decisivos"],
            "BLOWOUT_RISK": ["Garbage time potencial", "Rotação ampliada", "Reservas com upside"],
            "B2B_HOME": ["Time descansado", "Vantagem física", "Performance consistente"],
            "B2B_AWAY": ["Pernas cansadas", "Defesa comprometida", "Ritmo mais lento"]
        }
    
    def generate_narrative(self, players, game_ctx, strategy="GENERIC"):
        """Gera narrativa personalizada para uma trixie"""
        
        # Coletar informações básicas
        player_names = [p.get("name", "") for p in players]
        teams = list(set([p.get("team", "") for p in players]))
        opponent_teams = list(set([p.get("opponent", "") for p in players]))
        
        # Contexto do jogo
        pace = game_ctx.get("pace_expected", 100)
        total = game_ctx.get("total", 220)
        spread = game_ctx.get("spread_abs", 0)
        
        # Selecionar template baseado na estratégia
        template_key = strategy if strategy in self.narrative_templates else "GENERIC"
        template = random.choice(self.narrative_templates[template_key])
        
        # Substituir placeholders
        narrative = template
        
        # Substituir players
        for i, name in enumerate(player_names):
            narrative = narrative.replace(f"{{player{i+1}}}", name)
        
        # Substituir lista de players
        if "{players}" in narrative:
            players_str = ", ".join(player_names)
            narrative = narrative.replace("{players}", players_str)
        
        # Substituir informações do jogo
        narrative = narrative.replace("{pace}", f"{pace:.1f}")
        narrative = narrative.replace("{total}", str(total))
        narrative = narrative.replace("{spread}", f"{spread:.1f}")
        
        # Adicionar informações de times
        if teams and "{team}" in narrative:
            narrative = narrative.replace("{team}", teams[0])
        
        if opponent_teams and "{opponent_team}" in narrative:
            narrative = narrative.replace("{opponent_team}", opponent_teams[0])
        
        # Adicionar informações específicas de DvP se disponível
        dvp_info = self._extract_dvp_info(players)
        if dvp_info and "{stat_category}" in narrative:
            narrative = narrative.replace("{stat_category}", dvp_info["stat"])
            narrative = narrative.replace("{stat_value}", str(dvp_info["value"]))
            if "{rank}" in narrative:
                narrative = narrative.replace("{rank}", str(dvp_info["rank"]))
        
        # Adicionar insights contextuais
        context_notes = self._get_context_notes(game_ctx)
        if context_notes:
            narrative += " " + context_notes
        
        return narrative
    
    def _extract_dvp_info(self, players):
        """Extrai informações de DvP relevantes dos jogadores"""
        for player in players:
            dvp_data = player.get("dvp_data", {})
            if dvp_data:
                rankings = dvp_data.get("rankings", {})
                
                # Encontrar melhor matchup
                for stat, rank_info in rankings.items():
                    rank = rank_info.get("rank", 15)
                    if rank <= 10:  # Matchup favorável
                        return {
                            "stat": stat,
                            "rank": rank,
                            "value": rank_info.get("value", "alto"),
                            "player": player.get("name")
                        }
        
        return None
    
    def _get_context_notes(self, game_ctx):
        """Gera notas contextuais baseadas no jogo"""
        notes = []
        
        script_type = game_ctx.get("script_type", "")
        
        # Verificar ritmo
        if "PACE_ALTO" in script_type:
            notes.append(random.choice(self.context_insights["PACE_ALTO"]))
        elif "PACE_BAIXO" in script_type:
            notes.append(random.choice(self.context_insights["PACE_BAIXO"]))
        
        # Verificar total
        if "TOTAL_ALTO" in script_type:
            notes.append(random.choice(self.context_insights["TOTAL_ALTO"]))
        elif "TOTAL_BAIXO" in script_type:
            notes.append(random.choice(self.context_insights["TOTAL_BAIXO"]))
        
        # Verificar blowout risk
        if "BLOWOUT" in script_type:
            notes.append(random.choice(self.context_insights["BLOWOUT_RISK"]))
        
        # Verificar B2B
        b2b_info = game_ctx.get("b2b_info", {})
        if b2b_info.get("home_b2b"):
            notes.append(random.choice(self.context_insights["B2B_HOME"]))
        elif b2b_info.get("away_b2b"):
            notes.append(random.choice(self.context_insights["B2B_AWAY"]))
        
        if notes:
            return "Contexto: " + ", ".join(notes) + "."
        
        return ""
    
    def generate_player_insight(self, player_ctx):
        """Gera insight individual para um jogador"""
        insights = []
        
        # Archetypes
        archetypes = player_ctx.get("archetypes", [])
        if archetypes:
            insights.append(f"Perfil: {', '.join(archetypes[:2])}")
        
        # DvP advantage
        dvp_data = player_ctx.get("dvp_data", {})
        if dvp_data:
            overall = dvp_data.get("overall", 1.0)
            if overall > 1.05:
                insights.append(f"Matchup favorável ({overall:.2f}x)")
            elif overall < 0.95:
                insights.append(f"Matchup desafiador ({overall:.2f}x)")
        
        # Momentum
        momentum = player_ctx.get("momentum_score", 50)
        if momentum > 70:
            insights.append("Momento positivo")
        elif momentum < 40:
            insights.append("Momento difícil")
        
        # Probabilidade de teto
        prob_90 = player_ctx.get("prob_90p_pts", 50)
        if prob_90 > 65:
            insights.append(f"Alta probabilidade de teto ({prob_90}%)")
        
        if insights:
            return " | ".join(insights)
        
        return "Perfil consistente"