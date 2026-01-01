"""
Identificador de Estratégias para Trixies
"""
from itertools import combinations

class StrategyIdentifier:
    def __init__(self):
        self.strategies = {
            "THE_BATTERY": {
                "description": "Armador + Finalizador do mesmo time",
                "min_players": 2,
                "conditions": self._check_battery
            },
            "SHOOTOUT_PAIR": {
                "description": "Scorers de times opostos em jogo high-total",
                "min_players": 2,
                "conditions": self._check_shootout_pair
            },
            "GLASS_BANGERS_TRIO": {
                "description": "Reboteadores dominantes em matchup favorável",
                "min_players": 2,
                "conditions": self._check_glass_bangers
            },
            "FLOOR_GENERALS_DUO": {
                "description": "Dois armadores criadores",
                "min_players": 2,
                "conditions": self._check_floor_generals
            },
            "DVPA_EXPLOIT": {
                "description": "Exploração múltipla de matchups DvP",
                "min_players": 2,
                "conditions": self._check_dvpa_exploit
            },
            "BLOWOUT_SPECIAL": {
                "description": "Foco em garbage time e reservas",
                "min_players": 2,
                "conditions": self._check_blowout_special
            },
            "VALUE_HUNTER": {
                "description": "Jogadores subvalorizados com upside",
                "min_players": 3,
                "conditions": self._check_value_hunter
            },
            "SAFE_PLAY": {
                "description": "Combinação de baixa volatilidade",
                "min_players": 3,
                "conditions": self._check_safe_play
            }
        }
    
    def identify_strategy(self, players):
        """Identifica a estratégia principal de uma trixie"""
        strategies_found = []
        
        # Verificar todas as estratégias
        for strategy_name, strategy_info in self.strategies.items():
            if len(players) >= strategy_info["min_players"]:
                if strategy_info["conditions"](players):
                    strategies_found.append(strategy_name)
        
        # Retornar a estratégia principal (prioridade)
        if strategies_found:
            # Priorizar estratégias específicas
            priority_order = [
                "THE_BATTERY",
                "SHOOTOUT_PAIR", 
                "GLASS_BANGERS_TRIO",
                "DVPA_EXPLOIT",
                "BLOWOUT_SPECIAL",
                "FLOOR_GENERALS_DUO",
                "VALUE_HUNTER",
                "SAFE_PLAY"
            ]
            
            for strategy in priority_order:
                if strategy in strategies_found:
                    return strategy
        
        return "BALANCED"
    
    def _check_battery(self, players):
        """Verifica se há uma dupla Armador + Finalizador do mesmo time"""
        teams = {}
        for player in players:
            team = player.get("team")
            player_class = player.get("player_class", [])
            mercado = player.get("mercado", {})
            
            # Classificar por papel
            if "FLOOR_GENERAL" in player_class or mercado.get("tipo") == "AST":
                teams.setdefault(team, {})["playmaker"] = player
            elif mercado.get("tipo") == "PTS" or "SHOOTERS_LINES" in player_class:
                teams.setdefault(team, {})["scorer"] = player
        
        # Verificar se algum time tem ambos
        for team, roles in teams.items():
            if "playmaker" in roles and "scorer" in roles:
                return True
        
        return False
    
    def _check_shootout_pair(self, players):
        """Verifica scorers de times opostos"""
        if len(players) < 2:
            return False
        
        # Verificar se são de times diferentes
        teams = set(p.get("team") for p in players)
        if len(teams) < 2:
            return False
        
        # Verificar se são principalmente scorers
        scorers = 0
        for player in players:
            mercado = player.get("mercado", {})
            player_class = player.get("player_class", [])
            
            if mercado.get("tipo") == "PTS" or "SHOOTERS_LINES" in player_class:
                scorers += 1
        
        return scorers >= 2
    
    def _check_glass_bangers(self, players):
        """Verifica reboteadores dominantes"""
        glass_bangers = 0
        
        for player in players:
            player_class = player.get("player_class", [])
            mercado = player.get("mercado", {})
            
            if ("GLASS_BANGER" in player_class or 
                mercado.get("tipo") == "REB" or
                player.get("reb_per_min", 0) > 0.2):
                glass_bangers += 1
        
        return glass_bangers >= 2
    
    def _check_floor_generals(self, players):
        """Verifica múltiplos armadores"""
        floor_generals = 0
        
        for player in players:
            player_class = player.get("player_class", [])
            mercado = player.get("mercado", {})
            
            if ("FLOOR_GENERAL" in player_class or 
                mercado.get("tipo") == "AST" or
                player.get("ast_per_min", 0) > 0.15):
                floor_generals += 1
        
        return floor_generals >= 2
    
    def _check_dvpa_exploit(self, players):
        """Verifica exploração múltipla de DvP"""
        dvp_exploits = 0
        
        for player in players:
            dvp_data = player.get("dvp_data", {})
            if dvp_data:
                overall = dvp_data.get("overall", 1.0)
                if overall > 1.05:  # Matchup favorável
                    dvp_exploits += 1
        
        return dvp_exploits >= 2
    
    def _check_blowout_special(self, players):
        """Verifica foco em garbage time"""
        blowout_players = 0
        
        for player in players:
            # Jogadores com perfil para garbage time
            if (player.get("role") in ["rotation", "deep_bench"] and
                player.get("is_young") and
                player.get("garbage_time_profile") == "high"):
                blowout_players += 1
        
        return blowout_players >= 2
    
    def _check_value_hunter(self, players):
        """Verifica jogadores subvalorizados"""
        value_players = 0
        
        for player in players:
            # Jogadores com bom PRA/min mas baixa visibilidade
            min_l5 = player.get("min_L5", 1)
            pra_l5 = player.get("pra_L5", 0)
            efficiency = pra_l5 / max(min_l5, 1)
            
            if (efficiency > 0.8 and  # Bom PRA/min
                player.get("role") in ["rotation", "bench_scorer"] and  # Não é estrela
                player.get("expected_minutes", 0) < 30):  # Minutos limitados
                value_players += 1
        
        return value_players >= 2
    
    def _check_safe_play(self, players):
        """Verifica combinação de baixa volatilidade"""
        safe_players = 0
        
        for player in players:
            volatility = player.get("volatility", "medium")
            min_cv = player.get("min_cv", 1.0)
            
            if (volatility == "low" or 
                min_cv < 0.4 or
                "SAFE_PLAYS" in player.get("player_class", [])):
                safe_players += 1
        
        return safe_players >= 2
    
    def get_strategy_description(self, strategy_name):
        """Retorna descrição da estratégia"""
        if strategy_name in self.strategies:
            return self.strategies[strategy_name]["description"]
        return "Combinação balanceada"
    
    def suggest_improvements(self, players, current_strategy):
        """Sugere melhorias para a trixie"""
        suggestions = []
        
        # Análise de diversificação
        teams = set(p.get("team") for p in players)
        if len(teams) < 2:
            suggestions.append("Adicionar jogador de outro time para diversificar risco")
        
        # Análise de correlação
        point_players = [p for p in players if p.get("mercado", {}).get("tipo") == "PTS"]
        if len(point_players) > 1 and len(teams) == 1:
            suggestions.append("Evitar múltiplos scorers do mesmo time (canibalismo)")
        
        # Análise de minutes risk
        low_minute_players = [p for p in players if p.get("expected_minutes", 0) < 20]
        if len(low_minute_players) > 1:
            suggestions.append("Reduzir exposição a jogadores com minutos limitados")
        
        # Matchup analysis
        unfavorable_matchups = []
        for player in players:
            dvp_data = player.get("dvp_data", {})
            if dvp_data and dvp_data.get("overall", 1.0) < 0.95:
                unfavorable_matchups.append(player.get("name"))
        
        if unfavorable_matchups:
            suggestions.append(f"Reconsiderar {', '.join(unfavorable_matchups)} com matchup desfavorável")
        
        return suggestions