class BlowoutDesigner:
    def generate_blowout_trixies(self, team_players_ctx, game_ctx):
        """Gera trixies específicas para jogos com blowout potencial"""
        if game_ctx.get("spread_abs", 0) < 12:
            return []
        
        # Identifica reservas que vão jogar garbage time
        garbage_time_players = self._identify_garbage_time_players(team_players_ctx)
        
        # Gera combinações específicas
        trixies = self._build_garbage_time_trixies(garbage_time_players)
        
        return trixies
    
    def _identify_garbage_time_players(self, team_players_ctx):
        """Identifica jogadores com upside em garbage time"""
        garbage_players = []
        for team, players in team_players_ctx.items():
            for player in players:
                # Jovens, reservas, baixo salário DFS
                if (player.get("role") in ["rotation", "deep_bench"] and 
                    player.get("is_young") and
                    player.get("garbage_time_profile") == "high"):
                    garbage_players.append(player)
        return garbage_players