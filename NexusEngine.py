# ============================================================================
# NEXUS ENGINE - O CÉREBRO DE OPORTUNIDADES (SGP & VÁCUO)
# ============================================================================
import json
import os

# Tenta importar os módulos externos (Assumindo que existem e funcionam)
# Se não existirem, usamos mocks (simulações) para o código não quebrar.
try:
    from injuries import InjuryMonitor
    from modules.new.sinergy_engine import SinergyEngine
    from modules.new.pace_adjuster import PaceAdjuster
    from modules.new.dvp_analyzer import DvpAnalyzer
    from modules.new.archetype_engine import ArchetypeEngine
except ImportError:
    # Classes Dummy para evitar erro se os arquivos não estiverem perfeitos
    class InjuryMonitor: 
        def get_injured_players(self): return []
    class SinergyEngine: 
        def find_partner(self, player, team): return None
    class PaceAdjuster: 
        def get_game_pace(self, team): return 98
    class DvpAnalyzer: 
        def analyze_defense(self, team, position): return {"rating": "C", "rank": 15}
    class ArchetypeEngine: 
        def get_archetype(self, player): return "General"

class NexusEngine:
    def __init__(self, logs_cache, games):
        self.logs = logs_cache
        self.games = games
        
        # Inicializa os Consultores
        self.injury_monitor = InjuryMonitor()
        self.sinergy = SinergyEngine()
        self.pace = PaceAdjuster()
        self.dvp = DvpAnalyzer()
        self.archetype = ArchetypeEngine()
        
        # Cache de IDs para Fotos (Reuso da lógica do 5/7/10)
        self.player_ids = {}
        if os.path.exists("nba_players_map.json"):
            try:
                with open("nba_players_map.json", "r", encoding="utf-8") as f:
                    self.player_ids = json.load(f)
            except: pass

    def get_photo(self, name):
        # Lógica simplificada de foto
        pid = self.player_ids.get(name)
        if pid: return f"https://cdn.nba.com/headshots/nba/latest/1040x760/{pid}.png"
        return "https://cdn.nba.com/headshots/nba/latest/1040x760/fallback.png"

    def run_nexus_scan(self):
        """O Loop Principal de Inteligência"""
        opportunities = []
        
        # 1. SCANNER DE SGP (SIMBIOSE)
        # Procura Garçons (High AST) e seus parceiros
        sgp_ops = self._scan_sgp_opportunities()
        opportunities.extend(sgp_ops)

        # 2. SCANNER DE VÁCUO (REBOTE)
        # Procura Pivôs adversários machucados
        vacuum_ops = self._scan_vacuum_opportunities()
        opportunities.extend(vacuum_ops)

        # Ordena por Score de Oportunidade (0 a 100)
        return sorted(opportunities, key=lambda x: x['score'], reverse=True)

    def _scan_sgp_opportunities(self):
        found = []
        # Lógica: Varrer jogadores com média alta de AST no cache
        for p_name, data in self.logs.items():
            logs = data.get('logs', {})
            ast_logs = logs.get('AST', [])
            
            # Filtro 1: É um Garçom? (Média > 7 AST nos últimos 10)
            if not ast_logs or len(ast_logs) < 10: continue
            avg_ast = sum(ast_logs[:10]) / 10
            if avg_ast < 7.0: continue

            team = data.get('team')
            
            # CONSULTA 1: SINERGIA
            # Quem é o parceiro desse cara? (Ex: Trae -> Jalen)
            partner_name = self.sinergy.find_partner(p_name, team) 
            if not partner_name: continue # Se não tem parceiro claro, pula

            # CONSULTA 2: PACE
            game_pace = self.pace.get_game_pace(team)
            if game_pace < 100: continue # Jogo lento mata SGP

            # CONSULTA 3: DVP
            # Defesa adversária cede AST?
            opp_defense = self.dvp.analyze_defense(team, "PG") # Assume PG para quem dá assist
            
            # CALCULA SCORE
            score = 50 # Base
            score += 10 if game_pace > 103 else 0
            score += 20 if opp_defense.get('rank', 15) > 20 else 0 # Top 10 pior defesa
            
            if score >= 70:
                found.append({
                    "type": "SGP",
                    "title": "ECOSSISTEMA SIMBIÓTICO",
                    "score": score,
                    "hero": {"name": p_name, "photo": self.get_photo(p_name), "stat": "AST", "target": "8+"},
                    "partner": {"name": partner_name, "photo": self.get_photo(partner_name), "stat": "PTS", "target": "20+"},
                    "context": [f"Ritmo: {game_pace}", "Sinergia Alta"],
                    "color": "#eab308" # Amarelo
                })
        return found

    def _scan_vacuum_opportunities(self):
        found = []
        # Lógica: Olhar lista de lesionados e ver quem enfrenta eles
        injured_list = self.injury_monitor.get_injured_players()
        
        for injured in injured_list:
            # Filtro 1: O lesionado é Pivô (Center)?
            if "C" not in injured.get('position', ''): continue
            
            opp_team = injured.get('opponent_today')
            if not opp_team: continue

            # Quem é o Pivô do time adversário (O nosso Herói)?
            # Aqui varremos o cache procurando o C do opp_team com mais rebotes
            hero_name = self._find_best_rebounder(opp_team)
            if not hero_name: continue

            # CONSULTA: É Dynamite em Rebotes?
            # (Simplificado: média > 10)
            hero_data = self.logs.get(hero_name, {})
            reb_logs = hero_data.get('logs', {}).get('REB', [])
            if not reb_logs: continue
            avg_reb = sum(reb_logs[:10]) / len(reb_logs[:10])

            if avg_reb > 9.0:
                # Temos um Vácuo!
                score = 60 # Base
                score += 20 # Pivô titular fora é big deal
                score += 10 if avg_reb > 11 else 0

                found.append({
                    "type": "VACUUM",
                    "title": "VÁCUO DE REBOTE",
                    "score": score,
                    "hero": {"name": hero_name, "photo": self.get_photo(hero_name), "stat": "REB", "target": "12+"},
                    "villain": {"name": injured['name'], "status": "OUT 🚑"},
                    "context": ["Garrafão Aberto", "Oponente Baixo"],
                    "color": "#a855f7" # Roxo
                })
        return found

    def _find_best_rebounder(self, team):
        # Função auxiliar simples para achar o dono do garrafão do time
        best_reb = 0
        best_player = None
        for name, data in self.logs.items():
            if data.get('team') == team:
                rebs = data.get('logs', {}).get('REB', [])
                if rebs:
                    avg = sum(rebs[:5])/5
                    if avg > best_reb:
                        best_reb = avg
                        best_player = name
        return best_player
