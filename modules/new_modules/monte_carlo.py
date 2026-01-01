# monte_carlo.py
# VERSÃO ATUALIZADA - Compatível com StrategyEngine V68+
# Correção: __init__ aceita default_sims
# Melhoria: Poisson para STL/BLK/3PM

import numpy as np
from scipy.stats import poisson  # Adicionado para distribuições de contagem

class MonteCarloEngine:
    def __init__(self, default_sims=5000):  # <-- Agora aceita o parâmetro!
        self.num_sims = default_sims
        # Volatilidade Base por Mercado (Desvio Padrão / Média)
        self.default_cv = {
            "PTS": 0.25,   # Pontos variam ~25%
            "REB": 0.35,   # Rebotes variam mais
            "AST": 0.40,   # Assistências são voláteis
            "3PM": 0.50,   # Bola de 3 é alta variância
            "STL": 0.80,   # Eventos raros
            "BLK": 0.80,
            "PRA": 0.30    # Combinação mais estável
        }

    def analyze_bet_probability(self, mean, line, market_type="PTS", cv=None, odds_offered=1.85):
        """
        Executa a Simulação de Monte Carlo.
        
        Args:
            mean (float): Média projetada do jogador (L5 ajustada).
            line (float): A linha da casa de apostas (ex: 20.5).
            market_type (str): Tipo de stat para ajuste de volatilidade.
            cv (float): Coeficiente de Variação personalizado.
            odds_offered (float): Odd oferecida pela casa.
            
        Returns:
            dict: Probabilidade Real, Odd Justa e Edge.
        """
        if mean <= 0:
            return self._empty_result()

        # 1. Volatilidade
        if cv is None or cv == 0:
            used_cv = self.default_cv.get(market_type, 0.30)
        else:
            used_cv = float(cv)
        
        # 2. Simulações
        if market_type in ["STL", "BLK", "3PM"]:
            # Poisson para contagens raras (mais preciso que Normal)
            simulations = poisson.rvs(mu=mean, size=self.num_sims)
        else:
            # Normal para stats contínuas
            std_dev = mean * used_cv
            simulations = np.random.normal(mean, std_dev, self.num_sims)
            simulations = np.maximum(simulations, 0)  # Sem negativos

        # 3. Hits (acertos do over)
        # Para linha 20.5 → precisa >= 21; para 20.0 → >= 20
        hits = np.sum(simulations >= line)
        win_probability = hits / self.num_sims

        # 4. Cálculo financeiro
        fair_odd = 1 / win_probability if win_probability > 0.01 else 99.0
        edge = (win_probability * odds_offered) - 1

        return {
            "prob_percent": round(win_probability * 100, 1),
            "fair_odd": round(fair_odd, 2),
            "market_odd": odds_offered,
            "edge_percent": round(edge * 100, 1),
            "is_value": edge > 0.02,  # >2% edge = value
            "simulation_mean": round(np.mean(simulations), 1)
        }

    def _empty_result(self):
        return {
            "prob_percent": 0.0, "fair_odd": 0.0, 
            "edge_percent": -100.0, "is_value": False
        }