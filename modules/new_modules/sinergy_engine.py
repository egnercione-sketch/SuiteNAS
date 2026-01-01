# modules/new_modules/sinergy_engine_fixed.py

class SinergyEngine:
    """Motor de sinergia para cálculo de odds e linhas inteligentes - SEM ALEATORIEDADE"""
    
    def __init__(self):
        # Perfis de estratégia para diferentes categorias
        self.profiles = {
            "conservadora": {
                "line_mult": 0.85,        # Linha conservadora (85% da média)
                "base_odd": 1.75,          # Odd base
                "volatility_adjust": -0.2, # Penaliza volatilidade
                "description": "Estratégia conservadora - foco em segurança"
            },
            "ousada": {
                "line_mult": 0.95,         # Linha mais agressiva
                "base_odd": 1.90,
                "volatility_adjust": 0.1,  # Premia volatilidade controlada
                "description": "Estratégia ousada - busca de upside"
            },
            "explosao": {
                "line_mult": 1.05,         # Linha muito agressiva
                "base_odd": 2.10,
                "volatility_adjust": 0.3,  # Premia alta volatilidade
                "description": "Estratégia explosão - teto máximo"
            },
            "versatil": {
                "line_mult": 0.90,         # Linha balanceada
                "base_odd": 1.95,
                "volatility_adjust": 0.0,  # Neutro em volatilidade
                "description": "Estratégia versátil - especialidades"
            },
            "default": {
                "line_mult": 0.90,
                "base_odd": 1.85,
                "volatility_adjust": 0.0,
                "description": "Perfil padrão"
            }
        }
        
        # Coeficientes de variação por mercado (para normalização)
        self.market_volatility = {
            "PTS": 0.25,  # Pontos variam ~25%
            "REB": 0.35,  # Rebotes variam mais
            "AST": 0.40,  # Assistências são voláteis
            "3PM": 0.50,  # Alta variância
            "STL": 0.80,  # Eventos raros
            "BLK": 0.80,
            "PRA": 0.30   # Combinação PTS+REB+AST
        }

    def calculate_smart_odd(self, base_line, market, category, confidence, player_cv=None):
        """
        Calcula linha e odd inteligentes baseados em múltiplos fatores
        SEM ALEATORIEDADE - completamente determinístico
        """
        # Obtém perfil da categoria ou usa default
        profile = self.profiles.get(category, self.profiles["default"])
        
        # 1. Ajuste de Linha (Arredondamento inteligente)
        raw_line = base_line * profile["line_mult"]
        
        # Regra de arredondamento por mercado
        if market in ["PTS", "PRA"]:
            # PTS/PRA: inteiro mais próximo (ex: 22.4 -> 22+)
            final_line = round(raw_line)
            line_display = f"{final_line}+"
        elif market in ["REB", "AST"]:
            # REB/AST: meio ponto se >= 0.5, senão inteiro (ex: 5.5+, 6+)
            if raw_line - int(raw_line) >= 0.5:
                final_line = int(raw_line) + 0.5
            else:
                final_line = round(raw_line)
            line_display = f"{final_line}+"
        elif market == "3PM":
            # 3PM: sempre inteiro, mínimo 0.5
            final_line = max(0.5, round(raw_line))
            line_display = f"{final_line}+"
        elif market in ["STL", "BLK"]:
            # STL/BLK: 1.0 ou 1.5
            if raw_line >= 1.25:
                final_line = 1.5
            else:
                final_line = 1.0
            line_display = f"{final_line}+"
        else:
            # Default: inteiro
            final_line = round(raw_line)
            line_display = f"{final_line}+"

        # 2. Cálculo de Odd DETERMINÍSTICO
        base_odd = profile["base_odd"]
        
        # Se player_cv não fornecido, usar padrão do mercado
        if player_cv is None:
            player_cv = self.market_volatility.get(market, 0.3)
        
        # Ajuste por volatilidade (determinístico)
        cv_base = self.market_volatility.get(market, 0.3)
        vol_ratio = player_cv / cv_base if cv_base > 0 else 1.0
        
        if category == "conservadora":
            # Conservadora: penaliza alta volatilidade
            if vol_ratio > 1.1:
                vol_impact = -0.15
            elif vol_ratio < 0.9:
                vol_impact = 0.05  # Bônus para baixa volatilidade
            else:
                vol_impact = 0.0
        elif category == "explosao":
            # Explosão: premia volatilidade (mais chance de explosão)
            vol_impact = min(0.2, (vol_ratio - 1) * 0.1)
        else:
            # Outras: ajuste moderado
            vol_impact = (vol_ratio - 1) * profile["volatility_adjust"]
        
        # Ajuste por confiança (determinístico)
        conf_bonus = max(-0.2, min(0.2, (confidence - 0.6) * 0.3))
        
        # Cálculo final da odd (SEM ALEATORIEDADE)
        final_odd = base_odd * (1 + vol_impact + conf_bonus)
        
        # REMOVIDO: final_odd += random.uniform(-0.05, 0.05)
        
        # Caps de segurança
        final_odd = max(1.45, min(2.50, round(final_odd, 2)))
        
        # Garantir que a linha seja pelo menos 1.0 para a maioria dos mercados
        if market not in ["STL", "BLK"] and final_line < 1.0:
            final_line = 1.0
            line_display = "1.0+"
        
        return {
            "line": float(final_line),
            "line_display": line_display,
            "odds": final_odd,
            "base_line": base_line,
            "adjustment_factor": profile["line_mult"],
            "vol_impact": round(vol_impact, 3),
            "conf_bonus": round(conf_bonus, 3)
        }