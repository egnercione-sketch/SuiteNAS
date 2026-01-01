import math

class BankrollManager:
    def __init__(self, total_bankroll=1000.0, kelly_fraction=0.10):
        self.bankroll = total_bankroll
        self.fraction = kelly_fraction # 0.10 significa usar 10% do sugerido pelo Kelly (Seguro)

    def calculate_stake(self, prob_pct, odd):
        """Calcula stake baseada no Critério de Kelly Fracionado."""
        if odd <= 1.0 or prob_pct <= 0: return 0, 0
        
        p = prob_pct / 100.0
        q = 1.0 - p
        b = odd - 1.0
        
        # Fórmula de Kelly: f = (bp - q) / b
        f = (b * p - q) / b if b > 0 else 0
        
        if f <= 0: return 0, 0
        
        # Aplica o multiplicador de segurança (ex: 1/10 do Kelly)
        suggested_f = f * self.fraction
        
        # Teto de segurança: Nunca arriscar mais de 4% da banca em uma única Trixie
        suggested_f = min(suggested_f, 0.04)
        
        cash_amount = self.bankroll * suggested_f
        return round(suggested_f * 100, 2), round(cash_amount, 2)

    def get_risk_label(self, pct):
        if pct >= 2.5: return "🔥 ALTA CONFIANÇA"
        if pct >= 1.0: return "⚖️ MODERADA"
        return "🛡️ BAIXO RISCO / EXPLORAÇÃO"