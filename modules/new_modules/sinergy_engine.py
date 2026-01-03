# modules/new_modules/sinergy_engine.py
# ==============================================================================
# SINERGY ENGINE v2.0 - O CAÇADOR DE CORRELAÇÕES
# ==============================================================================
import statistics

class SinergyEngine:
    def __init__(self):
        pass

    def analyze_synergy(self, hero_name, hero_team, logs_cache, trigger_stat="AST", target_stat="PTS"):
        """
        Analisa qual companheiro tem a maior correlação positiva 
        quando o Herói explode em uma estatística (Ex: AST).
        """
        hero_data = logs_cache.get(hero_name, {})
        if not hero_data: return None, 0

        # Pega os logs do Herói
        hero_logs = hero_data.get('logs', {}).get(trigger_stat, [])
        if len(hero_logs) < 5: return None, 0

        # Define o "Gatilho" (Ex: Jogos onde deu mais que a média de assistências)
        avg_trigger = sum(hero_logs[:10]) / len(hero_logs[:10])
        threshold = max(5, avg_trigger * 1.1) # Jogos 10% acima da média

        # Identifica os índices dos jogos onde o Herói foi bem
        explosive_indices = [i for i, val in enumerate(hero_logs[:15]) if val >= threshold]
        
        if len(explosive_indices) < 2: 
            return None, 0 # Pouca amostra de explosão

        best_partner = None
        best_synergy_score = 0
        best_partner_avg = 0

        # Varre todos os jogadores do MESMO time
        for teammate_name, t_data in logs_cache.items():
            if teammate_name == hero_name: continue
            if t_data.get('team') != hero_team: continue

            t_logs = t_data.get('logs', {}).get(target_stat, [])
            if not t_logs: continue

            # Calcula média normal do parceiro
            normal_avg = sum(t_logs[:15]) / len(t_logs[:15])
            if normal_avg < 12: continue # Ignora bagres com poucos pontos

            # Calcula média do parceiro APENAS nos jogos onde o Herói explodiu
            synergy_vals = []
            for idx in explosive_indices:
                if idx < len(t_logs):
                    synergy_vals.append(t_logs[idx])
            
            if not synergy_vals: continue
            
            synergy_avg = sum(synergy_vals) / len(synergy_vals)
            
            # O Score é o quanto ele melhora (Ex: Média 20 -> Média 25 com o Herói = +25%)
            improvement_pct = ((synergy_avg - normal_avg) / normal_avg) * 100
            
            # Filtro: Tem que melhorar pelo menos 5% e ter média relevante
            if improvement_pct > 5 and synergy_avg > best_partner_avg:
                best_partner = teammate_name
                best_partner_avg = synergy_avg
                best_synergy_score = improvement_pct

        return best_partner, best_partner_avg
