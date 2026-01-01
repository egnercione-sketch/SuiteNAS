# modules/new_modules/rotation_analyzer.py
"""
RotationAnalyzer v2.2 - Com Integração Completa de Lesões (Cache + Roster)
Adiciona seção dedicada ao Departamento Médico.
"""
import os
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class RotationAnalyzer:
    def __init__(self, cache_dir="cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        # Arquivos de Cache
        self.lineup_cache_file = os.path.join(cache_dir, "lineup_signals.json")
        self.injuries_cache_file = os.path.join(cache_dir, "injuries_cache_v44.json")
        
        self.min_cache_validity = timedelta(hours=24)
        self.rotation_signals = self._load_cache()
        
    def _load_cache(self) -> Dict:
        """Carrega cache de sinais de rotação"""
        try:
            if os.path.exists(self.lineup_cache_file):
                with open(self.lineup_cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    # Validação simplificada de tempo
                    return cache_data.get('data', {})
        except Exception as e:
            logger.warning(f"⚠️ Erro cache rotação: {e}")
        return {}

    def _load_injuries_from_file(self) -> Dict:
        """Carrega o cache oficial de lesões (injuries_cache_v44.json)"""
        try:
            if os.path.exists(self.injuries_cache_file):
                with open(self.injuries_cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception:
            return {}
        return {}
    
    def _save_cache(self):
        try:
            cache_data = {
                "timestamp": datetime.now().isoformat(),
                "data": self.rotation_signals
            }
            with open(self.lineup_cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
        except Exception: pass
    
    def analyze_team_rotation(self, team_abbr: str, roster_entries: List[Dict], local_injuries: List[str], recent_minutes: Dict):
        """
        Analisa rotação considerando o Departamento Médico completo (Cache + Roster).
        """
        stable_lineups = []
        role_definitions = {}
        lineup_shocks = []
        medical_report = [] # Nova lista para o Depto Médico

        # --- 1. CARREGAR E UNIFICAR LESÕES ---
        # Carrega cache global
        global_injuries_data = self._load_injuries_from_file()
        
        # Normaliza estrutura do cache (pode vir como dict 'teams' ou lista direta)
        team_injuries_cache = []
        if isinstance(global_injuries_data, dict):
            # Tenta acessar pela chave do time (ex: 'LAL', 'GSW')
            teams_data = global_injuries_data.get('teams', global_injuries_data)
            team_injuries_cache = teams_data.get(team_abbr, [])
        
        # Cria mapa unificado de lesões {nome: status}
        injury_map = {}
        
        # A. Adiciona do Cache (Geralmente mais detalhado: "Out - Knee")
        if isinstance(team_injuries_cache, list):
            for item in team_injuries_cache:
                p_name = item.get('player') or item.get('name')
                p_status = item.get('status', 'Out')
                p_desc = item.get('description') or item.get('return_date', '')
                if p_name:
                    # Filtra apenas quem realmente está fora ou duvidoso
                    if any(x in str(p_status).lower() for x in ['out', 'inj', 'gtd', 'doubt', 'questionable']):
                        injury_map[p_name] = f"{p_status} ({p_desc})" if p_desc else p_status

        # B. Adiciona do Roster Local (Scan em tempo real)
        for name in local_injuries:
            if name not in injury_map:
                injury_map[name] = "Out (Roster)" # Fallback se não tiver no cache

        # Preenche o relatório médico formatado
        for name, status in injury_map.items():
            medical_report.append({"name": name, "status": status})

        # --- 2. ANÁLISE DE ROSTER E ROLES ---
        def get_val(p, keys, default=None):
            for k in keys:
                if k in p and p[k] is not None: return p[k]
            return default

        def is_starter(p):
            role = str(get_val(p, ['ROLE', 'role'], '')).lower()
            start = str(get_val(p, ['STARTER', 'is_starter'], '')).lower()
            return 'starter' in role or 'true' in start

        starters = [p for p in roster_entries if is_starter(p)]
        
        # Fallback para titulares se faltar info
        if len(starters) < 5:
            sorted_roster = sorted(roster_entries, key=lambda x: float(get_val(x, ['MIN_AVG', 'min_L5', 'MIN'], 0)), reverse=True)
            # Filtra quem está na lista de lesionados para não projetar lesionado como titular
            valid_roster = [p for p in sorted_roster if get_val(p, ['PLAYER', 'name']) not in injury_map]
            starters = valid_roster[:5]

        # Definição de Roles
        for player in roster_entries:
            name = get_val(player, ['PLAYER', 'name'], 'Unknown')
            position = get_val(player, ['POSITION', 'pos'], '').upper()
            min_avg = recent_minutes.get(name, float(get_val(player, ['MIN_AVG', 'min_L5', 'MIN'], 0)))
            
            player_is_starter = name in [get_val(s, ['PLAYER', 'name']) for s in starters]
            
            if player_is_starter: role = "starter"
            elif min_avg >= 25: role = "key_rotation"
            elif min_avg >= 15: role = "rotation"
            elif min_avg >= 8: role = "bench"
            else: role = "deep_bench"
                
            role_definitions[f"{team_abbr}_{name}"] = {
                "role": role,
                "minutes": min_avg,
                "position": position,
                "starter": player_is_starter
            }

        # --- 3. DETECÇÃO DE CHOQUES (STARTERS OUT) ---
        # Cruza titulares teóricos com a lista unificada de lesões
        injured_starters_list = []
        # Precisamos de uma lista de nomes de starters para verificar
        # Mas aqui verificamos se algum jogador KEY está na lista injury_map
        
        for p_name, p_status in injury_map.items():
            # Verifica se esse lesionado era um starter ou key rotation (minutos > 25)
            # Usamos recent_minutes como base histórica
            hist_min = recent_minutes.get(p_name, 0)
            if hist_min >= 24: # Consideramos impacto relevante
                injured_starters_list.append({
                    "name": p_name,
                    "status": p_status,
                    "minutes_lost": hist_min
                })

        if injured_starters_list:
            shock_desc = f"Ausências de Impacto: " + ", ".join([f"{p['name']} ({p['status']})" for p in injured_starters_list])
            lineup_shocks.append({
                "description": shock_desc,
                "impact": "Ajuste forçado na rotação",
                "severity": "high" if len(injured_starters_list) >= 2 else "medium"
            })
        
        # Formações Estáveis (Titulares Saudáveis)
        if len(starters) >= 1:
            starter_names = [get_val(p, ['PLAYER', 'name']) for p in starters[:5]]
            stable_lineups.append({
                "lineup": starter_names,
                "type": "starting_lineup",
                "net_rating": "+3.5"
            })

        # --- 4. SALVAR NO CACHE ---
        cache_key = f"{team_abbr}_rotation"
        self.rotation_signals[cache_key] = {
            "signals": {
                "stable_lineups": stable_lineups,
                "role_definitions": role_definitions,
                "lineup_shocks": lineup_shocks,
                "medical_report": medical_report, # <--- AQUI ESTÁ O CAMPO NOVO
                "injured_starters_count": len(injured_starters_list)
            },
            "game_info": {"team": team_abbr},
            "analyzed_at": datetime.now().isoformat()
        }
        self._save_cache()

    def get_lineup_insights(self, team: str, matchup_context: Dict) -> str:
        cache_key = f"{team}_rotation"
        if cache_key not in self.rotation_signals:
            return "🔍 Aguardando dados de rotação..."
        
        signals = self.rotation_signals[cache_key]["signals"]
        insights = []
        
        # 1. Lineup Titular
        stable = signals.get("stable_lineups", [])
        start_lineup = next((l for l in stable if l["type"] == "starting_lineup"), None)
        if start_lineup:
            names = ", ".join(start_lineup.get("lineup", [])[:5])
            insights.append(f"⭐ **Prováveis Titulares:** {names}")
            
        # 2. Roles/Rotação
        roles = signals.get("role_definitions", {})
        rotation_count = sum(1 for r in roles.values() if r["role"] == "rotation")
        insights.append(f"📊 **Banco Ativo:** ~{rotation_count} jogadores na rotação principal.")

        # 3. Alertas de Choque (Resumo Rápido)
        shocks = signals.get("lineup_shocks", [])
        if shocks:
            for s in shocks:
                icon = "🚨" if s.get("severity") == "high" else "⚠️"
                insights.append(f"\n{icon} **IMPACTO:** {s['description']}")

        # 4. DEPARTAMENTO MÉDICO (NOVO BLOCO)
        medical = signals.get("medical_report", [])
        # Filtra apenas jogadores relevantes para não poluir (ex: ignora quem não joga nunca)
        # Mas como a lista vem do injury_map cruzado com roster, já deve estar ok.
        if medical:
            insights.append(f"\n🏥 **Dept. Médico / Dúvidas:**")
            # Lista até 5 nomes para não ficar gigante
            for item in medical[:6]: 
                insights.append(f"   • {item['name']}: {item['status']}")
            if len(medical) > 6:
                insights.append(f"   • ... e mais {len(medical)-6} listados.")
        else:
            insights.append("\n✅ DM Vazio: Time saudável.")
            
        return "\n".join(insights)