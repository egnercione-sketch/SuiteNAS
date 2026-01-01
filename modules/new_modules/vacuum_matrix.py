# modules/new_modules/vacuum_matrix.py
# VERSÃO 2.0 - ROBUST VACUUM & USAGE REDISTRIBUTION

import logging
from typing import Dict, List, Any

logger = logging.getLogger("VacuumMatrix_V2")

class VacuumMatrixAnalyzer:
    """
    Analisador de Vácuo de Uso (Vacuum Matrix).
    Detecta ausências de impacto e redistribui oportunidades (Usage/Minutes)
    para companheiros de time (Beneficiários e Substitutos).
    """

    def __init__(self, injury_monitor=None):
        self.injury_monitor = injury_monitor
        
        # Mapeamento genérico de posições para encontrar substitutos
        self.position_groups = {
            "G": ["PG", "SG", "G"],
            "F": ["SF", "PF", "F"],
            "C": ["C", "F-C", "C-F"],
        }
        
        # Configuração de Boosts (Multiplicadores)
        self.boost_config = {
            "direct_backup": 1.25,     # O substituto direto ganha minutos e volume (ex: Jones no lugar de Morant)
            "usage_absorber": 1.10,    # Estrela restante que absorve usage (ex: Jaylen Brown sem Tatum)
            "rotation_expansion": 1.05 # Jogador de rotação que ganha +4-5 min marginais
        }

    def analyze_team_vacuum(self, team_roster: List[Dict], team_abbr: str) -> Dict[str, Any]:
        """
        Varre o roster para identificar 'buracos' na rotação (Vacuum)
        e calcular quem se beneficia matematicamente.
        """
        if not team_roster:
            return {}

        vacuum_report = {}
        absent_impact_players = []
        
        # 1. Identificar Ausências de Impacto (Ignora reservas irrelevantes)
        active_roster = []
        for p in team_roster:
            if self._is_player_out(p):
                # Só gera Vacuum se for Starter ou tiver min_L5 > 24
                if self._is_impact_player(p):
                    absent_impact_players.append(p)
            else:
                active_roster.append(p)

        if not absent_impact_players:
            return {} # Time saudável ou ausências irrelevantes

        logger.info(f"[{team_abbr}] Vacuum Detectado: {[p.get('name') for p in absent_impact_players]} fora.")

        # 2. Calcular Redistribuição para cada ausência
        for absent in absent_impact_players:
            pos_absent = self._normalize_position(absent)
            
            # --- CENÁRIO A: Substituto Direto (Bench -> Starter) ---
            # Busca jogadores do banco, da mesma posição
            direct_backups = [
                p for p in active_roster 
                if not self._is_starter(p) and self._normalize_position(p) == pos_absent
            ]
            
            if direct_backups:
                # O "Melhor Backup" (maior minutagem ou pontos prévios) pega o maior pedaço do bolo
                # Ordena por minutos médios anteriores
                best_backup = sorted(direct_backups, key=lambda x: x.get('min_L5', 0), reverse=True)[0]
                self._apply_boost_logic(vacuum_report, best_backup, "direct_backup", absent)
            
            # --- CENÁRIO B: Absorvedores de Usage (Starters Restantes) ---
            # Ex: Se Lillard sai, Giannis chuta mais bolas.
            starters = [p for p in active_roster if self._is_starter(p)]
            for starter in starters:
                # Evita dar boost duplo se ele já foi marcado como backup (caso raro de troca de pos)
                p_name = starter.get('name')
                if p_name in vacuum_report and vacuum_report[p_name]['type'] == 'direct_backup':
                    continue
                
                # Titulares ganham boost de usage (menor que o de minutos, mas impactante)
                self._apply_boost_logic(vacuum_report, starter, "usage_absorber", absent)

        return vacuum_report

    def apply_vacuum_boost(self, player_ctx: Dict, vacuum_data: Dict) -> Dict:
        """
        Aplica os boosts calculados aos stats do jogador no contexto.
        """
        if not player_ctx or not vacuum_data:
            return player_ctx

        p_name = player_ctx.get("name")
        if p_name in vacuum_data:
            info = vacuum_data[p_name]
            boost = info['boost']
            
            # Aplicar Boost nos Stats Projetados
            # Stats de volume (PTS, REB, AST, TOV, 3PM) sofrem boost direto
            # Percentagens (FG%, FT%) geralmente não mudam ou pioram (mais defesa focada)
            stats_to_boost = ['pts_L5', 'reb_L5', 'ast_L5', '3pm_L5', 'stl_L5', 'blk_L5', 'tov_L5']
            
            for stat in stats_to_boost:
                if stat in player_ctx:
                    original = float(player_ctx[stat])
                    
                    # Suavização para stats defensivos (variam menos com usage)
                    if stat in ['stl_L5', 'blk_L5']:
                        # Boost reduzido pela metade para stocks
                        factor = 1.0 + ((boost - 1.0) / 2)
                    else:
                        factor = boost
                        
                    player_ctx[stat] = round(original * factor, 1)

            # Injeta Metadata para ser usado na Tese e na UI
            player_ctx['_vacuum_active'] = True
            player_ctx['_vacuum_info'] = info
            
            # Log de debug para validar se está funcionando
            # logger.debug(f"Boost Vacuum aplicado em {p_name}: {boost}x (Motivo: {info['reason']})")

        return player_ctx

    # =========================================================================
    # HELPERS DE LÓGICA
    # =========================================================================

    def _apply_boost_logic(self, report, player, boost_type, absent_player):
        """
        Registra o boost no relatório. Se o jogador já tem boost, ACUMULA com segurança.
        """
        p_name = player.get('name')
        new_boost_val = self.boost_config[boost_type]
        reason_fragment = f"{'Sub' if boost_type == 'direct_backup' else 'Usage'}->{absent_player.get('name')}"

        if p_name in report:
            # Lógica Cumulativa:
            # Se já tem 1.25 e ganha mais 1.10, não multiplicamos (1.375).
            # Somamos o delta: 1.0 + 0.25 + 0.10 = 1.35.
            current_boost = report[p_name]['boost']
            delta = new_boost_val - 1.0
            
            # Cap de Segurança: Ninguém ganha mais de 50% de boost (salvo exceções manuais)
            final_boost = min(1.50, current_boost + delta)
            
            report[p_name]['boost'] = round(final_boost, 2)
            report[p_name]['reason'] += f" & {reason_fragment}"
        else:
            report[p_name] = {
                "boost": new_boost_val,
                "reason": reason_fragment,
                "type": boost_type,
                "source": absent_player.get('name')
            }

    def _is_player_out(self, p):
        """Verifica strings de status comuns na NBA API/Rotowire"""
        status = str(p.get("status", "")).lower()
        # GTD (Game Time Decision) geralmente tratamos como risco, mas aqui focamos em OUT
        return any(x in status for x in ["out", "inj", "hurt", "surgery"])

    def _is_impact_player(self, p):
        """Define se a ausência cria vácuo relevante"""
        # É titular OU tem média de minutos > 24
        return self._is_starter(p) or float(p.get('min_L5', 0)) >= 24.0

    def _is_starter(self, p):
        """Verifica flag de titularidade em vários formatos"""
        return (p.get('is_starter', False) is True or 
                str(p.get('STARTER', '')).upper() == 'TRUE' or 
                p.get('role') == 'starter')

    def _normalize_position(self, p):
        """Normaliza posições para G, F, C"""
        raw = (p.get("position") or p.get("POSITION", "")).upper()
        if "C" in raw: return "C"
        if "F" in raw: return "F"
        if "G" in raw: return "G"
        return "F" # Fallback seguro