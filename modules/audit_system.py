# modules/audit_system.py
# VERSÃO V3.9 - DYNAMIC HEADER MAPPING & SYNTAX FIX
# Resolve erro de colunas trocadas (Naz Reid) e erro de sintaxe.

import os
import json
import logging
import requests
import time
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(BASE_DIR, "cache")
AUDIT_FILE = os.path.join(CACHE_DIR, "audit_trixies.json")

logger = logging.getLogger("AuditSystem")

class AuditSystem:
    def __init__(self, cache_file=None):
        self.audit_file = cache_file or AUDIT_FILE
        self._ensure_file_exists()
        self.audit_data = self._load_data()

    def _ensure_file_exists(self):
        if not os.path.exists(CACHE_DIR):
            try: os.makedirs(CACHE_DIR)
            except: pass
        if not os.path.exists(self.audit_file):
            try:
                with open(self.audit_file, 'w', encoding='utf-8') as f:
                    json.dump([], f)
            except: pass

    def _load_data(self):
        try:
            if os.path.exists(self.audit_file):
                with open(self.audit_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return []
        except: return []

    def _persist(self):
        try:
            with open(self.audit_file, 'w', encoding='utf-8') as f:
                json.dump(self.audit_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Erro ao salvar audit: {e}")

    # =========================================================================
    # CORE: LOGGING (PADRONIZADO)
    # =========================================================================
    def log_trixie(self, trixie_data, game_info=None, category=None, source="StrategyEngine"):
        self.audit_data = self._load_data()
        
        t_id = trixie_data.get('id')
        if not t_id:
            t_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{hash(str(trixie_data))}"[:20]
        
        if any(t.get('id') == t_id for t in self.audit_data):
            return False

        raw_legs = trixie_data.get('players', []) or trixie_data.get('legs', [])
        clean_legs = []

        for l in raw_legs:
            # Tenta recuperar ID do jogo da perna ou do ticket
            leg_game_id = l.get('game_id')
            if not leg_game_id and game_info:
                ticket_gid = game_info.get('game_id')
                if ticket_gid and ticket_gid not in ['MULTI', 'MIX']:
                    leg_game_id = ticket_gid

            clean_leg = {
                "player_name": l.get('player_name') or l.get('name', 'Unknown'),
                "team": l.get('team', '?'),
                "market_type": l.get('market_type') or l.get('market', 'UNK'),
                "market_display": l.get('market_display', '-'),
                "line": float(l.get('line', 0)),
                "odds": float(l.get('odds', 1.0)),
                "thesis": l.get('thesis', ''),
                "game_id": leg_game_id, 
                "status": "PENDING",
                "actual_value": 0
            }
            clean_legs.append(clean_leg)

        final_ticket = {
            "id": t_id,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "timestamp": datetime.now().isoformat(),
            "status": "PENDING",
            "category": category or trixie_data.get('category', 'GENERAL'),
            "sub_category": trixie_data.get('sub_category', ''),
            "total_odd": float(trixie_data.get('total_odd', 0)),
            "source": source,
            "game_info": game_info or {},
            "legs": clean_legs
        }

        self.audit_data.append(final_ticket)
        # Mantém histórico limpo (últimos 500)
        if len(self.audit_data) > 500: self.audit_data = self.audit_data[-500:]
        self._persist()
        return True

    def delete_ticket(self, ticket_id):
        self.audit_data = [t for t in self.audit_data if t.get('id') != ticket_id]
        self._persist()

    # =========================================================================
    # INTEGRAÇÃO ESPN (COM MAPEAMENTO DINÂMICO DE COLUNAS)
    # =========================================================================
    def fetch_espn_boxscore(self, game_id):
        if not game_id or game_id in ['MULTI', 'MIX', 'UNK']: return None
        try:
            url = f"https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={game_id}"
            r = requests.get(url, timeout=6, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200: return r.json()
        except: pass
        return None

    def _extract_player_stats(self, boxscore, player_name):
        """
        Extração inteligente: lê os cabeçalhos (labels) para saber o índice correto.
        Corrige o problema de ler AST como REB se a ordem mudar.
        """
        try:
            bs = boxscore.get('boxscore', {})
            # Tenta diferentes caminhos comuns da API da ESPN
            teams = bs.get('players', []) 
            if not teams: teams = bs.get('teams', [])

            p_clean = player_name.lower().replace('.', '').replace("'", "").strip()
            
            for team in teams:
                stats_block = team.get('statistics', [])
                if not stats_block: continue
                
                # --- MAPA DINÂMICO DE ÍNDICES ---
                # Lê a lista de labels (ex: ["MIN", "FG", "3PT", "REB", ...])
                labels = stats_block[0].get('labels', [])
                idx_map = {label: i for i, label in enumerate(labels)}
                
                # Descobre os índices reais para este jogo
                idx_pts = idx_map.get("PTS", -1)
                idx_reb = idx_map.get("REB", -1)
                idx_ast = idx_map.get("AST", -1)
                idx_stl = idx_map.get("STL", -1)
                idx_blk = idx_map.get("BLK", -1)
                idx_to  = idx_map.get("TO", -1)
                
                # 3 Pontos pode vir como "3PT" ou "3PM"
                idx_3pt = idx_map.get("3PT", -1)
                if idx_3pt == -1: idx_3pt = idx_map.get("3PM", -1)

                athletes = stats_block[0].get('athletes', [])
                
                for ath in athletes:
                    ath_name = ath.get('athlete', {}).get('displayName', '').lower().replace('.', '').replace("'", "").strip()
                    
                    if p_clean in ath_name or ath_name in p_clean:
                        stats = ath.get('stats', [])
                        
                        # Função segura para pegar valor pelo índice descoberto
                        def get_val(idx):
                            if idx != -1 and idx < len(stats):
                                try: return float(stats[idx])
                                except: return 0.0
                            return 0.0
                        
                        # Tratamento especial para 3PM (Formato "2-5" -> 2.0)
                        t3 = 0.0
                        if idx_3pt != -1 and idx_3pt < len(stats):
                            val_str = str(stats[idx_3pt])
                            if "-" in val_str:
                                try: t3 = float(val_str.split("-")[0])
                                except: t3 = 0.0
                            else:
                                try: t3 = float(val_str)
                                except: t3 = 0.0

                        return {
                            "PTS": get_val(idx_pts),
                            "REB": get_val(idx_reb), # Agora usa o índice correto!
                            "AST": get_val(idx_ast),
                            "STL": get_val(idx_stl),
                            "BLK": get_val(idx_blk),
                            "3PM": t3,
                            "TOV": get_val(idx_to)
                        }
        except Exception as e:
            logger.error(f"Erro parsing stats: {e}")
        return None

    # =========================================================================
    # SMART VALIDATION COM DIAGNÓSTICO
    # =========================================================================
    def smart_validate_ticket(self, ticket_id):
        self.audit_data = self._load_data()
        ticket = next((t for t in self.audit_data if t.get('id') == ticket_id), None)
        if not ticket: return False, "Bilhete não encontrado."

        legs = ticket.get('legs', [])
        bs_cache = {}
        updates = False
        
        report = {"total": len(legs), "updated": 0, "no_id": 0, "game_pending": 0}
        
        all_resolved = True
        any_lost = False

        for leg in legs:
            # Pula se já ganhou (mas revalida LOSS/PENDING)
            if leg.get('status') == 'WIN': continue

            g_id = leg.get('game_id')
            
            # Tenta fallback se não tiver ID na perna
            if not g_id or g_id in ['MULTI', 'MIX', 'UNK']:
                if ticket.get('game_info', {}).get('game_id') not in ['MULTI', 'MIX']:
                    g_id = ticket.get('game_info', {}).get('game_id')
                else:
                    report["no_id"] += 1
                    all_resolved = False
                    continue

            if g_id not in bs_cache:
                bs = self.fetch_espn_boxscore(g_id)
                if bs: bs_cache[g_id] = bs
                else:
                    all_resolved = False
                    continue 
            
            boxscore = bs_cache[g_id]
            stats = self._extract_player_stats(boxscore, leg.get('player_name', ''))
            
            game_status = boxscore.get('header', {}).get('competitions', [{}])[0].get('status', {}).get('type', {})
            is_final = game_status.get('completed', False)
            if not is_final: report["game_pending"] += 1

            if stats:
                mkt = leg.get('market_type', 'UNK')
                line = float(leg.get('line', 0))
                actual = 0.0
                
                # Validação normal
                if mkt == "PTS": actual = stats['PTS']
                elif mkt == "REB": actual = stats['REB']
                elif mkt == "AST": actual = stats['AST']
                elif mkt == "STL": actual = stats['STL']
                elif mkt == "BLK": actual = stats['BLK']
                elif mkt == "3PM": actual = stats['3PM']
                elif mkt == "PRA": actual = stats['PTS'] + stats['REB'] + stats['AST']
                elif mkt == "DD2": actual = 1.0 if sum(1 for v in stats.values() if v >= 10) >= 2 else 0.0
                
                # Validação de Combos (PTS+REB)
                if "+" in mkt:
                    parts = mkt.split('+')
                    val_sum = 0
                    for p in parts:
                        val_sum += stats.get(p, 0)
                    actual = val_sum

                leg['actual_value'] = actual
                
                if actual >= line:
                    leg['status'] = 'WIN'
                elif is_final:
                    leg['status'] = 'LOSS'
                    any_lost = True
                
                updates = True
                report["updated"] += 1
            
            elif is_final:
                # Jogo acabou e jogador não apareceu (DNP)
                leg['status'] = 'LOSS'
                leg['actual_value'] = 0
                any_lost = True
                updates = True
                report["updated"] += 1

        # Atualiza Status Global do Bilhete
        current_legs = ticket.get('legs', [])
        has_loss = any(l.get('status') == 'LOSS' for l in current_legs)
        all_wins = all(l.get('status') == 'WIN' for l in current_legs)
        
        if has_loss: ticket['status'] = 'LOSS'
        elif all_wins: ticket['status'] = 'WIN'
        
        if updates: self._persist()

        if report["updated"] > 0:
            return True, f"✅ Atualizado! {report['updated']} estatísticas corrigidas."
        if report["game_pending"] > 0:
            return True, "Jogos ainda em andamento."
            
        return True, "Validação concluída (Sem alterações)."