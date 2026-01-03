# modules/audit_system.py
# VERSÃO V4.5 - CLOUD NATIVE + ANTI-BLOCK HEADERS
# Auditoria de apostas (Trixies/Simples) com persistência no Supabase.

import os
import json
import logging
import requests
import hashlib
from datetime import datetime

# Importa DbManager
try:
    from db_manager import db
except ImportError:
    db = None
    print("⚠️ AuditSystem: db_manager não encontrado. Usando memória local.")

# Chave do Supabase
KEY_AUDIT = "audit_trixies"
logger = logging.getLogger("AuditSystem")

# HEADERS DE NAVEGADOR (Para passar pelo bloqueio da ESPN)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.espn.com/",
    "Origin": "https://www.espn.com"
}

class AuditSystem:
    def __init__(self):
        self.audit_data = self._load_data()

    def _load_data(self):
        """Carrega dados: Prioridade Nuvem -> Vazio"""
        if db:
            try:
                data = db.get_data(KEY_AUDIT)
                if data and isinstance(data, list):
                    return data
            except Exception as e:
                logger.error(f"Erro ao baixar audit da nuvem: {e}")
        return []

    def _persist(self):
        """Salva dados na Nuvem"""
        if db:
            try:
                # Mantém apenas os últimos 500 registros para não estourar o banco
                if len(self.audit_data) > 500:
                    self.audit_data = self.audit_data[-500:]
                db.save_data(KEY_AUDIT, self.audit_data)
            except Exception as e:
                logger.error(f"Erro ao salvar audit na nuvem: {e}")

    # =========================================================================
    # CORE: LOGGING (PADRONIZADO)
    # =========================================================================
    def log_trixie(self, trixie_data, game_info=None, category=None, source="StrategyEngine"):
        # 1. Recarrega dados frescos da nuvem para evitar conflitos
        self.audit_data = self._load_data()
        
        # 2. Gera ID Determinístico (MD5 do conteúdo) para evitar duplicatas reais
        content_str = str(trixie_data.get('legs', [])) + str(trixie_data.get('total_odd', 0))
        t_id = hashlib.md5(content_str.encode()).hexdigest()[:16]
        
        # 3. Verifica duplicidade
        if any(t.get('id') == t_id for t in self.audit_data):
            return False # Já existe

        raw_legs = trixie_data.get('players', []) or trixie_data.get('legs', [])
        clean_legs = []

        for l in raw_legs:
            # Tenta recuperar ID do jogo da perna ou do ticket
            leg_game_id = l.get('game_id')
            
            # Fallback se não tiver ID na leg
            if (not leg_game_id or leg_game_id == "UNK") and game_info:
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

        self.audit_data.insert(0, final_ticket) # Adiciona no topo
        self._persist()
        return True

    def delete_ticket(self, ticket_id):
        self.audit_data = self._load_data()
        initial_len = len(self.audit_data)
        self.audit_data = [t for t in self.audit_data if t.get('id') != ticket_id]
        
        if len(self.audit_data) < initial_len:
            self._persist()
            return True
        return False

    # =========================================================================
    # INTEGRAÇÃO ESPN (COM HEADERS ANTI-BLOQUEIO)
    # =========================================================================
    def fetch_espn_boxscore(self, game_id):
        if not game_id or str(game_id) in ['MULTI', 'MIX', 'UNK', 'None']: 
            return None
        
        try:
            url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={game_id}"
            # USA OS HEADERS GLOBAIS
            r = requests.get(url, timeout=6, headers=HEADERS)
            if r.status_code == 200: 
                return r.json()
        except Exception as e: 
            logger.warning(f"Erro boxscore ESPN {game_id}: {e}")
        return None

    def _extract_player_stats(self, boxscore, player_name):
        """Extração inteligente baseada em labels da ESPN."""
        try:
            bs = boxscore.get('boxscore', {})
            teams = bs.get('players', []) 
            if not teams: teams = bs.get('teams', [])

            p_clean = player_name.lower().replace('.', '').replace("'", "").strip()
            
            for team in teams:
                stats_block = team.get('statistics', [])
                if not stats_block: continue
                
                # Mapa dinâmico de colunas (Labels)
                labels = stats_block[0].get('labels', [])
                idx_map = {label: i for i, label in enumerate(labels)}
                
                idx_pts = idx_map.get("PTS", -1)
                idx_reb = idx_map.get("REB", -1)
                idx_ast = idx_map.get("AST", -1)
                idx_stl = idx_map.get("STL", -1)
                idx_blk = idx_map.get("BLK", -1)
                idx_to  = idx_map.get("TO", -1)
                idx_3pt = idx_map.get("3PT", -1)
                if idx_3pt == -1: idx_3pt = idx_map.get("3PM", -1)

                athletes = stats_block[0].get('athletes', [])
                
                for ath in athletes:
                    ath_name = ath.get('athlete', {}).get('displayName', '').lower().replace('.', '').replace("'", "").strip()
                    
                    # Fuzzy match simples
                    if p_clean in ath_name or ath_name in p_clean:
                        stats = ath.get('stats', [])
                        
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
                            "REB": get_val(idx_reb),
                            "AST": get_val(idx_ast),
                            "STL": get_val(idx_stl),
                            "BLK": get_val(idx_blk),
                            "3PM": t3,
                            "TOV": get_val(idx_to)
                        }
        except Exception:
            pass
        return None

    # =========================================================================
    # SMART VALIDATION
    # =========================================================================
    def smart_validate_ticket(self, ticket_id):
        # 1. Carrega dados frescos
        self.audit_data = self._load_data()
        
        ticket = next((t for t in self.audit_data if t.get('id') == ticket_id), None)
        if not ticket: return False, "Bilhete não encontrado."

        legs = ticket.get('legs', [])
        bs_cache = {}
        updates = False
        
        report = {"total": len(legs), "updated": 0, "no_id": 0, "game_pending": 0}
        any_lost = False

        for leg in legs:
            # Se já ganhou ou perdeu, não valida de novo (a menos que queira forçar)
            if leg.get('status') in ['WIN', 'LOSS'] and leg.get('actual_value', 0) > 0:
                continue

            g_id = leg.get('game_id')
            
            # Tenta achar ID no ticket se não tiver na perna
            if not g_id or str(g_id) in ['MULTI', 'MIX', 'UNK']:
                ticket_gid = ticket.get('game_info', {}).get('game_id')
                if ticket_gid and str(ticket_gid) not in ['MULTI', 'MIX']:
                    g_id = ticket_gid
                else:
                    report["no_id"] += 1
                    continue

            # Cache de Boxscore para não spammar API
            if g_id not in bs_cache:
                bs = self.fetch_espn_boxscore(g_id)
                if bs: bs_cache[g_id] = bs
                else: continue 
            
            boxscore = bs_cache[g_id]
            stats = self._extract_player_stats(boxscore, leg.get('player_name', ''))
            
            # Verifica se jogo acabou
            try:
                game_status = boxscore.get('header', {}).get('competitions', [{}])[0].get('status', {}).get('type', {})
                is_final = game_status.get('completed', False)
                if not is_final: report["game_pending"] += 1
            except: is_final = False

            if stats:
                mkt = leg.get('market_type', 'UNK')
                line = float(leg.get('line', 0))
                actual = 0.0
                
                # Mapeamento de mercados
                if mkt == "PTS": actual = stats['PTS']
                elif mkt == "REB": actual = stats['REB']
                elif mkt == "AST": actual = stats['AST']
                elif mkt == "STL": actual = stats['STL']
                elif mkt == "BLK": actual = stats['BLK']
                elif mkt == "3PM": actual = stats['3PM']
                elif mkt == "PRA": actual = stats['PTS'] + stats['REB'] + stats['AST']
                
                # Combo markets (Ex: PTS+AST)
                if "+" in mkt:
                    parts = mkt.split('+')
                    val_sum = 0
                    for p in parts: val_sum += stats.get(p, 0)
                    actual = val_sum

                leg['actual_value'] = actual
                
                # Lógica de Win/Loss
                if actual >= line:
                    leg['status'] = 'WIN'
                elif is_final:
                    leg['status'] = 'LOSS'
                    any_lost = True
                
                updates = True
                report["updated"] += 1
            
            elif is_final:
                # Jogo acabou e jogador não encontrado -> DNP (Did Not Play) -> Loss
                leg['status'] = 'LOSS'
                leg['actual_value'] = 0
                any_lost = True
                updates = True
                report["updated"] += 1

        # Atualiza Status Global do Ticket
        current_legs = ticket.get('legs', [])
        has_loss = any(l.get('status') == 'LOSS' for l in current_legs)
        all_wins = all(l.get('status') == 'WIN' for l in current_legs)
        
        if has_loss: ticket['status'] = 'LOSS'
        elif all_wins: ticket['status'] = 'WIN'
        
        if updates: 
            self._persist() # Salva na nuvem

        if report["updated"] > 0:
            return True, f"✅ Atualizado! {report['updated']} estatísticas corrigidas."
        if report["game_pending"] > 0:
            return True, "Jogos ainda em andamento. Volte mais tarde."
            
        return True, "Validação concluída."
