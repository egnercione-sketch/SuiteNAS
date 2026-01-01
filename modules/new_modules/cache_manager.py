# modules/new_modules/cache_manager.py

"""
Cache Manager - Sistema de cache determinístico para Trixies
Garante que IDs e odds permaneçam consistentes entre execuções
"""

import json
import os
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional

class TrixieCacheManager:
    """Gerenciador de cache para trixies com IDs determinísticos"""
    
    def __init__(self, cache_dir: str = "cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.cache_file = os.path.join(cache_dir, "trixies_cache.json")
        self.cache = self._load_cache()
    
    def _load_cache(self) -> Dict[str, Any]:
        """Carrega cache do arquivo"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            print(f"❌ Erro ao carregar cache: {e}")
        
        return {
            "version": "1.0",
            "created_at": datetime.now().isoformat(),
            "trixies": {},
            "stats": {
                "total_trixies": 0,
                "last_updated": None
            }
        }
    
    def _save_cache(self):
        """Salva cache no arquivo"""
        try:
            self.cache["stats"]["last_updated"] = datetime.now().isoformat()
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"❌ Erro ao salvar cache: {e}")
    
    def generate_deterministic_id(self, trixie_data: Dict[str, Any]) -> str:
        """Gera ID determinístico baseado nos dados da trixie"""
        
        # Extrair dados essenciais para hash
        essential_data = {
            "category": trixie_data.get("category", ""),
            "sub_category": trixie_data.get("sub_category", ""),
            "game_info": trixie_data.get("game_info", {}),
            "legs": []
        }
        
        # Adicionar legs ordenadas
        for leg in sorted(trixie_data.get("legs", []), key=lambda x: (x.get('name', ''), x.get('market', ''))):
            essential_data["legs"].append({
                "name": leg.get("name", ""),
                "team": leg.get("team", ""),
                "market": leg.get("market", ""),
                "line": leg.get("line", 0),
                "odds": leg.get("odds", 0)
            })
        
        # Converter para string JSON ordenada
        json_str = json.dumps(essential_data, sort_keys=True, ensure_ascii=False)
        
        # Gerar hash MD5
        hash_obj = hashlib.md5(json_str.encode('utf-8'))
        return hash_obj.hexdigest()[:8]
    
    def save_trixies(self, trixies: List[Dict[str, Any]], game_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Salva trixies no cache, garantindo IDs consistentes
        
        Retorna as trixies com IDs atualizados (novos ou existentes)
        """
        updated_trixies = []
        
        for trixie in trixies:
            # Gerar ID determinístico
            trixie_id = self.generate_deterministic_id(trixie)
            
            # Verificar se já existe no cache
            cached_trixie = self.cache["trixies"].get(trixie_id)
            
            if cached_trixie:
                # Usar dados do cache (mantém consistência)
                cached_trixie["last_accessed"] = datetime.now().isoformat()
                updated_trixies.append(cached_trixie)
            else:
                # Nova trixie - adicionar ao cache
                trixie["id"] = trixie_id
                trixie["created_at"] = datetime.now().isoformat()
                trixie["last_accessed"] = datetime.now().isoformat()
                trixie["cache_key"] = self._generate_game_key(game_context)
                
                self.cache["trixies"][trixie_id] = trixie
                updated_trixies.append(trixie)
        
        # Atualizar estatísticas
        self.cache["stats"]["total_trixies"] = len(self.cache["trixies"])
        self.cache["stats"]["last_updated"] = datetime.now().isoformat()
        
        # Salvar cache
        self._save_cache()
        
        print(f"💾 Cache atualizado: {len(updated_trixies)} trixies (Total: {len(self.cache['trixies'])})")
        return updated_trixies
    
    def _generate_game_key(self, game_context: Dict[str, Any]) -> str:
        """Gera chave única para o jogo"""
        home = game_context.get("home", "UNK")
        away = game_context.get("away", "UNK")
        date = game_context.get("date", datetime.now().strftime("%Y%m%d"))
        
        return f"{home}_{away}_{date}"
    
    def get_trixies_by_category(self, category: str) -> List[Dict[str, Any]]:
        """Recupera trixies por categoria"""
        return [
            trixie for trixie in self.cache["trixies"].values()
            if trixie.get("category") == category
        ]
    
    def get_trixies_by_game(self, home: str, away: str, date: str = None) -> List[Dict[str, Any]]:
        """Recupera trixies por jogo"""
        if date is None:
            date = datetime.now().strftime("%Y%m%d")
        
        game_key = f"{home}_{away}_{date}"
        
        return [
            trixie for trixie in self.cache["trixies"].values()
            if trixie.get("cache_key") == game_key
        ]
    
    def clear_old_trixies(self, days_old: int = 7):
        """Remove trixies antigas do cache"""
        cutoff_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        old_count = 0
        new_trixies = {}
        
        for trixie_id, trixie in self.cache["trixies"].items():
            created_at = datetime.fromisoformat(trixie.get("created_at", "2000-01-01"))
            
            if (cutoff_date - created_at).days <= days_old:
                new_trixies[trixie_id] = trixie
            else:
                old_count += 1
        
        self.cache["trixies"] = new_trixies
        self.cache["stats"]["total_trixies"] = len(new_trixies)
        
        self._save_cache()
        print(f"🧹 Removidas {old_count} trixies antigas (> {days_old} dias)")
    
    def get_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas do cache"""
        return {
            "total_trixies": self.cache["stats"]["total_trixies"],
            "last_updated": self.cache["stats"]["last_updated"],
            "categories": self.get_category_stats(),
            "age_distribution": self.get_age_distribution()
        }
    
    def get_category_stats(self) -> Dict[str, int]:
        """Retorna contagem por categoria"""
        stats = {}
        for trixie in self.cache["trixies"].values():
            category = trixie.get("category", "UNKNOWN")
            stats[category] = stats.get(category, 0) + 1
        return stats
    
    def get_age_distribution(self) -> Dict[str, int]:
        """Distribuição por idade das trixies"""
        distribution = {"0-1 dias": 0, "1-3 dias": 0, "3-7 dias": 0, ">7 dias": 0}
        
        for trixie in self.cache["trixies"].values():
            created_at = datetime.fromisoformat(trixie.get("created_at", datetime.now().isoformat()))
            age_days = (datetime.now() - created_at).days
            
            if age_days < 1:
                distribution["0-1 dias"] += 1
            elif age_days < 3:
                distribution["1-3 dias"] += 1
            elif age_days < 7:
                distribution["3-7 dias"] += 1
            else:
                distribution[">7 dias"] += 1
        
        return distribution