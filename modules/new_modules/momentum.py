# modules/new_modules/momentum.py
import os
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

# Tentativa de importar utilitários do projeto; ajustar caminho conforme necessário
try:
    from modules.utils import SafetyUtils, load_json, save_json
except Exception:
    try:
        from utils import SafetyUtils, load_json, save_json
    except Exception:
        # Fallback mínimo para ambientes onde utils não esteja disponível
        class SafetyUtils:
            def safe_float(self, v, default: float = 0.0) -> float:
                try:
                    return float(v)
                except Exception:
                    return default

            def safe_get(self, d: dict, keys: List[str], default: Any = None) -> Any:
                try:
                    for k in keys:
                        d = d[k]
                    return d
                except Exception:
                    return default

        def load_json(path: str) -> Optional[Dict[str, Any]]:
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return None

        def save_json(path: str, obj: Any) -> None:
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(obj, f, indent=2, ensure_ascii=False)
            except Exception:
                pass

# Caminho de cache padrão (portável)
MOMENTUM_CACHE_FILE = os.path.join("cache", "momentum_cache.json")


class MomentumEngine:
    """
    Engine para cálculo de momentum de jogadores.

    Métodos públicos:
    - compute_momentum_series(player_timeseries, window=5) -> pd.Series
    - get_momentum_for_player(player_id, player_series, window=5) -> dict
    - get_cached_momentum(player_id) -> Optional[dict]
    - refresh_cache() -> bool
    """

    def __init__(self, cache_dir: str = "cache", ttl_seconds: int = 3600, safety: Optional[Any] = None):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.cache_file = MOMENTUM_CACHE_FILE
        self.ttl_seconds = ttl_seconds
        self.safety = safety if safety is not None else SafetyUtils()
        self._cache: Dict[str, Any] = self._load_cache()

    def _load_cache(self) -> Dict[str, Any]:
        try:
            cached = load_json(self.cache_file)
            if cached and "last_updated" in cached:
                last = datetime.fromisoformat(cached.get("last_updated"))
                if (datetime.now() - last).total_seconds() < self.ttl_seconds:
                    return cached.get("data", {})
            return {}
        except Exception:
            return {}

    def _save_cache(self) -> None:
        try:
            save_json(self.cache_file, {"last_updated": datetime.now().isoformat(), "data": self._cache})
        except Exception:
            pass

    def compute_momentum_series(self, player_timeseries: Sequence[float], window: int = 5) -> pd.Series:
        """
        Calcula uma série de momentum a partir de uma série histórica numérica.
        - player_timeseries: lista ou pd.Series com valores (mais antigos primeiro, mais recentes por último)
        - window: tamanho da janela para rolling mean sobre pct_change
        Retorna pd.Series com valores de momentum (float).
        """
        if player_timeseries is None:
            return pd.Series(dtype=float)

        try:
            s = pd.Series(list(player_timeseries)).astype(float)
        except Exception:
            # Se não for possível converter, retorna série vazia
            return pd.Series(dtype=float)

        if len(s) < 2:
            return pd.Series([0.0] * len(s))

        try:
            pct = s.pct_change().fillna(0.0)
            momentum = pct.rolling(window=window, min_periods=1).mean().fillna(0.0)
            return momentum
        except Exception:
            return pd.Series([0.0] * len(s))

    def get_momentum_for_player(self, player_id: Any, player_series: Sequence[float], window: int = 5) -> Dict[str, Any]:
        """
        Retorna um dict com:
        - player_id
        - latest_momentum (float)
        - series (list[float])
        - trend ('up'|'down'|'flat')
        Também atualiza cache interno por player_id.
        """
        series = self.compute_momentum_series(player_series, window=window)
        latest = float(series.iloc[-1]) if not series.empty else 0.0

        if latest > 0.01:
            trend = "up"
        elif latest < -0.01:
            trend = "down"
        else:
            trend = "flat"

        result = {
            "player_id": player_id,
            "latest_momentum": round(latest, 6),
            "series": [float(x) for x in series.tolist()],
            "trend": trend,
            "window": int(window),
            "computed_at": datetime.now().isoformat()
        }

        try:
            self._cache[str(player_id)] = {"momentum": result, "updated": datetime.now().isoformat()}
            self._save_cache()
        except Exception:
            pass

        return result

    def get_cached_momentum(self, player_id: Any) -> Optional[Dict[str, Any]]:
        """
        Retorna o item em cache para player_id, se existir.
        """
        return self._cache.get(str(player_id))

    def refresh_cache(self) -> bool:
        """
        Limpa o cache interno e persiste.
        """
        try:
            self._cache = {}
            self._save_cache()
            return True
        except Exception:
            return False


def get_momentum_data(engine: Optional[MomentumEngine], player_ctx: Any, window: int = 5,
                      series_key_candidates: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    """
    Compatibilidade: extrai a série do player_ctx e chama engine.get_momentum_for_player.
    - engine: instância de MomentumEngine (pode ser None)
    - player_ctx: dict com dados do jogador (de onde extraímos a série) ou lista simples
    - window: janela para cálculo
    - series_key_candidates: lista de chaves possíveis para a série (ex.: ['recent_points_series', 'points_series'])
    Retorna dict com momentum ou None se engine não disponível.
    """
    if engine is None:
        return None

    if series_key_candidates is None:
        series_key_candidates = ["recent_points_series", "points_series", "recent_minutes_series", "efficiency_series"]

    series = None
    if isinstance(player_ctx, dict):
        for k in series_key_candidates:
            if k in player_ctx:
                series = player_ctx.get(k)
                break
    elif isinstance(player_ctx, (list, tuple, pd.Series)):
        series = list(player_ctx)

    # Se ainda não encontrou, tentar chaves comuns
    if series is None and isinstance(player_ctx, dict):
        for fallback_key in ["series", "history", "values"]:
            if fallback_key in player_ctx:
                series = player_ctx.get(fallback_key)
                break

    # Garantir lista
    if series is None:
        series = []

    player_id = player_ctx.get("player_id") if isinstance(player_ctx, dict) else None
    return engine.get_momentum_for_player(player_id, series, window=window)


__all__ = [
    "MomentumEngine",
    "get_momentum_data",
]
