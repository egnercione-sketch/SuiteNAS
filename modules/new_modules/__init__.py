# modules/new_modules/__init__.py
"""
Módulos do sistema DeepDeepb
"""

# Defina __all__ primeiro, com uma lista vazia
__all__ = []

# Importações condicionais
try:
    from .monte_carlo import MonteCarloEngine
    __all__.append('MonteCarloEngine')
except ImportError:
    pass

try:
    from .pace_adjuster import PaceAdjuster
    __all__.append('PaceAdjuster')
except ImportError:
    pass

try:
    from .vacuum_matrix import VacuumMatrixAnalyzer
    __all__.append('VacuumMatrixAnalyzer')
except ImportError:
    pass

try:
    from .thesis_engine import ThesisEngine, create_thesis_engine, SimpleThesisFallback
    __all__.extend(['ThesisEngine', 'create_thesis_engine', 'SimpleThesisFallback'])
except ImportError:
    pass

try:
    from .dvp_analyzer import DvPAnalyzer
    __all__.append('DvPAnalyzer')
except ImportError:
    pass

try:
    from .rotation_ceiling_engine import RotationCeilingEngine
    __all__.append('RotationCeilingEngine')
except ImportError:
    pass

try:
    from .player_classifier import PlayerClassifier
    __all__.append('PlayerClassifier')
except ImportError:
    pass

try:
    from .narrative_intelligence import NarrativeIntelligence
    __all__.append('NarrativeIntelligence')
except ImportError:
    pass

try:
    from .correlation_filters import CorrelationValidator
    __all__.append('CorrelationValidator')
except ImportError:
    pass

try:
    from .trend_analyzer import TrendAnalyzer
    __all__.append('TrendAnalyzer')
except ImportError:
    pass

try:
    from .strategy_engine import StrategyEngine
    __all__.append('StrategyEngine')
except ImportError:
    pass

try:
    from .desdobrador_inteligente import DesdobradorInteligente
    __all__.append('DesdobradorInteligente')
except ImportError:
    pass

try:
    from .multipla_do_dia import StreakHunterMultipla
    __all__.append('StreakHunterMultipla')
except ImportError:
    pass

# Trixie Central Unificada - ADICIONE ESTAS LINHAS:
try:
    from .trixie_central_unified import UnifiedTrixieCentral, show_unified_trixie_central
    __all__.extend(['UnifiedTrixieCentral', 'show_unified_trixie_central'])
except ImportError as e:
    print(f"Aviso: Trixie Central Unificada não disponível: {e}")
    pass

# Se precisar do TrixieCacheManager separadamente (opcional)
try:
    from .trixie_central_unified import TrixieCacheManager
    __all__.append('TrixieCacheManager')
except ImportError:
    pass