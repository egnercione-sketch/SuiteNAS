"""
Sistema de verificação de saúde do pipeline
"""

def validate_pipeline_integrity():
    """
    Valida todos os componentes do pipeline
    Retorna (all_ok, checks_dict)
    """
    checks = {
        "l5_data": {"status": False, "message": "", "critical": True},
        "scoreboard": {"status": False, "message": "", "critical": True},
        "odds": {"status": False, "message": "", "warning": True},
        "dvp": {"status": False, "message": "", "warning": True},
        "injuries": {"status": False, "message": "", "warning": True},
        "cache": {"status": False, "message": "", "warning": True}
    }
    
    # Verificar dados L5
    if 'df_l5' in st.session_state and not st.session_state.df_l5.empty:
        checks["l5_data"]["status"] = True
        checks["l5_data"]["message"] = f"{len(st.session_state.df_l5)} jogadores"
    
    # Verificar scoreboard
    if 'scoreboard' in st.session_state and st.session_state.scoreboard:
        checks["scoreboard"]["status"] = True
        checks["scoreboard"]["message"] = f"{len(st.session_state.scoreboard)} jogos"
    
    # Verificar cache
    cache_dir = os.path.join(CACHE_DIR, "v2")
    if os.path.exists(cache_dir):
        cache_files = len(os.listdir(cache_dir))
        checks["cache"]["status"] = True
        checks["cache"]["message"] = f"{cache_files} arquivos em cache"
    
    # Determinar status geral
    all_critical_ok = all(c["status"] for c in checks.values() if c.get("critical"))
    
    return all_critical_ok, checks