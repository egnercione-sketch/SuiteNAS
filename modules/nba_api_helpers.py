import requests

def get_espn_boxscore(game_id):
    """Busca o JSON completo do jogo na ESPN."""
    if not game_id: return None
    
    # Endpoint de Resumo (Contém stats completos)
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={game_id}"
    
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except:
        return None
    return None