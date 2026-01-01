# modules/new_modules/render_engine.py

"""
Render Engine - Sistema de renderização profissional para Trixies
Design moderno e limpo, sem botões desnecessários
"""

import streamlit as st
from typing import Dict, List, Any
import pandas as pd

class TrixieRenderer:
    """Renderizador profissional de Trixies"""
    
    @staticmethod
    def render_trixie_card(trixie: Dict[str, Any], show_buttons: bool = False, index: int = 0):
        """
        Renderiza uma Trixie com design de cartão profissional
        
        Args:
            trixie: Dicionário com dados da trixie
            show_buttons: Se True, mostra botões (padrão: False)
            index: Índice para identificação única
        """
        
        # Configuração de cores por categoria
        category_config = {
            "CONSERVADORA": {
                "color": "#10B981",  # Verde
                "bg_color": "#ECFDF5",
                "icon": "🛡️",
                "title": "Conservadora"
            },
            "OUSADA": {
                "color": "#F59E0B",  # Laranja
                "bg_color": "#FFFBEB",
                "icon": "🔥",
                "title": "Ousada"
            },
            "EXPLOSAO": {
                "color": "#8B5CF6",  # Roxo
                "bg_color": "#F5F3FF",
                "icon": "💥",
                "title": "Explosão"

            },
            "EXPLOSÃO": {
                "color": "#8B5CF6",  # Roxo
                "bg_color": "#F5F3FF",
                "icon": "💥",
                "title": "Explosão"

            },
            "VERSATIL": {
                "color": "#3B82F6",  # Azul
                "bg_color": "#EFF6FF",
                "icon": "🎯",
                "title": "Versátil"
            },
            "VERSÁTIL": {
                "color": "#3B82F6",  # Azul
                "bg_color": "#EFF6FF",
                "icon": "🎯",
                "title": "Versátil"
            },
            "BANCO": {
                "color": "#6B7280",  # Cinza
                "bg_color": "#F9FAFB",
                "icon": "🪑",
                "title": "Banco"
            }
        }
        
        config = category_config.get(
            trixie['category'], 
            {"color": "#6B7280", "bg_color": "#F9FAFB", "icon": "📊", "title": trixie['category']}
        )
        
        with st.container():
            # Cabeçalho do Cartão
            st.markdown(f"""
            <div style="
                background: linear-gradient(135deg, {config['color']} 0%, {config['color']}99 100%);
                padding: 16px 20px;
                border-radius: 12px 12px 0 0;
                color: white;
                margin-bottom: 0;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            ">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <h3 style="margin:0; font-size: 1.1rem; font-weight: 600;">
                            {config['icon']} {config['title']} - {trixie['sub_category']}
                        </h3>
                        <p style="margin:4px 0 0 0; font-size:0.85rem; opacity:0.9;">
                            ID: <code>{trixie['id']}</code> • {len(trixie['legs'])} jogadores
                        </p>
                    </div>
                    <div style="
                        background: rgba(255,255,255,0.2);
                        padding: 6px 12px;
                        border-radius: 20px;
                        font-size: 1.2rem;
                        font-weight: bold;
                    ">
                        {trixie['total_odd']}
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Corpo do Cartão
            st.markdown(f"""
            <div style="
                background-color: {config['bg_color']};
                padding: 20px;
                border-radius: 0 0 12px 12px;
                border: 1px solid #E5E7EB;
                border-top: none;
                margin-bottom: 24px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            ">
            """, unsafe_allow_html=True)
            
            # Lista de Jogadores
            for i, leg in enumerate(trixie['legs'], 1):
                col1, col2, col3 = st.columns([3, 1, 2])
                
                with col1:
                    # Nome do jogador e time
                    st.markdown(f"**{leg['name']}** ({leg['team']})", help=f"ID: {leg.get('player_id', 'N/A')}")
                    
                    # Linha de mercado
                    market_display = leg.get('market_display', f"{leg['line']}+ {leg['market']}")
                    st.markdown(f"`{market_display}`")
                
                with col2:
                    # Odd
                    st.markdown(f"**Odd:** {leg['odds']}")
                    
                    # Confiança (se disponível)
                    if 'confidence' in leg:
                        confidence = leg['confidence']
                        color = "#10B981" if confidence >= 0.7 else "#F59E0B" if confidence >= 0.5 else "#EF4444"
                        st.markdown(f"<span style='font-size:0.8rem; color:{color};'>Conf: {confidence:.0%}</span>", 
                                  unsafe_allow_html=True)
                
                with col3:
                    # Tese
                    thesis = leg.get('primary_thesis', leg.get('thesis', 'Análise Técnica'))
                    # Formatar tese para melhor exibição
                    thesis_display = TrixieRenderer._format_thesis_display(thesis)
                    st.markdown(f"<div style='font-size:0.9rem; color:#4B5563;'>{thesis_display}</div>", 
                              unsafe_allow_html=True)
                
                # Linha divisória entre jogadores (não no último)
                if i < len(trixie['legs']):
                    st.markdown("<hr style='margin: 12px 0; border-color: #E5E7EB; opacity:0.5;'>", 
                              unsafe_allow_html=True)
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            # Estratégia Resumida (collapsible)
            with st.expander("📋 Estratégia Detalhada", expanded=False):
                st.markdown(f"**{trixie['strategy_narrative']}**")
                
                # Informações adicionais do jogo
                if 'game_info' in trixie:
                    game = trixie['game_info']
                    st.caption(f"Jogo: {game.get('away', '')} @ {game.get('home', '')} • {game.get('game_date', '')}")
            
            # REMOVIDO: Botões desnecessários
            # if show_buttons: ... (não renderiza nada)
            
            st.markdown("<br>", unsafe_allow_html=True)
    
    @staticmethod
    def _format_thesis_display(thesis_text: str) -> str:
        """Formata texto da tese para exibição mais limpa"""
        # Mapeamento de abreviações/formatos
        thesis_map = {
            "VolumeScorer": "Scorer Volume",
            "GlassCleaner": "Dominante Rebotes", 
            "Playmaker": "Playmaker Criativo",
            "DefensiveAnchor": "Ancora Defensivo",
            "Sniper": "Especialista 3PT",
            "SafeMinutes": "Minutos Seguros",
            "MatchupExploit": "Matchup Favorável",
            "VacuumBoost": "Oportunidade Vacuum",
            "CeilingUpside": "Alto Teto",
            "AnaliseTecnica": "Análise Técnica",
            "Unknown": "Análise Padrão"
        }
        
        # Se a tese está no mapa, substituir
        for key, value in thesis_map.items():
            if key in thesis_text:
                return value
        
        # Se não, retornar os primeiros 30 caracteres
        return thesis_text[:30] + "..." if len(thesis_text) > 30 else thesis_text
    
    @staticmethod
    def render_category_header(category: str, trixies_count: int, avg_odd: float = None):
        """Renderiza cabeçalho para uma categoria de trixies"""
        
        category_headers = {
            "CONSERVADORA": {"icon": "🛡️", "color": "#10B981"},
            "OUSADA": {"icon": "🔥", "color": "#F59E0B"},
            "EXPLOSÃO": {"icon": "💥", "color": "#8B5CF6"},
            "VERSÁTIL": {"icon": "🎯", "color": "#3B82F6"},
            "BANCO": {"icon": "🪑", "color": "#6B7280"}
        }
        
        config = category_headers.get(category, {"icon": "📊", "color": "#6B7280"})
        
        st.markdown(f"""
        <div style="
            background: linear-gradient(135deg, {config['color']}15 0%, {config['color']}05 100%);
            padding: 16px 20px;
            border-radius: 10px;
            border-left: 4px solid {config['color']};
            margin: 20px 0;
        ">
            <h3 style="margin:0; color:{config['color']};">
                {config['icon']} {category}
            </h3>
            <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 8px;">
                <span style="color:#6B7280; font-size:0.95rem;">
                    {trixies_count} Trixies disponíveis
                </span>
                {f'<span style="font-weight:bold; color:#374151;">Odd Média: {avg_odd:.2f}</span>' if avg_odd else ''}
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    @staticmethod
    def render_empty_state(category: str):
        """Renderiza estado vazio quando não há trixies"""
        st.info(f"⏳ Nenhuma trixie gerada para a categoria **{category}**.", icon="ℹ️")
    
    @staticmethod
    def render_trixies_grid(trixies_by_category: Dict[str, List[Dict]]):
        """Renderiza todas as trixies organizadas por categoria"""
        
        for category, trixies in trixies_by_category.items():
            if not trixies:
                TrixieRenderer.render_empty_state(category)
                continue
            
            # Calcular estatísticas da categoria
            avg_odd = sum(t['total_odd'] for t in trixies) / len(trixies)
            total_players = len(set(leg['name'] for t in trixies for leg in t['legs']))
            
            # Renderizar cabeçalho da categoria
            TrixieRenderer.render_category_header(category, len(trixies), avg_odd)
            
            # Estatísticas rápidas
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Trixies", len(trixies))
            with col2:
                st.metric("Odd Média", f"{avg_odd:.2f}")
            with col3:
                st.metric("Jogadores Únicos", total_players)
            
            # Renderizar cada trixie
            for i, trixie in enumerate(trixies):
                TrixieRenderer.render_trixie_card(trixie, show_buttons=False, index=i)
    
    @staticmethod
    def render_compact_view(trixies: List[Dict]):
        """Renderização compacta para visão geral"""
        
        if not trixies:
            st.warning("⚠️ Nenhuma trixie disponível no momento.")
            return
        
        # Criar DataFrame para visualização compacta
        data = []
        for trixie in trixies:
            for leg in trixie['legs']:
                data.append({
                    'Trixie ID': trixie['id'][:6],
                    'Categoria': trixie['category'],
                    'Jogador': leg['name'],
                    'Time': leg['team'],
                    'Mercado': leg.get('market_display', f"{leg['line']}+ {leg['market']}"),
                    'Odd': leg['odds'],
                    'Tese': leg.get('primary_thesis', 'N/A')
                })
        
        df = pd.DataFrame(data)
        
        # Agrupar por Trixie ID para visualização
        pivot_df = df.pivot_table(
            index=['Trixie ID', 'Categoria'],
            values=['Jogador', 'Mercado', 'Odd'],
            aggfunc=lambda x: ', '.join(str(v) for v in x)
        ).reset_index()
        
        # Exibir tabela
        st.dataframe(
            pivot_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                'Trixie ID': st.column_config.TextColumn(width="small"),
                'Categoria': st.column_config.TextColumn(width="small"),
                'Jogador': st.column_config.TextColumn("Jogadores"),
                'Mercado': st.column_config.TextColumn("Mercados"),
                'Odd': st.column_config.TextColumn("Odds")
            }
        )