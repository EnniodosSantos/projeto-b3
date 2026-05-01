import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
sys.path.append('../pipeline')
from conexao import get_engine
from sqlalchemy import create_engine
# Configuracao da pagina
st.set_page_config(
    page_title="B3 Analytics",
    page_icon="📈",
    layout="wide"
)

import os
NEON_URL = os.getenv("NEON_URL")

if NEON_URL:
    engine = create_engine(NEON_URL)
else:
    engine = get_engine()

# Cache para nao recarregar o banco a cada interacao
@st.cache_data
def carregar_dados():
    df = pd.read_sql("""
        SELECT a.ticker, a.tipo, c.data, c.fechamento, c.volume
        FROM cotacoes c
        JOIN ativos a ON a.id = c.ativo_id
        ORDER BY a.ticker, c.data
    """, engine)
    df['data'] = pd.to_datetime(df['data'])
    return df

@st.cache_data
def carregar_retornos():
    df = pd.read_sql("""
        SELECT ticker, tipo, data, fechamento, retorno_pct
        FROM vw_retorno_diario
        ORDER BY ticker, data
    """, engine)
    df['data'] = pd.to_datetime(df['data'])
    return df

@st.cache_data
def carregar_volatilidade():
    return pd.read_sql("""
        SELECT DISTINCT ticker, tipo, volatilidade_anual_pct
        FROM vw_volatilidade
        ORDER BY volatilidade_anual_pct DESC
    """, engine)

@st.cache_data
def carregar_ranking():
    return pd.read_sql("""
        SELECT ticker, tipo, retorno_1ano_pct, ranking
        FROM vw_ranking_retorno
        ORDER BY ranking
    """, engine)

# Carrega dados
df = carregar_dados()
df_retornos = carregar_retornos()
df_vol = carregar_volatilidade()
df_rank = carregar_ranking()

# Header
st.title("📈 B3 Analytics")
st.markdown("Pipeline de dados do mercado financeiro brasileiro — acoes da B3 e criptomoedas")

# Metricas no topo
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total de Ativos", df['ticker'].nunique())
col2.metric("Registros no Banco", f"{len(df):,}")
col3.metric("Periodo", f"{df['data'].min().year} — {df['data'].max().year}")
col4.metric("Melhor Retorno 1 Ano", f"{df_rank.iloc[0]['ticker']} +{df_rank.iloc[0]['retorno_1ano_pct']}%")

st.divider()

# Secao 1: Evolucao de preco
st.subheader("Evolucao de Preco")

col_filtro1, col_filtro2 = st.columns([1, 3])
with col_filtro1:
    tipo_sel = st.radio("Tipo", ["acao", "cripto"])

tickers_disponiveis = sorted(df[df['tipo'] == tipo_sel]['ticker'].unique())
with col_filtro2:
    tickers_sel = st.multiselect(
        "Ativos",
        tickers_disponiveis,
        default=tickers_disponiveis[:3]
    )

if tickers_sel:
    dados_graf = df[df['ticker'].isin(tickers_sel)]
    
    # Normaliza base 100
    frames = []
    for ticker in tickers_sel:
        d = dados_graf[dados_graf['ticker'] == ticker].copy()
        d['preco_norm'] = (d['fechamento'] / d['fechamento'].iloc[0]) * 100
        frames.append(d)
    dados_norm = pd.concat(frames)
    
    fig = px.line(dados_norm, x='data', y='preco_norm', color='ticker',
                  labels={'preco_norm': 'Preco Normalizado (base 100)', 'data': ''},
                  height=400)
    fig.add_hline(y=100, line_dash="dash", line_color="gray", opacity=0.5)
    fig.update_layout(legend_title="Ativo")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# Secao 2: Ranking e Volatilidade
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Ranking — Retorno Ultimo Ano")
    cores = df_rank['tipo'].map({'acao': '#3A86FF', 'cripto': '#FF6B6B'})
    fig2 = px.bar(df_rank, x='retorno_1ano_pct', y='ticker',
                  orientation='h', color='tipo',
                  color_discrete_map={'acao': '#3A86FF', 'cripto': '#FF6B6B'},
                  labels={'retorno_1ano_pct': 'Retorno (%)', 'ticker': ''},
                  height=500)
    fig2.add_vline(x=0, line_color="black", line_width=0.8)
    st.plotly_chart(fig2, use_container_width=True)

with col_b:
    st.subheader("Volatilidade Anualizada")
    fig3 = px.bar(df_vol, x='volatilidade_anual_pct', y='ticker',
                  orientation='h', color='tipo',
                  color_discrete_map={'acao': '#3A86FF', 'cripto': '#FF6B6B'},
                  labels={'volatilidade_anual_pct': 'Volatilidade (%)', 'ticker': ''},
                  height=500)
    fig3.add_vline(x=30, line_dash="dash", line_color="orange", line_width=1)
    st.plotly_chart(fig3, use_container_width=True)

st.divider()

# Secao 3: Distribuicao de retornos
st.subheader("Distribuicao de Retornos Diarios")

ticker_dist = st.selectbox("Selecione o ativo", sorted(df_retornos['ticker'].unique()))
dados_dist = df_retornos[df_retornos['ticker'] == ticker_dist]['retorno_pct'].dropna()

fig4 = px.histogram(dados_dist, nbins=80,
                    labels={'value': 'Retorno Diario (%)', 'count': 'Frequencia'},
                    height=350)
fig4.add_vline(x=0, line_dash="dash", line_color="red")
fig4.add_vline(x=dados_dist.mean(), line_dash="dot", line_color="orange",
               annotation_text=f"Media: {dados_dist.mean():.2f}%")
st.plotly_chart(fig4, use_container_width=True)

# Rodape
st.divider()
st.caption("Dados: Yahoo Finance via yfinance | Banco: PostgreSQL | Stack: Python, SQLAlchemy, Streamlit")
