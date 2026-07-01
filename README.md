# Projeto B3 — Pipeline de Dados do Mercado Financeiro

Pipeline de dados completo cobrindo ações da B3 e criptomoedas: extração, armazenamento em PostgreSQL, análise SQL avançada e dashboard interativo em produção.

**App em produção:** [projeto-b3-ennio.streamlit.app](https://projeto-b3-ennio.streamlit.app/)

---

## Visão geral

29.190 registros de cotações diárias (2021–2026) de 19 ações do IBOVESPA e 3 criptomoedas (BTC, ETH, SOL), coletados via Yahoo Finance, armazenados em PostgreSQL e expostos através de views SQL analíticas e um dashboard Streamlit conectado em tempo real a um banco PostgreSQL serverless (Neon).

## Stack

| Camada | Tecnologia |
|---|---|
| Coleta | Python, yfinance |
| Banco de dados | PostgreSQL 16, Docker (dev) / Neon (produção) |
| ETL | Pandas, SQLAlchemy |
| Análise | SQL (window functions, CTEs), Jupyter |
| Visualização | Streamlit, Plotly, Matplotlib/Seaborn |

## Arquitetura

```
Yahoo Finance (yfinance)
        │
        ▼
  extracao.py  →  carga.py  →  PostgreSQL
                                    │
                        ┌───────────┼───────────┐
                        ▼           ▼           ▼
                  views SQL    Jupyter EDA   Streamlit
                  (retorno,    (5 gráficos   (dashboard
                  volatilidade, analíticos)   interativo)
                  ranking)
```

## Banco de dados

Schema normalizado (3FN) com 4 tabelas: `ativos`, `cotacoes`, `indicadores`, `macroeconomico`. Chaves estrangeiras e constraints `UNIQUE` compostas garantem integridade referencial e idempotência na carga.

4 views analíticas usando window functions (`LAG`, `AVG OVER`, `STDDEV OVER`, `RANK`) e CTEs:

- `vw_retorno_diario` — variação percentual diária
- `vw_medias_moveis` — médias móveis de 20 e 50 dias
- `vw_volatilidade` — volatilidade anualizada
- `vw_ranking_retorno` — ranking de retorno acumulado em 1 ano

## Principais insights da EDA

- Solana (86,9%) e Magazine Luiza (69,8%) lideram em volatilidade anualizada — Bitcoin é proporcionalmente mais estável (46,3%)
- Correlação entre criptomoedas e ações da B3 é próxima de zero — mercados praticamente descorrelacionados no período
- PETR4 liderou o retorno acumulado de 1 ano (+75,4%), à frente de VALE3 e GGBR4

## Como rodar localmente

```bash
git clone git@github.com:EnniodosSantos/projeto-b3.git
cd projeto-b3
cp .env.example .env          # preencher com suas credenciais

docker compose up -d          # sobe PostgreSQL + pgAdmin

conda create -n projeto-b3 python=3.10 -y
conda activate projeto-b3
pip install -r requirements.txt

cd pipeline
python carga.py               # extrai e carrega os dados

cd ../dashboard
python -m streamlit run app.py
```

## Estrutura

```
projeto-b3/
├── pipeline/        # scripts de extração, carga e migração
├── analytics/        # notebook de EDA
├── dashboard/        # app Streamlit
├── docs/              # gráficos exportados
└── docker-compose.yml
```

## Próximos passos

- Indicadores fundamentalistas (P/L, ROE, Dividend Yield) via brapi.dev
- Dados macroeconômicos (SELIC, IPCA, câmbio) via API do Banco Central
- Modelo de forecasting com Prophet
- Agendamento automático da coleta diária

---

**Ennio Bernardo Pessoa dos Santos** — [Portfólio](https://enniodossantos.github.io/) · [LinkedIn](https://linkedin.com/in/enniobernardo)
