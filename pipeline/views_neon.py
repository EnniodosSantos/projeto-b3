from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv()

NEON_URL = "postgresql://neondb_owner:npg_Hez8ou6XJBNv@ep-rough-hall-acnxgi8w.sa-east-1.aws.neon.tech/neondb?sslmode=require"

from sqlalchemy import create_engine
engine_neon = create_engine(NEON_URL)

with engine_neon.begin() as conn:

    conn.execute(text("DROP VIEW IF EXISTS vw_retorno_diario CASCADE;"))
    conn.execute(text("DROP VIEW IF EXISTS vw_medias_moveis CASCADE;"))
    conn.execute(text("DROP VIEW IF EXISTS vw_volatilidade CASCADE;"))
    conn.execute(text("DROP VIEW IF EXISTS vw_ranking_retorno CASCADE;"))

    conn.execute(text("""
        CREATE VIEW vw_retorno_diario AS
        WITH precos AS (
            SELECT
                a.ticker, a.tipo, c.data, c.fechamento,
                LAG(c.fechamento) OVER (PARTITION BY a.ticker ORDER BY c.data) AS fechamento_anterior
            FROM cotacoes c
            JOIN ativos a ON a.id = c.ativo_id
        )
        SELECT ticker, tipo, data, fechamento,
            ROUND(((fechamento - fechamento_anterior) / fechamento_anterior * 100)::NUMERIC, 2) AS retorno_pct
        FROM precos
        WHERE fechamento_anterior IS NOT NULL;
    """))

    conn.execute(text("""
        CREATE VIEW vw_medias_moveis AS
        SELECT
            a.ticker, c.data, c.fechamento,
            ROUND(AVG(c.fechamento) OVER (
                PARTITION BY a.ticker ORDER BY c.data
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            )::NUMERIC, 4) AS mm20,
            ROUND(AVG(c.fechamento) OVER (
                PARTITION BY a.ticker ORDER BY c.data
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            )::NUMERIC, 4) AS mm50
        FROM cotacoes c
        JOIN ativos a ON a.id = c.ativo_id;
    """))

    conn.execute(text("""
        CREATE VIEW vw_volatilidade AS
        WITH retornos AS (
            SELECT
                a.ticker, a.tipo, c.data, c.fechamento,
                (c.fechamento - LAG(c.fechamento) OVER (PARTITION BY a.ticker ORDER BY c.data))
                / LAG(c.fechamento) OVER (PARTITION BY a.ticker ORDER BY c.data) AS retorno
            FROM cotacoes c
            JOIN ativos a ON a.id = c.ativo_id
        )
        SELECT ticker, tipo, data, fechamento,
            ROUND((STDDEV(retorno) OVER (PARTITION BY ticker) * SQRT(252) * 100)::NUMERIC, 2) AS volatilidade_anual_pct
        FROM retornos;
    """))

    conn.execute(text("""
        CREATE VIEW vw_ranking_retorno AS
        WITH base AS (
            SELECT
                a.ticker, a.tipo,
                FIRST_VALUE(c.fechamento) OVER (
                    PARTITION BY a.ticker ORDER BY c.data
                ) AS preco_inicio,
                LAST_VALUE(c.fechamento) OVER (
                    PARTITION BY a.ticker ORDER BY c.data
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS preco_fim
            FROM cotacoes c
            JOIN ativos a ON a.id = c.ativo_id
            WHERE c.data >= CURRENT_DATE - INTERVAL '1 year'
        )
        SELECT DISTINCT ticker, tipo,
            ROUND(((preco_fim - preco_inicio) / preco_inicio * 100)::NUMERIC, 2) AS retorno_1ano_pct,
            RANK() OVER (ORDER BY (preco_fim - preco_inicio) / preco_inicio DESC) AS ranking
        FROM base;
    """))

print("Views criadas no Neon com sucesso!")
