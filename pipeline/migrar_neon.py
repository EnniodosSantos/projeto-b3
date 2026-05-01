import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Engine local
engine_local = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@localhost:5432/{os.getenv('POSTGRES_DB')}"
)

# Engine Neon
NEON_URL = "postgresql://neondb_owner:npg_Hez8ou6XJBNv@ep-rough-hall-acnxgi8w.sa-east-1.aws.neon.tech/neondb?sslmode=require"
engine_neon = create_engine(NEON_URL)

def migrar_tabela(tabela):
    print(f"Migrando {tabela}...")
    df = pd.read_sql(f"SELECT * FROM {tabela}", engine_local)
    with engine_neon.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {tabela} CASCADE"))
    df.to_sql(tabela, engine_neon, if_exists='append', index=False)
    print(f"  {len(df)} registros migrados")

with engine_neon.connect() as conn:
    conn.execute(text("""
        DROP TABLE IF EXISTS cotacoes CASCADE;
        DROP TABLE IF EXISTS indicadores CASCADE;
        DROP TABLE IF EXISTS macroeconomico CASCADE;
        DROP TABLE IF EXISTS ativos CASCADE;
    """))
    conn.execute(text("""
        CREATE TABLE ativos (
            id          SERIAL PRIMARY KEY,
            ticker      VARCHAR(10) NOT NULL UNIQUE,
            nome        VARCHAR(100),
            tipo        VARCHAR(10),
            setor       VARCHAR(50),
            criado_em   TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE cotacoes (
            id          SERIAL PRIMARY KEY,
            ativo_id    INTEGER REFERENCES ativos(id),
            data        DATE NOT NULL,
            abertura    NUMERIC(12,4),
            fechamento  NUMERIC(12,4),
            maxima      NUMERIC(12,4),
            minima      NUMERIC(12,4),
            volume      BIGINT,
            UNIQUE (ativo_id, data)
        );
        CREATE TABLE indicadores (
            id             SERIAL PRIMARY KEY,
            ativo_id       INTEGER REFERENCES ativos(id),
            data           DATE NOT NULL,
            pl             NUMERIC(10,2),
            pvp            NUMERIC(10,2),
            dividend_yield NUMERIC(8,4),
            roe            NUMERIC(8,4),
            margem_liquida NUMERIC(8,4),
            UNIQUE (ativo_id, data)
        );
        CREATE TABLE macroeconomico (
            id      SERIAL PRIMARY KEY,
            data    DATE NOT NULL UNIQUE,
            selic   NUMERIC(8,4),
            ipca    NUMERIC(8,4),
            usd_brl NUMERIC(8,4)
        );
    """))
    conn.commit()

migrar_tabela('ativos')
migrar_tabela('cotacoes')

print("\nMigracao concluida!")
