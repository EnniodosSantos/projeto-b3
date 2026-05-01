import pandas as pd
from sqlalchemy import text
from conexao import get_engine

def carregar_ativos(df, tipo):
    """Insere os ativos na tabela ativos se ainda não existirem."""
    engine = get_engine()
    tickers = df["ticker"].unique()

    with engine.begin() as conn:
        for ticker in tickers:
            conn.execute(text("""
                INSERT INTO ativos (ticker, tipo)
                VALUES (:ticker, :tipo)
                ON CONFLICT (ticker) DO NOTHING
            """), {"ticker": ticker, "tipo": tipo})

    print(f"  Ativos registrados: {len(tickers)}")

def carregar_cotacoes(df, tipo):
    """Insere cotações diárias evitando duplicatas."""
    engine = get_engine()
    inseridos = 0
    ignorados = 0

    with engine.begin() as conn:
        for _, row in df.iterrows():
            # Busca o id do ativo
            resultado = conn.execute(text(
                "SELECT id FROM ativos WHERE ticker = :ticker"
            ), {"ticker": row["ticker"]}).fetchone()

            if not resultado:
                continue

            ativo_id = resultado[0]

            try:
                conn.execute(text("""
                    INSERT INTO cotacoes
                        (ativo_id, data, abertura, fechamento, maxima, minima, volume)
                    VALUES
                        (:ativo_id, :data, :abertura, :fechamento, :maxima, :minima, :volume)
                    ON CONFLICT (ativo_id, data) DO NOTHING
                """), {
                    "ativo_id":   ativo_id,
                    "data":       row["data"],
                    "abertura":   float(row["abertura"]),
                    "fechamento": float(row["fechamento"]),
                    "maxima":     float(row["maxima"]),
                    "minima":     float(row["minima"]),
                    "volume":     int(row["volume"])
                })
                inseridos += 1
            except Exception:
                ignorados += 1

    print(f"  Inseridos: {inseridos} | Ignorados: {ignorados}")

if __name__ == "__main__":
    from extracao import extrair_cotacoes, ACOES, CRIPTOS

    print("Extraindo ações...")
    df_acoes = extrair_cotacoes(ACOES)
    print("Carregando ações...")
    carregar_ativos(df_acoes, "acao")
    carregar_cotacoes(df_acoes, "acao")

    print("Extraindo criptos...")
    df_criptos = extrair_cotacoes(CRIPTOS)
    print("Carregando criptos...")
    carregar_ativos(df_criptos, "cripto")
    carregar_cotacoes(df_criptos, "cripto")

    print("\nCarga concluída!")
