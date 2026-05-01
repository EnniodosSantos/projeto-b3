import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# Lista de ativos
ACOES = [
    "PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBAS3.SA", "WEGE3.SA",
    "ABEV3.SA", "MGLU3.SA", "RENT3.SA", "BBDC4.SA", "SUZB3.SA",
    "RADL3.SA", "EQTL3.SA", "GGBR4.SA", "JBSS3.SA", "LREN3.SA",
    "TOTS3.SA", "HAPV3.SA", "CSAN3.SA", "VIVT3.SA", "B3SA3.SA"
]

CRIPTOS = [
    "BTC-USD", "ETH-USD", "SOL-USD"
]

def extrair_cotacoes(tickers, periodo="5y"):
    """Baixa cotações históricas do Yahoo Finance."""
    print(f"Extraindo {len(tickers)} ativos...")
    dados = []

    for ticker in tickers:
        try:
            ativo = yf.Ticker(ticker)
            hist  = ativo.history(period=periodo)

            if hist.empty:
                print(f"  Sem dados: {ticker}")
                continue

            hist = hist.reset_index()
            hist["ticker"] = ticker
            hist = hist.rename(columns={
                "Date":   "data",
                "Open":   "abertura",
                "Close":  "fechamento",
                "High":   "maxima",
                "Low":    "minima",
                "Volume": "volume"
            })
            hist["data"] = pd.to_datetime(hist["data"]).dt.date
            hist = hist[["ticker","data","abertura","fechamento","maxima","minima","volume"]]
            dados.append(hist)
            print(f"  OK: {ticker} ({len(hist)} registros)")

        except Exception as e:
            print(f"  Erro {ticker}: {e}")

    if dados:
        return pd.concat(dados, ignore_index=True)
    return pd.DataFrame()

if __name__ == "__main__":
    df_acoes  = extrair_cotacoes(ACOES)
    df_criptos = extrair_cotacoes(CRIPTOS)

    df_total = pd.concat([df_acoes, df_criptos], ignore_index=True)
    print(f"\nTotal extraído: {len(df_total)} registros")
    print(df_total.head(10))
