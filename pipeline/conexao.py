import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def get_engine():
    user     = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db       = os.getenv("POSTGRES_DB")
    host     = "localhost"
    port     = "5432"

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)

def testar_conexao():
    engine = get_engine()
    with engine.connect() as conn:
        resultado = conn.execute(text("SELECT version()"))
        print("Conectado:", resultado.fetchone()[0])

if __name__ == "__main__":
    testar_conexao()
