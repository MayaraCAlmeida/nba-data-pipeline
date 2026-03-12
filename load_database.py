"""
NBA Data Pipeline - Database Loading Module

    Crie um arquivo .env na mesma pasta com:
        DB_USER=postgres
        DB_PASSWORD=sua_senha
        DB_HOST=localhost
        DB_PORT=5432
        DB_NAME=nba_pipeline

"""

import os
import ssl
import logging
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv

ssl._create_default_https_context = ssl._create_unverified_context
load_dotenv()

# ─── Config ──────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_DIR = os.path.join(BASE_DIR, "dados_processados")
SQL_DIR = BASE_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "pipeline.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ─── Conexão ─────────────────────────────────────────────────────────────────
def get_engine():
    db_url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER', 'postgres')}:"
        f"{os.getenv('DB_PASSWORD', 'postgres')}@"
        f"{os.getenv('DB_HOST', 'localhost')}:"
        f"{os.getenv('DB_PORT', '5432')}/"
        f"{os.getenv('DB_NAME', 'nba_pipeline')}"
    )
    log.info(
        f"  Conectando em: {os.getenv('DB_HOST','localhost')}:{os.getenv('DB_PORT','5432')}/{os.getenv('DB_NAME','nba_pipeline')}"
    )
    return create_engine(db_url, pool_pre_ping=True)


# ─── Helpers ─────────────────────────────────────────────────────────────────
def run_sql_file(engine, path: str):
    if not os.path.exists(path):
        log.warning(f"  Arquivo SQL não encontrado: {path}")
        return False
    with open(path, encoding="utf-8") as f:
        sql = f.read()
    with engine.connect() as conn:
        for statement in sql.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))
        conn.commit()
    log.info(f"  ✔ SQL executado: {os.path.basename(path)}")
    return True


def load_table(
    engine, df: pd.DataFrame, table_name: str, conflict_cols: list, chunk_size: int = 50
):
    """
    Upsert genérico com chunk pequeno para evitar limite de parâmetros.
    Filtra automaticamente colunas que não existem na tabela do banco.
    """
    # Descobre quais colunas existem na tabela do banco
    insp = inspect(engine)
    db_cols = {col["name"] for col in insp.get_columns(table_name)}

    # Filtra o DataFrame para ter só as colunas que existem no banco
    valid_cols = [c for c in df.columns if c in db_cols]
    df = df[valid_cols].copy()
    df = df.where(pd.notnull(df), None)

    # Remove duplicatas pela chave de conflito (ex: jogador que trocou de time)
    df = df.drop_duplicates(subset=conflict_cols, keep="last")

    total = len(df)
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    with engine.connect() as conn:
        for i in range(0, total, chunk_size):
            chunk = df.iloc[i : i + chunk_size].to_dict(orient="records")
            stmt = insert(table).values(chunk)
            update_dict = {
                c.key: c for c in stmt.excluded if c.key not in conflict_cols
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_cols, set_=update_dict
            )
            conn.execute(stmt)
        conn.commit()
    log.info(f"  ✔ {table_name}  ({total} linhas)")


# ─── Main ─────────────────────────────────────────────────────────────────────
def run():
    log.info("=" * 55)
    log.info("  NBA Pipeline — Carga no banco de dados")
    log.info(f"  Entrada: {PROCESSED_DIR}")
    log.info("=" * 55)

    try:
        engine = get_engine()
    except Exception as e:
        log.error(f"  ✘ Falha na conexão com o banco: {e}")
        raise

    log.info("► Criando tabelas (se não existirem)...")
    sql_ok = run_sql_file(engine, os.path.join(SQL_DIR, "create_tables.sql"))
    if not sql_ok:
        log.error("  ✘ Coloque o create_tables.sql na pasta NBA\\ e rode novamente.")
        return

    log.info("► Carregando dados...")
    files = {
        "player_gamelogs_enriched": {
            "table": "player_gamelogs",
            "conflict": ["player_id", "game_id"],
        },
        "player_season_stats": {
            "table": "player_season_stats",
            "conflict": ["player_id"],
        },
        "games": {
            "table": "games",
            "conflict": ["team_id", "game_id"],
        },
    }

    sucesso = 0
    for filename, cfg in files.items():
        path = os.path.join(PROCESSED_DIR, f"{filename}.csv")
        if not os.path.exists(path):
            log.warning(f"  ✘ Arquivo não encontrado: {filename}.csv — pulando")
            continue
        try:
            df = pd.read_csv(path)
            load_table(engine, df, cfg["table"], cfg["conflict"])
            sucesso += 1
        except Exception as e:
            log.error(f"  ✘ Erro ao carregar '{filename}': {e}")

    log.info("=" * 55)
    log.info(f"  Concluído. {sucesso}/{len(files)} tabelas carregadas.")
    log.info("=" * 55)


if __name__ == "__main__":
    run()
