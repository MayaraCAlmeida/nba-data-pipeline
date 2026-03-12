"""
NBA Data Pipeline - Data Cleaning Module

"""

import os
import ssl
import logging
import pandas as pd
from glob import glob
from datetime import datetime

ssl._create_default_https_context = ssl._create_unverified_context

# ─── Config ──────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "dados_brutos")
PROCESSED_DIR = os.path.join(BASE_DIR, "dados_processados")
os.makedirs(PROCESSED_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "pipeline.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ─── Helpers ─────────────────────────────────────────────────────────────────
def latest_file(prefix: str) -> str:
    files = sorted(glob(os.path.join(RAW_DIR, f"{prefix}_*.csv")))
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado para: {prefix}")
    return files[-1]


def save_processed(df: pd.DataFrame, name: str) -> str:
    path = os.path.join(PROCESSED_DIR, f"{name}.csv")
    df.to_csv(path, index=False, encoding="utf-8")
    log.info(f"  ✔ Salvo: {os.path.basename(path)}  ({len(df)} linhas)")
    return path


# ─── Limpeza ─────────────────────────────────────────────────────────────────
def clean_player_gamelogs(df: pd.DataFrame) -> pd.DataFrame:
    log.info("► Limpando game logs dos jogadores...")

    rename = {
        "PLAYER_ID": "player_id",
        "PLAYER_NAME": "player_name",
        "TEAM_ID": "team_id",
        "TEAM_ABBREVIATION": "team_abbr",
        "GAME_ID": "game_id",
        "GAME_DATE": "game_date",
        "MATCHUP": "matchup",
        "WL": "win_loss",
        "MIN": "minutes",
        "FGM": "fg_made",
        "FGA": "fg_attempts",
        "FG_PCT": "fg_pct",
        "FG3M": "fg3_made",
        "FG3A": "fg3_attempts",
        "FG3_PCT": "fg3_pct",
        "FTM": "ft_made",
        "FTA": "ft_attempts",
        "FT_PCT": "ft_pct",
        "OREB": "off_rebounds",
        "DREB": "def_rebounds",
        "REB": "rebounds",
        "AST": "assists",
        "TOV": "turnovers",
        "STL": "steals",
        "BLK": "blocks",
        "BLKA": "blocked_att",
        "PF": "fouls",
        "PFD": "fouls_drawn",
        "PTS": "points",
        "PLUS_MINUS": "plus_minus",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    df["game_date"] = pd.to_datetime(df["game_date"])

    numeric_cols = [
        "minutes",
        "fg_made",
        "fg_attempts",
        "fg_pct",
        "fg3_made",
        "fg3_attempts",
        "fg3_pct",
        "ft_made",
        "ft_attempts",
        "ft_pct",
        "off_rebounds",
        "def_rebounds",
        "rebounds",
        "assists",
        "turnovers",
        "steals",
        "blocks",
        "points",
        "plus_minus",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["minutes"].notna() & (df["minutes"] > 0)]
    df["is_win"] = (df["win_loss"] == "W").astype(int)
    df["is_home"] = df["matchup"].str.contains(r"vs\.").astype(int)
    df = df.drop_duplicates(subset=["player_id", "game_id"])

    log.info(f"  → {len(df)} linhas após limpeza")
    return df


def clean_league_leaders(df: pd.DataFrame) -> pd.DataFrame:
    log.info("► Limpando líderes de estatísticas...")

    rename = {
        "PLAYER_ID": "player_id",
        "PLAYER": "player_name",
        "TEAM": "team_abbr",
        "GP": "games_played",
        "MIN": "minutes_per_game",
        "FGM": "fg_made",
        "FGA": "fg_attempts",
        "FG_PCT": "fg_pct",
        "FG3M": "fg3_made",
        "FG3A": "fg3_attempts",
        "FG3_PCT": "fg3_pct",
        "FTM": "ft_made",
        "FTA": "ft_attempts",
        "FT_PCT": "ft_pct",
        "OREB": "off_rebounds",
        "DREB": "def_rebounds",
        "REB": "rebounds",
        "AST": "assists",
        "TOV": "turnovers",
        "STL": "steals",
        "BLK": "blocks",
        "PTS": "points_per_game",
        "EFF": "efficiency",
        "STAT_CATEGORY": "stat_category",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    df = df.drop_duplicates(subset=["player_id", "stat_category"])

    log.info(f"  → {len(df)} linhas após limpeza")
    return df


def clean_games(df: pd.DataFrame) -> pd.DataFrame:
    log.info("► Limpando jogos...")

    rename = {
        "TEAM_ID": "team_id",
        "TEAM_ABBREVIATION": "team_abbr",
        "TEAM_NAME": "team_name",
        "GAME_ID": "game_id",
        "GAME_DATE": "game_date",
        "MATCHUP": "matchup",
        "WL": "win_loss",
        "PTS": "points",
        "PLUS_MINUS": "point_diff",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    df["game_date"] = pd.to_datetime(df["game_date"])
    df["is_home"] = df["matchup"].str.contains(r"vs\.").astype(int)
    df["is_win"] = (df["win_loss"] == "W").astype(int)
    df = df.drop_duplicates(subset=["team_id", "game_id"])

    log.info(f"  → {len(df)} linhas após limpeza")
    return df


# ─── Main ─────────────────────────────────────────────────────────────────────
def run():
    log.info("=" * 55)
    log.info("  NBA Pipeline — Limpeza de dados")
    log.info(f"  Entrada: {RAW_DIR}")
    log.info(f"  Saída:   {PROCESSED_DIR}")
    log.info("=" * 55)

    steps = {
        "player_gamelogs": (latest_file("player_gamelogs"), clean_player_gamelogs),
        "league_leaders": (latest_file("league_leaders"), clean_league_leaders),
        "games": (latest_file("games"), clean_games),
    }

    results = {}
    for name, (path, clean_fn) in steps.items():
        try:
            log.info(f"  Lendo: {os.path.basename(path)}")
            df = pd.read_csv(path)
            results[name] = clean_fn(df)
            save_processed(results[name], name)
        except Exception as e:
            log.error(f"  ✘ Erro em '{name}': {e}")

    log.info("=" * 55)
    log.info(f"  Concluído. {len(results)}/{len(steps)} etapas com sucesso.")
    log.info("=" * 55)
    return results


if __name__ == "__main__":
    run()
