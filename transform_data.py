"""
NBA Data Pipeline - Transformation & Feature Engineering Module

"""

import os
import ssl
import logging
import pandas as pd
import numpy as np
from scipy import stats

ssl._create_default_https_context = ssl._create_unverified_context

# ─── Config ──────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
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


# ─── Métricas avançadas ───────────────────────────────────────────────────────
def compute_true_shooting(pts, fga, fta):
    """True Shooting % — mede eficiência de arremesso real."""
    tsa = fga + 0.44 * fta
    return np.where(tsa > 0, pts / (2 * tsa), 0)


def compute_assist_to_turnover(ast, tov):
    """Assist/Turnover ratio — mede cuidado com a bola."""
    return np.where(tov > 0, ast / tov, ast)


def compute_impact_score(pts, ast, reb, stl, blk, tov):
    """
    Impact Score — métrica proprietária que resume o impacto do jogador.
    (PTS×0.35) + (AST×0.20) + (REB×0.20) + (STL×0.12) + (BLK×0.08) − (TOV×0.15)
    """
    return (
        (pts * 0.35)
        + (ast * 0.20)
        + (reb * 0.20)
        + (stl * 0.12)
        + (blk * 0.08)
        - (tov * 0.15)
    )


def compute_usage_proxy(fga, fta, tov):
    """Proxy de Usage Rate quando não temos dados de possessões."""
    return fga + 0.44 * fta + tov


def compute_game_score(pts, fgm, fga, ftm, fta, oreb, dreb, ast, stl, blk, pf, tov):
    """
    Game Score (John Hollinger) — resumo do jogo em um número.
    GmSc = PTS + 0.4*FGM − 0.7*FGA − 0.4*(FTA−FTM) + 0.7*OREB +
           0.3*DREB + STL + 0.7*AST + 0.7*BLK − 0.4*PF − TOV
    """
    return (
        pts
        + 0.4 * fgm
        - 0.7 * fga
        - 0.4 * (fta - ftm)
        + 0.7 * oreb
        + 0.3 * dreb
        + stl
        + 0.7 * ast
        + 0.7 * blk
        - 0.4 * pf
        - tov
    )


# ─── Detecção de outliers ─────────────────────────────────────────────────────
def flag_outlier_games(
    df: pd.DataFrame, col: str, threshold: float = 2.5
) -> pd.DataFrame:
    """Detecta jogos onde o jogador esteve muito acima/abaixo da sua média (Z-score por jogador)."""
    df = df.copy()
    df[f"{col}_zscore"] = df.groupby("player_id")[col].transform(
        lambda x: stats.zscore(x, nan_policy="omit")
    )
    df[f"{col}_outlier"] = df[f"{col}_zscore"].abs() > threshold
    return df


# ─── Rolling stats ────────────────────────────────────────────────────────────
def add_rolling_stats(df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
    """Adiciona médias móveis dos últimos N jogos por jogador."""
    df = df.sort_values(["player_id", "game_date"])
    for col in ["points", "assists", "rebounds", "impact_score"]:
        if col in df.columns:
            df[f"{col}_rolling{window}"] = df.groupby("player_id")[col].transform(
                lambda x: x.rolling(window, min_periods=1).mean()
            )
    return df


# ─── Agregados da temporada ───────────────────────────────────────────────────
def build_player_season_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega estatísticas da temporada por jogador."""
    agg = (
        df.groupby(["player_id", "player_name", "team_abbr"])
        .agg(
            games_played=("game_id", "count"),
            wins=("is_win", "sum"),
            avg_minutes=("minutes", "mean"),
            avg_points=("points", "mean"),
            avg_assists=("assists", "mean"),
            avg_rebounds=("rebounds", "mean"),
            avg_steals=("steals", "mean"),
            avg_blocks=("blocks", "mean"),
            avg_turnovers=("turnovers", "mean"),
            avg_plus_minus=("plus_minus", "mean"),
            avg_ts_pct=("true_shooting_pct", "mean"),
            avg_ast_tov=("ast_to_tov_ratio", "mean"),
            avg_impact_score=("impact_score", "mean"),
            avg_game_score=("game_score", "mean"),
            max_points=("points", "max"),
            max_impact=("impact_score", "max"),
            std_points=("points", "std"),
        )
        .reset_index()
    )

    agg["win_rate"] = agg["wins"] / agg["games_played"]
    agg["consistency_score"] = 1 / (1 + agg["std_points"])
    agg["impact_rank"] = agg["avg_impact_score"].rank(ascending=False).astype(int)

    return agg.sort_values("avg_impact_score", ascending=False)


# ─── Main ─────────────────────────────────────────────────────────────────────
def run():
    log.info("=" * 55)
    log.info("  NBA Pipeline — Transformação & Feature Engineering")
    log.info(f"  Pasta: {PROCESSED_DIR}")
    log.info("=" * 55)

    input_path = os.path.join(PROCESSED_DIR, "player_gamelogs.csv")
    if not os.path.exists(input_path):
        raise FileNotFoundError(
            f"Arquivo não encontrado: {input_path}\nRode clean.py primeiro."
        )

    log.info("► Carregando dados processados...")
    df = pd.read_csv(input_path, parse_dates=["game_date"])
    log.info(f"  → {len(df)} linhas carregadas")

    log.info("► Calculando métricas avançadas...")
    df["true_shooting_pct"] = compute_true_shooting(
        df["points"], df["fg_attempts"], df["ft_attempts"]
    )
    df["ast_to_tov_ratio"] = compute_assist_to_turnover(df["assists"], df["turnovers"])
    df["impact_score"] = compute_impact_score(
        df["points"],
        df["assists"],
        df["rebounds"],
        df["steals"],
        df["blocks"],
        df["turnovers"],
    )
    df["game_score"] = compute_game_score(
        df["points"],
        df["fg_made"],
        df["fg_attempts"],
        df["ft_made"],
        df["ft_attempts"],
        df["off_rebounds"],
        df["def_rebounds"],
        df["assists"],
        df["steals"],
        df["blocks"],
        df["fouls"],
        df["turnovers"],
    )
    df["usage_proxy"] = compute_usage_proxy(
        df["fg_attempts"], df["ft_attempts"], df["turnovers"]
    )

    log.info("► Detectando outliers...")
    df = flag_outlier_games(df, col="points", threshold=2.5)
    df = flag_outlier_games(df, col="impact_score", threshold=2.5)

    log.info("► Calculando médias móveis (5 e 10 jogos)...")
    df = add_rolling_stats(df, window=5)
    df = add_rolling_stats(df, window=10)

    log.info("► Agregando estatísticas da temporada...")
    season_stats = build_player_season_stats(df)

    log.info("► Salvando...")
    enriched_path = os.path.join(PROCESSED_DIR, "player_gamelogs_enriched.csv")
    season_path = os.path.join(PROCESSED_DIR, "player_season_stats.csv")
    df.to_csv(enriched_path, index=False, encoding="utf-8")
    season_stats.to_csv(season_path, index=False, encoding="utf-8")
    log.info(f"  ✔ player_gamelogs_enriched.csv  ({len(df)} linhas)")
    log.info(f"  ✔ player_season_stats.csv       ({len(season_stats)} jogadores)")

    log.info("=" * 55)
    log.info("  Concluído.")
    log.info("=" * 55)

    return {"gamelogs_enriched": df, "season_stats": season_stats}


if __name__ == "__main__":
    run()
