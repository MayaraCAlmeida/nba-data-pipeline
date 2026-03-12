"""
NBA Data Pipeline — Dashboard Generator
Puxa dados reais do PostgreSQL e gera nba_dashboard.html atualizado.

"""

import os
import json
import math
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


### Conexão
def get_engine():
    db_url = (
        f"postgresql+psycopg2://{os.getenv('DB_USER','postgres')}:"
        f"{os.getenv('DB_PASSWORD','postgres')}@"
        f"{os.getenv('DB_HOST','localhost')}:"
        f"{os.getenv('DB_PORT','5432')}/"
        f"{os.getenv('DB_NAME','nba_pipeline')}"
    )
    return create_engine(db_url, pool_pre_ping=True)


### Queries
def fetch_players(conn):
    rows = conn.execute(
        text(
            """
        SELECT
            player_name,
            team_abbr,
            games_played,
            ROUND(avg_points::numeric, 1)       AS ppg,
            ROUND(avg_assists::numeric, 1)      AS apg,
            ROUND(avg_rebounds::numeric, 1)     AS rpg,
            ROUND(avg_steals::numeric, 2)       AS spg,
            ROUND(avg_blocks::numeric, 2)       AS bpg,
            ROUND((avg_ts_pct*100)::numeric, 1) AS tspct,
            ROUND(avg_impact_score::numeric, 2) AS impact,
            ROUND(consistency_score::numeric, 4) AS consist,
            impact_rank,
            CASE
                WHEN avg_impact_score > 10 AND consistency_score > 0.15 THEN 'elite'
                WHEN avg_impact_score > 10                              THEN 'high'
                WHEN consistency_score > 0.15                          THEN 'consist'
                ELSE 'develop'
            END AS tier
        FROM player_season_stats
        WHERE games_played >= 10
        ORDER BY impact_rank
        LIMIT 50
    """
        )
    ).fetchall()
    return [dict(r._mapping) for r in rows]


def fetch_teams(conn):
    rows = conn.execute(
        text(
            """
        SELECT
            team_abbr                                    AS abbr,
            COUNT(*)                                     AS games,
            SUM(is_win)                                  AS wins,
            ROUND(AVG(points)::numeric, 1)               AS ppg,
            ROUND(AVG(point_diff)::numeric, 1)           AS margin
        FROM games
        GROUP BY team_abbr
        ORDER BY (SUM(is_win)::float / COUNT(*)) DESC
        LIMIT 30
    """
        )
    ).fetchall()
    return [dict(r._mapping) for r in rows]


def fetch_home_away(conn):
    rows = conn.execute(
        text(
            """
        SELECT
            team_abbr,
            is_home,
            COUNT(*)                                         AS games,
            SUM(is_win)                                      AS wins,
            ROUND((100.0 * SUM(is_win) / COUNT(*))::numeric, 1) AS win_pct
        FROM games
        GROUP BY team_abbr, is_home
        ORDER BY team_abbr, is_home DESC
    """
        )
    ).fetchall()
    result = {}
    for r in rows:
        d = dict(r._mapping)
        key = d["team_abbr"]
        if key not in result:
            result[key] = {"home": 0, "away": 0}
        if d["is_home"]:
            result[key]["home"] = float(d["win_pct"])
        else:
            result[key]["away"] = float(d["win_pct"])
    return result


def fetch_outliers(conn):
    rows = conn.execute(
        text(
            """
        SELECT
            player_name,
            team_abbr,
            points                              AS pts,
            ROUND(points_zscore::numeric, 2)    AS z,
            win_loss                            AS wl
        FROM player_gamelogs
        WHERE ABS(points_zscore) > 2.5
        ORDER BY ABS(points_zscore) DESC
        LIMIT 8
    """
        )
    ).fetchall()
    return [dict(r._mapping) for r in rows]


def fetch_top_games(conn):
    rows = conn.execute(
        text(
            """
        SELECT
            player_name,
            team_abbr,
            TO_CHAR(game_date, 'Mon DD') AS date,
            matchup,
            points   AS pts,
            assists  AS ast,
            rebounds AS reb,
            ROUND(game_score::numeric, 1) AS gmsc,
            win_loss AS wl
        FROM player_gamelogs
        ORDER BY game_score DESC
        LIMIT 10
    """
        )
    ).fetchall()
    return [dict(r._mapping) for r in rows]


def fetch_monthly(conn, player_ids):
    ids_str = ",".join(str(i) for i in player_ids)
    rows = conn.execute(
        text(
            f"""
        SELECT
            player_name,
            TO_CHAR(game_date, 'YYYY-MM') AS month,
            ROUND(AVG(points)::numeric, 1) AS avg_pts
        FROM player_gamelogs
        WHERE player_id IN ({ids_str})
        GROUP BY player_name, TO_CHAR(game_date, 'YYYY-MM')
        ORDER BY player_name, month
    """
        )
    ).fetchall()
    return [dict(r._mapping) for r in rows]


def fetch_top_player_ids(conn, n=5):
    rows = conn.execute(
        text(
            f"""
        SELECT player_id FROM player_season_stats
        WHERE games_played >= 10
        ORDER BY impact_rank LIMIT {n}
    """
        )
    ).fetchall()
    return [r[0] for r in rows]


def fetch_kpis(conn):
    r = conn.execute(
        text(
            """
        SELECT
            (SELECT COUNT(DISTINCT player_id) FROM player_gamelogs)  AS total_players,
            (SELECT COUNT(*) FROM games)                              AS total_games,
            (SELECT ROUND(MAX(avg_impact_score)::numeric,2)
             FROM player_season_stats)                               AS max_impact,
            (SELECT player_name FROM player_season_stats
             ORDER BY avg_impact_score DESC LIMIT 1)                 AS top_player,
            (SELECT team_abbr FROM player_season_stats
             ORDER BY avg_impact_score DESC LIMIT 1)                 AS top_team
    """
        )
    ).fetchone()
    return dict(r._mapping)


def fetch_impact_distribution(conn):
    rows = conn.execute(
        text(
            """
        SELECT
            CASE
                WHEN avg_impact_score < 3  THEN '0-3'
                WHEN avg_impact_score < 5  THEN '3-5'
                WHEN avg_impact_score < 7  THEN '5-7'
                WHEN avg_impact_score < 9  THEN '7-9'
                WHEN avg_impact_score < 12 THEN '9-12'
                ELSE '12+'
            END AS bucket,
            COUNT(*) AS cnt
        FROM player_season_stats
        WHERE games_played >= 10
        GROUP BY 1
        ORDER BY MIN(avg_impact_score)
    """
        )
    ).fetchall()
    return [dict(r._mapping) for r in rows]


### Gerador de HTML
def generate_html(
    players,
    teams,
    home_away,
    outliers,
    top_games,
    monthly_data,
    kpis,
    impact_dist,
    generated_at,
):

    # Serializa os dados para JSON embutido no JS
    def sanitize(o):
        import decimal

        if isinstance(o, list):
            return [sanitize(i) for i in o]
        if isinstance(o, dict):
            return {k: sanitize(v) for k, v in o.items()}
        if isinstance(o, decimal.Decimal):
            return float(o)
        if hasattr(o, "item"):
            return o.item()
        if isinstance(o, float) and math.isnan(o):
            return None
        if isinstance(o, (int, float, str, bool)) or o is None:
            return o
        return str(o)

    def jj(obj):
        return json.dumps(sanitize(obj), ensure_ascii=False)

    # Monta dados mensais por jogador (top 5)
    monthly_by_player = {}
    all_months = sorted(set(r["month"] for r in monthly_data))
    for r in monthly_data:
        name = r["player_name"].split(" ")[0]
        if name not in monthly_by_player:
            monthly_by_player[name] = {}
        monthly_by_player[name][r["month"]] = float(r["avg_pts"])

    monthly_js = []
    for name, month_map in list(monthly_by_player.items())[:5]:
        monthly_js.append(
            {"name": name, "data": [month_map.get(m, None) for m in all_months]}
        )

    month_labels = [m[5:] for m in all_months]  # "YYYY-MM" → "MM"

    # Home/Away para top 8 times
    top8_abbr = [t["abbr"] for t in teams[:8]]
    home_data = [home_away.get(a, {}).get("home", 0) for a in top8_abbr]
    away_data = [home_away.get(a, {}).get("away", 0) for a in top8_abbr]

    # Impact distribution
    dist_labels = [r["bucket"] for r in impact_dist]
    dist_counts = [int(r["cnt"]) for r in impact_dist]

    # Player options para o select
    player_options = "\n".join(
        f'<option value="{i}">{p["player_name"]}</option>'
        for i, p in enumerate(players[:20])
    )

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NBA Performance Monitor</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Mono:wght@300;400;500&family=DM+Sans:wght@300;400;600&display=swap" rel="stylesheet">
<style>
  :root {{
    --bg:      #07080c;
    --surface: #0f1118;
    --card:    #141720;
    --border:  #1e2230;
    --accent:  #e8501a;
    --accent2: #f5a623;
    --green:   #2dd4a0;
    --blue:    #4da6ff;
    --text:    #e2e6f0;
    --muted:   #5a6080;
    --glow:    rgba(232,80,26,0.18);
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: 'DM Sans', sans-serif; min-height: 100vh; }}
  header {{ display:flex; align-items:center; justify-content:space-between; padding:18px 32px; border-bottom:1px solid var(--border); background:var(--surface); position:sticky; top:0; z-index:100; }}
  .logo {{ font-family:'Bebas Neue',sans-serif; font-size:26px; letter-spacing:3px; color:var(--text); }}
  .logo span {{ color:var(--accent); }}
  .badge {{ background:var(--accent); color:#fff; font-size:10px; font-weight:600; padding:3px 8px; border-radius:20px; letter-spacing:1px; text-transform:uppercase; margin-left:10px; vertical-align:middle; }}
  .season-tag {{ font-family:'DM Mono',monospace; font-size:12px; color:var(--muted); border:1px solid var(--border); padding:4px 12px; border-radius:4px; }}
  nav {{ display:flex; gap:0; padding:0 32px; background:var(--surface); border-bottom:1px solid var(--border); }}
  nav button {{ background:none; border:none; border-bottom:2px solid transparent; color:var(--muted); font-family:'DM Sans',sans-serif; font-size:13px; font-weight:600; letter-spacing:.5px; padding:14px 20px; cursor:pointer; text-transform:uppercase; transition:all .2s; }}
  nav button:hover {{ color:var(--text); }}
  nav button.active {{ color:var(--accent); border-bottom-color:var(--accent); }}
  main {{ padding:28px 32px; max-width:1400px; margin:0 auto; }}
  .page {{ display:none; }}
  .page.active {{ display:block; }}
  .kpi-row {{ display:grid; grid-template-columns:repeat(4,1fr); gap:16px; margin-bottom:24px; }}
  .kpi {{ background:var(--card); border:1px solid var(--border); border-radius:10px; padding:20px 22px; position:relative; overflow:hidden; transition:border-color .2s; }}
  .kpi:hover {{ border-color:var(--accent); }}
  .kpi::before {{ content:''; position:absolute; top:0; left:0; right:0; height:2px; background:linear-gradient(90deg,var(--accent),transparent); }}
  .kpi-label {{ font-size:10px; font-weight:600; letter-spacing:1.5px; text-transform:uppercase; color:var(--muted); margin-bottom:8px; }}
  .kpi-value {{ font-family:'Bebas Neue',sans-serif; font-size:42px; line-height:1; color:var(--text); }}
  .kpi-sub {{ font-size:11px; color:var(--muted); margin-top:4px; font-family:'DM Mono',monospace; }}
  .kpi-value.green {{ color:var(--green); }}
  .kpi-value.orange {{ color:var(--accent2); }}
  .kpi-value.blue {{ color:var(--blue); }}
  .grid-2 {{ display:grid; grid-template-columns:1fr 1fr; gap:20px; margin-bottom:20px; }}
  .grid-3 {{ display:grid; grid-template-columns:1fr 1fr 1fr; gap:20px; margin-bottom:20px; }}
  .span2 {{ grid-column:span 2; }}
  .panel {{ background:var(--card); border:1px solid var(--border); border-radius:10px; padding:22px; margin-bottom:20px; }}
  .panel-header {{ display:flex; align-items:center; justify-content:space-between; margin-bottom:18px; }}
  .panel-title {{ font-family:'DM Mono',monospace; font-size:11px; font-weight:500; letter-spacing:2px; text-transform:uppercase; color:var(--muted); }}
  .panel-title strong {{ color:var(--text); font-weight:600; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  th {{ font-family:'DM Mono',monospace; font-size:10px; font-weight:500; letter-spacing:1.5px; text-transform:uppercase; color:var(--muted); padding:8px 12px; border-bottom:1px solid var(--border); text-align:left; }}
  th.num {{ text-align:right; }}
  td {{ padding:10px 12px; border-bottom:1px solid rgba(255,255,255,0.03); }}
  td.num {{ text-align:right; font-family:'DM Mono',monospace; font-size:12px; }}
  tr:hover td {{ background:rgba(255,255,255,0.02); }}
  tr:last-child td {{ border-bottom:none; }}
  .rank {{ font-family:'Bebas Neue',sans-serif; font-size:16px; color:var(--muted); min-width:28px; display:inline-block; text-align:center; }}
  .rank.top {{ color:var(--accent2); }}
  .bar-wrap {{ display:flex; align-items:center; gap:8px; }}
  .bar-track {{ flex:1; height:4px; background:var(--border); border-radius:2px; }}
  .bar-fill {{ height:100%; border-radius:2px; background:linear-gradient(90deg,var(--accent),var(--accent2)); transition:width .6s ease; }}
  .bar-num {{ font-family:'DM Mono',monospace; font-size:11px; color:var(--text); min-width:36px; text-align:right; }}
  .tier {{ font-size:10px; font-weight:600; letter-spacing:.8px; text-transform:uppercase; padding:2px 8px; border-radius:3px; }}
  .tier.elite   {{ background:rgba(232,80,26,.2);   color:var(--accent);  border:1px solid var(--accent); }}
  .tier.high    {{ background:rgba(245,166,35,.15); color:var(--accent2); border:1px solid var(--accent2); }}
  .tier.consist {{ background:rgba(77,166,255,.15); color:var(--blue);    border:1px solid var(--blue); }}
  .tier.develop {{ background:rgba(90,96,128,.15);  color:var(--muted);   border:1px solid var(--border); }}
  .wl {{ font-weight:700; font-family:'DM Mono',monospace; font-size:11px; }}
  .wl.w {{ color:var(--green); }}
  .wl.l {{ color:#f87171; }}
  .hi {{ color:var(--accent2); font-weight:600; }}
  .hi-green {{ color:var(--green); font-weight:600; }}
  .hi-blue {{ color:var(--blue); }}
  .chart-wrap {{ position:relative; height:240px; }}
  .chart-wrap.tall {{ height:320px; }}
  .section-title {{ font-family:'Bebas Neue',sans-serif; font-size:22px; letter-spacing:2px; margin-bottom:20px; color:var(--text); }}
  .section-title span {{ color:var(--accent); }}
  .outlier-dot {{ display:inline-block; width:7px; height:7px; border-radius:50%; background:var(--accent); margin-right:5px; box-shadow:0 0 6px var(--accent); }}
  .pipeline-step {{ display:flex; align-items:flex-start; gap:16px; padding:16px 0; border-bottom:1px solid var(--border); }}
  .pipeline-step:last-child {{ border-bottom:none; }}
  .step-num {{ font-family:'Bebas Neue',sans-serif; font-size:28px; color:var(--accent); min-width:36px; line-height:1; }}
  .step-name {{ font-family:'DM Mono',monospace; font-size:12px; font-weight:500; color:var(--text); margin-bottom:3px; }}
  .step-desc {{ font-size:12px; color:var(--muted); line-height:1.5; }}
  .code-tag {{ display:inline-block; background:rgba(232,80,26,.12); border:1px solid rgba(232,80,26,.25); color:var(--accent); font-family:'DM Mono',monospace; font-size:10px; padding:1px 7px; border-radius:3px; margin-right:4px; }}
  select {{ background:var(--surface); color:var(--text); border:1px solid var(--border); padding:5px 10px; border-radius:6px; font-size:12px; font-family:'DM Sans',sans-serif; cursor:pointer; }}
  select:focus {{ outline:none; border-color:var(--accent); }}
  @media (max-width:900px) {{
    .kpi-row {{ grid-template-columns:repeat(2,1fr); }}
    .grid-2, .grid-3 {{ grid-template-columns:1fr; }}
  }}
</style>
</head>
<body>

<header>
  <div>
    <span class="logo">NBA <span>MONITOR</span></span>
    <span class="badge">Live</span>
  </div>
  <div class="season-tag">2024-25 REGULAR SEASON &nbsp;·&nbsp; {generated_at}</div>
</header>

<nav>
  <button class="active" onclick="showPage('overview',this)">Visão Geral</button>
  <button onclick="showPage('players',this)">Jogadores</button>
  <button onclick="showPage('teams',this)">Times</button>
  <button onclick="showPage('trends',this)">Tendências</button>
  <button onclick="showPage('pipeline',this)">Pipeline</button>
</nav>

<main>

<!-- ═══ OVERVIEW ═══ -->
<div id="overview" class="page active">
  <div class="kpi-row">
    <div class="kpi">
      <div class="kpi-label">Jogadores Ativos</div>
      <div class="kpi-value">{kpis['total_players']}</div>
      <div class="kpi-sub">temporada 2024-25</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Jogos Registrados</div>
      <div class="kpi-value blue">{kpis['total_games']:,}</div>
      <div class="kpi-sub">season games</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Maior Impact Score</div>
      <div class="kpi-value orange">{kpis['max_impact']}</div>
      <div class="kpi-sub">{kpis['top_player']} — {kpis['top_team']}</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Gerado em</div>
      <div class="kpi-value green" style="font-size:28px">{generated_at[:5]}</div>
      <div class="kpi-sub">{generated_at}</div>
    </div>
  </div>

  <div class="grid-2">
    <div class="panel">
      <div class="panel-header"><div class="panel-title"><strong>Top 10</strong> — Impact Score</div></div>
      <table>
        <thead><tr><th>#</th><th>Jogador</th><th>Time</th><th class="num">PTS</th><th class="num">AST</th><th class="num">REB</th><th>Impact</th></tr></thead>
        <tbody id="topTable"></tbody>
      </table>
    </div>
    <div class="panel">
      <div class="panel-header"><div class="panel-title"><strong>Eficiência Ofensiva</strong> — TS%</div></div>
      <div class="chart-wrap"><canvas id="tsChart"></canvas></div>
    </div>
  </div>

  <div class="grid-3">
    <div class="panel span2">
      <div class="panel-header"><div class="panel-title"><strong>Impact Score</strong> — Top 15 da Temporada</div></div>
      <div class="chart-wrap"><canvas id="impactChart"></canvas></div>
    </div>
    <div class="panel">
      <div class="panel-header"><div class="panel-title"><strong>Outlier Games</strong></div></div>
      <table>
        <thead><tr><th>Jogador</th><th class="num">PTS</th><th class="num">Z</th><th>R</th></tr></thead>
        <tbody id="outlierTable"></tbody>
      </table>
    </div>
  </div>
</div>

<!-- ═══ PLAYERS ═══ -->
<div id="players" class="page">
  <div class="panel-header" style="margin-bottom:20px">
    <div class="section-title">Análise de <span>Jogadores</span></div>
    <select id="playerSelect" onchange="updatePlayerChart()">
      {player_options}
    </select>
  </div>

  <div class="kpi-row" id="playerKPIs"></div>

  <div class="grid-2">
    <div class="panel">
      <div class="panel-header"><div class="panel-title"><strong>Evolução</strong> — Últimos 20 Jogos (simulado)</div></div>
      <div class="chart-wrap tall"><canvas id="playerTrendChart"></canvas></div>
    </div>
    <div class="panel">
      <div class="panel-header"><div class="panel-title"><strong>Perfil</strong> — Radar de Atributos</div></div>
      <div class="chart-wrap tall"><canvas id="radarChart"></canvas></div>
    </div>
  </div>

  <div class="panel">
    <div class="panel-header"><div class="panel-title"><strong>Classificação</strong> — Tier de Jogadores</div></div>
    <table>
      <thead><tr><th>#</th><th>Jogador</th><th>Time</th><th class="num">Impact</th><th class="num">TS%</th><th class="num">Consist.</th><th>Tier</th></tr></thead>
      <tbody id="tierTable"></tbody>
    </table>
  </div>
</div>

<!-- ═══ TEAMS ═══ -->
<div id="teams" class="page">
  <div class="section-title" style="margin-bottom:20px">Performance de <span>Times</span></div>
  <div class="grid-2">
    <div class="panel">
      <div class="panel-header"><div class="panel-title"><strong>Win Rate</strong> — Classificação</div></div>
      <div class="chart-wrap tall"><canvas id="teamWinChart"></canvas></div>
    </div>
    <div class="panel">
      <div class="panel-header"><div class="panel-title"><strong>Home vs Away</strong> — Win %</div></div>
      <div class="chart-wrap tall"><canvas id="homeAwayChart"></canvas></div>
    </div>
  </div>
  <div class="panel">
    <div class="panel-header"><div class="panel-title"><strong>Tabela</strong> — Desempenho Completo</div></div>
    <table>
      <thead><tr><th>#</th><th>Time</th><th class="num">J</th><th class="num">V</th><th class="num">D</th><th class="num">Win%</th><th class="num">PPG</th><th class="num">Margem</th></tr></thead>
      <tbody id="teamsTable"></tbody>
    </table>
  </div>
</div>

<!-- ═══ TRENDS ═══ -->
<div id="trends" class="page">
  <div class="section-title" style="margin-bottom:20px">Tendências da <span>Temporada</span></div>
  <div class="grid-2">
    <div class="panel">
      <div class="panel-header"><div class="panel-title"><strong>Pontos por Mês</strong> — Top 5</div></div>
      <div class="chart-wrap tall"><canvas id="monthlyChart"></canvas></div>
    </div>
    <div class="panel">
      <div class="panel-header"><div class="panel-title"><strong>Distribuição</strong> — Impact Score da Liga</div></div>
      <div class="chart-wrap tall"><canvas id="distributionChart"></canvas></div>
    </div>
  </div>
  <div class="panel">
    <div class="panel-header"><div class="panel-title"><strong>Top Jogos</strong> — Maiores Game Scores da Temporada</div></div>
    <table>
      <thead><tr><th>#</th><th>Jogador</th><th>Time</th><th>Data</th><th>Partida</th><th class="num">PTS</th><th class="num">AST</th><th class="num">REB</th><th class="num">GmSc</th><th>R</th></tr></thead>
      <tbody id="topGamesTable"></tbody>
    </table>
  </div>
</div>

<!-- ═══ PIPELINE ═══ -->
<div id="pipeline" class="page">
  <div class="section-title" style="margin-bottom:20px">Arquitetura do <span>Pipeline</span></div>
  <div class="grid-2">
    <div class="panel">
      <div class="panel-header"><div class="panel-title"><strong>ETL Steps</strong> — Fluxo de Dados</div></div>
      <div class="pipeline-step"><div class="step-num">01</div><div><div class="step-name">EXTRACT <span class="code-tag">extract.py</span></div><div class="step-desc">Coleta dados via nba_api: player game logs, league leaders, team results. Rate-limit safe com delay automático.</div></div></div>
      <div class="pipeline-step"><div class="step-num">02</div><div><div class="step-name">CLEAN <span class="code-tag">clean.py</span></div><div class="step-desc">Padroniza colunas, converte tipos, remove duplicatas, filtra jogos sem minutos jogados.</div></div></div>
      <div class="pipeline-step"><div class="step-num">03</div><div><div class="step-name">TRANSFORM <span class="code-tag">transform.py</span></div><div class="step-desc">Feature engineering: Impact Score, True Shooting%, Game Score, rolling averages (5/10j), Z-score outlier detection.</div></div></div>
      <div class="pipeline-step"><div class="step-num">04</div><div><div class="step-name">LOAD <span class="code-tag">load.py</span></div><div class="step-desc">Upsert no PostgreSQL via SQLAlchemy. Sem duplicatas mesmo em múltiplas execuções.</div></div></div>
      <div class="pipeline-step"><div class="step-num">05</div><div><div class="step-name">DASHBOARD <span class="code-tag">generate_dashboard.py</span></div><div class="step-desc">Gera este HTML com dados reais do banco. Atualize rodando o script novamente.</div></div></div>
    </div>
    <div>
      <div class="panel" style="margin-bottom:20px">
        <div class="panel-header"><div class="panel-title"><strong>Stack</strong> — Tecnologias</div></div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:4px">
          <div style="background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:12px"><div style="font-size:10px;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:6px">Data</div><div style="font-family:'DM Mono',monospace;font-size:12px;line-height:2;color:var(--text)">nba_api<br>pandas<br>numpy<br>scipy</div></div>
          <div style="background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:12px"><div style="font-size:10px;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:6px">Database</div><div style="font-family:'DM Mono',monospace;font-size:12px;line-height:2;color:var(--text)">PostgreSQL 16<br>SQLAlchemy 2.0<br>psycopg2<br>SQL Views</div></div>
          <div style="background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:12px"><div style="font-size:10px;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:6px">Backend</div><div style="font-family:'DM Mono',monospace;font-size:12px;line-height:2;color:var(--text)">Python 3.13<br>python-dotenv<br>psycopg2<br>json</div></div>
          <div style="background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:12px"><div style="font-size:10px;color:var(--muted);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:6px">Viz</div><div style="font-family:'DM Mono',monospace;font-size:12px;line-height:2;color:var(--text)">Chart.js 4.4<br>HTML/CSS/JS<br>Power BI<br>PostgreSQL</div></div>
        </div>
      </div>
      <div class="panel">
        <div class="panel-header"><div class="panel-title"><strong>Impact Score</strong> — Fórmula</div></div>
        <div style="background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:16px;font-family:'DM Mono',monospace;font-size:12px;line-height:2;color:var(--text)">
          <span style="color:var(--accent)">Impact Score</span> =<br>
          &nbsp;&nbsp;(PTS × <span style="color:var(--accent2)">0.35</span>) +<br>
          &nbsp;&nbsp;(AST × <span style="color:var(--accent2)">0.20</span>) +<br>
          &nbsp;&nbsp;(REB × <span style="color:var(--accent2)">0.20</span>) +<br>
          &nbsp;&nbsp;(STL × <span style="color:var(--accent2)">0.12</span>) +<br>
          &nbsp;&nbsp;(BLK × <span style="color:var(--accent2)">0.08</span>) −<br>
          &nbsp;&nbsp;(TOV × <span style="color:var(--accent)">0.15</span>)
        </div>
      </div>
    </div>
  </div>
</div>

</main>

<script>
/* ═══ DATA (gerado em {generated_at}) ═══════════════════════════════════ */
const PLAYERS  = {jj(players)};
const TEAMS    = {jj(teams)};
const OUTLIERS = {jj(outliers)};
const TOP_GAMES = {jj(top_games)};
const MONTHLY_DATA = {jj(monthly_js)};
const MONTH_LABELS = {jj(month_labels)};
const DIST_LABELS  = {jj(dist_labels)};
const DIST_COUNTS  = {jj(dist_counts)};
const HOME_DATA = {jj(home_data)};
const AWAY_DATA = {jj(away_data)};
const TOP8_ABBR = {jj(top8_abbr)};

/* ═══ CHART DEFAULTS ══════════════════════════════════════════════════════ */
Chart.defaults.color = '#5a6080';
Chart.defaults.borderColor = '#1e2230';
Chart.defaults.font.family = "'DM Mono', monospace";
Chart.defaults.font.size = 11;
const C = {{ accent:'#e8501a', accent2:'#f5a623', green:'#2dd4a0', blue:'#4da6ff', muted:'#5a6080' }};
function alpha(hex, a) {{
  const r=parseInt(hex.slice(1,3),16), g=parseInt(hex.slice(3,5),16), b=parseInt(hex.slice(5,7),16);
  return `rgba(${{r}},${{g}},${{b}},${{a}})`;
}}

/* ═══ NAV ══════════════════════════════════════════════════════════════════ */
function showPage(id, btn) {{
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('nav button').forEach(b => b.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  btn.classList.add('active');
}}

/* ═══ OVERVIEW ════════════════════════════════════════════════════════════ */
function buildTopTable() {{
  const tbody = document.getElementById('topTable');
  const max = PLAYERS[0].impact;
  tbody.innerHTML = PLAYERS.slice(0,10).map((p,i) => `
    <tr>
      <td><span class="rank ${{i<3?'top':''}}">${{i+1}}</span></td>
      <td>${{p.player_name}}</td>
      <td style="color:var(--muted);font-family:'DM Mono',monospace;font-size:11px">${{p.team_abbr}}</td>
      <td class="num hi">${{p.ppg}}</td>
      <td class="num">${{p.apg}}</td>
      <td class="num">${{p.rpg}}</td>
      <td style="min-width:120px">
        <div class="bar-wrap">
          <div class="bar-track"><div class="bar-fill" style="width:${{(p.impact/max*100).toFixed(0)}}%"></div></div>
          <div class="bar-num">${{p.impact}}</div>
        </div>
      </td>
    </tr>`).join('');
}}

function buildOutlierTable() {{
  const tbody = document.getElementById('outlierTable');
  tbody.innerHTML = OUTLIERS.map(o => `
    <tr>
      <td><span class="outlier-dot"></span>${{o.player_name}}</td>
      <td class="num hi">${{o.pts}}</td>
      <td class="num" style="color:var(--accent2)">${{o.z}}</td>
      <td><span class="wl ${{o.wl.toLowerCase()}}">${{o.wl}}</span></td>
    </tr>`).join('');
}}

function buildImpactChart() {{
  const top15 = PLAYERS.slice(0,15);
  new Chart(document.getElementById('impactChart'), {{
    type: 'bar',
    data: {{
      labels: top15.map(p => p.player_name.split(' ').slice(-1)[0]),
      datasets: [{{ data: top15.map(p => p.impact), backgroundColor: top15.map((_,i) => i<3 ? alpha(C.accent,.9) : alpha(C.blue,.5)), borderRadius:3, borderSkipped:false }}]
    }},
    options: {{ indexAxis:'y', responsive:true, maintainAspectRatio:false, plugins:{{legend:{{display:false}}}}, scales:{{ x:{{grid:{{color:alpha('#fff',.04)}},ticks:{{color:C.muted}}}}, y:{{grid:{{display:false}},ticks:{{color:'#e2e6f0',font:{{size:11}}}}}} }} }}
  }});
}}

function buildTSChart() {{
  const top8 = [...PLAYERS].sort((a,b)=>b.tspct-a.tspct).slice(0,8);
  new Chart(document.getElementById('tsChart'), {{
    type: 'bar',
    data: {{
      labels: top8.map(p => p.player_name.split(' ').slice(-1)[0]),
      datasets: [{{ label:'TS%', data: top8.map(p => p.tspct), backgroundColor: alpha(C.green,.7), borderRadius:3, borderSkipped:false }}]
    }},
    options: {{ responsive:true, maintainAspectRatio:false, plugins:{{legend:{{display:false}}}}, scales:{{ x:{{grid:{{display:false}},ticks:{{color:C.muted,font:{{size:10}}}}}}, y:{{min:50,grid:{{color:alpha('#fff',.04)}},ticks:{{color:C.muted,callback:v=>v+'%'}}}} }} }}
  }});
}}

/* ═══ PLAYERS PAGE ════════════════════════════════════════════════════════ */
const playerCharts = {{}};
const tierLabel = {{ elite:'Elite', high:'High Impact', consist:'Consistent', develop:'Developing' }};

function buildTierTable() {{
  const tbody = document.getElementById('tierTable');
  tbody.innerHTML = PLAYERS.map((p,i) => `
    <tr>
      <td><span class="rank ${{i<3?'top':''}}">${{i+1}}</span></td>
      <td>${{p.player_name}}</td>
      <td style="color:var(--muted);font-family:'DM Mono',monospace;font-size:11px">${{p.team_abbr}}</td>
      <td class="num" style="color:var(--accent)">${{p.impact}}</td>
      <td class="num hi-green">${{p.tspct}}%</td>
      <td class="num hi-blue">${{(p.consist*100).toFixed(1)}}%</td>
      <td><span class="tier ${{p.tier}}">${{tierLabel[p.tier]}}</span></td>
    </tr>`).join('');
}}

function updatePlayerChart() {{
  const idx = +document.getElementById('playerSelect').value;
  const p = PLAYERS[idx];
  document.getElementById('playerKPIs').innerHTML = `
    <div class="kpi"><div class="kpi-label">Pontos/Jogo</div><div class="kpi-value orange">${{p.ppg}}</div><div class="kpi-sub">PPG</div></div>
    <div class="kpi"><div class="kpi-label">Assistências/Jogo</div><div class="kpi-value blue">${{p.apg}}</div><div class="kpi-sub">APG</div></div>
    <div class="kpi"><div class="kpi-label">Rebotes/Jogo</div><div class="kpi-value">${{p.rpg}}</div><div class="kpi-sub">RPG</div></div>
    <div class="kpi"><div class="kpi-label">Impact Score</div><div class="kpi-value green">${{p.impact}}</div><div class="kpi-sub">${{p.tier.toUpperCase()}}</div></div>
  `;
  const games = Array.from({{length:20}},(_,i)=> +(p.ppg + (Math.random()-.5)*10).toFixed(1));
  const rolling = games.map((_,i)=>{{ const s=games.slice(Math.max(0,i-4),i+1); return +(s.reduce((a,b)=>a+b,0)/s.length).toFixed(1); }});
  if (playerCharts.trend) playerCharts.trend.destroy();
  playerCharts.trend = new Chart(document.getElementById('playerTrendChart'), {{
    type:'line',
    data:{{ labels:Array.from({{length:20}},(_,i)=>`J${{i+1}}`), datasets:[
      {{ label:'Pontos', data:games, borderColor:alpha(C.blue,.6), backgroundColor:alpha(C.blue,.06), fill:true, tension:0.3, pointRadius:3 }},
      {{ label:'Média 5j', data:rolling, borderColor:C.accent2, borderDash:[4,3], borderWidth:2, pointRadius:0, tension:0.4 }}
    ]}},
    options:{{ responsive:true, maintainAspectRatio:false, plugins:{{legend:{{labels:{{color:'#5a6080',boxWidth:12}}}}}}, scales:{{ x:{{grid:{{display:false}},ticks:{{color:C.muted}}}}, y:{{grid:{{color:alpha('#fff',.04)}},ticks:{{color:C.muted}}}} }} }}
  }});
  if (playerCharts.radar) playerCharts.radar.destroy();
  playerCharts.radar = new Chart(document.getElementById('radarChart'), {{
    type:'radar',
    data:{{ labels:['Pontos','Assistências','Rebotes','Roubos','Bloqueios','Eficiência'],
      datasets:[{{ label:p.player_name, data:[p.ppg/35*100, p.apg/12*100, p.rpg/14*100, p.spg/2.5*100, p.bpg/2.5*100, p.tspct],
        borderColor:C.accent, backgroundColor:alpha(C.accent,.12), pointBackgroundColor:C.accent, pointRadius:4, borderWidth:2 }}]
    }},
    options:{{ responsive:true, maintainAspectRatio:false, plugins:{{legend:{{labels:{{color:'#5a6080'}}}}}},
      scales:{{ r:{{ grid:{{color:alpha('#fff',.07)}}, pointLabels:{{color:'#9ba3c0',font:{{size:11}}}}, ticks:{{display:false}}, suggestedMin:0, suggestedMax:100 }} }}
    }}
  }});
}}

/* ═══ TEAMS PAGE ══════════════════════════════════════════════════════════ */
function buildTeamsTable() {{
  const tbody = document.getElementById('teamsTable');
  tbody.innerHTML = TEAMS.map((t,i) => {{
    const wpct = (t.wins/t.games*100).toFixed(1);
    return `<tr>
      <td><span class="rank ${{i<3?'top':''}}">${{i+1}}</span></td>
      <td><strong>${{t.abbr}}</strong></td>
      <td class="num">${{t.games}}</td>
      <td class="num hi-green">${{t.wins}}</td>
      <td class="num" style="color:#f87171">${{t.games-t.wins}}</td>
      <td class="num hi">${{wpct}}%</td>
      <td class="num">${{t.ppg}}</td>
      <td class="num" style="color:${{t.margin>=0?'var(--green)':'#f87171'}}">${{t.margin>=0?'+':''}}${{t.margin}}</td>
    </tr>`;
  }}).join('');
}}

function buildTeamWinChart() {{
  new Chart(document.getElementById('teamWinChart'), {{
    type:'bar',
    data:{{ labels:TEAMS.map(t=>t.abbr), datasets:[{{ data:TEAMS.map(t=>+(t.wins/t.games*100).toFixed(1)), backgroundColor:TEAMS.map((_,i)=>i<3?alpha(C.green,.8):alpha(C.blue,.45)), borderRadius:3, borderSkipped:false }}] }},
    options:{{ responsive:true, maintainAspectRatio:false, plugins:{{legend:{{display:false}}}}, scales:{{ x:{{grid:{{display:false}},ticks:{{color:C.muted}}}}, y:{{min:30,grid:{{color:alpha('#fff',.04)}},ticks:{{color:C.muted,callback:v=>v+'%'}}}} }} }}
  }});
}}

function buildHomeAwayChart() {{
  new Chart(document.getElementById('homeAwayChart'), {{
    type:'bar',
    data:{{ labels:TOP8_ABBR, datasets:[
      {{ label:'Home W%', data:HOME_DATA, backgroundColor:alpha(C.accent,.75), borderRadius:3 }},
      {{ label:'Away W%', data:AWAY_DATA, backgroundColor:alpha(C.blue,.5),    borderRadius:3 }}
    ]}},
    options:{{ responsive:true, maintainAspectRatio:false, plugins:{{legend:{{labels:{{color:'#5a6080',boxWidth:12}}}}}}, scales:{{ x:{{grid:{{display:false}},ticks:{{color:C.muted}}}}, y:{{grid:{{color:alpha('#fff',.04)}},ticks:{{color:C.muted,callback:v=>v+'%'}}}} }} }}
  }});
}}

/* ═══ TRENDS PAGE ═════════════════════════════════════════════════════════ */
function buildMonthlyChart() {{
  const colors = [C.accent, C.blue, C.green, C.accent2, '#c084fc'];
  new Chart(document.getElementById('monthlyChart'), {{
    type:'line',
    data:{{ labels:MONTH_LABELS, datasets:MONTHLY_DATA.map((p,i)=>({{
      label:p.name, data:p.data, borderColor:colors[i], backgroundColor:'transparent', tension:0.4, pointRadius:3, borderWidth:2
    }})) }},
    options:{{ responsive:true, maintainAspectRatio:false, plugins:{{legend:{{labels:{{color:'#5a6080',boxWidth:12}}}}}}, scales:{{ x:{{grid:{{display:false}},ticks:{{color:C.muted}}}}, y:{{grid:{{color:alpha('#fff',.04)}},ticks:{{color:C.muted}}}} }} }}
  }});
}}

function buildDistributionChart() {{
  new Chart(document.getElementById('distributionChart'), {{
    type:'bar',
    data:{{ labels:DIST_LABELS, datasets:[{{ label:'Jogadores', data:DIST_COUNTS,
      backgroundColor: DIST_LABELS.map((_,i)=> i>=3 ? alpha(C.accent,.8) : alpha(C.blue,.45)),
      borderRadius:4, borderSkipped:false }}]
    }},
    options:{{ responsive:true, maintainAspectRatio:false, plugins:{{legend:{{display:false}}}}, scales:{{ x:{{grid:{{display:false}},ticks:{{color:C.muted}}}}, y:{{grid:{{color:alpha('#fff',.04)}},ticks:{{color:C.muted}}}} }} }}
  }});
}}

function buildTopGamesTable() {{
  const tbody = document.getElementById('topGamesTable');
  tbody.innerHTML = TOP_GAMES.map((g,i) => `
    <tr>
      <td><span class="rank ${{i<3?'top':''}}">${{i+1}}</span></td>
      <td>${{g.player_name}}</td>
      <td style="color:var(--muted);font-family:'DM Mono',monospace;font-size:11px">${{g.team_abbr}}</td>
      <td style="color:var(--muted);font-size:11px">${{g.date}}</td>
      <td style="font-size:11px">${{g.matchup}}</td>
      <td class="num hi">${{g.pts}}</td>
      <td class="num">${{g.ast}}</td>
      <td class="num">${{g.reb}}</td>
      <td class="num" style="color:var(--accent2);font-weight:600">${{g.gmsc}}</td>
      <td><span class="wl ${{g.wl.toLowerCase()}}">${{g.wl}}</span></td>
    </tr>`).join('');
}}

/* ═══ INIT ════════════════════════════════════════════════════════════════ */
buildTopTable();
buildOutlierTable();
buildImpactChart();
buildTSChart();
buildTierTable();
updatePlayerChart();
buildTeamsTable();
buildTeamWinChart();
buildHomeAwayChart();
buildMonthlyChart();
buildDistributionChart();
buildTopGamesTable();
</script>
</body>
</html>"""
    return html


### Main
def run():
    print("Conectando ao banco...")
    engine = get_engine()

    with engine.connect() as conn:
        print("  Buscando jogadores...")
        players = fetch_players(conn)

        print("  Buscando times...")
        teams = fetch_teams(conn)

        print("  Buscando home/away...")
        home_away = fetch_home_away(conn)

        print("  Buscando outliers...")
        outliers = fetch_outliers(conn)

        print("  Buscando top games...")
        top_games = fetch_top_games(conn)

        print("  Buscando tendência mensal...")
        top_ids = fetch_top_player_ids(conn, 5)
        monthly = fetch_monthly(conn, top_ids) if top_ids else []

        print("  Buscando KPIs...")
        kpis = fetch_kpis(conn)

        print("  Buscando distribuição...")
        impact_dist = fetch_impact_distribution(conn)

    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")
    print(f"\nGerando HTML ({len(players)} jogadores, {len(teams)} times)...")

    html = generate_html(
        players,
        teams,
        home_away,
        outliers,
        top_games,
        monthly,
        kpis,
        impact_dist,
        generated_at,
    )

    out_path = os.path.join(BASE_DIR, "nba_dashboard.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✔ Dashboard gerado: {out_path}")
    print(f"  Abra o arquivo no navegador para visualizar.")


if __name__ == "__main__":
    run()
