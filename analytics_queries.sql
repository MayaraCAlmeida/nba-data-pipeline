---- 1. Top 20 jogadores por Impact Score 
SELECT
    player_name,
    team_abbr,
    games_played,
    ROUND(avg_points, 1)       AS ppg,
    ROUND(avg_assists, 1)      AS apg,
    ROUND(avg_rebounds, 1)     AS rpg,
    ROUND(avg_steals, 1)       AS spg,
    ROUND(avg_blocks, 1)       AS bpg,
    ROUND(avg_ts_pct * 100, 1) AS ts_pct,
    ROUND(avg_impact_score, 2) AS impact_score,
    impact_rank
FROM player_season_stats
WHERE games_played >= 10
ORDER BY impact_rank
LIMIT 20;


--- 2. Evolução semanal de pontos (rolling 5 jogos) 
SELECT
    player_name,
    team_abbr,
    game_date,
    points,
    ROUND(points_rolling5, 1)  AS pts_avg_last5,
    ROUND(points_rolling10, 1) AS pts_avg_last10
FROM player_gamelogs
WHERE player_id IN (
    SELECT player_id FROM player_season_stats ORDER BY impact_rank LIMIT 10
)
ORDER BY player_name, game_date;

--- 3. Desempenho home vs away por time 
SELECT
    team_abbr,
    CASE WHEN is_home = 1 THEN 'Home' ELSE 'Away' END AS location,
    COUNT(*) AS games,
    SUM(is_win) AS wins,
    ROUND(100.0 * SUM(is_win) / COUNT(*), 1) AS win_pct,
    ROUND(AVG(points), 1) AS avg_points
FROM games
GROUP BY team_abbr, is_home
ORDER BY team_abbr, is_home DESC;

---- 4. Jogos outlier — performances fora da curva
SELECT
    player_name,
    team_abbr,
    game_date,
    matchup,
    points,
    assists,
    rebounds,
    ROUND(impact_score, 2)        AS impact_score,
    ROUND(points_zscore, 2)       AS pts_zscore,
    ROUND(impact_score_zscore, 2) AS impact_zscore,
    win_loss
FROM player_gamelogs
WHERE ABS(points_zscore) > 2.5
ORDER BY ABS(points_zscore) DESC
LIMIT 50;


--- 5. Eficiência ofensiva — True Shooting Top 30 
SELECT
    player_name,
    team_abbr,
    games_played,
    ROUND(avg_points, 1)        AS ppg,
    ROUND(avg_ts_pct * 100, 1)  AS ts_pct,
    ROUND(avg_ast_tov, 2)       AS ast_tov_ratio,
    ROUND(avg_impact_score, 2)  AS impact_score
FROM player_season_stats
WHERE games_played >= 15
ORDER BY avg_ts_pct DESC
LIMIT 30;

--- 6. Ranking de times na temporada 
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'games'
ORDER BY ordinal_position;

SELECT
    team_abbr,
    COUNT(*)                          AS games_played,
    SUM(is_win)                       AS wins,
    ROUND(100.0 * AVG(is_win), 1)     AS win_pct,
    ROUND(AVG(points), 1)             AS avg_points,
    ROUND(AVG(point_diff), 1)         AS avg_margin
FROM games
GROUP BY team_abbr
ORDER BY win_pct DESC;

CREATE VIEW vw_team_performance AS
SELECT
    team_abbr,
    COUNT(*)                      AS games_played,
    SUM(is_win)                   AS wins,
    ROUND(100.0 * AVG(is_win), 1) AS win_pct,
    ROUND(AVG(points), 1)         AS avg_points,
    ROUND(AVG(point_diff), 1)     AS avg_margin
FROM games
GROUP BY team_abbr;

SELECT
    team_abbr,
    games_played,
    wins,
    (games_played - wins) AS losses,
    ROUND(win_pct, 1)     AS win_pct,
    avg_points,
    ROUND(avg_margin, 1)  AS avg_margin
FROM vw_team_performance;


--- 7. Consistência vs produção por jogador 
-- Quadrante: Alto impacto + Alta consistência = Elite
SELECT
    player_name,
    team_abbr,
    ROUND(avg_impact_score, 2) AS avg_impact,
    ROUND(consistency_score, 3) AS consistency,
    CASE
        WHEN avg_impact_score > 20 AND consistency_score > 0.15 THEN 'Elite'
        WHEN avg_impact_score > 20                              THEN 'High Impact / Volatile'
        WHEN consistency_score > 0.15                          THEN 'Consistent / Lower Impact'
        ELSE 'Developing'
    END AS player_tier
FROM player_season_stats
WHERE games_played >= 10
ORDER BY avg_impact_score DESC;


--- 8. Comparação de dois jogadores (parametrizado) 
-- Substituir os nomes dos jogadores ao executar
SELECT
    player_name,
    games_played,
    ROUND(avg_points, 1)       AS ppg,
    ROUND(avg_assists, 1)      AS apg,
    ROUND(avg_rebounds, 1)     AS rpg,
    ROUND(avg_ts_pct * 100, 1) AS ts_pct,
    ROUND(avg_plus_minus, 2)   AS plus_minus,
    ROUND(avg_impact_score, 2) AS impact_score,
    win_rate
FROM player_season_stats
WHERE player_name IN ('Nikola Jokić', 'Giannis Antetokounmpo')
ORDER BY avg_impact_score DESC;

   


--- 9. Melhores jogos individuais da temporada 
SELECT
    player_name,
    team_abbr,
    game_date,
    matchup,
    points,
    assists,
    rebounds,
    steals,
    blocks,
    ROUND(impact_score, 2) AS impact_score,
    ROUND(game_score, 2)   AS game_score,
    win_loss
FROM player_gamelogs
ORDER BY game_score DESC
LIMIT 25;


---- 10. Tendência mensal — média de pontos por mês 
SELECT
    player_name,
    TO_CHAR(game_date, 'YYYY-MM') AS month,
    COUNT(*)                       AS games,
    ROUND(AVG(points), 1)          AS avg_pts,
    ROUND(AVG(impact_score), 2)    AS avg_impact
FROM player_gamelogs
WHERE player_id IN (
    SELECT player_id FROM player_season_stats ORDER BY impact_rank LIMIT 5
)
GROUP BY player_name, TO_CHAR(game_date, 'YYYY-MM')
ORDER BY player_name, month;