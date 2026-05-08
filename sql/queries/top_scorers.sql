-- Top scorers by competition
SELECT
    pl.player_name,
    t.team_name,
    c.competition_name,
    COUNT(*)                          AS goals,
    ROUND(SUM(s.shot_xg)::NUMERIC, 2) AS total_xg,
    COUNT(*) - ROUND(SUM(s.shot_xg)::NUMERIC, 2) AS xg_overperformance
FROM fact_shots s
JOIN fact_events    e USING (event_uuid)
JOIN dim_players   pl ON e.player_id = pl.player_id
JOIN dim_teams      t  ON e.team_id  = t.team_id
JOIN dim_matches    m  USING (match_id)
JOIN dim_competitions c USING (competition_id)
WHERE s.shot_outcome = 'Goal'
GROUP BY pl.player_name, t.team_name, c.competition_name
ORDER BY goals DESC
LIMIT 20;
