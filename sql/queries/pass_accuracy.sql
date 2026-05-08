-- Pass accuracy by team per match
SELECT
    m.match_id,
    m.home_team_name,
    m.away_team_name,
    t.team_name,
    COUNT(p.event_uuid)                                           AS total_passes,
    COUNT(CASE WHEN p.pass_outcome IS NULL THEN 1 END)           AS completed_passes,
    ROUND(
        100.0 * COUNT(CASE WHEN p.pass_outcome IS NULL THEN 1 END)
        / NULLIF(COUNT(p.event_uuid), 0), 1
    )                                                             AS pass_accuracy_pct
FROM fact_passes p
JOIN fact_events    e USING (event_uuid)
JOIN dim_teams      t ON e.team_id   = t.team_id
JOIN dim_matches    m USING (match_id)
GROUP BY m.match_id, m.home_team_name, m.away_team_name, t.team_name
ORDER BY m.match_id, t.team_name;
