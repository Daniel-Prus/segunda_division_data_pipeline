-- match results and schedule

CREATE OR REPLACE VIEW view_match_results AS (
	SELECT 	R.league_id
			,S.fixture_date_utc
			,T_home.team_name AS "team_home_name"
			,T_away.team_name AS "team_away_name"
			,R.season
			,R.round
			,CONCAT(R.goals_home, ':',  R.goals_away) as "match_result"
		FROM fact_results AS R
		LEFT JOIN dim_team AS T_home on T_home.team_id = R.team_home_id
		LEFT JOIN dim_team AS T_away on T_away.team_id = R.team_away_id
		RIGHT JOIN dim_fixtures AS S on S.fixture_id = R.fixture_id
		ORDER BY S.fixture_date_utc)


-- current standings Segunda Division

--DROP MATERIALIZED VIEW mat_view_segdiv_current_standings;
CREATE MATERIALIZED VIEW mat_view_segdiv_current_standings as(
SELECT s.standings_type_id, s.team_position, t.team_name, s.mp, s.w, s.d, s.l, s.gf, s.ga, s.gd, s.pts
FROM fact_standings as s
LEFT JOIN dim_team as t on t.team_id = s.team_id
WHERE s.league_id = 141 and s.season = get_current_season(141) and s.round = get_current_round(141));