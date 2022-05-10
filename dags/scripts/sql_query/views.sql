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
		LEFT JOIN dim_team AS T_home ON T_home.team_id = R.team_home_id
		LEFT JOIN dim_team AS T_away ON T_away.team_id = R.team_away_id
		RIGHT JOIN dim_fixtures AS S ON S.fixture_id = R.fixture_id
		ORDER BY S.fixture_date_utc)


-- current standings Segunda Division

--DROP MATERIALIZED VIEW mat_view_segdiv_current_standings;
CREATE MATERIALIZED VIEW mat_view_segdiv_current_standings AS (
SELECT s.standings_type_id, s.team_position, t.team_name, s.mp, s.w, s.d, s.l, s.gf, s.ga, s.gd, s.pts
FROM fact_standings as s
LEFT JOIN dim_team as t on t.team_id = s.team_id
WHERE s.league_id = 141 and s.season = get_current_season(141) AND s.round = get_current_round(141));

CREATE UNIQUE INDEX ON mat_view_segdiv_current_standings (standings_type_id,team_position);


-- full performance

CREATE OR REPLACE VIEW view_performance AS (
	SELECT r.league_id
			,r.fixture_id
			,r.season
			,r.round
			,r.team_home_id
			,r.team_away_id
			,r.goals_home
			,r.goals_away
			,r.score_halftime_home
			,r.score_halftime_away
			,r.match_result
			,s_home.team_position AS "team_home_table_pos"
			,s_home.d AS "team_home_draws"
			,s_home.gd AS "team_home_goals_diff"
			,s_home.pts AS "team_home_pts"
			,s_away.team_position AS "team_away_table_pos"
			,s_away.d AS "team_away_draws"
			,s_away.gd AS "team_away_goals_diff"
			,s_away.pts AS "team_away_pts"
			,ABS(s_home.team_position - s_away.team_position) as "table_pos_diff"
			,ROUND(s_home.d / s_home.MP::numeric, 2) as "draw_home_pcts"
			,ROUND(s_away.d / s_away.MP::numeric, 2) as "draw_away_pcts"

	FROM fact_results as r
	LEFT JOIN fact_standings as s_home on s_home.league_id = r.league_id AND s_home.season = r.season AND s_home.round = r.round AND s_home.team_id = r.team_home_id AND s_home.standings_type_id = 1
	LEFT JOIN fact_standings as s_away on s_away.league_id = r.league_id AND s_away.season = r.season AND s_away.round = r.round AND s_away.team_id = r.team_away_id AND s_away.standings_type_id = 1
	)