-- match results and schedule

CREATE OR REPLACE VIEW v_match_results AS (
	SELECT 	R.league_id
			,S.fixture_date_utc
			,T_home.team_name AS "team_home_name"
			,T_away.team_name AS "team_away_name"
			,R.season
			,R.round
			,CONCAT(R.goals_home, ':',  R.goals_away) AS "match_result"
		FROM fact_results AS R
		JOIN dim_team AS T_home ON T_home.team_id = R.team_home_id
		JOIN dim_team AS T_away ON T_away.team_id = R.team_away_id
		RIGHT JOIN dim_fixtures AS S ON S.fixture_id = R.fixture_id
		ORDER BY S.fixture_date_utc);


-- current standings Segunda Division

--DROP MATERIALIZED VIEW mv_segdiv_current_standings;
CREATE MATERIALIZED VIEW mv_segdiv_current_standings AS (
    SELECT s.standings_type_id, s.team_position, t.team_name, s.mp, s.w, s.d, s.l, s.gf, s.ga, s.gd, s.pts
    FROM fact_standings as s
    JOIN dim_team as t on t.team_id = s.team_id
    WHERE s.league_id = 141 and s.season = get_current_season(141) AND s.round = get_current_round(141));

    CREATE UNIQUE INDEX ON mv_segdiv_current_standings (standings_type_id ASC,team_position ASC);


-- full performance

CREATE OR REPLACE VIEW v_performance AS (
	SELECT r.league_id
			,r.fixture_id
			,r.season
			,r.round
			,r.team_home_id
			,r.team_away_id
			,r.no_draw_home
			,r.draw_home
			,r.no_draw_away
			,r.draw_away
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
	);


-- match results percentage share by league_id, season

CREATE OR REPLACE VIEW v_match_results_pcts_league_season AS (
			SELECT ls.league_name, ls.league_country, t1.season, t1.match_result, t1.results_pcts
			FROM
						(SELECT r.league_id, r.season
								,CASE WHEN r.match_result = 0 THEN 'draw' WHEN r.match_result = 1 THEN 'home_win' ELSE 'away_win' END AS match_result
								,round (COUNT(*) / r2.match_num_sum::numeric, 2) as results_pcts
						FROM fact_results AS r
						JOIN (SELECT r2.league_id, r2.season, COUNT(*) AS match_num_sum FROM fact_results AS r2 GROUP BY r2.league_id, r2.season) AS r2 ON r2.league_id = r.league_id AND r2.season = r.season
						GROUP BY r.league_id, r.season,  r.match_result, r2.match_num_sum) AS t1
			JOIN (SELECT DISTINCT league_id, league_name, league_country FROM dim_league_season) AS ls ON ls.league_id = t1.league_id
			ORDER BY  ls.league_name, t1.season, t1.match_result DESC);

	
