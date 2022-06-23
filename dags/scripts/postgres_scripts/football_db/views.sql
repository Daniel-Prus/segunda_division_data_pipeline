-- auxiliary view to calculate 'draw series'

CREATE OR REPLACE VIEW view_draw_series_aux AS

	(with cte2 AS (with cte1 AS (SELECT f.fixture_id
										,f.fixture_date
										,f.league_season
										,f.teams_home_id
										,f.teams_away_id
										,r.match_result
								FROM api.fixtures AS f
								LEFT JOIN api.results AS r ON r.fixture_id = f.fixture_id
								WHERE f.league_season = 2021)

				select fixture_id, fixture_date, teams_home_id as team_id, match_result, True as is_home
				from cte1
				UNION ALL
				select fixture_id, fixture_date, teams_away_id as team_id, match_result, False as is_home
				from cte1
				ORDER BY team_id, fixture_id)
	select *,
		CASE WHEN match_result = 1 OR match_result = 2 THEN 1 ELSE 0 END AS aux_no_draw,
		CASE WHEN match_result = 0 THEN 1 ELSE 0 END AS aux_draw
	from cte2);


--  no draw/draw series by team_id

CREATE OR REPLACE VIEW view_draw_series_final AS

	(with cte2 AS (with cte1 AS (SELECT *, LAG(aux_no_draw) OVER (PARTITION BY team_id ORDER BY team_id, fixture_date) as no_draw_lag
										 , LAG(aux_draw) OVER (PARTITION BY team_id ORDER BY team_id, fixture_date) as draw_lag
								 FROM view_draw_series_aux)

						SELECT *
						    ,COUNT(CASE WHEN no_draw_lag = 0 or no_draw_lag is null THEN 0 END) OVER (ORDER BY team_id, fixture_date) AS no_draw_series_aux
						    ,COUNT(CASE WHEN draw_lag = 0 THEN 0 END) OVER (ORDER BY team_id, fixture_date) AS draw_series_aux
						FROM cte1)

		SELECT fixture_id, team_id, is_home
				,(ROW_NUMBER() OVER (PARTITION BY team_id, no_draw_series_aux)) -1  as no_draw_series
				,(ROW_NUMBER() OVER (PARTITION BY team_id, draw_series_aux)) - 1  as draw_series
		FROM cte2);