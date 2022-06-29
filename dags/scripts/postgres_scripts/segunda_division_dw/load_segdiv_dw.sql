-- Load Segunda Division Data Warehouse (Data Pipeline)


-- delete old data
DELETE FROM fact_results
WHERE league_id = {{params.league_id}} AND season = {{params.season}};

DELETE FROM dim_fixtures
WHERE fixture_id IN (SELECT fixture_id
                    FROM dblink ('hostaddr=127.0.0.1 port=5432 dbname=football_db user=airflow password=airflow',
				                    'SELECT fixture_id
				                     FROM api.fixtures
				                     WHERE league_id = {{params.league_id}} AND league_season = {{params.season}}')
				    AS t (fixture_id bigint));

--DELETE FROM fact_standings
--WHERE league_id = {{params.league_id}} AND season = {{params.season}};


-- setting fact primary keys sequence
BEGIN;
LOCK TABLE fact_results IN EXCLUSIVE MODE;
--LOCK TABLE fact_standings IN EXCLUSIVE MODE;
SELECT setval('fact_results_id_seq', COALESCE((SELECT MAX(id)+1 FROM fact_results), 1), false);
--SELECT setval('fact_standings_id_seq', COALESCE((SELECT MAX(id)+1 FROM fact_standings), 1), false);
COMMIT;

drop extension if exists dblink;
create extension dblink;


-- dim_fixtures

INSERT INTO dim_fixtures
	SELECT fixture_id, fixture_date_utc, fixture_timestamp

    FROM dblink ('hostaddr=127.0.0.1 port=5432 dbname=football_db user=airflow password=airflow',
				 'SELECT fixture_id, fixture_date, fixture_timestamp
				  FROM api.fixtures
				  WHERE league_id = {{params.league_id}} AND league_season = {{params.season}}')
    AS t(
		fixture_id bigint,
        fixture_date_utc timestamp without time zone,
        fixture_timestamp varchar);


-- fact_results

INSERT INTO fact_results (fixture_id, league_id, season, round, team_home_id, team_away_id, no_draw_home, draw_home,
							  no_draw_away, draw_away, goals_home, goals_away, score_halftime_home,
							  score_halftime_away, halftime_goals_total, fulltime_goals_total, match_result)

    SELECT fixture_id, league_id, season, round, team_home_id, team_away_id, no_draw_home, draw_home, no_draw_away,
            draw_away, goals_home, goals_away, score_halftime_home, score_halftime_away, halftime_goals_total,
            fulltime_goals_total, match_result

    FROM dblink ('hostaddr=127.0.0.1 port=5432 dbname=football_db user=airflow password=airflow',
                    'SELECT f.fixture_id,
                            f.league_id,
                            f.league_season,
                            f.league_round,
                            f.teams_home_id,
                            f.teams_away_id,
                            dw.no_draw_home,
                            dw.draw_home,
                            dw.no_draw_away,
                            dw.draw_away,
                            r.goals_home,
                            r.goals_away,
                            r.score_halftime_home,
                            r.score_halftime_away,
                            r.halftime_goals_total,
                            r.fulltime_goals_total,
                            r.match_result
                        FROM api.fixtures as f
                        LEFT JOIN api.results as r on r.fixture_id = f.fixture_id
                        INNER JOIN cal.draw_series AS dw on dw.fixture_id = f.fixture_id
                        WHERE f.league_id = {{params.league_id}} AND f.league_season = {{params.season}}'
                        )
    AS t(
		fixture_id bigint,
		league_id smallint,
		season smallint,
		round smallint,
		team_home_id integer,
		team_away_id integer,
        no_draw_home smallint,
        draw_home smallint,
        no_draw_away smallint,
        draw_away smallint,
		goals_home smallint,
		goals_away smallint,
		score_halftime_home smallint,
		score_halftime_away smallint,
		halftime_goals_total smallint,
		fulltime_goals_total smallint,
		match_result smallint
	);



-- fact_standings

INSERT INTO fact_standings (standings_type_id, league_id, team_id, season, round, team_position, mp, w, d, l, gf, ga, gd, pts)

	SELECT standings_type_id, league_id, team_id, season, round, team_position, mp, w, d, l, gf, ga, gd, pts
	FROM dblink ('hostaddr=127.0.0.1 port=5432 dbname=football_db user=airflow password=airflow',
	                'SELECT *
                        FROM (
                            SELECT league_id, season, 1 as standings_type_id, round, team_position, team_id, mp, w, d, l, gf, ga, gd, pts
                            FROM cal.league_table
                            UNION ALL
                            SELECT league_id, season, 2 as standings_type_id, round, team_position, team_id, mp, w, d, l, gf, ga, gd, pts
                            FROM cal.league_table_home
                            UNION ALL
                            SELECT league_id, season, 3 as standings_type_id, round, team_position, team_id, mp, w, d, l, gf, ga, gd, pts
                            FROM cal.league_table_away
                            ORDER BY  league_id, season, round,  standings_type_id, team_position) as t
                        WHERE t.league_id = {{params.league_id}} AND t.season = {{params.season}}')
	AS t(
	    league_id smallint,
        season smallint,
        standings_type_id smallint,
        round smallint,
        team_position smallint,
        team_id integer,
        MP smallint,
        W smallint,
        D smallint,
        L smallint,
        GF smallint,
        GA smallint,
        GD smallint,
        Pts smallint)
    ON CONFLICT ON CONSTRAINT unique_fact_standings_multi_col_idx DO NOTHING;