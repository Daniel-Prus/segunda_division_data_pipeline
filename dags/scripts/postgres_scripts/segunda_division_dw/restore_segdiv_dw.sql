-- Build Segunda Division Data Warehouse from scratch

truncate table  dim_league_season, dim_team, dim_fixtures, fact_results restart identity cascade;
truncate table dim_standings_type, fact_standings restart identity cascade;

drop extension if exists dblink;
create extension dblink;

-- dim_league_season

insert into dim_league_season

    select *
    from dblink ('hostaddr=127.0.0.1 port=5432 dbname=football_db user=airflow password=airflow',
				 'SELECT l.league_id, s.SEASON, l.league_level, l.league_name, l.league_country,
                            s.start_date, s.end_date, s.season_info, s.rounds
                  FROM api.SEASON AS s
                  LEFT JOIN api.league AS l ON l.league_id = s.league_id')
    as t (
        league_id smallint,
        season smallint,
        league_level smallint,
        league_name varchar,
        league_country varchar,
        start_date date,
        end_date date,
        season_info varchar(9),
        rounds smallint);


-- dim_fixtures

insert into dim_fixtures
	select fixture_id, fixture_date_utc, fixture_timestamp

    from dblink ('hostaddr=127.0.0.1 port=5432 dbname=football_db user=airflow password=airflow',
				 'SELECT fixture_id, fixture_date, fixture_timestamp FROM api.fixtures')
    as t(
		fixture_id bigint,
        fixture_date_utc timestamp without time zone,
        fixture_timestamp varchar);

insert into dim_team
	select *

    from dblink ('hostaddr=127.0.0.1 port=5432 dbname=football_db user=airflow password=airflow',
				 'SELECT * FROM api.team
				 ORDER BY team_id')
    as t(
		team_id integer,
        team_name varchar);


-- insert into dim_standings_type

INSERT INTO dim_standings_type
VALUES
    (1, 'overall'),
    (2, 'home'),
    (3, 'away');

-- fact_results

insert into fact_results (fixture_id, league_id, season, round, team_home_id, team_away_id, no_draw_home, draw_home,
							  no_draw_away, draw_away, goals_home, goals_away, score_halftime_home,
							  score_halftime_away, halftime_goals_total, fulltime_goals_total, match_result)

    select fixture_id, league_id, season, round, team_home_id, team_away_id, no_draw_home, draw_home, no_draw_away,
            draw_away, goals_home, goals_away, score_halftime_home, score_halftime_away, halftime_goals_total,
            fulltime_goals_total, match_result

    from dblink ('hostaddr=127.0.0.1 port=5432 dbname=football_db user=airflow password=airflow',
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
                        FROM api.fixtures AS f
                        LEFT JOIN api.results AS r ON r.fixture_id = f.fixture_id
                        INNER JOIN cal.draw_series AS dw on dw.fixture_id = f.fixture_id')
    as t(
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

insert into fact_standings (league_id, season, standings_type_id, round, team_position, team_id, mp, w, d, l, gf, ga, gd, pts)

	select league_id, season, standings_type_id, round, team_position, team_id, mp, w, d, l, gf, ga, gd, pts
	from dblink ('hostaddr=127.0.0.1 port=5432 dbname=football_db user=airflow password=airflow',
				 	'SELECT league_id, SEASON, 1 as standings_type_id, round, team_position, team_id, mp, w, d, l, gf, ga, gd, pts
					FROM cal.league_table
					UNION ALL
					SELECT league_id, SEASON, 2 as standings_type_id, round, team_position, team_id, mp, w, d, l, gf, ga, gd, pts
					FROM cal.league_table_home
					UNION ALL
					SELECT league_id, SEASON, 3 as standings_type_id, round, team_position, team_id, mp, w, d, l, gf, ga, gd, pts
					FROM cal.league_table_away
					ORDER BY  league_id, SEASON, round,  standings_type_id, team_position;')
	as t(
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
        Pts smallint);
