-- Build Segunda Division Data Warehouse

truncate table  dim_season, dim_team, dim_league, dim_fixtures, fact_results restart identity cascade;
truncate table dim_standings_type, fact_standings restart identity cascade;

drop extension if exists dblink;
create extension dblink;


-- dim_league

insert into dim_league

	select *
	from dblink ('hostaddr=127.0.0.1 port=5432 dbname=football_db user=airflow password=airflow',
				 'SELECT * FROM api.league')
	as t(
		league_id smallint,
		league_name varchar,
		league_country varchar,
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

-- dim_season

insert into dim_season
	select *

    from dblink ('hostaddr=127.0.0.1 port=5432 dbname=football_db user=airflow password=airflow',
				 'SELECT * FROM api.season')
    as t(
		season integer,
        league_id integer,
        start_date date,
        end_date date,
        season_info varchar);

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

insert into fact_results (fixture_id, league_id, season, round, team_home_id, team_away_id,
							  team_home_winner, team_away_winner, goals_home, goals_away, score_halftime_home,
							  score_halftime_away, halftime_goals_total, fulltime_goals_total, match_result)

    select fixture_id, league_id, season, round, team_home_id, team_away_id,
		team_home_winner, team_away_winner, goals_home, goals_away, score_halftime_home, score_halftime_away,
		halftime_goals_total, fulltime_goals_total, match_result

    from dblink ('hostaddr=127.0.0.1 port=5432 dbname=football_db user=airflow password=airflow',
                    'SELECT f.fixture_id,
                            f.league_id,
                            f.league_season,
                            f.league_round,
                            f.teams_home_id,
                            f.teams_away_id,
                            r.teams_home_winner,
                            r.teams_away_winner,
                            r.goals_home,
                            r.goals_away,
                            r.score_halftime_home,
                            r.score_halftime_away,
                            r.halftime_goals_total,
                            r.fulltime_goals_total,
                            r.match_result
                        FROM api.fixtures as f
                        LEFT JOIN api.results as r on r.fixture_id = f.fixture_id')
    as t(
		fixture_id bigint,
		league_id smallint,
		season smallint,
		round smallint,
		team_home_id integer,
		team_away_id integer,
		team_home_winner boolean,
		team_away_winner boolean,
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
				 	'SELECT league_id, season, 1 as standings_type_id, round, team_position, team_id, mp, w, d, l, gf, ga, gd, pts
					FROM cal.league_table
					UNION ALL
					SELECT league_id, season, 2 as standings_type_id, round, team_position, team_id, mp, w, d, l, gf, ga, gd, pts
					FROM cal.league_table_home
					UNION ALL
					SELECT league_id, season, 3 as standings_type_id, round, team_position, team_id, mp, w, d, l, gf, ga, gd, pts
					FROM cal.league_table_away
					ORDER BY  league_id, season, round,  standings_type_id, team_position;')
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

-- add foreign keys

ALTER TABLE fact_results
    ADD CONSTRAINT FK_results_team_home FOREIGN KEY (team_home_id) REFERENCES dim_team (team_id),
	ADD CONSTRAINT FK_results_team_away FOREIGN KEY (team_away_id) REFERENCES dim_team (team_id),
	ADD CONSTRAINT FK_results_season FOREIGN KEY (league_id, season) REFERENCES dim_season (league_id, season),
	ADD CONSTRAINT FK_results_fixtures FOREIGN KEY (fixture_id) REFERENCES dim_fixtures (fixture_id),
	ADD CONSTRAINT FK_results_league FOREIGN KEY (league_id) REFERENCES dim_league (league_id);

ALTER TABLE fact_standings
	ADD CONSTRAINT FK_standings_team FOREIGN KEY (team_id) REFERENCES dim_team (team_id),
	ADD CONSTRAINT FK_standings_season FOREIGN KEY (league_id, season) REFERENCES dim_season (league_id, season),
	ADD CONSTRAINT FK_standings_league FOREIGN KEY (league_id) REFERENCES dim_league (league_id),
	ADD CONSTRAINT FK_standings_type FOREIGN KEY (standings_type_id) REFERENCES dim_standings_type (standings_type_id);

ALTER TABLE dim_season
	ADD CONSTRAINT FK_season_league FOREIGN KEY (league_id) REFERENCES dim_league (league_id);