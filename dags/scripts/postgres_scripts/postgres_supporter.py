class FootballDB:

    def __init__(self, season="", league_id=""):
        self.season = season
        self.league_id = league_id

    schemas = """
        CREATE SCHEMA IF NOT EXISTS api AUTHORIZATION airflow;
        CREATE SCHEMA IF NOT EXISTS val AUTHORIZATION airflow;
        CREATE SCHEMA IF NOT EXISTS cal AUTHORIZATION airflow;
    """

    api_fixtures = """
        CREATE TABLE api.fixtures(
            fixture_id bigint NOT NULL,
            fixture_date timestamp NULL,
            fixture_timestamp varchar(50) NULL,
            league_id smallint NULL,
            league_name varchar(100) NULL,
            league_country varchar(100) NULL,
            league_season smallint NULL,
            league_round smallint NULL,
            teams_home_id integer NULL,
            teams_home_name varchar(50) NULL,
            teams_away_id integer NULL,
            teams_away_name varchar(50) NULL, 
            created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    api_results = """
        CREATE TABLE api.results(
        fixture_id bigint NOT NULL,
        teams_home_winner boolean NULL,
        teams_away_winner boolean NULL,
        goals_home smallint NULL,
        goals_away smallint NULL,
        score_halftime_home smallint NULL,
        score_halftime_away smallint NULL,
        halftime_goals_total smallint NULL,
        fulltime_goals_total smallint NULL,
        match_result smallint NULL,
        created timestamp DEFAULT CURRENT_TIMESTAMP
    );
    """

    api_results_columns = [
        "fixture_id", "teams_home_winner", "teams_away_winner", "goals_home", "goals_away", "score_halftime_home",
        "score_halftime_away", "halftime_goals_total", "fulltime_goals_total", "match_result"
    ]
    # delete the newer versions
    api_results_drop_duplicates = """
        DELETE FROM api.results T1
        USING api.results T2
        WHERE  T1.ctid > T2.ctid       				
        AND  T1.fixture_id = T2.fixture_id;
    """

    api_season = """
        CREATE TABLE api.SEASON(
        SEASON smallint NOT NULL,
        league_id smallint NOT NULL,
        start_date date NOT NULL,
        end_date date NOT NULL,
        season_info varchar(9) GENERATED ALWAYS AS ((((date_part('year'::text, start_date))::text || '/'::text) || (date_part('year'::text, end_date))::text)) STORED,
        rounds smallint,
        CONSTRAINT PK_api_season PRIMARY KEY (SEASON, league_id)
    );
    """

    api_team = """
        CREATE TABLE api.team(
        team_id integer NOT NULL,
        team_name varchar(50),
        CONSTRAINT PK_api_team PRIMARY KEY (team_id)
    );
    """

    api_league = """
        CREATE TABLE api.league(
        league_id smallint NOT NULL,
        league_level smallint,
        league_name varchar(100),
        league_country varchar(100),
        CONSTRAINT PK_league PRIMARY KEY (league_id)
    );
    """

    cal_league_table = """
        CREATE TABLE cal.league_table(
        league_id smallint,
        team_id integer,
        SEASON smallint,
        round smallint,
        team_position smallint,
        MP smallint,
        W smallint,
        D smallint,
        L smallint,
        GF smallint,
        GA smallint,
        GD smallint,
        Pts smallint,
        created timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT PK_cal_league_table PRIMARY KEY (league_id, team_id, SEASON, round)  
    );
    """

    cal_league_table_home = """
        CREATE TABLE cal.league_table_home(
        league_id smallint,
        team_id integer,
        SEASON smallint,
        round smallint,
        team_position smallint,
        MP smallint,
        W smallint,
        D smallint,
        L smallint,
        GF smallint,
        GA smallint,
        GD smallint,
        Pts smallint,
        created timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT PK_cal_league_table_home PRIMARY KEY (league_id, team_id, SEASON, round)
    );
    """

    cal_league_table_away = """
        CREATE TABLE cal.league_table_away(
        league_id smallint,
        team_id integer,
        SEASON smallint,
        round smallint,
        team_position smallint,
        MP smallint,
        W smallint,
        D smallint,
        L smallint,
        GF smallint,
        GA smallint,
        GD smallint,
        Pts smallint,
        created timestamp DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT PK_cal_league_table_away PRIMARY KEY (league_id, team_id, SEASON, round)
    );
    """

    cal_draw_series = """
        CREATE TABLE cal.draw_series(
        fixture_id bigint NOT NULL,
        no_draw_home smallint,
        draw_home smallint,
        no_draw_away smallint,
        draw_away smallint,
        created timestamp DEFAULT CURRENT_TIMESTAMP
    );
    """
    # delete the older versions
    cal_draw_series_drop_duplicates = """
        DELETE FROM cal.draw_series T1
        USING cal.draw_series T2
        WHERE  T1.ctid < T2.ctid       				
        AND  T1.fixture_id = T2.fixture_id;
    """

    val_team_market_value = """
        CREATE TABLE val.team_market_value(
        id BIGSERIAL PRIMARY KEY NOT NULL,
        team_id smallint NULL,
        SEASON smallint NULL,
        squad smallint NULL,
        age numeric (3, 1) NULL,
        foreigners smallint NULL,
        market_value bigint NULL,
        total_market_value bigint NULL,
        created timestamp DEFAULT CURRENT_TIMESTAMP
    );
    """

    add_indexes = """
        CREATE INDEX fixtures_fixture_id_idx ON api.fixtures USING btree (fixture_id);
        CREATE INDEX results_fixture_id_idx ON api.results USING btree (fixture_id);
        CREATE INDEX draw_series_fixture_id_idx ON cal.draw_series USING btree (fixture_id);
    """

    def clear_season_data(self):
        clear_season_data = """
                    DELETE FROM api.fixtures WHERE league_season = {season} AND league_id = {league_id};
                    """.format(season=self.season, league_id=self.league_id)
        return clear_season_data


class SegundaDivisionDW:
    fact_results = """
    CREATE TABLE fact_results(
        id bigserial,
        fixture_id bigint NOT NULL,
        league_id smallint NULL,
        SEASON smallint NULL,
        round smallint NULL,
        team_home_id integer NULL,
        team_away_id integer NULL,
        no_draw_home smallint NULL,
        draw_home smallint NULL,
        no_draw_away smallint NULL,
        draw_away smallint NULL,
        goals_home smallint NULL,
        goals_away smallint NULL,
        score_halftime_home smallint NULL,
        score_halftime_away smallint NULL,
        halftime_goals_total smallint NULL,
        fulltime_goals_total smallint NULL,
        match_result smallint NULL,
        CONSTRAINT PK_fact_results PRIMARY KEY (id)
    );
    """

    fact_standings = """
    CREATE TABLE fact_standings(
        id bigserial,
        league_id smallint NOT NULL,
        SEASON smallint NOT NULL,
        standings_type_id smallint NOT NULL,
        round smallint NOT NULL,
        team_position smallint NULL,
        team_id integer NOT NULL,
        MP smallint NULL,
        W smallint NULL,
        D smallint NULL,
        L smallint NULL,
        GF smallint NULL,
        GA smallint NULL,
        GD smallint NULL,
        Pts smallint NULL,
        CONSTRAINT PK_fact_standings PRIMARY KEY (id)  
    );
    """
    dim_standings_type = """
    CREATE TABLE dim_standings_type(
        standings_type_id smallint,
        type_name varchar (7), 
        CONSTRAINT PK_dim_standings_type PRIMARY KEY (standings_type_id) 
    );
    """

    dim_league_season = """
    CREATE TABLE dim_league_season(
        league_id smallint NOT NULL,
        SEASON smallint NOT NULL,
        league_level smallint NOT NULL,
        league_name varchar(100),
        league_country varchar(100),
        start_date date NOT NULL,
        end_date date NOT NULL,
        season_info varchar(9),
        rounds smallint,
        CONSTRAINT PK_dim_league_season PRIMARY KEY (league_id, SEASON)
    );
    """
    dim_team = """
          CREATE TABLE dim_team(
          team_id integer NOT NULL,
          team_name varchar(50),
          CONSTRAINT PK_dim_team PRIMARY KEY (team_id)
      );
      """

    dim_fixtures = """
        CREATE TABLE dim_fixtures(
        fixture_id bigint,
        fixture_date_utc timestamp without time zone,
        fixture_timestamp varchar(50),
        CONSTRAINT PK_dim_fixtures PRIMARY KEY (fixture_id)
    );
    """

    add_indexes = """
        CREATE INDEX fact_results_multi_col_idx ON fact_results USING btree (league_id, season, round);
        CREATE INDEX fact_results_season_idx ON fact_results USING btree (season);
        CREATE INDEX fact_results_fixture_id_idx ON fact_results USING btree (fixture_id);
    """

    add_constraints = """
    ALTER TABLE fact_results
        ADD CONSTRAINT FK_results_team_home FOREIGN KEY (team_home_id) REFERENCES dim_team (team_id),
        ADD CONSTRAINT FK_results_team_away FOREIGN KEY (team_away_id) REFERENCES dim_team (team_id),
        ADD CONSTRAINT FK_results_league_season FOREIGN KEY (league_id, SEASON) REFERENCES dim_league_season (league_id, SEASON),
        ADD CONSTRAINT FK_results_fixtures FOREIGN KEY (fixture_id) REFERENCES dim_fixtures (fixture_id);

    ALTER TABLE fact_standings
        ADD CONSTRAINT FK_standings_team FOREIGN KEY (team_id) REFERENCES dim_team (team_id),
        ADD CONSTRAINT FK_standings_league_season FOREIGN KEY (league_id, SEASON) REFERENCES dim_league_season (league_id, SEASON),
        ADD CONSTRAINT FK_standings_type FOREIGN KEY (standings_type_id) REFERENCES dim_standings_type (standings_type_id),
        ADD CONSTRAINT unique_fact_standings_multi_col_idx UNIQUE (league_id, SEASON, standings_type_id, round, team_id);
    """
