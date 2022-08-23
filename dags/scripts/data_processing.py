from scripts.draw_analysis.get_api import GetApiFootballData
from scripts.draw_analysis.team_draw_analysis import TeamDrawAnalysis
from airflow.utils.db import provide_session
from airflow.models import XCom
import pandas as pd


@provide_session
def cleanup_xcom(dag_str, session=None):
    """Clear old xcoms."""
    session.query(XCom).filter(XCom.dag_id == dag_str).delete()


def get_api_data_to_csv(key, host, league_id, season):
    """ Download data from RapidAPi/API-Football-Beta, process and save to csv."""

    raw_data = GetApiFootballData({'x-rapidapi-key': key, 'x-rapidapi-host': host}).get_fixtures_data(
        league_id=league_id,
        seasons=season)

    raw_data.to_csv('/opt/airflow/dags/files/raw_data.csv', index=False)


def get_results():
    raw_data = pd.read_csv("/opt/airflow/dags/files/raw_data.csv")

    results = TeamDrawAnalysis(raw_data).fixtures_data
    results = results[[
        'fixture_id', 'teams_home_winner', 'teams_away_winner', 'goals_home', 'goals_away', 'score_halftime_home',
        'score_halftime_away', 'halftime_goals_total', 'fulltime_goals_total', 'match_result']]

    results[["teams_home_winner", "teams_away_winner"]] = results[["teams_home_winner", "teams_away_winner"]].astype(
        "str")
    results.teams_home_winner = results.teams_home_winner.map({"True": "True", "False": "False", "nan": None})
    results.teams_away_winner = results.teams_away_winner.map({"True": "True", "False": "False", "nan": None})

    results_json = results.to_json()
    return results_json


def get_and_push_data(season, **context):
    # clear old xcoms
    cleanup_xcom(dag_str="seg_div_data_pipeline")

    results = get_results()
    fixtures = get_fixtures()
    league_table = calulate_league_table(season)
    league_table_home = calulate_league_table_home_away(season=season, spot='home')
    league_table_away = calulate_league_table_home_away(season=season, spot='away')

    context['ti'].xcom_push(key="results", value=results)
    context['ti'].xcom_push(key="fixtures", value=fixtures)
    context['ti'].xcom_push(key="league_table", value=league_table)
    context['ti'].xcom_push(key="league_table_home", value=league_table_home)
    context['ti'].xcom_push(key="league_table_away", value=league_table_away)


def calulate_league_table_home_away(season, spot):
    """Calculate league table home/away round by round. Spot - 'home' or 'away'."""

    raw_data = pd.read_csv("/opt/airflow/dags/files/raw_data.csv")
    seg = TeamDrawAnalysis(raw_data)
    teams_id = seg.get_teams_id()
    league_id = seg.league_id

    df_list = []

    for season in seg.seasons:
        raw_data_filtered = raw_data[raw_data['league.season'] == season]
        df = pd.DataFrame()
        for rnd in range(1, 43):
            if rnd == 1:
                round_filtered = raw_data_filtered[raw_data_filtered['league.round'] == 'Regular Season - ' + str(rnd)]
                seg_div = TeamDrawAnalysis(round_filtered)
                league_table = seg_div.teams_standings_home_away(season=season, spot=spot)
                league_table['round'] = rnd
                df_list.append(league_table)
                df = round_filtered
            else:
                round_filtered = raw_data_filtered[raw_data_filtered['league.round'] == 'Regular Season - ' + str(rnd)]
                df = df.append(round_filtered)
                df.reset_index(drop=True, inplace=True)
                seg_div = TeamDrawAnalysis(df)
                if rnd not in seg_div.fixtures_data.league_round.to_list():
                    break
                league_table = seg_div.teams_standings_home_away(season=season, spot=spot)
                league_table['round'] = rnd
                df_list.append(league_table)

    concat_league_tables = pd.concat(df_list)
    concat_league_tables['Team'] = league_table['Team'].map(teams_id)
    concat_league_tables.reset_index(inplace=True)
    concat_league_tables['SEASON'] = season
    concat_league_tables['league_id'] = league_id
    concat_league_tables = concat_league_tables[
        ['league_id', 'Team', 'SEASON', 'round', 'index', 'MP', 'W', 'D', 'L', 'GF', 'GA', 'GD', 'Pts']]

    league_table_json = concat_league_tables.to_json()
    return league_table_json


def calulate_league_table(season):
    """Calculate league table round by round"""

    # '/opt/airflow/dags/files/raw_data.csv'
    raw_data = pd.read_csv("/opt/airflow/dags/files/raw_data.csv")
    seg = TeamDrawAnalysis(raw_data)
    teams_id = seg.get_teams_id()
    league_id = seg.league_id

    df_list = []

    for season in seg.seasons:
        raw_data_filtered = raw_data[raw_data['league.season'] == season]
        df = pd.DataFrame()
        for rnd in range(1 , 43):
            if rnd == 1:
                round_filtered = raw_data_filtered[raw_data_filtered['league.round'] == 'Regular Season - ' + str(rnd)]
                seg_div = TeamDrawAnalysis(round_filtered)
                league_table = seg_div.league_table(season=season)
                league_table['round'] = rnd
                df_list.append(league_table)
                df = round_filtered
            else:
                round_filtered = raw_data_filtered[raw_data_filtered['league.round'] == 'Regular Season - ' + str(rnd)]
                df = df.append(round_filtered)
                df.reset_index(drop=True, inplace=True)
                seg_div = TeamDrawAnalysis(df)
                if rnd not in seg_div.fixtures_data.league_round.to_list():
                    break
                league_table = seg_div.league_table(season=season)
                league_table['round'] = rnd
                df_list.append(league_table)

    concat_league_tables = pd.concat(df_list)
    concat_league_tables['Team'] = league_table['Team'].map(teams_id)
    concat_league_tables.reset_index(inplace=True)
    concat_league_tables['SEASON'] = season
    concat_league_tables['league_id'] = league_id
    concat_league_tables = concat_league_tables[
        ['league_id', 'Team', 'SEASON', 'round', 'index', 'MP', 'W', 'D', 'L', 'GF', 'GA', 'GD', 'Pts']]

    league_table_json = concat_league_tables.to_json()
    return league_table_json


def get_fixtures():
    raw_data = pd.read_csv("/opt/airflow/dags/files/raw_data.csv")
    fixtures = raw_data[raw_data['league.round'].str.contains('Regular')]
    fixtures.columns = fixtures.columns.str.replace('.', '_')
    fixtures = fixtures[
        ['fixture_id', 'fixture_date', 'fixture_timestamp', 'league_id', 'league_name', 'league_country',
         'league_season', 'league_round', 'teams_home_id', 'teams_home_name', 'teams_away_id', 'teams_away_name']]

    fixtures['league_round'] = fixtures['league_round'].apply(lambda x: x[-2:].strip())
    fixtures['fixture_date'] = pd.to_datetime(fixtures['fixture_date'], format='%Y-%m-%d')
    fixtures['fixture_date'] = fixtures['fixture_date'].astype('str')
    fixtures_json = fixtures.to_json()

    return fixtures_json
