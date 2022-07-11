import json
from draw_analysis.team_draw_analysis import TeamDrawAnalysis
import pandas as pd
pd.options.mode.chained_assignment = None


def read_json_api_credentials(url):
    f = open(url)
    f_json = json.load(f)
    return dict(f_json["api_football_beta"])


def get_fixtures_to_csv(raw_data):
    data = raw_data[raw_data['league.round'].str.contains('Regular')]
    data.columns = data.columns.str.replace('.', '_', regex=True)

    fixtures = data[
        ['fixture_id', 'fixture_date', 'fixture_timestamp', 'league_id', 'league_name', 'league_country',
         'league_season',
         'league_round', 'teams_home_id', 'teams_home_name', 'teams_away_id', 'teams_away_name']]

    fixtures['league_round'] = fixtures['league_round'].apply(lambda x: x[-2:].strip())
    fixtures['fixture_date'] = pd.to_datetime(fixtures['fixture_date'], format='%Y-%m-%d')

    fixtures.to_csv('./files/fixtures.csv')


def calulate_league_table_to_csv(raw_data, seasons):
    """Restore teams standings by rounds. """

    seg = TeamDrawAnalysis(raw_data)
    teams_id = seg.get_teams_id()
    league_id = seg.league_id

    df_list = []

    for season in seasons:
        fixtures_filtered = raw_data[raw_data['league.SEASON'] == season]
        df = pd.DataFrame()
        for rnd in range(1, 43):
            if rnd == 1:
                round_filtered = fixtures_filtered[fixtures_filtered['league.round'] == 'Regular Season - ' + str(rnd)]
                seg_div = TeamDrawAnalysis(round_filtered)
                league_table = seg_div.league_table(season=season)
                league_table['SEASON'] = season
                league_table['round'] = rnd
                df_list.append(league_table)
                df = round_filtered

            else:
                round_filtered = fixtures_filtered[fixtures_filtered['league.round'] == 'Regular Season - ' + str(rnd)]
                df = df.append(round_filtered)
                df.reset_index(drop=True, inplace=True)
                seg_div = TeamDrawAnalysis(df)
                if rnd not in seg_div.fixtures_data.league_round.to_list():
                    break
                league_table = seg_div.league_table(season=season)
                league_table['SEASON'] = season
                league_table['round'] = rnd
                df_list.append(league_table)

    concat_league_tables = pd.concat(df_list)
    concat_league_tables.reset_index(inplace=True)
    concat_league_tables['league_id'] = league_id
    concat_league_tables['Team'] = concat_league_tables['Team'].map(teams_id)
    concat_league_tables.to_csv('./files/league_table.csv', index=False)


def calulate_league_table_home_away_to_csv(raw_data, seasons, spot):
    """Restore teams home/away standings by rounds."""

    seg = TeamDrawAnalysis(raw_data)
    teams_id = seg.get_teams_id()
    league_id = seg.league_id

    df_list = []

    for season in seasons:
        fixtures_filtered = raw_data[raw_data['league.SEASON'] == season]
        df = pd.DataFrame()
        for rnd in range(1, 43):
            if rnd == 1:
                round_filtered = fixtures_filtered[fixtures_filtered['league.round'] == 'Regular Season - ' + str(rnd)]
                seg_div = TeamDrawAnalysis(round_filtered)
                league_table = seg_div.teams_standings_home_away(season=season, spot=spot)
                league_table['SEASON'] = season
                league_table['round'] = rnd
                df_list.append(league_table)
                df = round_filtered

            else:
                round_filtered = fixtures_filtered[fixtures_filtered['league.round'] == 'Regular Season - ' + str(rnd)]
                df = df.append(round_filtered)
                df.reset_index(drop=True, inplace=True)
                seg_div = TeamDrawAnalysis(df)
                if rnd not in seg_div.fixtures_data.league_round.to_list():
                    break
                league_table = seg_div.teams_standings_home_away(season=season, spot=spot)
                league_table['SEASON'] = season
                league_table['round'] = rnd
                df_list.append(league_table)

    concat_league_tables = pd.concat(df_list)
    concat_league_tables.reset_index(inplace=True)
    concat_league_tables['league_id'] = league_id
    concat_league_tables['Team'] = concat_league_tables['Team'].map(teams_id)
    concat_league_tables.to_csv(f'./files/league_table_{spot}.csv', index=False)


def calculate_draw_series(raw_data):
    """Method to calculate no draw/draw series for teams."""

    seg_div = TeamDrawAnalysis(raw_data)
    fixtures = seg_div.fixtures_data
    teams_id = seg_div.get_teams_id().values()

    draw_series_final = pd.DataFrame(index=fixtures['fixture_id'],
                                     columns=['no_draw_home', 'draw_home', 'no_draw_away', 'draw_away'])
    draw_series_aux = fixtures[
        ['fixture_id', 'league_season', 'teams_home_id', 'teams_away_id', 'match_result']]

    # add auxiliary column for no draw/draw series
    draw_series_aux['aux_no_draw'] = draw_series_aux['match_result'].apply(lambda x: 0 if x == 0 else 1)
    draw_series_aux['aux_draw'] = draw_series_aux['match_result'].apply(lambda x: 1 if x == 0 else 0)

    for team in teams_id:
        team_filter = draw_series_aux[
            (draw_series_aux['teams_home_id'] == team) | (draw_series_aux['teams_away_id'] == team)]
        seasons = team_filter['league_season'].unique()

        for season in seasons:
            season_filter = team_filter[team_filter['league_season'] == season]
            season_filter['aux_no_draw_series'] = season_filter.groupby(season_filter['aux_no_draw'].eq(0).cumsum())[
                'aux_no_draw'].cumsum()
            season_filter['aux_draw_series'] = season_filter.groupby(season_filter['aux_draw'].eq(0).cumsum())[
                'aux_draw'].cumsum()

            for a in range(0, len(season_filter)):
                fixture_id_idx = season_filter.iloc[a, 0]
                find_row_by_idx = draw_series_final.loc[fixture_id_idx]
                # True - team is 'home_team for fixture_id'
                if_team_home = season_filter.iloc[a, 2] == team

                if a == 0:
                    if if_team_home:
                        find_row_by_idx[0] = 0
                        find_row_by_idx[1] = 0
                    else:
                        find_row_by_idx[2] = 0
                        find_row_by_idx[3] = 0
                else:
                    if if_team_home:
                        find_row_by_idx[0] = season_filter.iloc[a - 1, 7]
                        find_row_by_idx[1] = season_filter.iloc[a - 1, 8]
                    else:
                        find_row_by_idx[2] = season_filter.iloc[a - 1, 7]
                        find_row_by_idx[3] = season_filter.iloc[a - 1, 8]

    draw_series_final.reset_index(inplace=True)
    draw_series_final.to_csv('./files/draw_series.csv', index=False)
