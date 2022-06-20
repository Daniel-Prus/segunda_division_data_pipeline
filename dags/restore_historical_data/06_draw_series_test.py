import pandas as pd
import numpy as np
from datetime import datetime

pd.options.mode.chained_assignment = None


def match_result_mapper(data):
    goals_home = data['goals_home']
    goals_away = data['goals_away']

    if np.isnan(goals_home):
        return None
    elif goals_home > goals_away:
        return 1

    elif goals_home < goals_away:
        return 2
    else:
        return 0


start = datetime.now()
raw_data = pd.read_csv('raw_data_test.csv')

fixtures = raw_data[
    ['fixture.id', 'fixture.date', 'league.season', 'teams.home.id', 'teams.away.id', 'goals.home', 'goals.away']]
fixtures = raw_data[raw_data['league.round'].str.contains('Regular')]
fixtures.sort_values(by='fixture.date', inplace=True)
fixtures.drop(columns='fixture.date', inplace=True)
fixtures.columns = fixtures.columns.str.replace('.', '_', regex=True)
fixtures['match_result'] = fixtures.apply(match_result_mapper, 1)
teams = fixtures['teams_home_id'].append(fixtures['teams_away_id']).unique()

fixtures = fixtures[fixtures['league_season'] == 2021]


def calculate_draw_series(fixtures_data, teams_id):
    """Method to calculate no draw/draw series for teams. Based on 'fixtures_data'."""

    draw_series_final = pd.DataFrame(index=fixtures['fixture_id'],
                                     columns=['no_draw_home', 'draw_home', 'no_draw_away', 'draw_away'])
    draw_series_aux = fixtures_data[
        ['fixture_id', 'league_season', 'teams_home_id', 'teams_away_id', 'goals_home', 'match_result']]

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

            # check if the season is full and set a 'range'
            if np.isnan(season_filter['goals_home'][max(season_filter.index)]):
                loop_range = range(0, len(season_filter[season_filter['goals_home'].notnull()]) + 1)
            else:
                loop_range = range(0, len(season_filter))

            for a in loop_range:
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
                        find_row_by_idx[0] = season_filter.iloc[a - 1, 8]
                        find_row_by_idx[1] = season_filter.iloc[a - 1, 9]
                    else:
                        find_row_by_idx[2] = season_filter.iloc[a - 1, 8]
                        find_row_by_idx[3] = season_filter.iloc[a - 1, 9]

    draw_series_final.reset_index(inplace=True)
    draw_series_final.to_csv('./draw_series.csv', index=False)


calculate_draw_series(fixtures, teams)
end = datetime.now()
print(f"Data execution time: {end - start}")
