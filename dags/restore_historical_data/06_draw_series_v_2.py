import pandas as pd
from draw_analysis.team_draw_analysis import TeamDrawAnalysis
from datetime import datetime

start = datetime.now()
raw_data = pd.read_csv('raw_data.csv')
seg_div = TeamDrawAnalysis(raw_data)

fixtures = seg_div.fixtures_data

teams = seg_div.get_teams_id().values()

start = datetime.now()


def calculate_draw_series(fixtures_data, teams_id):
    """Method to calculate no draw/draw series for teams. Based on 'fixtures_data'."""

    pd.options.mode.chained_assignment = None
    draw_series_final = pd.DataFrame(index=fixtures['fixture_id'],
                                     columns=['no_draw_home', 'draw_home', 'no_draw_away', 'draw_away'])
    draw_series_aux = fixtures_data[
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
    draw_series_final.to_csv('./draw_series.csv', index=False)

calculate_draw_series(fixtures, teams)
end = datetime.now()
print(f"Data execution time: {end - start}")
