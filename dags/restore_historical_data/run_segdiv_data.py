from draw_analysis.get_api import GetApiFootballData
from draw_analysis.team_draw_analysis import TeamDrawAnalysis
from datetime import datetime
from segdiv_data_scripts import read_json_api_credentials, calulate_league_table_to_csv, \
    calulate_league_table_home_away_to_csv, calculate_draw_series, get_fixtures_to_csv

start = datetime.now()

LEAGUE_ID = 141
SEASONS = [2018, 2019, 2020, 2021]
API_CREDENTIALS = read_json_api_credentials("./api_football_beta.json")


# get and save raw_data.csv:
raw_data = GetApiFootballData(API_CREDENTIALS).get_fixtures_data(league_id=LEAGUE_ID, seasons=SEASONS)
raw_data.to_csv('./files/raw_data.csv', index=False)

# get results.csv:
fixtures_data = TeamDrawAnalysis(raw_data).fixtures_data
results = fixtures_data[[
    'fixture_id', 'teams_home_winner', 'teams_away_winner', 'goals_home', 'goals_away', 'score_halftime_home',
    'score_halftime_away', 'halftime_goals_total', 'fulltime_goals_total', 'match_result']]
results.to_csv('./files/results.csv', index=False)

# get fixtures.csv:
get_fixtures_to_csv(raw_data)

# calculate standings round by round - total, home, away and save to csv:
calulate_league_table_to_csv(raw_data, SEASONS)
calulate_league_table_home_away_to_csv(raw_data, SEASONS, spot='home')
calulate_league_table_home_away_to_csv(raw_data, SEASONS, spot='away')

# calculate no draw/ draw series to csv:

calculate_draw_series(raw_data)

end = datetime.now()
print(f"Data execution time: {end - start}")