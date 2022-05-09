from draw_analysis.get_api import GetApiFootballData
from draw_analysis.team_draw_analysis import TeamDrawAnalysis

credentials = {}
with open("api_credentials.txt") as file:
    for line in file.readlines():
        code = line.split(": ")
        credentials[code[0]] = code[1].strip("\n")

data = GetApiFootballData(credentials).get_fixtures_data(league_id=141, seasons=[2018, 2019, 2020, 2021])

seg_div_results = TeamDrawAnalysis(data).fixtures_data
seg_div_results = seg_div_results[[
    'fixture_id', 'teams_home_winner', 'teams_away_winner', 'goals_home', 'goals_away', 'score_halftime_home',
    'score_halftime_away', 'halftime_goals_total', 'fulltime_goals_total', 'match_result']]

seg_div_results.to_csv('results.csv', index=False)
data.to_csv('raw_data.csv', index=False)
