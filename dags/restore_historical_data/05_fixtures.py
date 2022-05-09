import pandas as pd

raw_data = pd.read_csv('raw_data.csv')
segdiv = raw_data.copy()

segdiv = segdiv[segdiv['league.round'].str.contains('Regular')]
segdiv.columns = segdiv.columns.str.replace('.', '_')
fixtures = segdiv[
    ['fixture_id', 'fixture_date', 'fixture_timestamp', 'league_id', 'league_name', 'league_country', 'league_season',
     'league_round', 'teams_home_id', 'teams_home_name', 'teams_away_id', 'teams_away_name']]

fixtures['league_round'] = fixtures['league_round'].apply(lambda x: x[-2:].strip())
fixtures['fixture_date'] = pd.to_datetime(fixtures['fixture_date'], format='%Y-%m-%d')

fixtures.to_csv('./fixtures.csv')
