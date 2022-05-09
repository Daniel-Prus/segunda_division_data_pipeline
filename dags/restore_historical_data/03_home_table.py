from draw_analysis.team_draw_analysis import TeamDrawAnalysis
import pandas as pd

segdiv = pd.read_csv('raw_data.csv')
seg = TeamDrawAnalysis(segdiv)
teams_id = seg.get_teams_id()
league_id = seg.league_id

# print (seg.league_table(season=2020))
df_list = []

for season in seg.seasons:
    fixtures_filtered = segdiv[segdiv['league.season'] == season]
    df = pd.DataFrame()
    for rnd in range(1, 43):
        if rnd == 1:
            round_filtered = fixtures_filtered[fixtures_filtered['league.round'] == 'Regular Season - ' + str(rnd)]
            seg_div = TeamDrawAnalysis(round_filtered)
            league_table = seg_div.teams_standings_home_away(season=season, spot='home')
            league_table['Team'] = league_table['Team'].map(teams_id)
            league_table.reset_index(inplace=True)
            league_table['season'] = season
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
            league_table = seg_div.teams_standings_home_away(season=season, spot='home')
            league_table['Team'] = league_table['Team'].map(teams_id)
            league_table.reset_index(inplace=True)
            league_table['season'] = season
            league_table['round'] = rnd
            df_list.append(league_table)

concat_league_tables = pd.concat(df_list)
concat_league_tables['league_id'] = league_id
concat_league_tables.to_csv('./league_table_home.csv', index=False)
