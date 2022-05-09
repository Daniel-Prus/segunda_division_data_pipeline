import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from .utils.validation import (
    teams_arg_validation,
    seasons_arg_validation,
    teams_standings_arg_validation
)


class _DataOrganizer:
    """DataOrganizer class sets up API fixtures data and configures environment for analysis.

        'Class attributes'
        ------------------
            graph_colors(list): list of colors for matplotlib.
            text_font(dict): font parameters dict.
            explode_tuple(tuple): explode settings for pie plots.
            style(str): matplotlib style.
            params(dict): matplotlib.pyplot parameteres.

        'Attributes'
        ------------
            rawdata (pd.DataFrame): provided API fixtures data.
            fixtures_data (pd.DataFrame): automatically cleaned and optimized data from raw data fixtures.
            country (str): country name.
            league (str): league name.
            league_id (int): api league id.
            seasons (list): league seasons from 'fixtures data'.
            teams (list): league teams from 'fixtures data'.
            draw_score (float): total draw score.
    """

    # Matplotlib parameters:

    _graph_colors = ['tab:red', 'tab:orange', 'tab:blue']

    _text_font = {'fontname': 'Arial', 'size': '12', 'color': 'black', 'weight': 'normal',
                  'verticalalignment': 'bottom'}

    _explode_tuple = (0.03, 0.01, 0.01)

    _style = 'ggplot'

    _params = {'legend.fontsize': 10,
               'figure.figsize': [10, 6],
               'axes.labelsize': 14,
               'axes.titlesize': 14,
               'xtick.labelsize': 12,
               'ytick.labelsize': 12,
               'font.size': 18,
               'legend.facecolor': 'gainsboro',
               'figure.facecolor': 'whitesmoke'}

    def __init__(self, rawdata):
        self._fixtures_data = transfrom_rawdata_api_fixtures(rawdata, nan=False)
        self._country = self._fixtures_data.league_country[0]
        self._league = self._fixtures_data.league_name[0]
        self._league_id = int(self._fixtures_data.league_id[0])
        self._seasons = _DataOrganizer.__get_seasons(self)
        self._teams = _DataOrganizer.__get_teams(self)
        self._draw_score = _DataOrganizer.__draw_score(self)
        _DataOrganizer.__matplotlib_params_setter(_DataOrganizer._style, _DataOrganizer._params)

    def __repr__(self):
        return f'{self._fixtures_data.league_name[0]} [{str(self._seasons)[1:-1]}]'

    def __matplotlib_params_setter(style, params):

        """Launch matplolib styles.
        """

        plt.style.use(style)

        plt.rcParams.update(params)

    def __get_seasons(self):

        """Get seasons from optimized data.
        """

        seasons_list = []
        for season in self._fixtures_data['league_season'].unique():
            seasons_list.append(int(season))
        return seasons_list

    def __get_teams(self):

        """Get teams from optimized data.
        """

        teams_unique_list = self._fixtures_data['teams_home_name'].append(self._fixtures_data['teams_away_name']) \
            .drop_duplicates().to_list()
        teams_unique_list = sorted(teams_unique_list)
        return teams_unique_list

    def __draw_score(self):

        """Calculate total draw score.
        """

        match_results = self._fixtures_data['match_result']
        draw_score = match_results.value_counts()[0] / len(match_results)
        return draw_score

    @staticmethod
    def __winner_team_in_direct_matches(fixtures_filtered, first_team, second_team):
        """Check teams with equal points and find the winner in direct matches."""

        equal_teams_dict = dict({first_team: 0, second_team: 0})

        def calculate_team(home_team, away_team):
            """Calculate points and winner in direct matches."""
            teams_home_filter = fixtures_filtered['teams_home_name'] == home_team
            away_home_filter = fixtures_filtered['teams_away_name'] == away_team
            team_fixtures = fixtures_filtered[teams_home_filter & away_home_filter]

            if team_fixtures.empty:
                return None
            if team_fixtures.match_result.iloc[0] == 1:
                equal_teams_dict[home_team] += 3
            elif team_fixtures.match_result.iloc[0] == 2:
                equal_teams_dict[away_team] += 3

        calculate_team(first_team, second_team)
        calculate_team(second_team, first_team)

        if equal_teams_dict[first_team] == equal_teams_dict[second_team]:
            winner_team = None
        else:
            winner_team = max(equal_teams_dict, key=equal_teams_dict.get)

        return winner_team

    def __compare_teams_with_equal_points(self, league_table, season):
        """Check teams with equal points and change index in league table."""

        fixtures_filtered = self.fixtures_data[self.fixtures_data['league_season'] == season]
        fixtures_filtered = fixtures_filtered[
            ['teams_home_name', 'teams_away_name', 'match_result', 'goals_home', 'goals_away']]

        new_league_table_index = []

        for index in range(1, len(league_table) + 1):
            try:
                if league_table.Pts.loc[index] == league_table.Pts.loc[index + 1]:
                    first_team = league_table.Team.loc[index]
                    second_team = league_table.Team.loc[index + 1]
                    winner = self.__winner_team_in_direct_matches(fixtures_filtered, first_team, second_team)
                    if first_team == winner:
                        if index in new_league_table_index:
                            pass
                        else:
                            new_league_table_index.append(index)
                    elif winner is None:
                        if index in new_league_table_index:
                            pass
                        else:
                            new_league_table_index.append(index)

                    else:
                        new_league_table_index.append(index + 1)
                        if index in new_league_table_index:
                            pass
                        else:
                            new_league_table_index.append(index)

                else:
                    if index in new_league_table_index:
                        pass
                    else:
                        new_league_table_index.append(index)

            except KeyError:
                if index in new_league_table_index:
                    pass
                else:
                    new_league_table_index.append(index)
                break
        return new_league_table_index

    def _teams_filter(self, dataframe, teams):

        """Filter teams in fixtures data."""

        teams_arg_validation(self, teams)

        if isinstance(teams, list):
            df = dataframe[dataframe['teams_home_name'].isin(teams) | dataframe['teams_away_name'].isin(teams)]
        elif teams == 'all':
            df = dataframe
        else:
            df = dataframe[dataframe['teams_home_name'].isin([teams]) | dataframe['teams_away_name'].isin([teams])]

        return df

    def _seasons_filter(self, dataframe, seasons):

        """Filter seasons in fixtures data."""

        seasons_arg_validation(self, seasons)

        if len(self._seasons) == 1 or seasons == 'all':
            df = dataframe

        elif isinstance(seasons, list):
            df = dataframe[dataframe['league_season'].isin(seasons)]

        else:
            df = dataframe[dataframe['league_season'] == seasons]

        return df

    @staticmethod
    def _yticks_plot_auto_setter(series, start=1, step=5):

        """Automatic calculate of array for yticks plot argument. Auxiliary function for plot methods.

            Parameters
            ----------
                series (pandas.Series): Series with the maximum value for ytick plot.
                start (int): first value for the y tick.
                step(int): value to calculate yticks range.

            Returns
            ----------
                numpy.ndarray: array for plot yticks argument.
        """

        max_value = round(series.max(), 0)

        ytick_step = start if max_value < 30 else step if 30 <= max_value < 100 \
            else step * 4 if 100 <= max_value < 220 else step * 8 \
            if 220 <= max_value < 400 else step * 16

        return np.arange(0, max_value + ytick_step, ytick_step)

    @property
    def fixtures_data(self):
        """Get full fixtures data."""
        return self._fixtures_data

    @property
    def country(self):
        """Get country from fixtures data."""
        return self._country

    @property
    def league(self):
        """Get league from fixtures data."""
        return self._league

    @property
    def league_id(self):
        """Get league_id from fixtures data."""
        return self._league_id

    @property
    def seasons(self):
        """Get seasons from fixtures data."""
        return self._seasons

    @property
    def teams(self):
        """Get teams from fixtures data."""
        return self._teams

    @property
    def draw_score(self):
        """Get draw score from fixtures data."""
        return self._draw_score

    def data_info(self):

        """Print description for fixtures data.

            Returns
            ----------
                str: print - country, league name, league id, league seasons for data fixtures.

        """
        print(f" Data description: \n {'=' * 40}")
        print(f"\tCountry: {self.country} \n",
              f"\tLeague name: {self.league} \n",
              f"\tLeague id: {self.league_id} \n",
              f"\tSeasons: {str(self._seasons)[1:-1]} \n",
              '=' * 40 + '\n')

    def teams_standings_home_away(self, season, spot):

        """Calculate home/away teams standings by seasons.

            Parameters
            ----------
                seasons (int): provided league season.
                spot(str): 'home' for home results standings, 'away' - away results standings

            Returns
            ----------
                pd.DataFrame: home/away league table.

        """
        # argument validation
        teams_standings_arg_validation(self, season, spot)

        # data filter
        fixtures_data = self._fixtures_data[
            ['league_season', 'teams_home_name', 'teams_away_name', 'goals_home', 'goals_away', 'match_result']]
        fixtures_data_filtered = fixtures_data[fixtures_data['league_season'] == season]

        # mapper for 'spot' argument
        mapper = {'home': 'teams_home_name', 'away': 'teams_away_name'}

        # Matches played - MP
        matches_played = fixtures_data_filtered.groupby([mapper[spot]])['league_season'].count().to_frame()
        matches_played.columns = ['MP']

        # grouping part results 1,0,2 - W, D, L
        results = fixtures_data_filtered.groupby([mapper[spot], 'match_result'])['match_result'].count().unstack()
        results_final = pd.DataFrame(index=results.index)
        results_final[['W', 'D', 'L']] = results[[1, 0, 2]]

        # goals for, goals against, goals difference - GF, GA, GD
        goals = fixtures_data_filtered.groupby([mapper[spot]])[['goals_home', 'goals_away']].sum()
        goals['Difference'] = goals['goals_home'] - goals['goals_away']
        goals.columns = ['GF', 'GA', 'GD']

        # grouping part results 1,0,2 - W, D, L
        results = fixtures_data_filtered.groupby([mapper[spot], 'match_result'])['match_result'].count().unstack()
        results_final = pd.DataFrame(index=results.index)
        results_final[['W', 'D', 'L']] = results[[1, 0, 2]]

        # goals for, goals against, goals difference - GF, GA, GD
        goals = fixtures_data_filtered.groupby([mapper[spot]])[['goals_home', 'goals_away']].sum()
        goals['Difference'] = goals['goals_home'] - goals['goals_away']
        goals.columns = ['GF', 'GA', 'GD']

        # merging - away teams need reversal of data columns!!
        teams_part_standings = pd.DataFrame(index=matches_played.index)
        teams_part_standings['MP'] = matches_played['MP']

        if spot == 'home':
            teams_part_standings[['W', 'D', 'L']] = results_final[['W', 'D', 'L']]
            teams_part_standings[['GF', 'GA', 'GD']] = goals[['GF', 'GA', 'GD']]

        elif spot == 'away':
            teams_part_standings[['W', 'D', 'L']] = results_final[['L', 'D', 'W']]
            teams_part_standings[['GF', 'GA']] = goals[['GA', 'GF']]
            teams_part_standings['GD'] = teams_part_standings['GF'] - teams_part_standings['GA']

        # calculate teams points
        teams_part_standings['Pts'] = teams_part_standings['W'] * 3 + teams_part_standings['D'] * 1
        teams_part_standings.sort_values(by=['Pts', 'GD'], ascending=[False, False], inplace=True)

        teams_part_standings = teams_part_standings[teams_part_standings.iloc[:, 0] > 0]
        teams_part_standings.index.rename('Team', inplace=True)

        teams_part_standings.reset_index(inplace=True)
        teams_part_standings.index += 1

        if True in teams_part_standings[teams_part_standings.columns[-1]].duplicated().value_counts().index:
            new_leage_table_index = self.__compare_teams_with_equal_points(teams_part_standings, season)
            if new_leage_table_index == teams_part_standings.index.to_list():
                return teams_part_standings
            else:
                league_table = teams_part_standings.reindex(index=new_leage_table_index)
                league_table.reset_index(drop=True, inplace=True)
                league_table.index += 1
                return teams_part_standings
        else:
            return teams_part_standings

    def league_table(self, season):

        """Calculate league standings table for seasons.

            Parameters
            ----------
                season (int): provided league season.

            Returns
            ----------
                pd.DataFrame: league table.
        """

        home_table = self.teams_standings_home_away(season, spot='home')
        away_table = self.teams_standings_home_away(season, spot='away')

        # combine home and away table, group by Team name(index) and sum
        league_table = home_table.append(away_table)
        league_table.Team = league_table.Team.astype('object')
        league_table = league_table.groupby('Team').agg('sum')

        league_table.sort_values(by=['Pts', 'GD'], ascending=[False, False], inplace=True)
        league_table.reset_index(inplace=True)
        league_table.index += 1

        if True in league_table[league_table.columns[-1]].duplicated().value_counts().index:
            new_leage_table_index = self.__compare_teams_with_equal_points(league_table, season)
            if new_leage_table_index == league_table.index.to_list():
                return league_table
            else:
                league_table = league_table.reindex(index=new_leage_table_index)
                league_table.reset_index(drop=True, inplace=True)
                league_table.index += 1
                return league_table
        else:
            return league_table

    def get_teams_id(self):
        """Get unique teams id.

            Returns
            ----------
                dict: {id:team}
        """

        home_team_id = self._fixtures_data[['teams_home_id', 'teams_home_name']]
        away_team_id = self._fixtures_data[['teams_away_id', 'teams_away_name']]
        away_team_id.columns = ['teams_home_id', 'teams_home_name']

        concat = pd.concat([home_team_id, away_team_id])
        concat.sort_values(by='teams_home_name', inplace=True)
        concat = concat.drop_duplicates()
        unique_id_teams = {key: value for value, key in zip(concat['teams_home_id'], concat['teams_home_name'])}
        return unique_id_teams


def transfrom_rawdata_api_fixtures(rawdata, nan):
    """Clean and optimize dataframe to the further analysis.

        Parameters
        ----------
            rawdata (pandas.DataFrame): API raw data.
            nan(bool): True - to keep whole schedule. False - only for held fixtures.

        Returns
        ----------
            df_optimized (pandas.DataFrame): Cleaned and optimized fixtures data.

    """

    # 1. Df copy
    df_optimized = rawdata.copy()

    # 2. Dropping columns
    columns_to_drop = ['fixture.referee', 'fixture.timezone', 'fixture.periods.first', 'fixture.periods.second',
                       'fixture.venue.id', 'fixture.venue.name', 'fixture.venue.city', 'fixture.status.long',
                       'fixture.status.short',
                       'fixture.status.elapsed', 'league.logo', 'league.flag', 'teams.home.logo', 'teams.away.logo',
                       'score.extratime.home', 'score.extratime.away', 'score.penalty.home', 'score.penalty.away',
                       'score.fulltime.home', 'score.fulltime.away']

    df_optimized.drop(columns=columns_to_drop, inplace=True)

    # 3. Keep only regular season
    reg_season_bool_series = (df_optimized['league.round'].str.contains('Regular Season'))
    df_optimized = df_optimized[reg_season_bool_series].reset_index()
    df_optimized.drop(columns='index', inplace=True)

    # 4. Columns name to 'snake case'
    df_optimized.columns = df_optimized.columns.str.replace('.', '_', regex=True)

    # 5. Keep or not 'nan' results:
    if nan is True:
        pass
    else:
        df_optimized = df_optimized[df_optimized['goals_home'].notnull()]

    # 6. Optimizing columns type

    # changing object to datetime type
    df_optimized['fixture_date'] = pd.to_datetime(df_optimized['fixture_date'], format='%Y-%m-%d')

    # str to int 'league_round'
    df_optimized['league_round'] = df_optimized['league_round'].apply(lambda x: x[-2:].strip())
    df_optimized['league_round'] = df_optimized['league_round'].astype('int64')

    # changing object to category type
    columns_to_category_type = ['league_name', 'league_country', 'teams_home_name',
                                'teams_home_winner', 'teams_away_name', 'teams_away_winner']
    df_optimized[columns_to_category_type] = df_optimized[columns_to_category_type].astype('category')

    # changing float to int type
    df_optimized['score_halftime_home'].fillna(0, inplace=True)  # np.NaN to 0
    df_optimized['score_halftime_away'].fillna(0, inplace=True)  # np.NaN to 0
    df_optimized[['score_halftime_home', 'score_halftime_away']] = df_optimized[
        ['score_halftime_home', 'score_halftime_away']].astype('int64')
    if nan is False:
        df_optimized[['goals_home', 'goals_away']] = df_optimized[['goals_home', 'goals_away']].astype('int64')

    # 7. Adding auxiliary columns

    #  total number of goals: halftime and fulltime
    df_optimized['halftime_goals_total'] = df_optimized['score_halftime_home'] + df_optimized['score_halftime_away']
    df_optimized['fulltime_goals_total'] = df_optimized['goals_home'] + df_optimized['goals_away']

    #  adding match result column

    def match_result_mapper(data):
        goals_home = data['goals_home']
        goals_away = data['goals_away']

        if goals_home > goals_away:
            return 1

        elif goals_home < goals_away:
            return 2
        else:
            return 0

    df_optimized['match_result'] = df_optimized.apply(match_result_mapper, 1)

    # precised match result as string - halftime and fulltime

    def match_result_str(goal_home_col, goal_away_col):
        return str(goal_home_col) + ':' + str(goal_away_col)

    df_optimized['halftime_match_result'] = df_optimized.apply(
        lambda x: match_result_str(x['score_halftime_home'], x['score_halftime_away']), 1)
    df_optimized['fulltime_match_result'] = df_optimized.apply(
        lambda x: match_result_str(x['goals_home'], x['goals_away']), 1)
    df_optimized[['halftime_match_result', 'fulltime_match_result']] = df_optimized[
        ['halftime_match_result', 'fulltime_match_result']].astype('category')

    # 8. Sort and reset index:
    df_optimized.sort_values(by='fixture_date', inplace=True)
    df_optimized.reset_index(drop=True, inplace=True)
    return df_optimized
