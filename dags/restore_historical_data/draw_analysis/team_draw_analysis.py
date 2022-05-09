from ._data_organizer import _DataOrganizer, pd, plt

from .utils.validation import (
    seasons_arg_validation,
    teams_arg_validation,
    plot_type_arg_validation,
    draw_results_plot_arg_validation,
    draw_series_arg_validation
)


class TeamDrawAnalysis(_DataOrganizer):
    """TeamDrawAnalysis class provides and visualizes draw results performance for teams.

        'Attributes'
        ------------
            rawdata (pd.DataFrame): provided raw fixtures data.
            fixtures_data (pd.DataFrame): automatically cleaned and optimized data from raw data fixtures.
            seasons (list): calculated league seasons from 'fixtures data'.
            teams (list): league teams from 'fixtures data'.
            draw_score (float): total draw score.
    """

    def __init__(self, rawdata):
        super().__init__(rawdata)

    def __calculate_teams_no_draw_series(self, series_type):

        """Method to calculate no draw/draw series for teams. Based on 'fixtures_data'.

            Parameters
            ----------

            series_type (int): 0 - for no draw series, 1 - draw series.

            Returns
            ----------
            pd.DataFrame: concatenated no draw/draw series of matches for teams.
        """

        pd.options.mode.chained_assignment = None

        # data optimizing
        fixtures_data = self.fixtures_data.copy()
        fixtures_data = fixtures_data[
            ['fixture_date', 'league_season', 'teams_home_name', 'teams_away_name', 'match_result']]
        fixtures_data['fixture_date'] = pd.to_datetime(fixtures_data['fixture_date']).dt.strftime('%Y-%m-%d')
        fixtures_data.sort_values(by='fixture_date', inplace=True)
        teams_list = self.teams

        def calucate_series_for_team(_fixtures_data, _teams_list, _series_type):

            """Group and calculate no draw/draw series for teams.

            Parameters
            ----------

            fixtures_data (pd.DataFrame): optimized attribute 'fixtures_data'.
            teams_list (list): list of available 'fixtures_data' teams.
            series_type (int): 0 - for no draw series, 1 - draw series.

            Returns
            ----------
            list: list of pd.DataFrame with calculated draw/no draw series by teams.
            """

            _teams_series_to_concat_list = []

            # add auxiliary column for no draw/draw series
            if series_type == 0:
                fixtures_data['aux_result'] = fixtures_data['match_result'].apply(lambda x: 0 if x == 0 else 1)
            elif series_type == 1:
                fixtures_data['aux_result'] = fixtures_data['match_result'].apply(lambda x: 1 if x == 0 else 0)

            for team in teams_list:
                team_filter = fixtures_data[
                    (fixtures_data['teams_home_name'] == team) | (fixtures_data['teams_away_name'] == team)]
                team_filter['series_without_draw'] = team_filter.groupby(team_filter['aux_result'].eq(0).cumsum())[
                    'aux_result'].cumsum()
                team_filter['from_to_date'] = ''

                for a in range(0, len(team_filter)):
                    if team_filter.iloc[a, 6] != 0:
                        try:
                            if team_filter.iloc[a + 1, 6] == 0:
                                if a - team_filter.iloc[a, 6] < 0:
                                    team_filter.iloc[a, 7] = 'NoData' + '__' + str(team_filter.iloc[a + 1, 0])
                                else:
                                    team_filter.iloc[a, 7] = str(
                                        team_filter.iloc[a - team_filter.iloc[a, 6] + 1, 0]) + '__' + str(
                                        team_filter.iloc[a + 1, 0])
                        except IndexError:
                            if a + 1 == len(team_filter):
                                if a - team_filter.iloc[a, 6] < 0:
                                    team_filter.iloc[a, 7] = 'NoData__' + 'ToDate'
                                else:
                                    team_filter.iloc[a, 7] = str(
                                        team_filter.iloc[a - team_filter.iloc[a, 6] + 1, 0]) + '__ToDate'
                            else:
                                team_filter.iloc[a, 7] = 'NoData__' + str(
                                    team_filter.iloc[a - team_filter.iloc[a, 6], 0])

                team_second_filter = team_filter[team_filter['from_to_date'] > '']
                _team_series = pd.DataFrame(index=range(0, len(team_second_filter)),
                                            data={'team_name': team,
                                                  'league_season': team_second_filter['league_season'].to_list(),
                                                  'series_without_draw': team_second_filter[
                                                      'series_without_draw'].to_list(),
                                                  'from_to_date': team_second_filter['from_to_date'].to_list()})

                _team_series.sort_values(by='series_without_draw', ascending=False, inplace=True)
                _teams_series_to_concat_list.append(_team_series)

            return _teams_series_to_concat_list

        teams_series_to_concat_list = calucate_series_for_team(fixtures_data, teams_list, series_type)
        team_series = pd.concat(teams_series_to_concat_list, ignore_index=True)

        return team_series

    def __draw_results_data_filter(self, seasons):

        """Auxiliry method for 'draw_results_by_team_plot'. Filter attribute 'teams_draw_results_summarize' for plot.
        """
        teams_draw_results = TeamDrawAnalysis.teams_draw_results_summarize(self)

        if seasons == 'total':
            draw_results_filtered = teams_draw_results.iloc[:, -3:]
            draw_results_sorted = draw_results_filtered.sort_values(draw_results_filtered.columns[0], ascending=False)
            return draw_results_sorted

        elif isinstance(seasons, int):
            columns_to_filter = []
            for column in teams_draw_results.columns:
                if str(seasons) in column:
                    columns_to_filter.append(column)

            draw_results_filtered = teams_draw_results[columns_to_filter]
            draw_results_sorted = draw_results_filtered.sort_values(draw_results_filtered.columns[0], ascending=False)
            return draw_results_sorted

    def __team_draw_results_plot(self, _team_draw_results, plot_type, top, _seasons):

        """Auxiliry method for 'draw_results_by_team_plot' public method. Draw bar plot based on attribute
           'teams_draw_results_summarize'.


        Parameters
        ----------
        team_draw_results(pd.DataFrame): argument provided by public method - 'draw_results_by_team_plot'.

        plot_type (str): results number - 'rslts_num', percentage share of draw results - 'pcts'.

        seasons(str,int): provided and validated season.

        top (int): top (positive) or last (negative) of teams to ilustrate.


        Returns
        ----------
        matplotlib.pyplot.bar: bar plot number/percentage share of draw results.
        """

        plot_type_arg_validation(plot_type)

        #
        def draw_results_number_plot(_team_draw_results, _seasons, ytick_step):

            """Plot bar chart for plot_type = 'rslts_num'.
            """

            _draw_results_number_plot = _team_draw_results[
                [_team_draw_results.columns[0], 'total_matches_aux_col']].plot(
                kind='bar', color=self._graph_colors[0::2], alpha=0.8, stacked=True, yticks=ytick_setter,
                rot=80,
                title=f"Number of draws to the total results - {_seasons}")
            pcts_line = _team_draw_results[_team_draw_results.columns[-1]].plot(secondary_y=True, rot=80,
                                                                                color='tab:olive', legend=True)

            _draw_results_number_plot.legend(['draw_results_num', 'total_matches_num'], loc='center left',
                                             bbox_to_anchor=(1.07, 0.93))
            pcts_line.legend(loc='center left', bbox_to_anchor=(1.07, 0.844))

            plt.show()

        def draw_pcts_to_total_plot(_team_draw_results, _seasons, ytick_step):

            """Plot bar chart for plot_type='pcts'
            """

            _draw_pcts_to_total_plot = _team_draw_results[_team_draw_results.columns[-1]] \
                .plot(kind='bar',
                      yticks=ytick_setter, rot=80,
                      ylabel='%',
                      color=_DataOrganizer._graph_colors[2],
                      legend=True,
                      title=f"Draws percentage share to the total results - {_seasons}")
            plt.show()

        if plot_type == 'count':
            ytick_setter = self._yticks_plot_auto_setter(_team_draw_results[_team_draw_results.columns[1]] + 5)
            _team_draw_results.insert(2, column='total_matches_aux_col',
                                      value=_team_draw_results[_team_draw_results.columns[1]] - _team_draw_results[
                                          _team_draw_results.columns[0]])
            if top > 0:
                _team_draw_results = _team_draw_results.head(top)
                return draw_results_number_plot(_team_draw_results, _seasons, ytick_setter)
            else:
                _team_draw_results = _team_draw_results.tail(abs(top))
                return draw_results_number_plot(_team_draw_results, _seasons, ytick_setter)

        elif plot_type == 'pcts':
            ytick_setter = self._yticks_plot_auto_setter(
                _team_draw_results[_team_draw_results.columns[-1]] + 5)
            _team_draw_results = _team_draw_results.sort_values(_team_draw_results.columns[-1], ascending=False)
            if top > 0:
                _team_draw_results = _team_draw_results.head(top)
                return draw_pcts_to_total_plot(_team_draw_results, _seasons, ytick_setter)
            else:
                _team_draw_results = _team_draw_results.tail(abs(top))
                return draw_pcts_to_total_plot(_team_draw_results, _seasons, ytick_setter)

    def __calculate_draw_performance(self):

        """Calculate draw results performance for all teams and seasons by merging:
            - draws percentage share (TeamDrawAnalysis.teams_draw_results_summarize())
            - team table position (self.league_table())
            - no draw series max/mean per season/total (self.teams_draw_series())
            - draw series max/mean per season/total (self.teams_draw_series())

            Returns
            ----------
            pandas.DataFrame: draw results performance.

        """

        # 1. Filter columns from 'teams_draw_results_summarize' attribute
        # - columns to filter
        draw_pcts_col_list = []
        teams_draw_results_summarize = TeamDrawAnalysis.teams_draw_results_summarize(self)

        # - add draw pcts results per seasons to draw_pcts_col_list
        for season in self._seasons:
            for col in teams_draw_results_summarize.columns.to_list():
                if 'pcts_' + str(season) in col and 'match_num' not in col:
                    draw_pcts_col_list.append(col)
                if len(self.seasons) > 1:
                    if season == self.seasons[-1] and 'pcts_total' in col:
                        draw_pcts_col_list.append(col)

        # - filter
        draw_results_performance = teams_draw_results_summarize.copy()
        draw_results_performance = draw_results_performance[draw_pcts_col_list]

        # 2. Teams position in table from
        table_pos_col_list = []

        for season in self._seasons:
            table_position = self.league_table(season)['Team'].to_frame()
            table_position['table_pos_' + str(season)] = table_position.index
            table_position.set_index('Team', inplace=True)
            table_pos_col_list.append(table_position.columns[0])

            draw_results_performance = draw_results_performance.merge(table_position, how='left', left_index=True,
                                                                      right_index=True)

        # 3. New columns order - teams table positions and draw pcts results
        new_columns_order_list = []

        for col in zip(table_pos_col_list, draw_pcts_col_list):
            new_columns_order_list.append(col[0])
            new_columns_order_list.append(col[1])

        # add total draw pcts results column if len(seasons) > 1
        if len(self._seasons) > 1:
            new_columns_order_list.append(draw_pcts_col_list[-1])

        # draw_results_performance new column order if len(seasons) > 1
        draw_results_performance = draw_results_performance[new_columns_order_list]

        # 5. Draw/no draw series

        def merge_draw_performance_with_draw_series(_draw_results_performance, series_type):
            """Calculate max/mean, no draw/draw series for teams and merge with 'draw_results_performance'


            Parameters
            ----------
            draw_results_performance (padnas.DataFrame): organized DF with teams positions and draws percentage share

            series_type (int): 0 - series without draw, 1 - draw series.

            Returns
            ----------
            pandas.DataFrame: merged draw_results_performance with draw/no draws series.

            """

            no_draw_series_dataframe_list = []

            # mapper to change the column name for draw/ no draw series
            series_type_mapper = {0: 'no_draw_', 1: 'draw_'}

            # no draw/draw series - max, mean per seasons
            for _season in self._seasons:
                no_draw_series_max_season = self.teams_draw_series(seasons=_season,
                                                                   series_type=series_type,
                                                                   func='max').iloc[:, 1].to_frame()
                no_draw_series_max_season.rename(
                    {no_draw_series_max_season.columns[0]: series_type_mapper[series_type] + 'max_' + str(_season)},
                    axis=1, inplace=True)
                no_draw_series_mean_season = self.teams_draw_series(seasons=_season,
                                                                    series_type=series_type,
                                                                    func='mean').iloc[:, 0].to_frame()
                no_draw_series_mean_season.rename(
                    {no_draw_series_mean_season.columns[0]: series_type_mapper[series_type] + 'mean_' + str(_season)},
                    axis=1, inplace=True)
                no_draw_series_dataframe_list.append(no_draw_series_max_season)
                no_draw_series_dataframe_list.append(no_draw_series_mean_season)

            # no draw/draw total from data
            if len(self._seasons) > 1:
                no_draw_series_max_total = self.teams_draw_series(seasons='all', series_type=series_type,
                                                                  func='max').iloc[:, 1].to_frame()
                no_draw_series_max_total.rename(
                    {no_draw_series_max_total.columns[0]: series_type_mapper[series_type] + 'max_total'}, axis=1,
                    inplace=True)
                no_draw_series_mean_total = self.teams_draw_series(seasons='all', series_type=series_type,
                                                                   func='mean').iloc[:, 0].to_frame()
                no_draw_series_mean_total.rename(
                    {no_draw_series_mean_total.columns[0]: series_type_mapper[series_type] + 'mean_total'}, axis=1,
                    inplace=True)
                no_draw_series_dataframe_list.append(no_draw_series_max_total)
                no_draw_series_dataframe_list.append(no_draw_series_mean_total)

            # merge no draw/draw series with 'draw_results_performance'
            for df in no_draw_series_dataframe_list:
                _draw_results_performance = _draw_results_performance.merge(df, how='left', left_index=True,
                                                                            right_index=True)

            return _draw_results_performance

        # 6. Merge draw_results_performance with draw/no draw series
        draw_results_performance = merge_draw_performance_with_draw_series(draw_results_performance, series_type=0)
        draw_results_performance = merge_draw_performance_with_draw_series(draw_results_performance, series_type=1)

        # 7.Fill Nan values and change columns type:
        draw_results_performance.fillna(0, inplace=True)
        for col in draw_results_performance.columns:
            if 'table_pos' in col or 'max' in col:
                draw_results_performance[col] = draw_results_performance[col].astype('int64')

        # 8. Goals performance
        teams_goals_performance_season = TeamDrawAnalysis.teams_goals_performance(self, seasons='all', teams='all')
        draw_results_performance = draw_results_performance.merge(teams_goals_performance_season, how='left',
                                                                  left_index=True, right_index=True)

        return draw_results_performance

    def __transpose_draw_performance_table(self, draw_performance_filtered_teams, seasons):

        """Auxiliary method for draw_results_performance_filter.
           Transpose table to multiindex teams and seasons.

            Parameters
            ----------
            draw_performance_filtered_teams(pandas.DataFrame): filtered draw performance table.

            Returns
            ----------
            pandas.DataFrame: transposed table.
        """
        seasons_list = []
        if seasons == 'all':
            seasons_list = self._seasons

        elif isinstance(seasons, int):
            seasons_list = [seasons]

        else:
            seasons_list = seasons

        # df list to further concatenation
        df_list = []

        # cerate df for seasons with unified columns
        for season in seasons_list:
            # columns for particular season
            columns_list = []
            # new unified columns names
            new_columns_name = []
            for column in draw_performance_filtered_teams.columns:
                if str(season) in column:
                    columns_list.append(column)
                    new_columns_name.append(column[0:column.rfind('_')])

            df_filtered = draw_performance_filtered_teams[columns_list]
            df_filtered.columns = new_columns_name
            # add multi column with season number
            df_filtered = pd.concat([df_filtered], axis=1, keys=[str(season)])
            df_list.append(df_filtered)

            # concatenation and transpose
        df_concat = pd.concat(df_list, axis=1)
        transposed = df_concat.stack(level=0)
        transposed.index.set_names(['team_name', 'league_season'], inplace=True)
        transposed = transposed[
            ['table_pos', 'draws_pcts', 'no_draw_max', 'no_draw_mean', 'draw_max', 'draw_mean', 'goals_mean',
             'goals_diff']]

        # drop indexesr with 0 values
        index_with_no_data = transposed[transposed[transposed.columns] == 0].dropna().index
        transposed.drop(index_with_no_data, inplace=True)

        return transposed

    def __draw_results_seasons_filter(self, seasons):
        draw_performance = TeamDrawAnalysis.__calculate_draw_performance(self)
        columns_to_filter = []
        draw_performance_filtered = None
        if len(self._seasons) == 1:
            for column in draw_performance.columns:
                if str(*self._seasons) in column:
                    columns_to_filter.append(column)

            draw_performance_filtered = draw_performance[columns_to_filter]
            draw_performance_filtered = draw_performance_filtered[
                draw_performance_filtered[draw_performance_filtered.columns[0]] > 0]

        elif isinstance(seasons, list):
            for season in seasons:
                for column in draw_performance.columns:
                    if str(season) in column:
                        columns_to_filter.append(column)

            draw_performance_filtered = draw_performance[columns_to_filter]

        elif isinstance(seasons, int):
            for column in draw_performance.columns:
                if str(seasons) in column:
                    columns_to_filter.append(column)

            draw_performance_filtered = draw_performance[columns_to_filter]
            draw_performance_filtered = draw_performance_filtered[
                draw_performance_filtered[draw_performance_filtered.columns[0]] > 0]
        else:
            draw_performance_filtered = draw_performance

        return draw_performance_filtered

    def teams_draw_results_summarize(self):

        """Group 'fixtures_data' to calculate number of draws, number of total matches and percentage share of draw
           results by teams.

            Returns
            ----------
                pd.DataFame: teams_draw_results_summarize.
        """

        def team_draw_results_count(_fixtures_data, _season):

            # group and count 0,1,2 results by team
            teams_home_rslts = \
                _fixtures_data.groupby(by=['league_season', 'teams_home_name', 'match_result']).agg('count')[
                    'fixture_id']
            teams_away_rslts = \
                _fixtures_data.groupby(by=['league_season', 'teams_away_name', 'match_result']).agg('count')[
                    'fixture_id']

            # combine teams_home_rslts and teams_away_rslts
            teams_results = teams_home_rslts.append(teams_away_rslts)

            # group and agg sum teams_home_rslts and teams_away_rslts
            teams_results_total = teams_results.groupby(by=['teams_home_name', 'match_result']).agg('sum')
            teams_results_total.index.set_names(names=['team_name', 'match_result'], inplace=True)

            # draw count processing
            team_draw_counted = teams_results_total.unstack()[0].to_frame()
            team_draw_counted.columns = ['draws_' + str(_season)]
            draws_number = team_draw_counted[team_draw_counted.columns[0]]

            # total matches count
            teams_total_matches = teams_results_total.groupby('team_name').agg('sum')

            # combine and calculate percentage share of draws
            if isinstance(_season, int):
                team_draw_counted['match_num_' + str(_season)] = teams_total_matches
                total_matches_number = team_draw_counted[team_draw_counted.columns[1]]
                team_draw_counted['draws_pcts_' + str(_season) + '(%)'] = round(
                    draws_number / total_matches_number * 100, 2)
            else:
                team_draw_counted['match_num_' + str(_season)] = teams_total_matches
                total_matches_number = team_draw_counted[team_draw_counted.columns[1]]
                team_draw_counted['draws_pcts_' + str(_season) + '(%)'] = round(
                    draws_number / total_matches_number * 100, 2)

            return team_draw_counted

        fixtures_data = self._fixtures_data.copy()
        seasons_list = self._seasons
        teams_draw_results_merged = pd.DataFrame()

        for season in seasons_list:
            fixtures_data_filtered = fixtures_data[fixtures_data['league_season'] == season]
            teams_draw_counted = team_draw_results_count(fixtures_data_filtered, season)
            if teams_draw_results_merged.empty:
                teams_draw_results_merged = teams_draw_results_merged.append(teams_draw_counted)
            else:
                teams_draw_results_merged = teams_draw_results_merged.merge(teams_draw_counted, left_index=True,
                                                                            right_index=True, how='outer')

        if len(seasons_list) > 1:
            teams_draw_total = team_draw_results_count(fixtures_data, 'total')
            teams_draw_results_merged = teams_draw_results_merged.merge(teams_draw_total, left_index=True,
                                                                        right_index=True, how='outer')
            teams_draw_results_merged.fillna(value=0, inplace=True)

        else:
            teams_draw_results_merged.fillna(value=0, inplace=True)

        return teams_draw_results_merged

    def draw_results_by_team_plot(self, plot_type='count', seasons='total', top=15):

        """Bar plot for draw results by team.

            Parameters
            ----------

            plot_type (str): type of bar plot - 'count' or 'pcts'.

            seasons(str,int): league season to plot. Deafult - 'total' data.

            top (int): top (positive) or last (negative) of teams to ilustrate.

            Raises
            ----------
                TypeError: Seasons argument must be int or str type.
                ValueError: Season does not exist.
                ValueError: Incorrect string value. Seasons must be int or 'total' for total data calculation.
                ValueError: Incorrect string value. The plot_type argument must be named 'rslts_num' or 'pcts'.
                TypeError: The 'plot_type' argument must be str type.
                TypeError: The 'top' argument must be int type.

            Returns
            ----------
            matplotlib.pyplot.bar: bar plot number/percentage share of draw results by team.
        """

        draw_results_plot_arg_validation(self, seasons, plot_type, top)

        team_draw_results = TeamDrawAnalysis.__draw_results_data_filter(self, seasons)

        TeamDrawAnalysis.__team_draw_results_plot(self, team_draw_results, plot_type, top, seasons)

    def teams_draw_series(self, series_type=0, seasons='all', teams='all', func='all'):

        """Calculate no draw/ draw results series for teams.

            Parameters
            ----------

            series_type (int): 0 - series without draw, 1 - draw series.

            seasons (int or list): Default - 'all' leauge seasons. Int or list of ints of league seasons for request.

            teams (str or list): Default 'all' - teams series, 'str' name for single team or list(str) of team names for
                                 request.

            func (str): Default 'all' - all teams series, 'max' - the longest series, 'mean' - average value of series.

            Raises
            ----------
                TypeError: 'series_type' argument must be int.
                ValueError: the value of 'series_type' argument must be 0 or 1.
                TypeError: 'seasons' argument must be integer, list of integers or 'all' string for all seasons.
                ValueError: Season does not exist.
                ValueError: one or more of seasons do not exists.
                ValueError: incorrect string value. 'Seasons' must be int, list of ints, 'all' for all seasons.
                TypeError: 'teams' argument must be string, list of strings.
                ValueError: team does not exist.
                ValueError: one or more of teams do not exists.
                TypeError: 'func' argument must be string type.
                ValueError: 'func' argument must be 'all', 'max' or 'mean'.

            Returns
            ----------
                pandas.DataFrame: filtered no draw/draw series for teams.
        """

        # arguments validation
        draw_series_arg_validation(self, series_type, seasons, teams, func)

        # 0 - no draw series, 1 - draw series
        if series_type == 0:
            draw_series = TeamDrawAnalysis.__calculate_teams_no_draw_series(self, series_type=0)
        else:
            draw_series = TeamDrawAnalysis.__calculate_teams_no_draw_series(self, series_type=1)

        # seasons filter
        if seasons == 'all':
            pass
        elif isinstance(seasons, list):
            draw_series = draw_series[draw_series['league_season'].isin(seasons)]
        else:
            draw_series = draw_series[draw_series['league_season'] == seasons]

        # teams filter
        if teams == 'all':
            pass
        elif isinstance(teams, list):
            draw_series = draw_series[draw_series['team_name'].isin(teams)]
        else:
            draw_series = draw_series[draw_series['team_name'] == teams]

        # func - 'all', 'max', 'mean'
        if func == 'max':
            max_rows = draw_series.groupby('team_name')['series_without_draw'].idxmax(axis=0).to_list()
            draw_series = draw_series.loc[max_rows]

        elif func == 'mean':
            draw_series = draw_series.groupby('team_name', as_index=False)['series_without_draw'].mean()
            draw_series['series_without_draw'] = round(draw_series['series_without_draw'], 2)

        draw_series.set_index('team_name', inplace=True)

        if series_type == 0:
            return draw_series
        elif series_type == 1:
            draw_series.rename({'series_without_draw': 'draw_series'}, axis=1, inplace=True)
            return draw_series

    def draw_results_performance(self, seasons='all', teams='all', transpose=False):

        """Calculate draw results performance.

            Parameters
            ----------

            seasons (int or list): Default - 'all' leauge seasons. Int or list of ints of league seasons for request.

            teams (str or list): Default 'all' - teams series, 'str' name for single team or list(str) of team names for
                                 request.

            transpose (bool): True to transpose table. Deafult = False.

            Raises
            ----------
                TypeError: 'seasons' argument must be integer, list of integers or 'all' string for all seasons.
                ValueError: Season does not exist.
                ValueError: one or more of seasons do not exists.
                ValueError: incorrect string value. 'Seasons' must be int, list of ints, 'all' for all seasons.
                TypeError: 'teams' argument must be string, list of strings.
                ValueError: team does not exist.
                ValueError: one or more of teams do not exists.
                TypeError: 'transpose' argument must be bool.

            Returns
            ----------
            pandas.DataFrame: filtered attribute draw_results_performance
        """

        # arg validation:
        seasons_arg_validation(self, seasons)
        teams_arg_validation(self, teams)

        if not isinstance(transpose, bool):
            raise TypeError(f"The 'transpose' argument must be bool, not {type(transpose).__name__} type.")

        draw_performance_filtered = self.__draw_results_seasons_filter(seasons)

        # teams filter
        if teams == 'all':
            draw_performance_filtered_teams = draw_performance_filtered

        elif isinstance(teams, str):
            draw_performance_filtered_teams = draw_performance_filtered.loc[teams].to_frame().transpose()

        else:
            draw_performance_filtered_teams = draw_performance_filtered.loc[teams]

        # transpose table
        if transpose is True:
            draw_performance_transposed = self.__transpose_draw_performance_table(draw_performance_filtered_teams,
                                                                                  seasons)
            return draw_performance_transposed
        else:
            return draw_performance_filtered_teams

    def teams_correct_scores(self, seasons='all', teams='all'):

        """Calculate counted of teams correct scores by seasons.

            Parameters
            ----------

            seasons (int or list): Default - 'all' leauge seasons. Int or list of ints of league seasons for request.

            teams (str or list): Default 'all' - teams series, 'str' name for single team or list(str) of team names for
                                 request.

            Raises
            ----------
                TypeError: 'seasons' argument must be integer, list of integers or 'all' string for all seasons.
                ValueError: Season does not exist.
                ValueError: one or more of seasons do not exists.
                ValueError: incorrect string value. 'Seasons' must be int, list of ints, 'all' for all seasons.
                TypeError: 'teams' argument must be string, list of strings.
                ValueError: team does not exist.
                ValueError: one or more of teams do not exists.

            Returns
            ----------
            pandas.DataFrame: counted correct scores by teams.
        """

        # arg validation and seasons, teams filter
        fixtures_data = self._fixtures_data.copy()
        fixtures_data_filtered_seasons = self._seasons_filter(fixtures_data, seasons)
        fixtures_data_filtered_teams = self._teams_filter(fixtures_data_filtered_seasons, teams)

        # teams home correct scores
        home_scores = \
            fixtures_data_filtered_teams.groupby(['teams_home_name', 'league_season', 'fulltime_match_result']).agg(
                'count')['fixture_id'].to_frame()
        home_scores.rename(columns={'fixture_id': 'home_scores'}, inplace=True)

        # teams away correct scores
        away_scores = \
            fixtures_data_filtered_teams.groupby(['teams_away_name', 'league_season', 'fulltime_match_result']).agg(
                'count')['fixture_id'].to_frame()

        # merge
        teams_coorect_scores = home_scores.copy()
        teams_coorect_scores['away_scores'] = away_scores['fixture_id']

        # drop index == 0
        index_with_no_data = teams_coorect_scores[
            teams_coorect_scores[teams_coorect_scores.columns] == 0].dropna().index
        teams_coorect_scores.drop(index_with_no_data, inplace=True)

        # detailed team filter
        if isinstance(teams, str) and not teams == 'all':
            teams_coorect_scores = teams_coorect_scores.loc[teams]

        return teams_coorect_scores

    def standings_draws(self):

        """Calculate compilation of positions in table and draws results.

            Returns
            ---------
            pd.DataFrame: league table with positions and counted draws by seasons.
        """

        df_list = []

        for season in self._seasons:
            df_list.append(self.league_table(season=season)['D'])

        draws_table = pd.DataFrame(index=df_list[0].index,
                                   data={str(season): df for df, season in zip(df_list, self._seasons)})

        return draws_table

    def teams_goals_performance(self, seasons='all', teams='all'):

        """Calculate goals mean per match and total goals difference for season.

            Parameters
            ----------

            seasons (int or list): Default - 'all' leauge seasons. Int or list of ints of league seasons for request.

            teams (str or list): Default 'all' - teams series, 'str' name for single team or list(str) of team names for
                                request.

            Raises
            ----------
                TypeError: 'seasons' argument must be integer, list of integers or 'all' string for all seasons.
                ValueError: Season does not exist.
                ValueError: one or more of seasons do not exists.
                ValueError: incorrect string value. 'Seasons' must be int, list of ints, 'all' for all seasons.
                TypeError: 'teams' argument must be string, list of strings.
                ValueError: team does not exist.
                ValueError: one or more of teams do not exists.

            Returns
            ----------
            pandas.DataFrame: teams goals performance.
        """

        # arg validation
        seasons_arg_validation(self, seasons)
        teams_arg_validation(self, teams)

        def teams_goals_mean_and_diff(_season):

            """Calculate goals mean per match and total goals difference for season.
            """
            season_league_table = TeamDrawAnalysis.league_table(self, season=_season)
            season_league_table['goals_mean_' + str(_season)] = round(
                season_league_table.apply(lambda x: (x['GF'] + x['GA']) / x['MP'], 1), 2)
            season_league_table.rename(columns={'GD': 'goals_diff_' + str(_season)}, inplace=True)
            teams_goals = season_league_table[
                ['Team', 'goals_mean_' + str(_season), 'goals_diff_' + str(_season)]]
            teams_goals.set_index('Team', drop=True, inplace=True)

            return teams_goals

        # seasons filter
        seasons_list = []
        if len(self._seasons) > 1:
            if seasons == 'all':
                seasons_list = self._seasons
            else:
                seasons_list = seasons

        team_goals_performance = pd.DataFrame()
        if len(self._seasons) > 1:
            teams_goals_perf_list = []
            team_goals_performance = pd.DataFrame(index=self._teams)
            if seasons == 'all' or isinstance(seasons, list):
                for season in seasons_list:
                    teams_goals_perf_season = teams_goals_mean_and_diff(season)
                    teams_goals_perf_list.append(teams_goals_perf_season)
                for df in teams_goals_perf_list:
                    team_goals_performance = team_goals_performance.merge(df, how='left', left_index=True,
                                                                          right_index=True)

            elif isinstance(seasons, int):
                team_goals_performance = teams_goals_mean_and_diff(seasons)

        elif len(self._seasons) == 1:
            team_goals_performance = teams_goals_mean_and_diff(self._seasons[0])

        # columns type
        team_goals_performance.fillna(value=0, inplace=True)
        for col in team_goals_performance.columns:
            if 'diff' in col:
                team_goals_performance[col] = team_goals_performance[col].astype('int')

        # team filter
        if teams == 'all':
            return team_goals_performance
        else:
            team_goals_performance_filtered = team_goals_performance.loc[teams]
            return team_goals_performance_filtered
