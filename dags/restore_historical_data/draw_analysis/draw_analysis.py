from ._data_organizer import _DataOrganizer, pd, np, plt

from .utils.validation import (
    seasons_arg_validation,
    plot_type_arg_validation,
    rslts_type_arg_validation
)


class DrawAnalysis(_DataOrganizer):
    """
    DrawAnalysis class provides and visualizes an overall analysis for leagues data in terms of draw scores.


        'Attributes'
        ------------
            rawdata (pd.DataFrame): provided raw fixtures data.
            fixtures_data (pd.DataFrame): automatically cleaned and optimized data from raw data fixtures.
            seasons (list): calculated league seasons from 'fixtures data'.
            teams (list): league teams from 'fixtures data'.
            draw_score (float): total draw results score.
    """

    def __init__(self, rawdata):
        super().__init__(rawdata)

    def __seasons_for_plot_title(self, seasons):

        """Seasons validation and conversion to string for plot title argument.
           Auxiliary function for the private method - __correct_scores_plot.

            Parameters
            ----------
                seasons (int, str, list): provided seasons.

            Returns
            ----------
                str: seasons converted to string.
        """

        if len(self._seasons) == 1:
            seasons_to_call = self._seasons[0]
            return seasons_to_call
        elif seasons == 'all':
            seasons_to_call = self._seasons
            return seasons_to_call
        elif isinstance(seasons, int):
            seasons_to_call = f'season {seasons}'
            return seasons_to_call
        else:
            seasons_to_call = f'seasons {sorted(seasons)}'
            return seasons_to_call

    @staticmethod
    def __correct_scores_data_setter(self, correct_scores, seasons):

        """Prepare 'correct_score' dataframe to correct_scores_plot methods according to the 'seasons'.

            Parameters
            ----------
                correct_scores (pandas.DataFrame): correct score attribute
                seasons (int, str, list): provided seasons.

            Raises
            ----------
                ValueError: Season does not exist.
                ValueError: One or more of seasons do not exists.
                ValueError: Incorrect string value. Seasons must be int, list of ints, 'all' for all seasons.

            Returns
            ----------
                pd.DataFrame: organized correct scores data.
        """

        if isinstance(seasons, list):
            correct_scores_filtered = correct_scores.loc[seasons]['count']
            correct_scores_grouped = correct_scores_filtered.swaplevel().groupby(level=0).agg('sum').to_frame()
            correct_scores_grouped['pcts(%)'] = round(correct_scores_grouped / correct_scores_grouped.sum() * 100, 2)
            correct_scores_grouped['pcts(%)'] = correct_scores_grouped['pcts(%)'].astype('float')
            correct_scores_grouped.sort_values(by='count', ascending=False, inplace=True)

        elif seasons == 'all':
            correct_scores_filtered = correct_scores['count']
            correct_scores_grouped = correct_scores_filtered.swaplevel().groupby(level=0).agg('sum').to_frame()
            correct_scores_grouped['pcts(%)'] = round(correct_scores_grouped / correct_scores_grouped.sum() * 100, 2)
            correct_scores_grouped['pcts(%)'] = correct_scores_grouped['pcts(%)'].astype('float')
            correct_scores_grouped.sort_values(by='count', ascending=False, inplace=True)
        else:
            correct_scores_grouped = correct_scores.loc[seasons]

        return correct_scores_grouped

    def __correct_scores_plot(self, plot_type, dataframe, seasons, title, ytick_series):

        """Draw bar chart for correct scores according to plot_type argument.

            Parameters
            ----------
                plot_type (str): 'count' - number of scores, 'pcts' - percentage share of correct draw scores.
                dataframe (pd.DataFrame): data to draw the chart.
                seasons (int, str, list): provided seasons.
                title (str): chart title.
                ytick_series (pd.Series): Series to calculate yticks array.

            Returns
            ----------
                matplotlib.pyplot.plot: data bar plot.
         """
        seasons_filter = self.__seasons_for_plot_title(seasons)
        ytick_setter = self._yticks_plot_auto_setter(ytick_series)

        if plot_type == 'count':
            dataframe['pcts(%)'] = round(dataframe['pcts(%)'], 0)
            dataframe['pcts(%)'] = dataframe['pcts(%)'].astype('int32')
            correct_scores_plot = dataframe[dataframe.columns[0]].plot(kind='bar', grid=True, legend=True,
                                                                       yticks=ytick_setter, rot=30,
                                                                       title=f'{title} {seasons_filter}')
            pcts_line = dataframe[dataframe.columns[1]].plot(secondary_y=True, xlabel='', grid=False, legend=True,
                                                             rot=80, color='tab:olive')
            pcts_line.legend(loc='center left', bbox_to_anchor=(1.05, 0.89))
            correct_scores_plot.legend(loc='center left', bbox_to_anchor=(1.05, 0.95))

            def addlabels(y, **kwargs):
                for i in range(len(y)):
                    plt.text(i, y[i], f'{y[i]}%', kwargs)

            chart_text = dataframe[dataframe.columns[1]].to_list()[:10]

            addlabels(chart_text, **_DataOrganizer._text_font)
            plt.show()

        elif plot_type == 'pcts':
            correct_scores_pcts_plot = dataframe.plot(kind='bar', title=f'{title} {seasons_filter}', ylabel='%',
                                                      yticks=ytick_setter, rot=30)
            plt.show()

    def results_sum(self, rslts_type):

        """
        Calculate number/percentage share of matches results by seasons.

         Parameters
         ----------
            rsls_type(str): 'count' to calculate number of results, 'pcts' - percentage share.

         Returns
         ----------
            pd.DataFrame: match results summarize
        """
        rslts_type_arg_validation(self, rslts_type)

        # Calculate results summary by pivot table.
        fixtures_data_pivot = self._fixtures_data.pivot_table(index='match_result', columns='league_season',
                                                              values='fixture_id', aggfunc='count')
        fixtures_data_pivot.rename(mapper={0: 'draw', 1: 'home_win', 2: 'away_win'}, inplace=True)
        fixtures_data_pivot['total'] = 0
        for season in self._seasons:
            fixtures_data_pivot['total'] += fixtures_data_pivot[season]

        results_summarize_pcts = fixtures_data_pivot.copy().unstack().groupby(level=0).apply(
            lambda x: round(100 * x / int(x.sum()), 2)).to_frame().swaplevel().unstack()[0].reindex(
            ['draw', 'home_win', 'away_win'])

        if rslts_type == 'count':
            results_summarize = fixtures_data_pivot
            return results_summarize

        elif rslts_type == 'pcts':
            results_summarize_pcts = fixtures_data_pivot.copy().unstack().groupby(level=0).apply(
                lambda x: round(100 * x / int(x.sum()), 2)).to_frame().swaplevel().unstack()[0].reindex(
                ['draw', 'home_win', 'away_win'])
            return results_summarize_pcts

    def match_results_plot(self, rslts_type='count'):

        """Draw bar chart of matches results by seasons.

            Parameters
            ----------
                rsls_type(str): default 'count' to calculate number of results, 'pcts' - percentage share.

            Returns
            ----------
               matplotlib.pyplot.plot: plotted data.

        """
        results_summarize = DrawAnalysis.results_sum(self, rslts_type=rslts_type)
        y_tick = _DataOrganizer._yticks_plot_auto_setter(results_summarize.loc['home_win', self._seasons])

        if rslts_type == 'count':
            ylabel = rslts_type
        else:
            ylabel = '%'

        results_summarize[self._seasons].transpose().plot(kind='bar', color=_DataOrganizer._graph_colors,
                                                          yticks=y_tick,
                                                          title='Matches results by seasons',
                                                          rot=30,
                                                          ylabel=ylabel)
        plt.legend(bbox_to_anchor=(1.0, 1.0))
        plt.show()

    def match_results_total_plot(self, rslts_type='count', seasons='all'):

        """Draw bar chart of total matches results.

            Parameters
            ----------
                rsls_type(str): default 'count' to calculate number of results, 'pcts' - percentage share.

            Returns
            ----------
                matplotlib.pyplot.plot: plotted data.
        """
        results_summarize = self.results_sum(rslts_type=rslts_type)
        seasons_arg_validation(self, seasons)

        if seasons == 'all':
            title = 'total'
        else:
            title = seasons

        x_tick = _DataOrganizer._yticks_plot_auto_setter(
            results_summarize[seasons][seasons[-1]] if isinstance(seasons, list)
            else results_summarize[title])

        results_summarize[title].plot(kind='barh',
                                      title=f'Number of matches results summary '
                                            f'({title})',
                                      color=_DataOrganizer._graph_colors,
                                      xticks=x_tick,
                                      xlabel='',
                                      ylabel=f'seasons - {self._seasons}',
                                      figsize=[12, 5])

    def results_pcts_pie_plot(self, seasons='all', subplots=False):

        """Draw pie chart for percentage share of matches results.


            Parameters
            ----------
                subplots (bool): True value - for multipled seasons. Deafult - False.

            Raises
            ----------
                TypeError: Cannot draw subplots for one season data.

            Returns
            ----------
                matplotlib.pyplot.plot: plotted data.
        """
        seasons_arg_validation(self, seasons)
        results_summarize_pcts = DrawAnalysis.results_sum(self, rslts_type='pcts')

        def draw_pcts_pie(title='', _seasons='all', layout=(2, 2)):
            """Nested function to draw plot according to condictions."""

            if subplots is True:

                results_percentage_share_plot = results_summarize_pcts.plot(kind='pie', subplots=subplots,
                                                                            layout=layout, autopct='%.0f%%',
                                                                            fontsize=12, legend=True,
                                                                            explode=_DataOrganizer._explode_tuple,
                                                                            textprops={'color': 'whitesmoke'},
                                                                            colors=_DataOrganizer._graph_colors,
                                                                            figsize=[15, 10], labeldistance=None,
                                                                            title=title)

            else:
                results_percentage_share_plot = results_summarize_pcts['total' if _seasons == 'all' else _seasons] \
                    .plot(kind='pie', title=title,
                          explode=_DataOrganizer._explode_tuple,
                          autopct='%.0f%%',
                          colors=_DataOrganizer._graph_colors,
                          ylabel='')

            plt.show()

        if subplots is True:
            if len(self._seasons) == 1:
                raise TypeError('Cannot draw subplots for one season data.')
            elif len(self._seasons) >= 4:
                draw_pcts_pie(title='Results percentage share per seasons', layout=(3, 3))
            else:
                draw_pcts_pie(title='Results percentage share per seasons')
        else:
            if len(self._seasons) == 1:
                draw_pcts_pie(title=f'Percentage share of results - season {self._seasons[0]} (%)',
                              _seasons=seasons)
            elif isinstance(seasons, list) or (isinstance(seasons, int)):
                draw_pcts_pie(title=f'Percentage share of results - season {seasons} (%)',
                              _seasons=seasons)
            else:
                draw_pcts_pie(title=f'Percentage share of results to the total (%)')

    def month_results(self, seasons='all'):

        """Calculate number of matches results per month.

            Parameters
            ----------
                seasons (int, list, str): Default - 'all' leauge seasons. Int or list of ints of league seasons
                                        for request.

            Raises
            ----------
                TypeError: Seasons argument must be integer, list of integers or 'all' string for all seasons.
                ValueError: Season does not exist.
                ValueError: One or more of seasons do not exists.
                ValueError: Incorrect string value. Seasons must be int, list of ints, 'all' for all seasons.

            Returns
            ----------
                pd.DataFrame: sum of matches results per month.
        """

        seasons_arg_validation(self, seasons)

        df_month_col_filter = self._fixtures_data[
            ['fixture_date', 'league_season', 'league_name', 'match_result', 'halftime_match_result',
             'fulltime_match_result']]
        df_month = self._seasons_filter(df_month_col_filter, seasons)

        df_month.insert(loc=1, column='month', value=pd.DatetimeIndex(df_month['fixture_date']).month)

        # calculating summary quantity of the results per month (total)
        results_per_month = df_month.pivot_table(index='match_result', columns=['month'], values='league_name',
                                                 aggfunc=['count'])
        results_per_month.rename(mapper={0: 'draw', 1: 'home_win', 2: 'away_win'}, inplace=True)
        results_per_month = results_per_month['count']

        return results_per_month

    def results_per_month_pcts_plot(self, seasons='all'):

        """Draw bar chart for percentage share of matches results per seasons with season average line.

            Parameters
            ----------
                seasons (int, list, str): Default - 'all' leauge seasons. Int or list of ints of league seasons
                                          for request.

            Returns
            ----------
                matplotlib.pyplot.plot: plotted data.
        """

        results_per_month = self.month_results(seasons)

        # Calculating percentage share for matches results number
        results_per_month_pcts = results_per_month.unstack().groupby(level=0).apply(
            lambda x: round(100 * x / int(x.sum()), 2)).to_frame().swaplevel().unstack()[0].reindex(
            ['draw', 'home_win', 'away_win'])

        def draw_plot(pcts_rslts_per_month, seasons_='all'):

            draw_mean = round(pcts_rslts_per_month.loc['draw'].mean(), 2)
            home_win_mean = round(pcts_rslts_per_month.loc['home_win'].mean(), 2)
            away_win_mean = round(pcts_rslts_per_month.loc['away_win'].mean(), 2)

            results_per_month_pcts_plot = results_per_month_pcts.transpose() \
                .plot(kind='bar', stacked=True, ylabel='%',
                      color=_DataOrganizer._graph_colors,
                      legend=True, alpha=0.85,
                      yticks=np.arange(0, 105, 5),
                      title=f'Monthly percentage share of results - {seasons_} (total)',
                      rot=30, figsize=(12, 8))

            legend1 = plt.legend(bbox_to_anchor=(1.0, 1.0), fontsize=14, fancybox=True)
            plt.gca().add_artist(legend1)

            plt.axhline(draw_mean, color=_DataOrganizer._graph_colors[0], linestyle='--', )
            plt.axhline(home_win_mean, color=_DataOrganizer._graph_colors[1], linestyle='-.')
            plt.axhline(away_win_mean, color=_DataOrganizer._graph_colors[2], linestyle=':')

            plt.legend(['draw_avg', 'home_win_avg', 'away_win_avg'], title=f'Average {seasons_}:', title_fontsize=13,
                       fontsize=12, bbox_to_anchor=(1.0, 0.83))

            print(results_per_month_pcts)
            plt.show()

        if len(self._seasons) == 1:
            return draw_plot(results_per_month_pcts, seasons_=self._seasons[0])
        elif seasons == 'all':
            return draw_plot(results_per_month_pcts, seasons_='all seasons')
        else:
            return draw_plot(results_per_month_pcts, seasons)

    def correct_scores(self, scores_type):

        """Calculate summary for correct scores.

            Parameters
            ----------
               scores_type (str): 'fulltime', 'halftime', 'draw'.

            Returns
            ----------
                pd.DataFrame: summarize of count and percentage share of correct scores
        """

        mapper = {'fulltime': 'fulltime_match_result', 'halftime': 'halftime_match_result',
                  'draw': 'fulltime_match_result'}

        correct_scores = self.fixtures_data.copy()

        if scores_type in ['halftime', 'fulltime']:
            correct_scores_col_filter = correct_scores[['league_season', mapper[scores_type], 'fixture_id']]
            correct_scores_grouped = correct_scores_col_filter.groupby(
                by=['league_season', mapper[scores_type]])['fixture_id'].count().rename('count').to_frame()
            correct_scores_grouped['pcts(%)'] = \
                round(correct_scores_grouped / correct_scores_grouped.groupby(level=0).sum() * 100, 2)
            correct_scores_full_or_half = correct_scores_grouped.sort_values(by=['league_season', 'count'],
                                                                             ascending=[True, False])

            # drop index with values 0
            index_with_no_data = correct_scores_full_or_half[
                correct_scores_full_or_half[correct_scores_full_or_half.columns] == 0].dropna().index
            correct_scores_full_or_half.drop(index_with_no_data, inplace=True)

            return correct_scores_full_or_half

        elif scores_type == 'draw':
            correct_scores_col_filter = correct_scores[['league_season', 'fulltime_match_result', 'match_result']]
            df_draw_filter = correct_scores_col_filter[correct_scores_col_filter.match_result == 0]
            correct_scores_grouped = df_draw_filter.groupby(by=['league_season', mapper[scores_type]])[
                'match_result'].count().rename('count').to_frame()
            correct_scores_grouped['pcts(%)'] = round(
                correct_scores_grouped / correct_scores_grouped.groupby(level=0).sum() * 100, 2)
            correct_scores_draw = correct_scores_grouped[correct_scores_grouped['count'] > 0]
            correct_scores_draw = correct_scores_draw.sort_values(by=['league_season', 'count'],
                                                                  ascending=[True, False])

            # drop index with values 0
            index_with_no_data = correct_scores_draw[
                correct_scores_draw[correct_scores_draw.columns] == 0].dropna().index
            correct_scores_draw.drop(index_with_no_data, inplace=True)

            return correct_scores_draw

    def correct_scores_fulltime_plot(self, plot_type='count', seasons='all'):

        """Draw a plot for calculated number of fulltime correct scores.
           Requires calculated attribute - 'correct_scores_halftime'.

            Parameters
            ----------
               seasons (int, list, str): Default - 'all' leauge seasons. Int or list of ints for league seasons request.
               count_plot (bool): False value - scores dataframe. Deafult - True.

            Raises
            ----------
                ValueError: No numeric data to plot. Correct_scores_halftime attribute must be calculated first.
                TypeError: The count_plot argument must be type boolean.
                TypeError: The overall_plot argument must be type boolean.
                ValueError: Season does not exist.
                ValueError: One or more of seasons do not exists.
                ValueError: Incorrect string value. Seasons must be int, list of ints, 'all' for all seasons.

            Returns
            ----------
                matplotlib.pyplot.plot: plot for count/percentage share of correct fulltime scores.
        """

        # arg validation
        seasons_arg_validation(self, seasons)
        plot_type_arg_validation(plot_type)

        correct_scores_fulltime = DrawAnalysis.correct_scores(self, 'fulltime')
        correct_scores_fulltime = DrawAnalysis.__correct_scores_data_setter(self, correct_scores_fulltime, seasons)

        if plot_type == 'count':

            plot_title = 'Correct scores summary - fulltime'
            ytick_series = correct_scores_fulltime['count']

            return DrawAnalysis.__correct_scores_plot(self, plot_type, correct_scores_fulltime, seasons=seasons,
                                                      title=plot_title, ytick_series=ytick_series)

        elif plot_type == 'pcts':
            correct_scores_fulltime_pcts = correct_scores_fulltime['pcts(%)']
            plot_title = 'Percentage share of correct fulltime scores'
            ytick_series = correct_scores_fulltime_pcts.iloc[0]

            if seasons == 'all':
                return DrawAnalysis.__correct_scores_plot(self, plot_type, correct_scores_fulltime_pcts,
                                                          seasons=seasons, title=plot_title, ytick_series=ytick_series)
            else:
                return DrawAnalysis.__correct_scores_plot(self, plot_type, correct_scores_fulltime_pcts,
                                                          seasons=seasons, title=plot_title, ytick_series=ytick_series)

    def correct_scores_halftime_plot(self, plot_type='count', seasons='all'):

        """Draw a plot for calculated number/percentage share of correct halftime scores.

            Parameters
            ----------
               plot_type (str): 'count' - number of draw scores, 'pcts' - percentage share of correct draw scores.
               seasons (int, list, str): Default - 'all' leauge seasons. Int or list of ints for league seasons request.

            Raises
            ----------
                TypeError: The plot_type argument must be string type.
                ValueError: Incorrect string value. The plot_type argument must be named 'count' or 'pcts'.
                TypeError: Seasons argument must be integer, list of integers or 'all' string for all seasons.
                ValueError: Season does not exist.
                ValueError: One or more of seasons do not exists.
                ValueError: Incorrect string value. Seasons must be int, list of ints, 'all' for all seasons.

            Returns
            ----------
                matplotlib.pyplot.plot: plot for count/percentage share of correct halftime scores.
        """

        # arg validation
        seasons_arg_validation(self, seasons)
        plot_type_arg_validation(plot_type)

        correct_scores_halftime = DrawAnalysis.correct_scores(self, 'halftime')
        correct_scores_halftime = DrawAnalysis.__correct_scores_data_setter(self, correct_scores_halftime, seasons)

        if plot_type == 'count':

            plot_title = 'Correct scores summary - halftime'
            ytick_series = correct_scores_halftime['count']

            return DrawAnalysis.__correct_scores_plot(self, plot_type, correct_scores_halftime, seasons=seasons,
                                                      title=plot_title, ytick_series=ytick_series)

        elif plot_type == 'pcts':
            correct_scores_halftime_pcts = correct_scores_halftime['pcts(%)']
            plot_title = 'Percentage share of correct halftime scores'
            ytick_series = correct_scores_halftime_pcts.iloc[0]

            if seasons == 'all':
                return DrawAnalysis.__correct_scores_plot(self, plot_type, correct_scores_halftime_pcts,
                                                          seasons=seasons, title=plot_title, ytick_series=ytick_series)
            else:
                return DrawAnalysis.__correct_scores_plot(self, plot_type, correct_scores_halftime_pcts,
                                                          seasons=seasons, title=plot_title, ytick_series=ytick_series)

    def correct_scores_draw_plot(self, plot_type='count', seasons='all'):

        """Draw a plot for calculated number/percentage share of draw correct scores.

            Parameters
            ----------
               plot_type (str): 'count' - number of draw scores, 'pcts' - percentage share of correct draw scores.
               seasons (int, list, str): Default - 'all' leauge seasons. Int or list of ints for league seasons request.

            Raises
            ----------
                TypeError: The plot_type argument must be string type.
                ValueError: Incorrect string value. The plot_type argument must be named 'count' or 'pcts'.
                TypeError: Seasons argument must be integer, list of integers or 'all' string for all seasons.
                ValueError: Season does not exist.
                ValueError: One or more of seasons do not exists.
                ValueError: Incorrect string value. Seasons must be int, list of ints, 'all' for all seasons.

            Returns
            ----------
                matplotlib.pyplot.plot: plot for count/percentage share of correct draw results.
        """

        # arg validation
        seasons_arg_validation(self, seasons)
        plot_type_arg_validation(plot_type)

        correct_scores_draw = DrawAnalysis.correct_scores(self, 'draw')
        correct_scores_draw = DrawAnalysis.__correct_scores_data_setter(self, correct_scores_draw, seasons)

        if plot_type == 'count':

            plot_title = 'Number of draw correct scores - summary'
            ytick_series = correct_scores_draw['count']
            correct_scores_draw = correct_scores_draw[correct_scores_draw['count'] > 0]
            return DrawAnalysis.__correct_scores_plot(self, plot_type, correct_scores_draw, seasons=seasons,
                                                      title=plot_title, ytick_series=ytick_series)

        elif plot_type == 'pcts':
            correct_scores_pcts = correct_scores_draw['pcts(%)']
            correct_scores_pcts = correct_scores_pcts[correct_scores_pcts > 0]
            plot_title = 'Percentage share of correct draw scores'
            ytick_series = correct_scores_pcts.iloc[0]

            if seasons == 'all':
                return DrawAnalysis.__correct_scores_plot(self, plot_type, correct_scores_pcts, seasons=seasons,
                                                          title=plot_title, ytick_series=ytick_series)
            else:
                return DrawAnalysis.__correct_scores_plot(self, plot_type, correct_scores_pcts, seasons=seasons,
                                                          title=plot_title, ytick_series=ytick_series)
