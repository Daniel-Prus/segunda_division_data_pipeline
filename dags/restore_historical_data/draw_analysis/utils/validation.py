def seasons_arg_validation(self, seasons):
    """'seasons' argument validation.

        Parameters
        ----------
            seasons (int, str, list): provided seasons argument to the main methods.

        Raises
        ----------
            TypeError: Seasons argument must be integer, list of integers or 'all' string for all seasons.
            ValueError: Season does not exist.
            ValueError: One or more of seasons do not exists.
            ValueError: Incorrect string value. Seasons must be int, list of ints, 'all' for all seasons.
    """
    if not (isinstance(seasons, int) or isinstance(seasons, list) or isinstance(seasons, str)):
        raise TypeError("Seasons argument must be integer, list of integers or 'all' string for all seasons.")

    if isinstance(seasons, int):
        if seasons not in self._seasons:
            raise ValueError(f"Season does not exist - {seasons}.")
    if isinstance(seasons, list):
        for season in seasons:
            if season not in self._seasons:
                raise ValueError(f"One or more of seasons do not exists - {seasons}.")
    if isinstance(seasons, str):
        if seasons != "all":
            raise ValueError("Incorrect string value. Seasons must be int, list of ints, 'all' for all seasons.")


def teams_arg_validation(self, teams):
    """'teams' argument validation.

        Parameters
        ----------
            teams (str, list): provided 'teams' argument to the main methods.

        Raises
        ----------
            TypeError: 'teams' argument must be string, list of integers or 'all' string for all seasons.
            ValueError: Team does not exist.
            ValueError: One or more of teams do not exists
    """

    if not (isinstance(teams, str) or isinstance(teams, list)):
        raise TypeError(f"'Teams' argument must be string, list of strings, not {type(teams).__name__} type.")
    if isinstance(teams, str):
        if teams not in self._teams and teams != 'all':
            raise ValueError(f"Team does not exist - {teams}.")
    if isinstance(teams, list):
        for team in teams:
            if team not in self.teams:
                raise ValueError(f"One or more of teams do not exists - {team}.")


def rslts_type_arg_validation(self, rslts_type):
    """'rslts_type' argument validation.

        Parameters
        ----------
            rslts_type(str): 'count' to calculate number of results, 'pcts' - percentage share.

        Raises
        ----------
            TypeError: 'rsls_type' argument must be string type.
            ValueError: 'rslts_type' argument must be named 'count' or 'pcts'.
    """

    if not isinstance(rslts_type, str):
        raise TypeError(f"rslts_type argument must be string not - {type(rslts_type).__name__} type.")

    if isinstance(rslts_type, str):
        if rslts_type not in ['count', 'pcts']:
            raise ValueError(f"rslts_type argument must be named 'count' or 'pcts'.")


def teams_standings_arg_validation(self, season, spot):
    """Validation of - season, spot arguments . Auxiliry function for 'teams_standings_home_away' method.
    """

    # season arg
    if not isinstance(season, int):
        raise TypeError(f"Season argument must be integer not - {type(season).__name__} type.")

    if isinstance(season, int):
        if season not in self._seasons:
            raise ValueError(f"Season does not exist - {season}.")

    # spot arg
    if not isinstance(spot, str):
        raise TypeError(f"Spot argument must be string not - {type(spot).__name__} type.")

    if isinstance(spot, str):
        if spot not in ['home', 'away']:
            raise ValueError(f"Spot argument must be named 'home' or 'away'.")


def plot_type_arg_validation(plot_type):
    """'plot_type' argument validation."""

    if isinstance(plot_type, str):
        if plot_type != 'count' and plot_type != 'pcts':
            raise ValueError(
                f"Incorrect string value. The plot_type argument must be named 'count' or 'pcts', not - ' \
                {plot_type}'.")

    if not isinstance(plot_type, str):
        raise TypeError(f'The plot_type argument must be str, not {type(plot_type).__name__} type.')


def draw_results_plot_arg_validation(self, seasons, plot_type, top):
    """Validation for arguments provided by user."""

    if not (isinstance(seasons, int) or isinstance(seasons, str)):
        raise TypeError(f'Seasons argument must be int or str, not - {type(seasons).__name__} type.')

    if isinstance(seasons, int):
        if seasons not in self._seasons:
            raise ValueError(f'Season does not exist - {seasons}.')
    if isinstance(seasons, str):
        if seasons != 'total':
            raise ValueError("Incorrect string value. Seasons must be int or 'total' for total data calculation.")

    plot_type_arg_validation(plot_type)

    if not isinstance(top, int):
        raise TypeError(f'The top argument must be int, not {type(top).__name__} type.')


def draw_series_arg_validation(self, series_type, seasons='all', teams='all', func='all'):
    """Arguments validation function for 'teams_draw_series_filter' method.
    """

    # series_type
    if not isinstance(series_type, int):
        raise TypeError(f"'Series_type' argument must be int, not - {type(series_type).__name__} type.")
    if isinstance(series_type, int):
        if series_type not in [0, 1]:
            raise ValueError(f"The value of 'series_type' argument must be 0 or 1.")

    # seasons
    seasons_arg_validation(self, seasons)

    # teams
    teams_arg_validation(self, teams)

    # func
    if not isinstance(func, str):
        raise TypeError(f"'Func' argument must be string not {type(func).__name__} type.")
    if isinstance(func, str):
        if func not in ['all', 'max', 'mean']:
            raise ValueError(f"'Func' argument must be 'all', 'max' or 'mean', not {func}.")
