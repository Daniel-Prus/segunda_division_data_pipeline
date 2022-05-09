import requests
import pandas as pd


class GetApiFootballData:

    """GetApiFootballData class is used to search and download fixtures data from API-Football-Beta.

       The class allows to easily search for the looking league ID by calling function'get_country_leagues_overview'
       with country name.

       To check details and league seasons availability in API-Football-Beta source call function 'get_seasons_details'.

       Proper operation require API headers, which can be obtained by registering at https://rapidapi.com/.

       For more information about API-Football-Beta visit https://rapidapi.com/api-sports/api/api-football-beta/.


        'Class attributes'
        ------------------
            fixtures_url(str): API-Football-Beta url to download fixtures data.
            league_url(str): API-Football-Beta url to find lique ID.

        'Attributes'
        ------------
            credentials (dict): Rapid API headers - { 'x-rapidapi-key':'',  'x-rapidapi-host':''}.
            country_leagues_overview (pd.DataFrame): established in paralell with calling function
                                                    'get_country_leagues_overview'

    """

    fixtures_url = "https://api-football-beta.p.rapidapi.com/fixtures"
    league_url = "https://api-football-beta.p.rapidapi.com/leagues"

    def __init__(self, credentials):
        self.__credentials = credentials
        self.country_leagues_overview = pd.DataFrame()
        self.__check_status_code()

    def __check_status_code(self):
        response = requests.get(GetApiFootballData.fixtures_url, headers=self.__credentials)
        status_code = response.status_code
        if status_code == 200:
            print(f"Response status code: {response.status_code}\n"
                  f"Connection successfull.")
        elif status_code == 403:
            message = response.json()['message']
            print(f"Response status code: {response.status_code}\n"
                  f"Connection unsuccessfull.\n"
                  f"Message: {message}")
        else:
            message = response.json()['message']
            print(f"Response status code: {response.status_code}\n"
                  f"Message: {message}")

    def __get_api(self, url, **api_parameters):

        """Get Api data and convert to pandas dataframe."""

        response = requests.request("GET", url, headers=self.__credentials, params=api_parameters)
        json_data = response.json()["response"]
        df = pd.json_normalize(json_data)
        return df

    def get_fixtures_data(self, league_id, seasons):

        """ Get fixtures data from Api-Football-Beta.

            Parameters
            ----------

            leauge_id (str or int): API-Football-Beta league ID for request.

            seasons (list or int): 'list (str or int)' of league seasons. 'Int' for single season for request.

            Returns
            ----------
            pandas.core.frame.DataFrame: concatenated pandas DataFrame object.

        """

        if isinstance(seasons, int):
            querystring = {"league": league_id, "season": seasons}
            df = GetApiFootballData.__get_api(self, GetApiFootballData.fixtures_url, **querystring)
            return df

        if isinstance(seasons, list):
            df_list = []
            for season in seasons:
                df = GetApiFootballData.__get_api(self, GetApiFootballData.fixtures_url,
                                                  **{"league": league_id, "season": season})
                df_list.append(df)

            concat_df = pd.concat(df_list)
            concat_df.reset_index(drop=True, inplace=True)
            return concat_df

    def get_country_leagues_overview(self, country):

        """ Find and get league ID by providing country name.

            Parameters
            ----------

            country (str): country name in english language.

            Returns
            ----------
            pandas.core.frame.DataFrame: table with leagues description for requested country.

        """
        querystring = {"search": country}
        country_leagues_overview = GetApiFootballData.__get_api(self, GetApiFootballData.league_url, **querystring)
        country_leagues_overview.set_index(keys='league.id', inplace=True)

        if self.country_leagues_overview.empty is True:
            self.country_leagues_overview = country_leagues_overview[
                ['league.name', 'league.type', 'country.name', 'seasons']]

        country_leagues_overview = country_leagues_overview[['league.name', 'league.type', 'country.name']]

        return country_leagues_overview

    def get_seasons_details(self, league_id):

        """ Get seasons details - availability, years, start and end of season.
            Method requires calculated 'country_leagues_overview' attribute by running method
            'get_country_leagues_overview'.

            Parameters
            ----------

            league_id (str): API-Football-Beta league ID.

            Returns
            ----------
            pandas.core.frame.DataFrame: table with leagues description for requested country.

        """

        country_leagues_overview = self.country_leagues_overview.copy()
        seasons_details = pd.json_normalize(country_leagues_overview['seasons'].loc[league_id])
        seasons_details = seasons_details[['year', 'start', 'end']]

        print(country_leagues_overview.loc[league_id][['country.name', 'league.name']])
        return seasons_details
