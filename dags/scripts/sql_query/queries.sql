-- get last results segdiv

SELECT vmr.fixture_date_utc, vmr.team_home_name, vmr.team_away_name, vmr.season, vmr.round, vmr.match_result
FROM view_match_results AS vmr
WHERE league_id = 141 AND season = get_current_season(141) AND round = get_next_round(141)


-- get next round segdiv

SELECT vmr.fixture_date_utc, vmr.team_home_name, vmr.team_away_name, vmr.season, vmr.round, vmr.match_result
FROM view_match_results AS vmr
WHERE league_id = 141 AND season = get_current_season(141) AND round = get_next_round(141)


-- get current standings segdiv (standings_type_id = {1: league table, 2: home, 3: away})

select team_position, team_name, mp, w, d, l, gf, ga, gd, pts
from mat_view_segdiv_current_standings
where standings_type_id = 1