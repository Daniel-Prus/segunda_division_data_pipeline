-- segunda_division league_id (141)


-- get current season (league_id)

CREATE OR REPLACE FUNCTION get_current_season (league_id_param int)
RETURNS int
AS
$$
DECLARE
	current_season int;
BEGIN
	SELECT MAX(season) INTO current_season FROM fact_results WHERE league_id = league_id_param;
	return current_season;
END;
$$ LANGUAGE plpgsql;


-- get current round (league_id)

CREATE OR REPLACE FUNCTION get_current_round (league_id_param int)
RETURNS int
AS
$$
DECLARE
	current_round int;
BEGIN
	SELECT MAX(round) INTO current_round FROM fact_results WHERE season = (SELECT get_current_season (league_id_param)) AND goals_home IS NOT NULL;
	return current_round;
END;
$$ LANGUAGE plpgsql;


-- get next round (league_id)

CREATE OR REPLACE FUNCTION get_next_round (league_id_param int)
    RETURNS int
AS $$
DECLARE
	max_rounds int;
	current_round int;
BEGIN
	SELECT rounds INTO max_rounds FROM dim_league WHERE league_id = league_id_param;
	SELECT * INTO current_round FROM get_current_round (league_id_param);

	IF current_round + 1 < max_rounds THEN
		return current_round + 1;
	END IF;
	IF current_round + 1 > max_rounds THEN
		return max_rounds;
	END IF;
END;
$$ LANGUAGE plpgsql;