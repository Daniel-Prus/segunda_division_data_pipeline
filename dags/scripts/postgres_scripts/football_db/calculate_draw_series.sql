-- based on 'views' available in section football_db/views

INSERT INTO cal.draw_series
	SELECT v1.fixture_id
		,v1.no_draw_series AS no_draw_home
		,v1.draw_series AS draw_home
		,v2.no_draw_series AS no_draw_away
		,v2.draw_series AS draw_away
	FROM view_draw_series_final AS v1
	INNER JOIN v_draw_series_final AS v2 ON v1.fixture_id = v2.fixture_id AND v2.is_home IS False
	WHERE v1.is_home IS True;