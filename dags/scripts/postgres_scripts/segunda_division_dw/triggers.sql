-- refresh materalized views
-- unique index is required - 'CREATE UNIQUE INDEX ON mat_view_segdiv_current_standings (standings_type_id,team_position'

CREATE OR REPLACE FUNCTION trigger_refresh_mat_view()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_segdiv_current_standings;
    RETURN NULL;
END;
$$;


CREATE TRIGGER trigger_refresh_mat_view
    AFTER INSERT OR UPDATE OR DELETE
    ON fact_standings
    FOR EACH STATEMENT
        EXECUTE PROCEDURE trigger_refresh_mat_view();