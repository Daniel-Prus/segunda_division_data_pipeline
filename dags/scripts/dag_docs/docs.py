class DagDocs:
    build_database_doc = """
    
    ## Build Databases   
    <br />
    Dag creates Postgres connections and databases.
    ### Warning ! Pipeline deletes existing data.
    <br />
    ### Outputs:
    <br />
    #### Connections:
    
    <br />
    - 'postgres_localhost'
    - 'postgres_football_db'
    - 'postgres_segunda_division_dw'   
    <br />
    #### Databases, schemas, tables:
    <br />
    - `football_db`:
    
        - api.results
        - api.fixtures
        - api.season
        - api.team
        - cal.league_table
        - cal.league_table_home
        - cal.league_table_away
        - val.teams_market_value
 
    <br />
    - `segunda_divison_dw`
    
    """
    
    
    
    
    
    
