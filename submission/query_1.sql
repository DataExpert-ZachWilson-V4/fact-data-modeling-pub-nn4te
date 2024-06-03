WITH t AS (
    SELECT 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY 
                game_id,
                team_id,
                player_id 
            ORDER BY 
                game_id, 
                team_id, 
                player_id
        ) as row_number 
    FROM bootcamp.nba_game_details
)
SELECT * FROM t WHERE row_number = 1
