{{ config(materialized='view') }}

WITH CTE AS (
    SELECT 
    (CASE
        WHEN home_score > away_score THEN home_team
        WHEN away_score > home_score THEN away_team
        ELSE 'DRAW'
    END) AS WINNER, 
    tournament
    FROM {{source('public','match_results')}}
    WHERE tournament IN ('FIFA World Cup', 'Copa América', 'UEFA Euro')
),
Ranked_CTE AS (
    SELECT 
        WINNER, 
        tournament, 
        COUNT(*) AS wins,
        ROW_NUMBER() OVER (PARTITION BY tournament ORDER BY COUNT(*) DESC) AS rank
    FROM CTE WHERE WINNER != 'DRAW'
    GROUP BY WINNER, tournament
)

SELECT WINNER, wins, tournament
FROM Ranked_CTE
WHERE rank = 1