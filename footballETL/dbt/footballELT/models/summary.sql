{{ config(materialized='view') }}

with cte as(
select x.date,y.winner,x.home_team,x.away_team,x.tournament,x.home_score,x.away_score, true AS endInPen
from {{source('public','match_results')}} x left join {{source('public','penalty_shootouts')}} y ON
x.date = y.date and
x.home_team = y.home_team and
x.away_team = y.away_team
where winner is not null
)
select a.date,a.winner,a.tournament,a.home_team,a.home_score,a.away_team,a.away_score,b.scorerteam,b.scoreminute,b.scorername,b.penalty,b.og, a.endInPen
from cte a left join {{source('public','goal_scorers')}} b ON
a.date = b.matchdate and
a.home_team = b.hometeam and
a.away_team = b.awayteam 
order by a.date desc