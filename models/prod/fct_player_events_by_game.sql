Select
         a.GAME_ID,
         a.GAME_TYPE,
         a.GAME_SEASON,
         a.DATETIME as EVENT_TIME,
         a.PERIOD_TYPE,
         a.PERIOD,
         a.EVENT_ID,
         b.player_id,
         sum(case when EVENT_TYPE_ID = 'GOAL' and PLAYER_TYPE = 'Scorer' then 1 else 0 end) goals_scored,
         sum(case when EVENT_TYPE_ID = 'GOAL' and PLAYER_TYPE = 'Assist' then 1 else 0 end) assists,
         sum(case when EVENT_TYPE_ID = 'GOAL' and PLAYER_TYPE = 'Goalie' then 1 else 0 end) goals_against,
         sum(case when EVENT_TYPE_ID = 'SHOT' and PLAYER_TYPE = 'Shooter' then 1 else 0 end) + sum(case when EVENT_TYPE_ID = 'GOAL' and PLAYER_TYPE = 'Scorer' then 1 else 0 end) shots_on_goal,
         sum(case when EVENT_TYPE_ID = 'SHOT' and PLAYER_TYPE = 'Goalie' then 1 else 0 end) saves,
         sum(case when EVENT_TYPE_ID = 'HIT' and PLAYER_TYPE = 'Hitter' then 1 else 0 end) hits,
         sum(case when EVENT_TYPE_ID = 'HIT' and PLAYER_TYPE = 'Hittee' then 1 else 0 end) received_hits,
         sum(case when EVENT_TYPE_ID = 'BLOCKED_SHOT' and PLAYER_TYPE = 'Shooter' then 1 else 0 end) had_shots_blocked,
         sum(case when EVENT_TYPE_ID = 'BLOCKED_SHOT' and PLAYER_TYPE = 'Blocker' then 1 else 0 end) blocked_shots,
         sum(case when EVENT_TYPE_ID = 'FACEOFF' and PLAYER_TYPE = 'Winner' then 1 else 0 end) faceoffs_won,
         sum(case when EVENT_TYPE_ID = 'FACEOFF' and PLAYER_TYPE = 'Loser' then 1 else 0 end) faceoffs_lost,
         sum(case when EVENT_TYPE_ID = 'TAKEAWAY' then 1 else 0 end) takeaways,
         sum(case when EVENT_TYPE_ID = 'GIVEAWAY' then 1 else 0 end) giveaways,
         sum(case when EVENT_TYPE_ID = 'GOAL' and PLAYER_TYPE = 'Shooter' and PERIOD_S = 'SO' then 1 else 0 end) shootout_goals,
         sum(case when EVENT_TYPE_ID in ('SHOT', 'MISSED_SHOT') and PLAYER_TYPE = 'Shooter' and PERIOD_S = 'SO' then 1 else 0 end) + sum(case when EVENT_TYPE_ID = 'GOAL' and PLAYER_TYPE = 'Shooter' and PERIOD_S = 'SO' then 1 else 0 end) shootout_shots,
         sum(case when EVENT_TYPE_ID = 'GOAL' and PLAYER_TYPE = 'Goalie' and PERIOD_S = 'SO' then 1 else 0 end) shootout_goals_against,
         sum(case when EVENT_TYPE_ID in ('SHOT', 'MISSED_SHOT') and PLAYER_TYPE = 'Goalie' and PERIOD_S = 'SO' then 1 else 0 end) shootout_saves,
         sum(case when EVENT_TYPE_ID = 'GOAL' and PLAYER_TYPE = 'Goalie' and PERIOD_S = 'SO' then 1 else 0 end) + sum(case when EVENT_TYPE_ID in ('SHOT', 'MISSED_SHOT') and PLAYER_TYPE = 'Goalie' and PERIOD_S = 'SO' then 1 else 0 end) shootout_shots_faced

  from {{ref ('stg_game_events')}} a

  left join {{ref ('stg_game_events_players')}} b 
  on a.GAME_ID = b.GAME_ID and a.EVENT_ID = b.EVENT_ID and a.EVENT_IDX = b.EVENT_IDX
  
  left join {{ref ('stg_team_dim_by_game')}} c 
  on a.GAME_ID = b.GAME_ID and a.EVENT_ID = b.EVENT_ID and a.EVENT_IDX = b.EVENT_IDX

  group by 1,2,3,4,5,6,7,8