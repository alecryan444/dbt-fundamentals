Select  
      game_id,
      game_season,
      home_team_id,
      away_team_id,
      event_id,
      event_idx,
      players.value:player:id::string as player_id,
      players.value:playerType::string as player_type
  from (
    Select 
      JSON_EXTRACT:gameData:game:pk::string as game_id,
      JSON_EXTRACT:gameData:game:season::string as game_season,
      plays.value:about:eventId::int as event_id,
      plays.value:about:eventIdx::int as event_idx,
      plays.value:players as players,
      JSON_EXTRACT:gameData:teams:away.id as away_team_id,
      JSON_EXTRACT:gameData:teams:home.id as home_team_id

    from {{source('NHL_DB_RAW', 'RAW_GAME_DATA')}}, table(flatten(JSON_EXTRACT:liveData.plays.allPlays)) plays

    {% if is_incremental() %}
    where game_id > (select max(game_id) from {{ this }})
    {% endif %}

  ), table(flatten(players)) players