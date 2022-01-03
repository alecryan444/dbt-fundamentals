Select 
    JSON_EXTRACT:gameData:game:pk::string||'_'||plays.value:about:eventId::string as play_id,
    JSON_EXTRACT:gameData:game:pk::string as game_id,
    JSON_EXTRACT:gameData:game:season::string as game_season,
    JSON_EXTRACT:gameData:game:type::string as game_type,
    plays.value:about:dateTime::timestamp as datetime,
    plays.value:about:eventId::int as event_id,
    plays.value:about:eventIdx::int as event_idx,
    plays.value:team.id:: int as event_team_id,
    plays.value:coordinates.x::int as x_coor,
    plays.value:coordinates.y::int as y_coor,
    plays.value:result.description::string as description,
    plays.value:result.event::string as event,
    plays.value:result.eventCode::string as event_code,
    plays.value:result.eventTypeId::string as event_type_id,
    plays.value:about.period::int as period,
    plays.value:about.periodType::string as period_type,
    plays.value:about.ordinalNum::string as period_s

from {{source('NHL_DB_RAW', 'RAW_GAME_DATA')}}, table(flatten(JSON_EXTRACT:liveData.plays.allPlays)) plays

{% if is_incremental() %}
where game_id > (select max(game_id) from {{ this }})
{% endif %}