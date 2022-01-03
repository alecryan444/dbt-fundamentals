Select 
    JSON_EXTRACT:gameData:game:pk::string as game_id,
    JSON_EXTRACT:gameData:game:season::string as game_season,
    trim(split_part(officials.value:official.fullName::string,' ',1))  as official_first_name,
    trim(split_part(officials.value:official.fullName::string,' ',2))  as official_last_name,
    officials.value:official.id::int as official_id,
    officials.value:officialType::string as offical_type
    
from {{source('NHL_DB_RAW', 'RAW_GAME_DATA')}}, table(flatten(JSON_EXTRACT:liveData:boxscore:officials)) officials

{% if is_incremental() %}
where game_id > (select max(game_id) from {{ this }})
{% endif %}