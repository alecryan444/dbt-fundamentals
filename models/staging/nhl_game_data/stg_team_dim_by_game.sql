Select 

    JSON_EXTRACT:gameData:game:pk::string as game_id,
    teams.value:conference.id::integer as conference_id,
    teams.value:conference.name::string as conference_name,
    teams.value:division.id::int as division_id,
    teams.value:division.name::string as division_name,
    teams.value:firstYearOfPlay::int as first_year_of_play,
    teams.value:franchise.franchiseId::string as franchise_id,
    teams.value:franchise.teamName::string as team_name,
    teams.value:locationName::string as team_location,
    teams.value:officialSiteUrl::string as url,
    teams.value:triCode::string as tri_code,
    teams.value:venue.city::string as venue_city,
    teams.value:venue.id::int as  venue_id,
    teams.value:venue.name::string as venue_name,
    teams.value:venue:timeZone.id::string as venue_timezone,
    teams.value:venue:timeZone.offset::int as venue_timezone_offset,
    teams.value:active::boolean as active_status,
    teams.value:id::int as team_id
   
from {{source('NHL_DB_RAW', 'RAW_GAME_DATA')}}, table(flatten(JSON_EXTRACT:gameData:teams)) teams

{% if is_incremental() %}
where game_id > (select max(game_id) from {{ this }})
{% endif %}