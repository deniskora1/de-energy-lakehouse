select
    *
from {{ source('gold', 'gold_energy_weather') }}
where renewable_share_pct < 0