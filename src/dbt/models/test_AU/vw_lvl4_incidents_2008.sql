SELECT * FROM {{ ref('vw_lvl2_incidents_2008') }}
union distinct
SELECT * FROM {{ ref('vw_lvl3_incidents_2008') }}
