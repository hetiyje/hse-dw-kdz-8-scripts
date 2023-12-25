drop table if exists kdz_8_etl.weather_2;

CREATE TABLE kdz_8_etl.weather_2 AS 
SELECT 
  a.airport_dk,
  CONCAT(
    CASE WHEN kw.t_air_temperature < 0 THEN 1 ELSE 0 END,
    CASE WHEN kw.ww_present ILIKE '%rain%' THEN 1 ELSE 0 END,
    CASE WHEN kw.ww_present ILIKE '%snow%' THEN 1 ELSE 0 END,
    CASE WHEN kw.ww_present ILIKE '%thunderstorm%' THEN 1 ELSE 0 END,
    CASE WHEN kw.ww_present ILIKE '%drizzle%' THEN 1 ELSE 0 END,
    CASE WHEN kw.ww_present ILIKE '%fog%' OR kw.ww_present ILIKE '%mist%' THEN 1 ELSE 0 END
  ) AS weather_type_dk,
  CASE WHEN kw.t_air_temperature < 0 THEN 1 ELSE 0 END AS cold,
  CASE WHEN kw.ww_present ILIKE '%rain%' THEN 1 ELSE 0 END AS rain,
  CASE WHEN kw.ww_present ILIKE '%snow%' THEN 1 ELSE 0 END AS snow,
  CASE WHEN kw.ww_present ILIKE '%thunderstorm%' THEN 1 ELSE 0 END AS thunderstorm,
  CASE WHEN kw.ww_present ILIKE '%drizzle%' THEN 1 ELSE 0 END AS drizzle,
  CASE WHEN kw.ww_present ILIKE '%fog%' OR kw.ww_present LIKE '%mist%' THEN 1 ELSE 0 END AS fog_mist,
  ff10_max_gust_value AS max_gws,
  ff_wind_speed AS w_speed,
  kw.t_air_temperature AS t,
  TO_TIMESTAMP(kw.local_datetime, 'DD.MM.YYYY HH24:MI') AS date_start,
  COALESCE(
    lead(TO_TIMESTAMP(kw.local_datetime, 'DD.MM.YYYY HH24:MI')) 
      over(partition by a.airport_dk order by TO_TIMESTAMP(kw.local_datetime, 'DD.MM.YYYY HH24:MI')),
    '3000-01-01 00:00:00'::timestamp
  ) AS date_end
FROM 
  kdz_8_staging.weather kw 
  JOIN dds.airport a ON kw.icao_code = a.icao_code
 order by date_start;

insert into kdz_8_dds.airport_weather (
	airport_dk,
	weather_type_dk,
	cold,
	rain,
	snow,
	thunderstorm,
	drizzle,
	fog_mist,
	max_gws,
	w_speed,
	t,
	date_start,
	date_end
)
select 
	airport_dk,
	weather_type_dk,
	cold,
	rain,
	snow,
	thunderstorm,
	drizzle,
	fog_mist,
	max_gws,
	w_speed,
	t,
	date_start,
	date_end
from kdz_8_etl.weather_2 
on conflict(airport_dk, date_start) do update
set 
	weather_type_dk = excluded.weather_type_dk,
	cold = excluded.cold,
	rain = excluded.rain,
	snow = excluded.snow,
	thunderstorm = excluded.thunderstorm,
	drizzle = excluded.drizzle,
	fog_mist = excluded.fog_mist,
	max_gws = excluded.max_gws,
	w_speed = excluded.w_speed,
	t = excluded.t,
	date_end = excluded.date_end,
	loaded_ts = now()
;