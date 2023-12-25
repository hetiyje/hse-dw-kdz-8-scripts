drop table if exists kdz_8_etl.flights_1;

create table kdz_8_etl.flights_1 as 
select distinct 
	year,
	quarter,
	month,
	to_date(flight_date, 'MM/DD/YYYY') as flight_date,
	CASE 
        WHEN dep_time = '2400' THEN '00:00'::TIME
        ELSE (TO_TIMESTAMP(dep_time, 'HH24MI'))::TIME 
    END AS dep_time,
	CASE 
        WHEN crs_dep_time = '2400' THEN '00:00'::TIME
        ELSE (TO_TIMESTAMP(crs_dep_time, 'HH24MI'))::TIME 
    END AS crs_dep_time,
	air_time,
	dep_delay_minutes,
	cancelled,
	cancellation_code,
	weather_delay,
	reporting_airline,
	tail_number,
	flight_number,
	distance,
	origin,
	dest
from kdz_8_src.flights kf;





insert into kdz_8_staging.flights(
	year,
	quarter,
	month,
	flight_date,
	dep_time,
    crs_dep_time,
	air_time,
	dep_delay_minutes,
	cancelled,
	cancellation_code,
	weather_delay,
	reporting_airline,
	tail_number,
	flight_number,
	distance,
	origin,
	dest)
select 
	year,
	quarter,
	month,
	flight_date,
	dep_time,
    crs_dep_time,
	air_time,
	dep_delay_minutes,
	cancelled,
	cancellation_code,
	weather_delay,
	reporting_airline,
	tail_number,
	flight_number,
	distance,
	origin,
	dest
from kdz_8_etl.flights_1 
on conflict(flight_date, crs_dep_time, flight_number, origin, dest) do update
set 
	year = excluded.year,
	quarter = excluded.quarter,
	month = excluded.month,
	dep_time = excluded.dep_time,
	air_time = excluded.air_time,
	dep_delay_minutes = excluded.dep_delay_minutes,
	cancelled = excluded.cancelled,
	cancellation_code = excluded.cancellation_code,
	weather_delay = excluded.weather_delay,
	reporting_airline = excluded.reporting_airline,
	tail_number = excluded.tail_number,
	distance = excluded.distance,
	loaded_ts = now()
;


drop table if exists kdz_8_etl.weather_1;

create table kdz_8_etl.weather_1 as 
select distinct 
  icao_code,
  local_datetime,
  t_air_temperature,
  p0_sea_lvl,
  p_station_lvl,
  u_humidity,
  dd_wind_direction,
  ff_wind_speed,
  ff10_max_gust_value,
  ww_present,
  ww_recent,
  c_total_clouds,
  vv_horizontal_visibility,
  td_temperature_dewpoint
from kdz_8_src.weather
WHERE t_air_temperature IS NOT NULL;

insert into kdz_8_staging.weather(
	icao_code,
  	local_datetime,
  	t_air_temperature,
  	p0_sea_lvl,
  	p_station_lvl,
  	u_humidity,
  	dd_wind_direction,
  	ff_wind_speed,
  	ff10_max_gust_value,
  	ww_present,
  	ww_recent,
  	c_total_clouds,
  	vv_horizontal_visibility,
  	td_temperature_dewpoint
	)
select 
	icao_code,
  	local_datetime,
  	t_air_temperature,
  	p0_sea_lvl,
  	p_station_lvl,
  	u_humidity,
  	dd_wind_direction,
  	ff_wind_speed,
  	ff10_max_gust_value,
  	ww_present,
  	ww_recent,
  	c_total_clouds,
  	vv_horizontal_visibility,
  	td_temperature_dewpoint
from kdz_8_etl.weather_1 
on conflict(icao_code, local_datetime) do update
set 
	t_air_temperature = excluded.t_air_temperature,
	p0_sea_lvl = excluded.p0_sea_lvl,
  	p_station_lvl = excluded.p_station_lvl,
  	u_humidity = excluded.u_humidity,
  	dd_wind_direction = excluded.dd_wind_direction,
  	ff_wind_speed = excluded.ff_wind_speed,
  	ff10_max_gust_value = excluded.ff10_max_gust_value,
  	ww_present = excluded.ww_present,
  	ww_recent = excluded.ww_recent,
  	c_total_clouds = excluded.c_total_clouds,
  	vv_horizontal_visibility = excluded.vv_horizontal_visibility,
  	td_temperature_dewpoint = excluded.td_temperature_dewpoint,
	loaded_ts = now()
;