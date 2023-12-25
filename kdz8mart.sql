drop table if exists kdz_8_etl.mart;

CREATE TABLE kdz_8_etl.mart as
select distinct 
	f.airport_origin_dk, 
	f.airport_dest_dk as airport_destination_dk, 
	wt.weather_type_dk, 
	f.flight_dep_scheduled_ts as flight_scheduled_ts, 
	f.flight_dep_actual_ts as flight_actual_time,
	f.flight_number_reporting_airline as flight_number,
	f.distance,
	f.tail_number,
	f.report_airline as airline,
	f.dep_delay_minutes as dep_delay_min,
	f.cancelled,
	f.cancellation_code,
	aw.t,
	aw.max_gws,
	aw.w_speed,
	f.air_time,
	'8' as author,
	now() as loaded_ts
from kdz_8_dds.flights f 
inner join kdz_8_dds.airport_weather aw on f.flight_dep_scheduled_ts between aw.date_start and aw.date_end 
join dds.weather_type wt on wt.weather_type_dk = aw.weather_type_dk
where airport_origin_dk = 1464;

WITH ranked_duplicates AS (
    SELECT 
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY airport_origin_dk, airport_destination_dk, flight_scheduled_ts, flight_number
            ORDER BY (SELECT NULL)
        ) as row_num
    FROM kdz_8_etl.mart
)
DELETE FROM kdz_8_etl.mart
WHERE ctid IN (
    SELECT ctid
    FROM ranked_duplicates
    WHERE row_num = 2
);



insert into mart.fact_departure 
select distinct 
	airport_origin_dk,
	airport_destination_dk,
	weather_type_dk,
	flight_scheduled_ts,
	flight_actual_time,
	flight_number,
	distance,
	tail_number,
	airline,
	dep_delay_min,
	cancelled,
	cancellation_code,
	t,
	max_gws,
	w_speed,
	air_time,
	author,
	now() as loaded_ts
from kdz_8_etl.mart 
on conflict (airport_origin_dk, airport_destination_dk, flight_scheduled_ts, flight_number) do update
	set
		weather_type_dk = excluded.weather_type_dk,
		flight_actual_time  = excluded.flight_actual_time,
		distance = excluded.distance,
		tail_number = excluded.tail_number,
		airline = excluded.airline,
		dep_delay_min = excluded.dep_delay_min,
		cancelled = excluded.cancelled,
		cancellation_code = excluded.cancellation_code,
		t = excluded.t,
		max_gws = excluded.max_gws,
		w_speed = excluded.w_speed,
		air_time = excluded.air_time,
		author = '8',
		loaded_ts = now()
;
