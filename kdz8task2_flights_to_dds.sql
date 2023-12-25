drop table if exists kdz_8_etl.flights_2;

CREATE TABLE kdz_8_etl.flights_2 AS 
SELECT distinct
    f.year,
    f.quarter,
    f.month,
    f.flight_date AS flight_scheduled_date,
    DATE_TRUNC('day', CASE 
        WHEN f.cancelled = 1 THEN NULL
        ELSE TO_TIMESTAMP(concat(f.flight_date, ' ', f.crs_dep_time), 'YYYY-MM-DD HH24:MI:SS') + (f.dep_delay_minutes * INTERVAL '1 minute')
    END) AS flight_actual_date,
    TO_TIMESTAMP(concat(f.flight_date, ' ', f.crs_dep_time), 'YYYY-MM-DD HH24:MI:SS') AS flight_dep_scheduled_ts,
    CASE 
        WHEN f.cancelled = 1 THEN NULL
        ELSE TO_TIMESTAMP(concat(f.flight_date, ' ', f.crs_dep_time), 'YYYY-MM-DD HH24:MI:SS') + (f.dep_delay_minutes * INTERVAL '1 minute')
    END AS flight_dep_actual_ts,
    f.reporting_airline AS report_airline,
    f.tail_number,
    f.flight_number AS flight_number_reporting_airline,
    a.airport_dk AS airport_origin_dk, 
    f.origin AS origin_code,
    a2.airport_dk AS airport_dest_dk, 
    f.dest AS dest_code,
    f.dep_delay_minutes,
    f.cancelled,
    f.cancellation_code,
    f.weather_delay,
    f.air_time,
    f.distance
FROM 
    kdz_8_staging.flights f JOIN dds.airport a ON f.origin = a.iata_code JOIN dds.airport a2 ON f.dest = a2.iata_code
where f.tail_number is not null;
   
   
   
   
   
insert into kdz_8_dds.flights(
 year,
    quarter,
    month,
    flight_scheduled_date,
    flight_actual_date,
    flight_dep_scheduled_ts,
    flight_dep_actual_ts,
    report_airline,
    tail_number,
    flight_number_reporting_airline,
    airport_origin_dk, 
    origin_code,
    airport_dest_dk, 
    dest_code,
    dep_delay_minutes,
    cancelled,
    cancellation_code,
    weather_delay,
    air_time,
    distance
) select * from kdz_8_etl.flights_2 
on conflict (flight_dep_scheduled_ts, flight_number_reporting_airline, origin_code, dest_code) do update set 
 year = excluded.year,
    quarter = excluded.quarter,
    month = excluded.month,
 flight_scheduled_date = excluded.flight_scheduled_date,
    flight_actual_date = excluded.flight_actual_date,
    flight_dep_actual_ts = excluded.flight_dep_actual_ts,
    report_airline = excluded.report_airline,
    tail_number = excluded.tail_number,
    airport_origin_dk = excluded.airport_origin_dk, 
    airport_dest_dk = excluded.airport_dest_dk, 
    dep_delay_minutes = excluded.dep_delay_minutes,
    cancelled = excluded.cancelled,
    cancellation_code = excluded.cancellation_code,
    weather_delay = excluded.weather_delay,
    air_time = excluded.air_time,
    distance = excluded.distance,
    loaded_ts = now();