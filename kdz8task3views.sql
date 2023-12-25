CREATE or replace VIEW mart.kdz_8_airline_delays_and_cancelations AS
SELECT
    airline,
    COUNT(*) AS total_flights,
    AVG(dep_delay_min) AS avg_delay,
    SUM(cancelled) AS total_cancelled,
    (SUM(cancelled) * 100.0 / COUNT(*)) AS cancellation_percentage
FROM
    mart.fact_departure
GROUP BY
    airline;
    
create or replace VIEW mart.kdz_8_weather_and_cancelations as
SELECT
    wt.cold,
    wt.rain,
    wt.snow,
    wt.thunderstorm,
    wt.drizzle,
    wt.fog_mist,
    COUNT(*) AS cancellation_count
FROM
    mart.fact_departure fd
JOIN
    dds.weather_type wt ON fd.weather_type_dk = wt.weather_type_dk
WHERE
    fd.cancelled = 1
GROUP BY
    fd.weather_type_dk, wt.cold, wt.rain, wt.snow, wt.thunderstorm, wt.drizzle, wt.fog_mist;
    
   
CREATE or replace view mart.kdz_8_airports_by_arrivals as
select 
    a.air_name,
    a.icao_code,
    COUNT(*) AS arrival_count
FROM
    mart.fact_departure fd
JOIN
    dds.airport a ON fd.airport_destination_dk = a.airport_dk
GROUP BY
    fd.airport_destination_dk, a.air_name, a.icao_code
ORDER BY
    arrival_count desc
limit 10;

   
CREATE or replace view mart.kdz_8_airlines_by_flights AS
SELECT
    airline,
    COUNT(*) AS flight_count
FROM
    mart.fact_departure
GROUP BY
    airline
ORDER BY
    flight_count DESC;
   