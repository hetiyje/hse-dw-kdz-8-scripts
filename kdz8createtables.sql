CREATE TABLE kdz_8_staging.weather (
	icao_code varchar(10) NOT NULL,
	local_datetime varchar(25) NOT NULL,
	t_air_temperature numeric(3, 1) NOT NULL,
	p0_sea_lvl numeric(4, 1) NOT NULL,
	p_station_lvl numeric(4, 1) NOT NULL,
	u_humidity int4 NOT NULL,
	dd_wind_direction varchar(100) NULL,
	ff_wind_speed int4 NULL,
	ff10_max_gust_value int4 NULL,
	ww_present varchar(100) NULL,
	ww_recent varchar(50) NULL,
	c_total_clouds varchar(200) NOT NULL,
	vv_horizontal_visibility numeric(3, 1) NOT NULL,
	td_temperature_dewpoint numeric(3, 1) NOT NULL,
	loaded_ts timestamp NOT NULL DEFAULT now(),
	PRIMARY KEY (icao_code, local_datetime)
); 


CREATE TABLE kdz_8_staging.flights (
	year int NOT NULL,
	quarter int NULL,
	month int NOT NULL,
	flight_date date NOT NULL,
	dep_time time NULL,
	crs_dep_time time NOT NULL,
	air_time float NULL,
	dep_delay_minutes float NULL,
	cancelled int NOT NULL,
	cancellation_code char(1) NULL,
	weather_delay float NULL,
	reporting_airline varchar(10) NULL,
	tail_number varchar(10) NULL,
	flight_number varchar(15) NOT NULL,
	distance float NULL,
	origin varchar(10) NULL,
	dest varchar(10) NULL,
	loaded_ts timestamp default(now()),
	CONSTRAINT flights_pkey PRIMARY KEY (flight_date, flight_number, origin, dest, crs_dep_time)
);

CREATE TABLE kdz_8_dds.airport_weather (
	airport_dk int NOT NULL, -- постоянный ключ аэропорта. нужно взять из таблицы аэропортов
	weather_type_dk char(6) NOT NULL, -- постоянный ключ типа погоды. заполняется по формуле
	cold smallint default(0),
	rain smallint default(0),
	snow smallint default(0),
	thunderstorm smallint default(0),
	drizzle smallint default(0),
	fog_mist smallint default(0),
	t int NULL,
	max_gws int NULL,
	w_speed int NULL,
	date_start timestamp NOT NULL,
	date_end timestamp NOT NULL default('3000-01-01'::timestamp),
	loaded_ts timestamp default(now()),
	PRIMARY KEY (airport_dk, date_start)
);

CREATE TABLE kdz_8_dds.flights (
	year int NULL,
	quarter int NULL,
	month int NULL,
	flight_scheduled_date date NULL,
	flight_actual_date date NULL,
	flight_dep_scheduled_ts timestamp NOT NULL,
	flight_dep_actual_ts timestamp NULL,
	report_airline varchar(10) NOT NULL,
	tail_number varchar(10) NOT NULL,
	flight_number_reporting_airline varchar(15) NOT NULL,
	airport_origin_dk int NULL, --постоянный ключ аэропорта. нужно взять из таблицы аэропортов
	origin_code varchar(5) null,
	airport_dest_dk int NULL,  --постоянный ключ аэропорта. нужно взять из таблицы аэропортов
	dest_code varchar(5) null,
	dep_delay_minutes float NULL,
	cancelled int NOT NULL,
	cancellation_code char(1) NULL,
	weather_delay float NULL,
	air_time float NULL,
	distance float NULL,
	loaded_ts timestamp default(now()),
	CONSTRAINT lights_pk PRIMARY KEY (flight_dep_scheduled_ts, flight_number_reporting_airline, origin_code, dest_code)
);
