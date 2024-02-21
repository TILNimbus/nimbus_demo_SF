/*--------------------------------------------------------------------------------
  In this chapter you will see how you can:
    - have developers do dev/test work in a full-scale, isolated environment
    - extend Snowflake to run external logic
    - use spatial intelligence to our analytics
--------------------------------------------------------------------------------*/

-- set the context
use role dev_citibike;
alter warehouse dev_wh set warehouse_size = 'medium';
use warehouse dev_wh;
use schema citibike_dev.demo;


-- let's connect the weather data from the marketplace to the trip data
-- create a view that just looks at data for New York...

create or replace view weather_vw as
  select 'New York'                                   state,
    date_valid_std                                    observation_date,
    doy_std                                           day_of_year,
    avg(min_temperature_air_2m_f)                     temp_min_f,
    avg(max_temperature_air_2m_f)                     temp_max_f,
    avg(avg_temperature_air_2m_f)                     temp_avg_f,
    avg(utils.degFtoC(min_temperature_air_2m_f))      temp_min_c,
    avg(utils.degFtoC(max_temperature_air_2m_f))      temp_max_c,
    avg(utils.degFtoC(avg_temperature_air_2m_f))      temp_avg_c,
    avg(tot_precipitation_in)                         tot_precip_in,
    avg(tot_snowfall_in)                              tot_snowfall_in,
    avg(tot_snowdepth_in)                             tot_snowdepth_in,
    avg(avg_wind_direction_100m_deg)                  wind_dir,
    avg(avg_wind_speed_100m_mph)                      wind_speed_mph,
    truncate(avg(avg_wind_speed_100m_mph * 1.61), 1)  wind_speed_kph,
    truncate(avg(tot_precipitation_in * 25.4), 1)     tot_precip_mm,
    truncate(avg(tot_snowfall_in * 25.4), 1)          tot_snowfall_mm,
    truncate(avg(tot_snowdepth_in * 25.4), 1)         tot_snowdepth_mm
  from weather.standard_tile.history_day
  where postal_code in ('10257', '10060', '10128', '07307', '10456')
  group by 1, 2, 3;

select * from weather_vw;


/*--------------------------------------------------------------------------------
  Unfortunately the reference data for stations and regions isn't available on 
  the data marketplace. However we can still get it automatically from the 
  online data source using Snowflake's extensibility features.

  Citibike has reference data sets for stations and regions
    https://gbfs.citibikenyc.com/gbfs/en/station_information.json
    https://gbfs.citibikenyc.com/gbfs/en/system_regions.json

  And we have some NYC neighborhood spatial data
    https://data.cityofnewyork.us/City-Government/Neighborhood-Tabulation-Areas-NTA-/cpf4-rkhq
--------------------------------------------------------------------------------*/

-- We've downloaded these three files and put them in an internal stage
-- this returns a multi-record JSON package which we need to load, unwrap and write
-- into a working table

create or replace table utils.stage_data
  (type varchar, 
   payload variant)
  change_tracking = true;
  
create or replace file format JSON TYPE = JSON;
  
copy into utils.stage_data
   from (
select  'station',$1 from @citibike_v4_reset.reset.weights/station_information.json (file_format => JSON) );
copy into utils.stage_data
   from (
select  'neighborhood',$1 from @citibike_v4_reset.reset.weights/neighborhoods.geojson (file_format => JSON) );
copy into utils.stage_data
   from (
select  'region',$1 from @citibike_v4_reset.reset.weights/system_regions.json (file_format => JSON) ); 

create or replace table utils.spatial_data as
  select type, value v
    from utils.stage_data, lateral flatten (input => payload:data.regions)
    where type = 'region'
  union all
  select type, value v
    from utils.stage_data, lateral flatten (input => payload:data.stations)
    where type = 'station'
  union all
  select type, value v
    from utils.stage_data, lateral flatten (input => payload:features)
    where type = 'neighborhood';

drop table utils.stage_data;
drop file format JSON;


-- check the results
select * from utils.spatial_data;


-- create the station table, accessing the JSON data and using
-- spatial joins to link stations to neighborhoods

create or replace table stations as with 
  -- extract the station data
    s as (select 
        v:station_id::number station_id,
        v:region_id::number region_id,
        v:name::string station_name,
        v:lat::float station_lat,
        v:lon::float station_lon,
        st_point(station_lon, station_lat) station_geo,
        v:station_type::string station_type,
        v:capacity::number station_capacity,
        v:rental_methods rental_methods
    from utils.spatial_data
    where type = 'station'),
    -- extract the region data
    r as (select
        v:region_id::number region_id,
        v:name::string region_name
    from utils.spatial_data
    where type = 'region'),
    -- extract the neighborhood data
    n as (select
        v:properties.neighborhood::string nhood_name,
        v:properties.borough::string borough_name,
        to_geography(v:geometry) nhood_geo
    from utils.spatial_data
    where type = 'neighborhood')   
-- join it all together using a spatial join
select station_id, station_name, station_lat, station_lon, station_geo,
  station_type, station_capacity, rental_methods, region_name,
  nhood_name, borough_name, nhood_geo
from s inner join r on s.region_id = r.region_id
       left outer join n on st_contains(n.nhood_geo, s.station_geo);


-- view the result
select * from stations;


/*--------------------------------------------------------------------------------
  Now we can join it all together - TRIPS + WEATHER + STATIONS  
--------------------------------------------------------------------------------*/

-- join the two tables together to make future queries much easier to write
create or replace view trips_stations_vw as (
  with
    t as (select * from trips_vw),
    ss as (select * from stations),
    es as (select * from stations)
  select tripid, starttime, endtime, duration, start_station_id,
    ss.station_name start_station, ss.region_name start_region,
    ss.borough_name start_borough, ss.nhood_name start_nhood, 
    ss.station_geo start_geo, ss.station_lat start_lat, ss.station_lon start_lon,
    ss.nhood_geo start_nhood_geo, 
    end_station_id, es.station_name end_station, 
    es.region_name end_region, es.borough_name end_borough, 
    es.nhood_name end_nhood, es.station_geo end_geo, 
    es.station_lat end_lat, es.station_lon end_lon,
    es.nhood_geo end_nhood_geo,
    bikeid, bike_type, dob, gender, member_type, payment, payment_type, payment_num
  from t 
    left outer join ss on start_station_id = ss.station_id
    left outer join es on end_station_id = es.station_id);


-- add the weather
create or replace view trips_stations_weather_vw as (
  select t.*, temp_avg_c, temp_avg_f,
         wind_dir, wind_speed_mph, wind_speed_kph
  from trips_stations_vw t 
       left outer join weather_vw w on date_trunc('day', starttime) = observation_date);


-- let's review the integrated data view
select * from trips_stations_vw limit 200;
select * from trips_stations_weather_vw limit 200;


/*--------------------------------------------------------------------------------
  We are done in DEV so now let's copy it all back to PROD
--------------------------------------------------------------------------------*/

use role dba_citibike;

create or replace database citibike clone citibike_dev;

grant usage on database citibike to analyst_citibike;
grant select on all tables in schema citibike.demo to role analyst_citibike;
grant select on all views in schema citibike.demo to role analyst_citibike;
grant select on all materialized views in schema citibike.demo to role analyst_citibike;
grant usage on all functions in database citibike to role analyst_citibike;

drop database citibike_dev;
drop warehouse dev_wh;
