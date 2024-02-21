-- how big do you want the demo to be? 50M makes a nice, fast demo
-- 150M creates an equivalent size to the real citibike demo
set num_trips_mill = 50;


/*--------------------------------------------------------------------------------
  Recreate the demo database in a clean state.
--------------------------------------------------------------------------------*/

use role accountadmin;
use database citibike;

-- drop all the objects - clean slate
drop integration if exists fetch_http_data;
drop warehouse if exists reset_wh;
drop warehouse if exists bi_medium_wh;
drop warehouse if exists bi_large_wh;
drop warehouse if exists task_wh;
drop warehouse if exists load_wh;
drop warehouse if exists dev_wh;
drop database if exists citibike;
drop database if exists citibike_dev;
drop database if exists weather;

-- create the initial demo structure
use role dba_citibike;

create warehouse reset_wh warehouse_size = 'xxlarge';
create database citibike;
create schema citibike.demo;
create schema citibike.utils;
create schema citibike.security;
drop schema if exists citibike.public;

use schema citibike.utils;


/*--------------------------------------------------------------------------------
  Stored proc to run a query multiple times (for benchmark tests)
--------------------------------------------------------------------------------*/

create or replace procedure RunQueryNTimes (QRY STRING, N FLOAT)
  returns float
  language javascript
  execute as caller
as
$$
  duration = 0;
  for (i = 0; i < N; i++) {
    startTime = Date.now();
    snowflake.execute({sqlText: QRY});
    endTime = Date.now()
    duration += endTime-startTime;
  }

  // Return the average time as secs, rounded to 1 dec place
  return Math.round(duration/N/100)/10;
$$;


/*--------------------------------------------------------------------------------
  Create the UDFs used throughout the demo.
--------------------------------------------------------------------------------*/

-- UDF to convert Kelvin to Celcius
create or replace function degKtoC(k float)
returns float
as
$$
  k - 273.15
$$;


-- UDF to convert Kelvin to Farenheit
create or replace function degKtoF(k float)
returns float
as
$$
  truncate((k - 273.15) * 9/5 + 32 , 2)
$$;


-- UDF to convert Farenheit to Celcius
create or replace function degFtoC(k float)
returns float
as
$$
  truncate((k - 32) * 5/9, 2)
$$;


-- create the API integration
create or replace api integration fetch_http_data
  api_provider = aws_api_gateway
  api_aws_role_arn = 'arn:aws:iam::148887191972:role/ExecuteLambdaFunction'
  enabled = true
  api_allowed_prefixes = ('https://dr14z5kz5d.execute-api.us-east-1.amazonaws.com/prod/fetchhttpdata');


-- create an external function to call a Lambda that downloads data from a URL
create or replace external function fetch_http_data(v varchar)
    returns variant
    api_integration = fetch_http_data
    as 'https://dr14z5kz5d.execute-api.us-east-1.amazonaws.com/prod/fetchhttpdata';


/*--------------------------------------------------------------------------------
  Create the template tables that describe the shape of the data
--------------------------------------------------------------------------------*/

create or replace table weight_birthyear (birth_year integer, weight float, cum_weight float);
create or replace table weight_gender (gender integer, weight float, cum_weight float);
create or replace table weight_payment (payment_type string, weight float, cum_weight float);
create or replace table weight_membership (member_type string, weight float, cum_weight float);
create or replace table weight_route (start_station_id integer, end_station_id integer, duration number , weight float, cum_weight float);
create or replace table weight_wk (wk timestamp_ntz, weight float, cum_weight float);
create or replace table weight_dow (dow integer, weight float, cum_weight float);
create or replace table weight_hod (dow integer, hod integer, weight float, cum_weight float);
create or replace table riders (riderid integer, first_name string, last_name string, email string, gender string, phone_type string, phone_num string, cc_type string, cc_num string, member_type string, dob date);

ls @citibike_V4_reset.reset.weights;

create or replace file format weights_csv field_delimiter='\t' skip_header=1;

copy into weight_birthyear from @citibike_V4_reset.reset.weights/weight_birthyear.csv.gz file_format=weights_csv;
copy into weight_gender from @citibike_V4_reset.reset.weights/weight_gender.csv.gz file_format=weights_csv;
copy into weight_payment from @citibike_V4_reset.reset.weights/weight_payment.csv.gz file_format=weights_csv;
copy into weight_membership from @citibike_V4_reset.reset.weights/weight_membership.csv.gz file_format=weights_csv;
copy into weight_route from @citibike_V4_reset.reset.weights/weight_route.csv.gz file_format=weights_csv;
copy into weight_wk from @citibike_V4_reset.reset.weights/weight_wk.csv.gz file_format=weights_csv;
copy into weight_dow from @citibike_V4_reset.reset.weights/weight_dow.csv.gz file_format=weights_csv;
copy into weight_hod from @citibike_V4_reset.reset.weights/weight_hod.csv.gz file_format=weights_csv;
copy into riders from @citibike_V4_reset.reset.weights/riders.csv.gz file_format=weights_csv;


/*--------------------------------------------------------------------------------
  Create the trip records - use a table generator function to generate the 
  nominated number of records with the shape defined by the weights tables above
--------------------------------------------------------------------------------*/

alter warehouse reset_wh set warehouse_size = 'xxlarge' wait_for_completion = true;

-- build the TRIPS table
create or replace transient table trips_full as (
with trips_rand as (
    -- generate a bunch of records with random dimension values
    select 
        (abs(mod(random(),1000000000))+1)/1000000 wk_r,
        (abs(mod(random(),1000000000))+1)/1000000 dow_r,
        (abs(mod(random(),1000000000))+1)/1000000 hod_r,
        (abs(mod(random(),1000000000))+1)/1000000 route_r,
        (abs(mod(random(),1000000000))+1)/1000000 payment_r,
        abs(mod(random(),100000))+1 rider_r,
        abs(mod(random(),1000))+1 bikeid,
        abs(mod(random(),60)) mins,
        abs(mod(random(),20))-10 duration_r
    from table(generator(rowcount=>$num_trips_mill * 1000000))
    )
select  
    -- map the random values to dimension values from the weights tables
    dateadd('minute', mins,
        dateadd('hour', thod.hod,
            dateadd('day', tdow.dow,twk.wk)
                )) starttime,

    iff((tr.duration + duration_r) <= 0, dateadd('minutes', tr.duration, starttime), 
        dateadd('minutes', tr.duration + duration_r, starttime)) endtime,
    
    tr.start_station_id, tr.end_station_id,
    to_varchar(year(starttime)) || '-' || to_varchar(t.bikeid) bikeid,
    
    iff(year(starttime)>2019, iff(t.bikeid <= 200, 'ebike', 'classic'),'classic') bike_type,
    
    tp.payment_type,
    r.*
from trips_rand t
    inner join weight_wk twk on (wk_r between twk.cum_weight-twk.weight and twk.cum_weight)
    inner join weight_dow tdow on (dow_r between tdow.cum_weight-tdow.weight and tdow.cum_weight)
    inner join weight_hod thod on (hod_r between thod.cum_weight-thod.weight and thod.cum_weight and tdow.dow = thod.dow)
    inner join weight_route tr on (route_r between tr.cum_weight-tr.weight and tr.cum_weight)
    inner join weight_payment tp on (payment_r between tp.cum_weight-tp.weight and tp.cum_weight)
    inner join riders r on (rider_r = r.riderid)
 );
 

select * from trips_full limit 1000;

-- create a view that trims the future records
create or replace view trips as
select * from trips_full where starttime <= date_trunc('week', current_date());


/*--------------------------------------------------------------------------------
  Create the demo load stages for TRIPS. This is an internal stage for each 
  demo account.
--------------------------------------------------------------------------------*/

-- create a stage to unload files
create or replace file format citibike.utils.json type = 'json';
create or replace stage trips;

alter session set date_output_format = 'YYYY/MM/DD';

-- unload TRIPS as JSON
copy into @trips/json
  from (select object_construct(
    'STARTTIME', starttime, 
    'ENDTIME', endtime,
    'START_STATION_ID', start_station_id, 
    'END_STATION_ID', end_station_id,
    'BIKE', object_construct('BIKEID', bikeid, 'BIKE_TYPE', bike_type), 
    'RIDER', object_construct('RIDERID', riderid, 'FIRST_NAME', first_name, 'LAST_NAME', last_name, 
      'EMAIL', email, 'GENDER', gender, 'MEMBER_TYPE', member_type, 'DOB', dob,
      'PAYMENT', object_construct('TYPE', iff(left(payment_type, 1)='a', 'phone', 'ccard'),
         iff(left(payment_type, 1)='a', 'PHONE_TYPE', 'CC_TYPE'), iff(left(payment_type, 1)='a', phone_type, cc_type),
         iff(left(payment_type, 1)='a', 'PHONE_NUM', 'CC_NUM'), iff(left(payment_type, 1)='a', phone_num, cc_num)
         ))
  ) v from citibike.utils.trips)
  partition by to_varchar(date_trunc('month', v:STARTTIME::timestamp_ntz)::date)
  file_format=json max_file_size=5000000;

alter warehouse reset_wh set warehouse_size = 'small' wait_for_completion = true;


/*--------------------------------------------------------------------------------
  Set up the Tableau query performance view
--------------------------------------------------------------------------------*/

create or replace view tableau_query_history as
  select *
  from table(citibike.information_schema.query_history_by_user('JANE', dateadd('minutes',-60,current_timestamp()),current_timestamp(), 500))
  where query_tag='QueryFromTableau';


/*--------------------------------------------------------------------------------
  Create warehouses for Analyst use
--------------------------------------------------------------------------------*/

create or replace warehouse bi_medium_wh
  with warehouse_size = 'medium'
  auto_suspend = 300
  auto_resume = true
  min_cluster_count = 1
  max_cluster_count = 5
  initially_suspended = true;

create or replace warehouse bi_large_wh
  with warehouse_size = 'large'
  auto_suspend = 300
  auto_resume = true
  min_cluster_count = 1
  max_cluster_count = 5
  initially_suspended = true;

create or replace warehouse task_wh
  with warehouse_size = 'xsmall'
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true;

grant all on warehouse bi_medium_wh to role analyst_citibike;
grant all on warehouse bi_large_wh to role analyst_citibike;
grant all on warehouse task_wh to role analyst_citibike;


/*--------------------------------------------------------------------------------
  Grant privileges
--------------------------------------------------------------------------------*/

grant usage on database citibike to role analyst_citibike;

grant usage on schema citibike.demo to role analyst_citibike;
grant usage on schema citibike.utils to role analyst_citibike;
grant usage on future schemas in database citibike to role analyst_citibike;

grant create table on schema citibike.demo to role analyst_citibike;
grant usage on all functions in database citibike to role analyst_citibike;

grant select on all tables in schema citibike.demo to role analyst_citibike;
grant select on future tables in database citibike to role analyst_citibike;
grant select on all views in database citibike to role analyst_citibike;
grant select on future views in database citibike to role analyst_citibike;
grant select on all materialized views in database citibike to role analyst_citibike;
grant select on future materialized views in database citibike to role analyst_citibike;


/*--------------------------------------------------------------------------------
  Clean up
--------------------------------------------------------------------------------*/

use schema citibike.utils;

drop table weight_birthyear;
drop table weight_gender;
drop table weight_payment;
drop table weight_membership;
drop table weight_route;
drop table weight_wk;
drop table weight_dow;
drop table weight_hod;
drop table riders;
drop table trips_full;
drop view trips;

drop warehouse reset_wh;


/*--------------------------------------------------------------------------------
--------------------------------------------------------------------------------*/

/*--------------------------------------------------------------------------------
  In this chapter you will see how you can:
    - easily load your data into Snowflake, no matter the format
    - meet changing workloads by scaling up/down without interruption
    - "load and go" querying data without needing to tune for performance
    - leverage your existing ecosystem of skills and tools for ETL, BI, etc.
--------------------------------------------------------------------------------*/

-- set the context
use role dba_citibike;
create warehouse if not exists load_wh warehouse_size = 'medium' auto_suspend=120;
use warehouse load_wh;
use schema citibike.demo;

-- create the table to store our trip data, creating a unique trip ID for each record
create or replace table trips
  (tripid number autoincrement, 
   v variant)
  change_tracking = true;

alter warehouse load_wh set warehouse_size = 'xxlarge' wait_for_completion = true;

copy into trips (v) from 
  (select $1 from @utils.trips/json/)
  file_format=utils.json;

create or replace view trips_vw 
  as select 
    tripid,
    v:STARTTIME::timestamp_ntz starttime,
    v:ENDTIME::timestamp_ntz endtime,
    datediff('minute', starttime, endtime) duration,
    v:START_STATION_ID::integer start_station_id,
    v:END_STATION_ID::integer end_station_id,
    v:BIKE.BIKEID::string bikeid,
    v:BIKE.BIKE_TYPE::string bike_type,
    v:RIDER.RIDERID::integer riderid,
    v:RIDER.FIRST_NAME::string || ' ' || v:RIDER.LAST_NAME::string rider_name,
    to_date(v:RIDER.DOB::string, 'YYYY/MM/DD') dob,
    v:RIDER.GENDER::string gender,
    v:RIDER.MEMBER_TYPE::string member_type,
    v:RIDER.PAYMENT.TYPE::string payment,
    ifnull(v:RIDER.PAYMENT.CC_TYPE::string, 
      v:RIDER.PAYMENT.PHONE_TYPE::string) payment_type,
    ifnull(v:RIDER.PAYMENT.PHONE_NUM::string,
      v:RIDER.PAYMENT.CC_NUM::string) payment_num
  from trips;

alter warehouse load_wh set warehouse_size = 'medium';


/*--------------------------------------------------------------------------------
--------------------------------------------------------------------------------*/

/*--------------------------------------------------------------------------------
  In this chapter you will see how you can:
  - browse the Data Marketplace and Data Exchanges to view published data sets
  - subscribe to data sets curated by 3rd party data providers
  - use this data as if it were loaded into your account
--------------------------------------------------------------------------------*/

-- set the context
use role dba_citibike;
alter warehouse load_wh set warehouse_size = 'medium';
use warehouse load_wh;
use schema citibike.demo;


-- You can also do it via code if you know the account/share details...
set weather_acct_name = '*** put account name here as part of demo setup ***';
set weather_share_name = '*** put account share here as part of demo setup ***';
set weather_share = $weather_acct_name || '.' || $weather_share_name;

create or replace database weather 
  from share identifier($weather_share);

grant imported privileges on database weather to role public;


/*--------------------------------------------------------------------------------
--------------------------------------------------------------------------------*/

/*--------------------------------------------------------------------------------
  In this chapter you will see how you can:
  - dynamically provision, deprovision and refresh multiple environments
  - secure PII data in a development cycle while still allowing full scale testing
--------------------------------------------------------------------------------*/

-- set the context
use role dba_citibike;
alter warehouse load_wh set warehouse_size = 'medium';
use warehouse load_wh;
use schema citibike.demo;


-- create masking policies to protect our PII data   
create or replace masking policy security.mask_string_simple as
  (val string) returns string ->
  case
    when current_role() in ('DBA_CITIBIKE', 'ANALYST_CITIBIKE') then val
      else '**masked**'
    end;
    
create or replace masking policy security.mask_int_simple as
  (val integer) returns integer ->
  case
    when current_role() in ('DBA_CITIBIKE', 'ANALYST_CITIBIKE') then val
      else -999999
    end;

create or replace masking policy security.mask_date_round_month as
  (val date) returns date ->
  case
    when current_role() in ('DBA_CITIBIKE', 'ANALYST_CITIBIKE') then val
      else date_trunc('month', val)
    end;

-- apply the masking policy to TRIPS_VW view
alter view trips_vw modify
  column riderid set masking policy security.mask_int_simple,
  column rider_name set masking policy security.mask_string_simple,
  column payment_type set masking policy security.mask_string_simple,
  column payment_num set masking policy security.mask_string_simple,
  column dob set masking policy security.mask_date_round_month,
  column gender set masking policy security.mask_string_simple;


/*--------------------------------------------------------------------------------
--------------------------------------------------------------------------------*/

/*--------------------------------------------------------------------------------
  In this chapter you will see how you can:
    - have developers do dev/test work in a full-scale, isolated environment
    - extend Snowflake to run external logic
    - use spatial intelligence to our analytics
--------------------------------------------------------------------------------*/

-- set the context
use role dba_citibike;
alter warehouse load_wh set warehouse_size = 'medium';
use warehouse load_wh;
use schema citibike.demo;


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
    from utils.stage_data, lateral flatten (input => payload:response.data.regions)
    where type = 'region'
  union all
  select type, value v
    from utils.stage_data, lateral flatten (input => payload:response.data.stations)
    where type = 'station'
  union all
  select type, value v
    from utils.stage_data, lateral flatten (input => payload:response.features)
    where type = 'neighborhood';

drop table utils.stage_data;
drop file format JSON;

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


create or replace view trips_stations_weather_vw as (
  select t.*, temp_avg_c, temp_avg_f,
         wind_dir, wind_speed_mph, wind_speed_kph
  from trips_stations_vw t 
       left outer join weather_vw w on date_trunc('day', starttime) = observation_date);


/*--------------------------------------------------------------------------------
--------------------------------------------------------------------------------*/

-- set the context
use role analyst_citibike;
use warehouse bi_medium_wh;
use schema citibike.demo;

-- what are the top 20 most popular cycle routes this year and how long do they take?
select start_station, end_station,
    count(*) num_trips,
    avg(duration)::integer avg_duration_mins,
    truncate(avg(st_distance(start_geo, end_geo))/1000,1) avg_distance_kms
  from trips_stations_weather_vw
  where year(starttime) = 2020
  group by 1, 2
  order by 3 desc;
