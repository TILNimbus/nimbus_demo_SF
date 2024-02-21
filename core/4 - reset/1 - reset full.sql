/*--------------------------------------------------------------------------------

  The first time you do this, you must first download three reference files into the
  core/4 - reset/weights directory.
  
  Open each of the following locations in a browser, and File -> Save As to the weights 
  directory on your local machine
  
https://gbfs.citibikenyc.com/gbfs/en/system_regions.json (save as json)
https://gbfs.citibikenyc.com/gbfs/en/station_information.json (save as json)
https://snowflake-demo-stuff.s3.amazonaws.com/neighborhoods.geojson (save as geojson)
  
  open a SNOWSQL terminal from the folder that 
  contains the WEIGHT_* files and run the following commands to upload the table 
  data. This is preserved across resets, but if you ever blow away the CITIBIKE
  database or the RESET schema therein, just run this again:

    use role dba_citibike;
    create database if not exists citibike;
    create database if not exists citibike_V4_reset;
    create or replace schema citibike_V4_reset.reset;
    create stage if not exists weights;
    rm @weights;
    put file://?*.csv @weights;
    put file://?*.json @weights;
    put file://?*.geojson @weights;

  Set the size of the demo you want to generate (the number of records) below.

  Then run this whole script.

--------------------------------------------------------------------------------*/

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
