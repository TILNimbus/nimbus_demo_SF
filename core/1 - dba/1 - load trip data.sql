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

/*--------------------------------------------------------------------------------
  We are working with the Citibike trip data
    https://www.citibikenyc.com/system-data
--------------------------------------------------------------------------------*/

-- the trip records are JSON files a blob storage container, partitioned by month
list @utils.trips/json;

-- how much data is that?
select floor(sum($2)/power(1024, 3),1) total_compressed_storage_gb,
    floor(avg($2)/power(1024, 2),1) avg_file_size_mb,
    count(*) as num_files
  from table(result_scan(last_query_id()));

-- what does the data look like?
select $1 from @utils.trips/json/2020/
  (file_format=>utils.json)
  limit 1000;


/*--------------------------------------------------------------------------------
  Load the data from the blob store into a Snowflake table
--------------------------------------------------------------------------------*/

-- create the table to store our trip data, creating a unique trip ID for each record
create or replace table trips
  (tripid number autoincrement, 
   v variant)
  change_tracking = true;

-- initially, we just copy one year of data
copy into trips (v) from 
  (select $1 from @utils.trips/json/2021/)
  file_format=utils.json;

-- check the results
select count(*) from trips;

select * from trips limit 1000;


/*--------------------------------------------------------------------------------
  Now we have the rest of the data to load... 
  At this speed it would take over a minute which would be rather dull to watch.

  Let's elastically scale up the warehouse to get the job done faster.

  How would you make a query run faster in your current environment? Can you?
--------------------------------------------------------------------------------*/

alter warehouse load_wh set warehouse_size = 'xxlarge' wait_for_completion = true;

copy into trips (v) from 
  (select $1 from @utils.trips/json/)
  file_format=utils.json;

-- check the results
select count(*) from trips;


/*--------------------------------------------------------------------------------
  If we wanted to cast it to a structured format...

  Depending on whether you want to use a materialized view or a normal view, you can
  select from the following definitions. MVs are good if you are generating large
  data volumes - views are fine for < 100M rows with M warehouses

  create or replace materialized view trips_vw 
    cluster by (date_trunc('day', starttime))
  OR
  create or replace view trips_vw 
--------------------------------------------------------------------------------*/

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

select * from trips_vw sample(1000 rows);

-- drop the warehouse size back down
alter warehouse load_wh set warehouse_size = 'medium';


/*--------------------------------------------------------------------------------
  Let's allow our analyst, JANE, take a look at this data...
--------------------------------------------------------------------------------*/
