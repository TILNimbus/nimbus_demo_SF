/*--------------------------------------------------------------------------------
  Here you will see how you can use Snowsight to quickly query your data
    - use auto-complete to quickly write queries
    - do post-query aggregations and filters
    - use charts to visualise query results
    - build dashboards over your data
--------------------------------------------------------------------------------*/

use role analyst_citibike;
use schema citibike.demo;
use warehouse bi_medium_wh;


-- how much data do we have?
-- select count(*) from trips;


-- what does it look like?
select * from trips limit 200;
select * from trips_vw sample (1000 rows);


-- what are the most popular cycle routes in 2020 and how long do they take?
select start_station_id, end_station_id,
    count(*) num_trips,
    truncate(avg(datediff('minute', starttime, endtime)),2) avg_duration_mins
  from trips_vw
  where year(starttime) = 2020
  group by 1, 2 order by 3 desc;

-- where are these stations? 
-- it would be useful to translate these to locations

-- trips over time
select date_trunc('day', starttime) start_day,
    count(*) num_trips
  from trips_vw
  group by 1 order by 1;

-- notice the cyclical nature of the data year over year
-- what is causing this? maybe the seasons?


/*--------------------------------------------------------------------------------
  Note that these queries ran over millions of records of semi-structured data
  that we just uploaded. No tuning, no indexes, no distribution keys...

  It just works...
--------------------------------------------------------------------------------*/
