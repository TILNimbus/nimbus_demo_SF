/*--------------------------------------------------------------------------------
  In this chapter you will see how you can:
    - combine structured and semi-structured data to answer business questions
    - use Snowflake's geospatial capabilities
    - use Snowflake's worksheets and dashboards for an analytic user experience
--------------------------------------------------------------------------------*/

-- set the context
use role analyst_citibike;
use warehouse bi_medium_wh;
use schema citibike.demo;


-- so this seems like a really simple query (and it runs very quickly)...
select * from trips_stations_weather_vw sample(1000 rows);


/*--------------------------------------------------------------------------------
  But - think about what just happened for that query...
  30M+ JSON trips records
  ... dynamically cast to a structure suitable for our queries
  ... masked based on user role,
  ... joined with regularly updated weather data from the marketplace
  ... enriched with spatial data for stations and neighborhoods
  ... continuously protected from accidental deletion/corruption

  How long would all this take you today with your current systems?
  Let's run some exploratory queries to test our data model...
--------------------------------------------------------------------------------*/

-- in hourly groups for a specific starting station, how many trips were taken,
-- how long did they last, and how far did they ride?
select date_trunc(hour, starttime) hour,
    count(*) num_trips,
    avg(duration)::integer avg_duration_mins,
    truncate(avg(st_distance(start_geo, end_geo))/1000,1) avg_distance_kms
  from trips_stations_weather_vw
  where start_station = 'Central Park S & 6 Ave'
  group by 1
  order by 2 desc;


-- what are the top 20 most popular cycle routes this year and how long do they take?
select start_station, end_station,
    count(*) num_trips,
    avg(duration)::integer avg_duration_mins,
    truncate(avg(st_distance(start_geo, end_geo))/1000,1) avg_distance_kms
  from trips_stations_weather_vw
  where year(starttime) = 2020
  group by 1, 2
  order by 3 desc;


/*--------------------------------------------------------------------------------
  Both of these queries are non-trivial:
    - temporal and geospatial functions
    - aggregations
    - parsing semi-structured data
    - joining across multiple tables - trips, stations and weather
    
  And they just work... no tuning, no indexing, no distribution keys, ...
  With fast response times on one of our smaller warehouse configurations.
  This is our philosophy... just focus on using the data, not wrestling with it.
--------------------------------------------------------------------------------*/

-- there seems to be a buch of NULL data - maybe from retired stations?
-- let's clean it up

-- how many are there?
select count(*) from trips_stations_weather_vw 
  where start_station is null or end_station is null;

use role dba_citibike;
use warehouse load_wh;
alter warehouse load_wh set warehouse_size = 'xxlarge' wait_for_completion = true;

-- delete them!
delete from trips
  where tripid in (
    select tripid from trips_stations_weather_vw 
      where start_station is null or end_station is null);
