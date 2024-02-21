/*--------------------------------------------------------------------------------
  Query to set up the years filter
--------------------------------------------------------------------------------*/

-- get list of years
select distinct year(starttime)
from trips_stations_weather_vw
order by 1;


/*--------------------------------------------------------------------------------
  Queries for the dashboard
--------------------------------------------------------------------------------*/

-- start stations
select start_station,
    ifnull(start_borough, 'Not in NY') borough,
    count(*) num_trips
  from demo.trips_stations_weather_vw
  where year(starttime) = :years
  group by 1, 2
  order by 3 desc;


-- total trips
select count(*) num_trips 
  from demo.trips_stations_weather_vw
  where year(starttime) = :years;


-- end stations
select end_station,
    ifnull(end_borough, 'Not in NY') borough,
    count(*) num_trips
  from demo.trips_stations_weather_vw
  where year(starttime) = :years
  group by 1, 2
  order by 3 desc;


-- trips vs temp
select :datebucket(starttime), 
    count(*) / max(count(*)) over () num_trips,
    avg(temp_avg_c) / max(avg(temp_avg_c)) over () avg_temp
  from demo.trips_stations_weather_vw
  where year(starttime) = :years
  group by 1;


-- day of week
select dayofweek(starttime) week_day,
    ifnull(start_borough, 'Not in NY') borough,
    count(*) num_trips
  from demo.trips_stations_weather_vw
  where year(starttime) = :years
  group by 1, 2;


-- hour of day
select hour(starttime) hour_of_day,
    ifnull(start_borough, 'Not in NY') borough,
    count(*) num_trips
  from demo.trips_stations_weather_vw
  where year(starttime) = :years
  group by 1, 2;
