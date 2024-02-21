/*--------------------------------------------------------------------------------
  Queries for the dashboard
--------------------------------------------------------------------------------*/

-- start stations
select start_station_id,
    count(*) num_trips
  from demo.trips_vw
  group by 1
  order by 2 desc;


-- total trips
select count(*) num_trips 
from demo.trips_vw;


-- end stations
-- select start_station_id,
select end_station_id,
    count(*) num_trips
  from demo.trips_vw
  group by 1
  order by 2 desc;


-- timeline
select date_trunc('day', starttime) day,
    count(*) num_trips
  from demo.trips_vw
  group by 1;


-- day of week
select dayofweek(starttime) week_day,
    count(*) num_trips
  from demo.trips_vw
  group by 1;


-- hour of day
select hour(starttime) hour_of_day,
    count(*) num_trips
  from demo.trips_vw
  group by 1;
