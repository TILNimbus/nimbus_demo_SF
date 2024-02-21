/*--------------------------------------------------------------------------------

  This is the fast reset script - it is much faster than the full reset but is
  less complete. If you have any problems after running this script, run a
  full reset to make sure everything is correct.

--------------------------------------------------------------------------------*/

use role accountadmin;
use database citibike;

-- drop all the objects - clean slate
drop integration if exists fetch_http_data;
drop warehouse if exists load_wh;
drop warehouse if exists dev_wh;
drop database if exists citibike_dev;
drop database if exists weather;
drop schema citibike.demo;

-- create the initial demo structure
use role dba_citibike;
create schema citibike.demo;

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

