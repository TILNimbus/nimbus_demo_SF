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


/*--------------------------------------------------------------------------------
  In the last script, we loaded our TRIP data - this was quick and easy as (for
  this demo) the data is static and we don't need to maintain it.
  
  But what about data that needs constant updating - like the WEATHER data that
  JANE now wants us to load? We would need to build a pipeline process to 
  constantly update that data to keep it fresh.

  Perhaps a better way to get this external data would be to source it from a 
  trusted data supplier. Let them manage the data, keeping it accurate and up
  to date.

  Enter the Snowflake Data Cloud...

  Let's connect to the "Global Weather & Climate Data for BI data" feed from Weather Source 
  in the Snowflake Data Marketplace and map it to a database called WEATHER.
  
  We can then use this data as if it had been loaded directly into our account
--------------------------------------------------------------------------------*/


-- You can also do it via code if you know the account/share details...
set weather_acct_name = '*** put account name here as part of demo setup ***';
set weather_share_name = '*** put account share here as part of demo setup ***';
set weather_share = $weather_acct_name || '.' || $weather_share_name;

create or replace database weather 
  from share identifier($weather_share);


-- grant permissions on the database so all users can access it
grant imported privileges on database weather to role public;


-- let's look at the data - same 3-part naming convention as any other table
select count(*) from weather.standard_tile.history_day;

select * from weather.standard_tile.history_day limit 100;

-- data for multiple countries, for the past 2 years
select country, min(date_valid_std) from_date, max(date_valid_std) to_date
  from weather.standard_tile.history_day
  group by 1;


/*--------------------------------------------------------------------------------
  That's it... we don't have to do anything from here to keep this data updated.
  The provider will do that for us and data sharing means we are always seeing
  whatever they they have published.
  
  FYI - while this view of data does not seem very big (~750K records) it is
  actually a curated view over ~9TB of observation data managed by the provider,
  The Weather Source.
--------------------------------------------------------------------------------*/

