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


/*--------------------------------------------------------------------------------
  Our analysts need more data so we will engage our DEV team to enrich the
  trips data with information on stations and weather.
  
  But JOHN is a good DBA and his response is...
  People are actually using this data now. NO DEV/TEST IN PROD!!!!!

  We need a DEV environment...
    - we use a role to control our developers... dev_citibike
    - dev_citibike should not have access to PROD data, only DEV
    - within the DEV environment they should have open permissions to create/modify...
    - they should be able to test on the full scale of our data
  But...
    - we have PII data they should not be allowed to see
 
  We don't want to create new views specifically for the developers... that would
  make migrating back to production more complicated. Instead, we just want to
  limit their access to data. We can do that with policies.

  Let's implement some governance processes to obscure PII data when in DEV
--------------------------------------------------------------------------------*/

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
  Let's clone our current state PROD database to use as DEV. All the policies and
  other metadata we have created in PROD will carry over.
  
  We will also make a DEV warehouse so our DEV workload is isolated from PROD
--------------------------------------------------------------------------------*/

create or replace database citibike_dev clone citibike;

create warehouse if not exists dev_wh warehouse_size = 'medium' auto_suspend=120;
use schema citibike_dev.demo;

-- deny access to the PROD environment
revoke all on schema citibike.demo from role dev_citibike;
revoke all on warehouse load_wh from role dev_citibike;

-- grant explicit permissions on the DEV environment

grant usage on database citibike_v4_reset to role dev_citibike;
grant usage on schema citibike_v4_reset.reset to role dev_citibike;
grant read on stage citibike_v4_reset.reset.weights to role dev_citibike;
grant all on warehouse dev_wh to role dev_citibike;
grant all on database citibike_dev to role dev_citibike;
grant all on all schemas in database citibike_dev to role dev_citibike;
grant all on all tables in database citibike_dev to role dev_citibike;
grant all on all views in database citibike_dev to role dev_citibike;
grant all on all materialized views in database citibike_dev to role dev_citibike;
grant all on all functions in database citibike_dev to role dev_citibike;
revoke all on table citibike_dev.demo.trips from role dev_citibike;



/*--------------------------------------------------------------------------------
  The data is now secure for developers but available to admins
--------------------------------------------------------------------------------*/

use warehouse dev_wh;

use role dba_citibike;
select * from citibike_dev.demo.trips_vw limit 200;

use role dev_citibike;
select * from citibike_dev.demo.trips_vw limit 200;


/*--------------------------------------------------------------------------------
  OK - now we can hand the DEV environment over to our developer team
--------------------------------------------------------------------------------*/
