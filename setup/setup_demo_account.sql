/*--------------------------------------------------------------------------------
  SETUP DEMO ACCOUNT V4

  Setup script for the demo account. Creates the users (john & jane), and
  roles (analyst_citibike, dev_citibike & dba_citibike) for the Citibike demo.

  Run this script as admin in your demoX account.
  Should only be run once for initial setup.

  Author:   Alan Eldridge
  Updated:  08 Jun 2020 - aeldridge - V3
  Updated:  21 Jan 2021 - gmullen - Modified to reflect new SE account creation process through ORGs. Now dynamically generate NYCHA/JHA Reader Account names to prefix Account Locator. Only one named NYCHA allowed per Organization.
  Updated:  03 Feb 2021 - gmullen - Added dynamic WH creation to execute procedure for accounts with no default WH.
  Updated:  30 Jun 2021 - aeldridge - V4
  
--------------------------------------------------------------------------------*/

-- IMPORTANT!!!
-- edit the following line to set the password for John and Jane
--set pwd = '*** your password goes here ***';

use role accountadmin;
set account_locator = (select current_account());

-- create roles
create role if not exists dba_citibike comment = "Database administration";
create role if not exists dev_citibike comment = "Developer";
create role if not exists analyst_citibike comment = "Business analyst & Tableau user";

grant create warehouse on account to role dba_citibike;
grant create warehouse on account to role analyst_citibike;
grant create warehouse on account to role dev_citibike;
grant create database on account to role dba_citibike;

grant monitor usage on account to role dba_citibike;
grant create share on account to role dba_citibike;
grant import share on account to role dba_citibike;
grant create integration on account to role dba_citibike;
grant execute task on account to role dba_citibike;
grant manage grants on account to role dba_citibike;

grant role dba_citibike to role accountadmin;
grant role dev_citibike to role dba_citibike;
grant role analyst_citibike to role dba_citibike;

-- create users
create user if not exists john password=$pwd email='john@snowflakedemo.com' comment='John DBA';
create user if not exists jane password=$pwd email='jane@snowflakedemo.com' comment='Jane Analyst';

grant role dba_citibike to user john;
grant role accountadmin to user john;
alter user john set default_role=dba_citibike;

grant role analyst_citibike to user jane;
alter user jane set default_role=analyst_citibike;

-- check results
show roles;
show users;
show warehouses;
show managed accounts;
