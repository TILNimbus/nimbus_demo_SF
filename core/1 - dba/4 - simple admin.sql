/*--------------------------------------------------------------------------------
  In this chapter you will see how you can:
    - radically simplify your approach to data protection, backup and recovery
    - easily and immediately recover from inadvertant data deletion or corruption
    - provide multiple point-of-time views of data to multiple user communities
      without needing to manage snapshots or mutltiple copies of data
    - run queries as at an arbitrary point in time up to 90 days ago
--------------------------------------------------------------------------------*/

-- set the context
use role dba_citibike;
alter warehouse load_wh set warehouse_size = 'medium';
use warehouse load_wh;
use schema citibike.demo;


/*--------------------------------------------------------------------------------
  Finally, we look at how continuous data protection can help against disaster
--------------------------------------------------------------------------------*/

-- we've all had one of those oops moments...
select count(*) from trips;

drop table trips;

-- ta da! CDP to the rescue!
undrop table trips;

select count(*) from trips;

-- in fact this also works for much bigger mistakes...
show objects in database citibike;

drop database citibike;

undrop database citibike;

use schema citibike.demo;

/*--------------------------------------------------------------------------------
  An important thing to note is that UNDROP didn't just restore the tables - 
  it restored all the objects (tables, views, functions, stages, sprocs, etc)
  along with all the metadata and change capture/streams/time travel/etc.

  It is super-powerful!
----------------------------------------------------------------------------------*/
