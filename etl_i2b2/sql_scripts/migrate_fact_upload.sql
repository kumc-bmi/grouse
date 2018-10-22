/** migrate_fact_upload -- append data from a workspace table.

We use an observation_fact table partitioned by upload_id for ETL performance:

create table &&I2B2STAR.observation_fact
 partition by hash(upload_id)
 ... ;

*/

/* Post-hoc fix: observation_fact_NNNN chunks were created with
   some missing null constraints and some different precision / lengths. */
create table obs_fix_&&upload_id
as select * from &&I2B2STAR.observation_fact where 1 = 0;

insert /*+ append */ into obs_fix_&&upload_id
as select * from &&workspace_star.observation_fact_&&upload_id;


alter table &&I2B2STAR.observation_fact
exchange partition for (&&upload_id) with table obs_fix_&&upload_id;

drop table obs_fix_&&upload_id;

insert into &&I2B2STAR.upload_status
select * from &&workspace_star.upload_status where upload_id = &&upload_id
-- Handle the case where workspace_star and I2B2STAR are the same.
and upload_id not in (select upload_id from &&I2B2STAR.upload_status)
;

update &&I2B2STAR.upload_status set load_status = 'OK'
where upload_id = &&upload_id;

commit ;


select 1 complete
from "&&I2B2STAR".upload_status
where upload_id = &&upload_id
;
