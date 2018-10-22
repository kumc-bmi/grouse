/** migrate_fact_upload -- append data from a workspace table.

We use an observation_fact table partitioned by upload_id for ETL performance:

create table &&I2B2STAR.observation_fact
 partition by hash(upload_id)
 ... ;

*/

/* Post-hoc fix: observation_fact_NNNN chunks were created without null constraints. */
alter table &&I2B2STAR.observation_fact_&&upload_id modify (
      ENCOUNTER_NUM not null,
      patient_NUM not null,
      concept_cd not null,
      provider_id not null,
      start_date not null,
      modifier_cd not null,
      instance_num not null
);


alter table &&I2B2STAR.observation_fact
exchange partition for (&&upload_id) with table &&workspace_star.observation_fact_&&upload_id;

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
