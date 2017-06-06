/** cms_facts_load - load i2b2 observation facts from CMS

A &&fact_view provides data from CMS transformed row-by-row to i2b2 norms,
with the exception of patient_num and encounter_num. At this point, we
join with patient_mapping and encounter_mapping to get those numbers.

Note the use of per-upload temporary tables and partitions.
*/

select bene_cd from cms_key_sources where 'dep' = 'cms_keys.pls';

create table observation_fact_&&upload_id nologging compress as
select * from "&&I2B2STAR".observation_fact where 1 = 0;

-- pat_day_medpar_rollup() assumes cms_medpar_mapping is populated
alter index "&&I2B2STAR".em_idx_encpath rebuild; -- ISSUE: only rebuild once?
select 1 / count(*) check_medpar_map_exists
from cms_medpar_mapping
where rownum = 1;


insert /*+ append */
into
  observation_fact_&&upload_id
  (
    encounter_num
  , patient_num
  , concept_cd
  , provider_id
  , start_date
  , modifier_cd
  , instance_num
  , valtype_cd
  , tval_char
  , nval_num
  , valueflag_cd
  , quantity_num
  , units_cd
  , end_date
  , location_cd
  -- , observation_blob
  , confidence_num
  , update_date
  , download_date
  , import_date
  , sourcesystem_cd
  , upload_id
  )
  select
    pat_day_medpar_rollup(f.bene_id, f.start_date) encounter_num
  , pat_map.patient_num
  , f.concept_cd
  , f.provider_id
  , f.start_date
  , f.modifier_cd
  , f.instance_num
  , f.valtype_cd
  , f.tval_char
  , f.nval_num
  , f.valueflag_cd
  , f.quantity_num
  , f.units_cd
  , f.end_date
  , f.location_cd
  , f.confidence_num
  , f.update_date
  , :download_date
  , sysdate import_date
  , f.sourcesystem_cd
  , :upload_id
  from &&fact_view f
  join bene_id_mapping pat_map on pat_map.bene_id = f.bene_id
  where f.bene_id is not null
    and f.bene_id between coalesce(:bene_id_first, f.bene_id)
                      and coalesce(:bene_id_last, f.bene_id)
;

alter table "&&I2B2STAR".observation_fact
split partition upload_other values(&&upload_id)
into( partition upload_&&upload_id, partition upload_other) ;

alter table observation_fact_&&upload_id
add constraint obs_pk_&&upload_id primary key(
  patient_num, concept_cd, modifier_cd, start_date, encounter_num, instance_num, provider_id) ;

alter table observation_fact exchange partition upload_&&upload_id
with table observation_fact_&&upload_id;

-- TODO: finally:
drop table observation_fact_&&upload_id;

/* TODO: try / finally cleanup? */
create or replace view cms_design_obs_cleanup as
with i2b2_schema as
  (select *
  from all_tab_columns
  where owner = '&&I2B2STAR'
    and table_name not like 'SYS_%'
  )
, ea as
  (select distinct table_name
  from i2b2_schema
  where table_name like 'OBSERVATION_FACT_%')
  select 'drop table ' || table_name || ';' sql_snippet from ea;

select 1 complete
from "&&I2B2STAR".observation_fact f
where f.upload_id =
  (select max(upload_id) -- cheating?
  from "&&I2B2STAR".upload_status
  where transform_name = :task_id
  )
  and rownum = 1;
