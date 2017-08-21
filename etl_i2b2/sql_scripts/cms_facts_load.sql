/** cms_facts_load - load i2b2 observation facts from CMS

A &&fact_view provides data from CMS transformed row-by-row to i2b2 norms,
with the exception of patient_num and encounter_num. At this point, we
use obs_fact_map() to join with patient_mapping and encounter_mapping
to get those numbers.

See also obs_fact_pipe.sql re bulk insert into per-upload tables.
*/

select design_digest from obs_fact_pipe_design where 'dep' = 'obs_fact_pipe.sql';

create table observation_fact_&&upload_id nologging compress as
select * from "&&I2B2STAR".observation_fact where 1 = 0;

select 1 / count(*) check_medpar_map_exists
from cms_medpar_mapping
where rownum = 1;


with io as (
  select clock_access('bulk load clock') clock,
         bene_id_mapper(upload_id => null) bm,
         medpar_mapper(upload_id => null) mm
         -- ISSUE: access to the fact table should be explicit (reified) too
  from dual
)
, f_mapped as
  (select f.*
  from io, table(obs_fact_map(
    mm => io.mm, bm => io.bm,
    cms_obs_cur => cursor(select * from &&fact_view ))) f
  )
select progress.*
from io, table(obs_load_progress(
  source_info => '&&fact_view',
  obs_data => cursor(select * from f_mapped),
  clock => io.clock,
  download_date => :download_date,
  upload_id => :upload_id,
  chunk_size => 50000)) progress ;


alter table "&&I2B2STAR".observation_fact
split partition upload_other values(&&upload_id)
into( partition upload_&&upload_id, partition upload_other) ;

alter table observation_fact_&&upload_id
add constraint obs_pk_&&upload_id primary key(
  patient_num, concept_cd, modifier_cd, start_date, encounter_num, instance_num, provider_id) ;


/* diagnosing duplicate keys:

create index obs_pk_debug on observation_fact_1014 (
patient_num, concept_cd, modifier_cd, start_date, encounter_num, instance_num, provider_id);

select count(*), patient_num, concept_cd, modifier_cd, start_date, encounter_num, instance_num, provider_id
from observation_fact_1018
group by patient_num, concept_cd, modifier_cd, start_date, encounter_num, instance_num, provider_id
having count(*) > 1
;
drop index obs_pk_debug;
*/


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
