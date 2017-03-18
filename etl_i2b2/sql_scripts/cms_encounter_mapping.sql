/** cms_encounter_mapping - map CMS claims to i2b2 encounters

We have two sorts of mappings:
  1. one encounter_num for each hospital encounter (medpar_all row)
  2. for each patient day:
     - if the patient day is (temporally) subsumed by a hospital encounter,
       we map to that hospital encounter's encounter_num; else
     - allocated an encounter_num for that patient day.

TODO: We currently only choose patient days based on clm_from_dt; we should
      enumerate each of clm_from_dt, clm_from_dt + 1, clm_from_dt + 2, ... clm_thru_dt

These substitution variables are provided by calling tasks, but for convenience in sqldeveloper:
define I2B2STAR = NIGHTHERONDATA;
define cms_source_cd = '''ccwdata.org''';

*/
select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql';
select bene_cd from cms_key_sources where 'dep' = 'cms_keys.pls';


whenever sqlerror continue;
create unique index "&&I2B2STAR".encounter_mapping_pk on "&&I2B2STAR".encounter_mapping(encounter_ide,
  encounter_ide_source, project_id, patient_ide, patient_ide_source) nologging;
  alter table "&&I2B2STAR".encounter_mapping enable constraint encounter_mapping_pk;
-- ISSUE: either rebuild these indexes at some point or drop them
alter index "&&I2B2STAR".em_encnum_idx rebuild;
alter index "&&I2B2STAR".em_idx_encpath rebuild;
alter index "&&I2B2STAR".em_uploadid_idx rebuild;
whenever sqlerror exit;


create or replace view rollup_test_data as
  select pat_day.bene_id
  ,      pat_day.clm_id
  ,      pat_day.clm_from_dt
  ,      clm_thru_dt
  ,      medpar.medpar_id
  ,      admsn_dt
  ,      dschrg_dt
  from "&&CMS_RIF".bcarrier_claims pat_day
    join "&&CMS_RIF".medpar_all medpar on medpar.bene_id = pat_day.bene_id
    and
      medpar.admsn_dt <= pat_day.clm_from_dt
    and
      medpar.dschrg_dt >= pat_day.clm_from_dt
  where rownum <= 50;


create or replace view pat_day_bcarrier_thru_TODO as
  select bene_id
  ,      clm_id
  ,      clm_from_dt
  ,      clm_thru_dt
  ,      delta
  ,      clm_from_dt + delta each_dt
  from "&&CMS_RIF".bcarrier_claims
    join (
        select rownum - 1 delta
        from dual
        connect by
          level <= 200 * 365 -- claim dur < lifetime < 200 yrs
      ) on delta <= clm_thru_dt - clm_from_dt;


create or replace view patient_day_bcarrier as
  select bc.bene_id
  ,      bc.clm_from_dt
  ,      max(nch_wkly_proc_dt) update_date
  from "&&CMS_RIF".bcarrier_claims bc
  group by
    bc.bene_id
  , bc.clm_from_dt;


/* eyeball it
select *
from patient_day_bcarrier
where bene_id in (
    select distinct
           bene_id
    from rollup_test_data
  )
order by bene_id, clm_from_dt;
*/

create or replace view patient_day_bcarrier_rollup as
  select pat_day.*
  ,      (
      select min(emap.encounter_num)
      from cms_medpar_mapping emap
      where emap.medpar_id = (
          select min(medpar_id)
          from "&&CMS_RIF".medpar_all medpar
          where medpar.bene_id = pat_day.bene_id
          and
            medpar.admsn_dt <= pat_day.clm_from_dt
          and
            medpar.dschrg_dt >= pat_day.clm_from_dt
        )
    ) medpar_encounter_num
  from patient_day_bcarrier pat_day;

/*
explain plan for
select *
from patient_day_bcarrier_rollup;

SELECT PLAN_TABLE_OUTPUT line FROM TABLE(DBMS_XPLAN.DISPLAY());
*/

/* At least some medpar_encounter_num are not null, right?
ISSUE: pass/fail test?

select *
from patient_day_bcarrier_rollup
where bene_id in (
    select distinct bene_id from rollup_test_data
  )
  order by bene_id, clm_from_dt;
*/


/** patient_day mappings rolled up to medpar */
explain plan for
insert
  /*+ append */
into "&&I2B2STAR".encounter_mapping
  (
    encounter_ide
  , encounter_ide_source
  , project_id
  , encounter_num
  , patient_ide
  , patient_ide_source
  , encounter_ide_status
  , upload_date
  , update_date
  , download_date
  , import_date
  , sourcesystem_cd
  , upload_id
  )
select fmt_patient_day(pat_day.bene_id, pat_day.clm_from_dt) encounter_ide
, key_sources.patient_day_cd encounter_ide_source
, :project_id project_id
, coalesce(medpar_encounter_num, "&&I2B2STAR".sq_up_encdim_encounternum.nextval) encounter_num
, pat_day.bene_id patient_ide
, key_sources.bene_cd patient_ide_source
, i2b2_status.active encounter_ide_status
, sysdate upload_date
, pat_day.update_date
, :download_date
, sysdate import_date
,
  &&cms_source_cd sourcesystem_cd
, :upload_id upload_id
from
  patient_day_bcarrier_rollup pat_day
cross join cms_key_sources key_sources
cross join i2b2_status ;

commit;


create or replace view cms_patient_day_mapping
as
  select emap.encounter_ide as patient_day
  , emap.encounter_num
  from "&&I2B2STAR".encounter_mapping emap
  join cms_key_sources key_sources
  on key_sources.patient_day_cd = emap.encounter_ide_source ;


-- Test for completeness: any records with this task's upload_id?
select 1 task_upload_found
from "&&I2B2STAR".encounter_mapping emap
where emap.upload_id =
  (select max(upload_id)
  from "&&I2B2STAR".upload_status
  where transform_name = :task_id
  and load_status = 'OK'
  )
  and rownum = 1;
