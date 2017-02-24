/** cms_encounter_mapping - map CMS claims to i2b2 encounters

These are provided by calling tasks, but for convenience in sqldeveloper:
define I2B2STAR = NIGHTHERONDATA;
define cms_source_cd = '''ccwdata.org''';
*/

select start_date from cms_visit_dimension_medpar where 'dep' = 'cms_enc_txform.sql';


truncate table "&&I2B2STAR".encounter_mapping;

  insert /*+ append */
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
select cvd.encounter_ide
  , cvd.encounter_ide_source
  , :project_id project_id
  , "&&I2B2STAR".sq_up_encdim_encounternum.nextval encounter_num
  , cvd.bene_id patient_ide
  , key_sources.bene_cd patient_ide_source
  , i2b2_status.active encounter_ide_status
  , sysdate upload_date
  , cvd.update_date
  , :download_date
  , sysdate import_date
  , &&cms_source_cd sourcesystem_cd
  , :upload_id upload_id
  from cms_visit_dimension_medpar cvd
  cross join cms_key_sources key_sources
  cross join i2b2_status ;

commit;  -- avoid ORA-12838: cannot read/modify an object after modifying it in parallel


-- ISSUE: drop table cms_encounter_rollup;
create table cms_encounter_rollup as
with medpar_map as
  (select vd.bene_id
  , vd.start_date
  , vd.end_date
  , vd.inout_cd
  , emap.encounter_num
  from cms_visit_dimension_medpar vd
  join "&&I2B2STAR".encounter_mapping emap
  on emap.encounter_ide = vd.encounter_ide
  )
select bc.bene_id
, bc.encounter_ide
, bc.encounter_ide_source
, bc.patient_day
, bc.start_date
, bc.end_date
, bc.inout_cd
, bc.location_cd
, bc.length_of_stay
, bc.update_date
, min(medpar_map.encounter_num) medpar_encounter_num
from cms_visit_dimension_bc bc
left join medpar_map
on bc.bene_id                = medpar_map.bene_id
  and medpar_map.start_date <= bc.start_date
  and medpar_map.end_date   >= bc.end_date
group by bc.bene_id
, bc.encounter_ide
, bc.encounter_ide_source
, bc.patient_day
, bc.start_date
, bc.end_date
, bc.inout_cd
, bc.location_cd
, bc.length_of_stay
, bc.update_date
, bc.sourcesystem_cd ;


/* How many of them got rolled up vs. not?

select rolled_up, count(*) from (
  select case when encounter_num is not null then 1 else 0 end rolled_up
  from cms_encounter_rollup
) group by rolled_up
;
*/

/** patient_day mappings for those not subsumed by a medpar */
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
select cer.patient_day
, key_sources.patient_day_cd encounter_ide_source
, :project_id project_id
, "&&I2B2STAR".sq_up_encdim_encounternum.nextval encounter_num
, cer.bene_id patient_ide
, key_sources.bene_cd patient_ide_source
, i2b2_status.active encounter_ide_status
, sysdate upload_date
, cer.update_date
, :download_date
, sysdate import_date
,
  &&cms_source_cd sourcesystem_cd
, :upload_id upload_id
from
  (select patient_day
  , max(bene_id) bene_id  -- necessarily only one
  , max(update_date) update_date
  from cms_encounter_rollup
  where medpar_encounter_num is null
  group by patient_day
  ) cer
cross join cms_key_sources key_sources
cross join i2b2_status
  ;

commit;

create or replace view cms_patient_day_mapping as
select emap.encounter_ide as patient_day, emap.encounter_num
from "&&I2B2STAR".encounter_mapping emap
join cms_key_sources key_sources on key_sources.patient_day_cd = emap.encounter_ide_source
;

/* Map CLM_ID, LINE_NUM to medpar or patient day. */
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
select cer.encounter_ide
, cer.encounter_ide_source
, :project_id project_id
, coalesce(cer.medpar_encounter_num, pdmap.encounter_num) encounter_num
, cer.bene_id patient_ide
, key_sources.bene_cd patient_ide_source
, i2b2_status.active encounter_ide_status
, sysdate upload_date
, cer.update_date
, :download_date
, sysdate import_date
,
  &&cms_source_cd sourcesystem_cd
, :upload_id upload_id
from cms_encounter_rollup cer
left join cms_patient_day_mapping pdmap
  on cer.patient_day = pdmap.patient_day
cross join cms_key_sources key_sources
cross join i2b2_status ;

commit;

select *
from "&&I2B2STAR".encounter_mapping
where encounter_ide_source not in ('ccwdata.org(MEDPAR_ID)', 'ccwdata.org(CLM_ID,LINE_NUM)')
;

-- Test for completeness.
select count(*) loaded_record
from "&&I2B2STAR".encounter_mapping emap
cross join cms_key_sources key_sources
where emap.encounter_ide_source in (key_sources.medpar_cd, key_sources.patient_day_cd)
    ;
