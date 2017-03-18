/** medpar_encounter_map - map CMS medpar events to i2b2 encounters

Seel also cms_encounter_mapping for patient-day rollup.

Reasonable performance probably depends on indexes such as:
create index medpar_bene on "&&CMS_RIF".medpar_all (bene_id);
*/

select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql';
select bene_cd from cms_key_sources where 'dep' = 'cms_keys.pls';

-- for debugging: truncate table "&&I2B2STAR".encounter_mapping;
alter table "&&I2B2STAR".encounter_mapping disable constraint encounter_mapping_pk;
whenever sqlerror continue;
drop index "&&I2B2STAR".encounter_mapping_pk;
whenever sqlerror exit;
alter index "&&I2B2STAR".em_encnum_idx unusable;
alter index "&&I2B2STAR".em_idx_encpath unusable;
alter index "&&I2B2STAR".em_uploadid_idx unusable;


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
select /*+ index(medpar) */ medpar.medpar_id encounter_ide
  , key_sources.medpar_cd encounter_ide_source
  , :project_id project_id
  , "&&I2B2STAR".sq_up_encdim_encounternum.nextval encounter_num
  , medpar.bene_id patient_ide
  , key_sources.bene_cd patient_ide_source
  , i2b2_status.active encounter_ide_status
  , sysdate upload_date
  , medpar.ltst_clm_acrtn_dt update_date
  , :download_date
  , sysdate import_date
  , &&cms_source_cd sourcesystem_cd
  , :upload_id upload_id
  from "&&CMS_RIF".medpar_all medpar
  cross join cms_key_sources key_sources
  cross join i2b2_status
  where medpar.bene_id between coalesce(:bene_id_first, medpar.bene_id)
                           and coalesce(:bene_id_last, medpar.bene_id);

commit;  -- avoid ORA-12838: cannot read/modify an object after modifying it in parallel

create or replace view cms_medpar_mapping
as
  select /*+ index(emap)*/ emap.encounter_ide as medpar_id
  , emap.encounter_num
  from "&&I2B2STAR".encounter_mapping emap
  join cms_key_sources key_sources
  on key_sources.medpar_cd = emap.encounter_ide_source ;


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
