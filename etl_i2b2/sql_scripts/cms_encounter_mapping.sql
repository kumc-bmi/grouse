/** cms_encounter_mapping - map CMS claims to i2b2 encounters
*/

select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql';
select start_date from cms_visit_dimension where 'dep' = 'cms_dem_txform.sql';
select domain from cms_ccw where 'dep' = 'cms_ccw_spec.sql';

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
select cvd.clm_id encounter_ide
  , cms_ccw.domain encounter_ide_source
  , :project_id project_id
  , "&&I2B2STAR".sq_up_encdim_encounternum.nextval encounter_num
  , cvd.bene_id patient_ide
  , cms_ccw.domain patient_ide_source
  , i2b2_status.active encounter_ide_status
  , sysdate upload_date
  , cvd.update_date
  , :download_date
  , sysdate import_date
  , cms_ccw.domain sourcesystem_cd
  , :upload_id upload_id
  from cms_visit_dimension cvd
  , cms_ccw
  , i2b2_status ;

create or replace view clm_id_mapping as (
select encounter_ide clm_id
  , encounter_num
  from "&&I2B2STAR".encounter_mapping en_map
  join cms_ccw
  on encounter_ide_source = cms_ccw.domain
  and patient_ide_source  = cms_ccw.domain
  join i2b2_status
  on encounter_ide_status = i2b2_status.active);

-- Test for completeness.
select count(*) loaded_record
from "&&I2B2STAR".encounter_mapping
    ;
