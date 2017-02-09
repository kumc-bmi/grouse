/** cms_patient_mapping - map CMS beneficiaries to i2b2 patients
*/

select id from grouse_project where 'dep' = 'grouse_project.sql';
select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql';
select birth_date from cms_patient_dimension where 'dep' = 'cms_dem_txform.sql';

  truncate table "&&I2B2STAR".patient_mapping;

  insert /* append */
  into "&&I2B2STAR".patient_mapping
    (
      patient_ide
    , patient_ide_source
    , patient_num
    , patient_ide_status
    , project_id
    , upload_date
    , update_date
    , download_date
    , import_date
    , sourcesystem_cd
    , upload_id
    )
  select cpd.bene_id patient_ide
  , cms_ccw.domain patient_ide_source
  , "&&I2B2STAR".sq_up_patdim_patientnum.nextval patient_num
  , i2b2_status.active patient_ide_status
  , grouse_project.id project_id
  , sysdate upload_date
  , cpd.update_date
  , cpd.download_date
  , sysdate import_date
  , cms_ccw.domain sourcesystem_cd
  , :upload_id upload_id
  from cms_patient_dimension cpd
  , cms_ccw
  , i2b2_status
  , grouse_project ;


-- Test for completeness.
select count(*) complete
from "&&I2B2STAR".patient_mapping;
