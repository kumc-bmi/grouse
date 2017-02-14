/** cms_patient_mapping - map CMS beneficiaries to i2b2 patients
*/

select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql';
select birth_date from cms_patient_dimension where 'dep' = 'cms_dem_txform.sql';
select domain from cms_ccw where 'dep' = 'cms_ccw_spec.sql';

  truncate table "&&I2B2STAR".patient_mapping;
/* ISSUE: .nextval is not idempotent/functional
Perhaps reset it along with truncating the patient_mapping?

There are some reports that this works:
alter sequence "&&I2B2STAR".sq_up_patdim_patientnum restart start with 0
-- Ack Jon Heller Oct 2013 http://stackoverflow.com/a/19673327

But I get: ORA-02286: no options specified for ALTER SEQUENCE
*/
  insert /*+ append */
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
  , :project_id project_id
  , sysdate upload_date
  , cpd.update_date
  , :download_date
  , sysdate import_date
  , cms_ccw.domain sourcesystem_cd
  , :upload_id upload_id
  from cms_patient_dimension cpd
  , cms_ccw
  , i2b2_status ;


-- Test for completeness and report records loaded.
select count(*) loaded_record
from "&&I2B2STAR".patient_mapping;
