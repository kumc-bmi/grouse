/** mapping_reset - reset patient, encounter mappings, sequences
*/

select bene_cd from cms_key_sources where 'dep' = 'cms_keys.pls';

/* Note: if drop/create is a problem,
         see reset_seq Doug Porter Sep 18 '08 http://stackoverflow.com/a/93633 */

drop sequence "&&I2B2STAR".sq_up_patdim_patientnum;
create sequence "&&I2B2STAR".sq_up_patdim_patientnum cache 1024;
  truncate table "&&I2B2STAR".patient_mapping;

drop sequence "&&I2B2STAR".sq_up_encdim_encounternum;
create sequence "&&I2B2STAR".sq_up_encdim_encounternum cache 1024;
  truncate table "&&I2B2STAR".encounter_mapping;

select 1 complete
from "&&I2B2STAR".upload_status up
where up.transform_name = :task_id
and up.load_status = 'OK';