select birth_date from cms_patient_dimension where 'dep' = 'cms_dem_txform.sql';

insert
  /*+ append */
into "&&I2B2STAR".patient_dimension
  (
    patient_num
  , vital_status_cd
  , birth_date
  , death_date
  , sex_cd
  , age_in_years_num
  , race_cd
  , update_date
  , download_date
  , import_date
  , sourcesystem_cd
  , upload_id
  )
select pmap.patient_num
, vital_status_cd
, birth_date
, death_date
, sex_cd
, age_in_years_num
, race_cd
, update_date
, download_date
, sysdate import_date
, sourcesystem_cd
, :upload_id upload_id
from cms_patient_dimension cpd
join
  bene_id_mapping pmap on cpd.bene_id      = pmap.patient_ide ;
