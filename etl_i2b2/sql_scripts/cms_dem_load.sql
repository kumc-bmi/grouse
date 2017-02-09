select birth_date from cms_patient_dimension where 'dep' = 'cms_dem_txform.sql';

insert
  /* append */
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
  (select patient_ide
  , patient_num
  from "&&I2B2STAR".patient_mapping pmap
  join i2b2_status
  on pmap.patient_ide_status = i2b2_status.active
  join cms_ccw
  on pmap.patient_ide_source = cms_ccw.domain
  ) pmap on cpd.bene_id      = pmap.patient_ide ;
