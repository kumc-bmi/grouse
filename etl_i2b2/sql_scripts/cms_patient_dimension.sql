/** cms_patient_dimension - Load patient dimension from CMS.
*/

select birth_date from cms_patient_dimension where 'dep' = 'cms_dem_txform.sql';

truncate table "&&I2B2STAR".patient_dimension;

insert
into "&&I2B2STAR".patient_dimension
  (
    patient_num
  , sex_cd
  , race_cd
  , vital_status_cd
  , birth_date
  , death_date
  , age_in_years_num
    -- TODO:    , state_cd
  , import_date
  , upload_id
  , download_date
  , sourcesystem_cd
  )
select pat_map.patient_num
, cms_pat_dim.sex_cd
, cms_pat_dim.race_cd
, cms_pat_dim.vital_status_cd
, cms_pat_dim.birth_date
, cms_pat_dim.death_date
, cms_pat_dim.age_in_years_num
, sysdate as import_date
, :upload_id
, :download_date
, &&cms_source_cd as sourcesystem_cd
from cms_patient_dimension cms_pat_dim
join bene_id_mapping pat_map on pat_map.bene_id = cms_pat_dim.bene_id ;


select 1 complete
from "&&I2B2STAR".patient_dimension
where rownum = 1;
