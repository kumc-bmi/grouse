/** cms_visit_dimension - load visit_dimension from CMS
*/

select start_date from cms_visit_dimension where 'dep' = 'cms_dem_txform.sql';

truncate table "&&I2B2STAR".visit_dimension;

insert
into nightherondata.visit_dimension
  (
    encounter_num
  , active_status_cd
  , patient_num
  , start_date
  , end_date
  , import_date
  , upload_id
  , download_date
  , sourcesystem_cd
  )
select enc_map.encounter_num
, cms_vd.active_status_cd
, pat_map.patient_num
, cms_vd.start_date
, cms_vd.end_date
, sysdate as import_date
 , :upload_id -- ISSUE: express dependency?
 , :download_date
, cms_vd.sourcesystem_cd
from cms_visit_dimension cms_vd
join
  clm_id_mapping enc_map on cms_vd.clm_id = enc_map.clm_id
join
  bene_id_mapping pat_map on cms_vd.bene_id = pat_map.bene_id ;

select count(*) record_loaded
from nightherondata.visit_dimension;
