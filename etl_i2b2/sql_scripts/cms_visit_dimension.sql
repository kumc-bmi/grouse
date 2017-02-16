/** cms_visit_dimension - load visit_dimension from CMS
*/

select start_date from cms_visit_dimension_medpar where 'dep' = 'cms_dem_txform.sql';

truncate table "&&I2B2STAR".visit_dimension;

/* medpar visits */
insert /*+ append */
into "&&I2B2STAR".visit_dimension
  (
    encounter_num
  , patient_num
  , active_status_cd
  , start_date
  , end_date
  , inout_cd
  , location_cd
  , import_date
  , upload_id
  , download_date
  , sourcesystem_cd
  )
select enc_map.encounter_num
, pat_map.patient_num
, cms_vd.active_status_cd
, cms_vd.start_date
, cms_vd.end_date
, cms_vd.inout_cd
, cms_vd.location_cd
, sysdate as import_date
 , :upload_id -- ISSUE: express dependency?
 , :download_date
, cms_vd.sourcesystem_cd
from cms_visit_dimension_medpar cms_vd
join
  "&&I2B2STAR".encounter_mapping enc_map on cms_vd.encounter_ide = enc_map.encounter_ide
  and cms_vd.encounter_ide_source = enc_map.encounter_ide_source
join
  bene_id_mapping pat_map on cms_vd.bene_id = pat_map.bene_id ;

/* patient-day visits */
insert /*+ append */
into "&&I2B2STAR".visit_dimension
  (
    encounter_num
  , patient_num
  , active_status_cd
  , start_date
  , end_date
  , inout_cd
  , location_cd
  , import_date
  , upload_id
  , download_date
  , sourcesystem_cd
  )
with mapped as (
select pday.encounter_num encounter_num
, pmap.patient_num
, cer.start_date
, cer.end_date
, cer.inout_cd
, cer.location_cd
, cer.update_date
from cms_encounter_rollup cer
join bene_id_mapping pmap on pmap.bene_id = cer.bene_id
join cms_patient_day_mapping pday on pday.patient_day = cer.patient_day
where cer.medpar_encounter_num is null
)
, visit as (
select encounter_num, patient_num
, i2b2_status.active active_status_cd
, min(start_date) start_date
, max(end_date) end_date
, listagg(inout_cd, ',') within group (order by inout_cd)  inout_cd  -- ISSUE: prioritize? pick one?
, listagg(location_cd, ',') within group (order by location_cd)  location_cd  -- ISSUE: prioritize? pick one?
from mapped
cross join i2b2_status
group by encounter_num, patient_num, i2b2_status.active
)
select visit.*
, sysdate as import_date
 , :upload_id upload_id -- ISSUE: express dependency?
 , :download_date download_date
, &&cms_source_cd sourcesystem_cd
from visit
;

commit;

select count(*) record_loaded
from nightherondata.visit_dimension;
