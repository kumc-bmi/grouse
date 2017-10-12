/** cms_visit_dimension - load visit_dimension from CMS
*/

select drg from "&&I2B2STAR".visit_dimension where 'dep' = 'vdim_add_cols.sql';
select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql';
select start_date from cms_visit_dimension_medpar where 'dep' = 'cms_enc_txform.sql';

truncate table "&&I2B2STAR".visit_dimension;


/* There are none of these. TODO: pass/fail test.


select /*+ parallel(24) *-/ medpar_id, drg_cd
from cms_deid.medpar_all
where DRG_CD is null
;

select /*+ parallel(24) *-/ bene_id
from cms_deid.maxdata_ip
where DRG_REL_GROUP is null
;
*/



/***@@@ Test data */ 
truncate table observation_fact; --@@

insert /*+ parallel(24) append */ into observation_fact
select * from observation_fact_4790; -- MEDPAR_Upload #1 of 2; 98628 bene_ids
commit;

insert /*+ parallel(24) append */ into observation_fact
select * from observation_fact_4791; -- MEDPAR_Upload #2 of 2; 98628 bene_ids
commit;

insert /*+ parallel(24) append */ into observation_fact
select * from observation_fact_4787; -- MAXDATA_IP_Upload #1 of 2; 98628 bene_ids
commit;

insert /*+ parallel(24) append */ into observation_fact
select * from observation_fact_4786; -- MAXDATA_IP_Upload #2 of 2; 98628 bene_ids
commit;

/*@@*/
drop table pcornet_enc_ty;
create table pcornet_enc_ty as
select * from pcorimetadata.pcornet_enc e
where e.c_fullname like '\PCORI\ENCOUNTER\ENC_TYPE\%'
union all
select * from cms_pcornet_terms e
where e.c_fullname like '\PCORI\ENCOUNTER\ENC_TYPE\%'
order by c_basecode
;
/***@@@ Test data */ 


/* Insert an encounter for each MEDPAR_ALL, MAXDATA_IP record. */
insert /*+ parallel(8) append */ into "&&I2B2STAR".visit_dimension (
       encounter_num
     , patient_num
     , providerid
     , start_date
     , end_date
     , drg
     , active_status_cd
)
select encounter_num
     , patient_num
     , provider_id
     , start_date
     , end_date
     , (select active from i2b2_status)
     , SUBSTR(obs.concept_cd, length('MSDRG:%')) drg
from "&&I2B2STAR".observation_fact obs
where concept_cd like 'MSDRG:%';
-- 25,698 rows inserted. for 1%
commit;


merge /*+ parallel(vd, 8) append */ into "&&I2B2STAR".visit_dimension vd
using (
  select /*+ parallel(obs, 20) */
         encounter_num, patient_num, provider_id, start_date, end_date, concept_cd
       , e.pcori_basecode enc_type
  from "&&I2B2STAR".observation_fact obs
  join /**@@ pcorimeta.pcornet_enc*/ pcornet_enc_ty e on obs.concept_cd = e.c_basecode
  where e.c_fullname like '\PCORI\ENCOUNTER\ENC_TYPE\%'
) obs on (obs.encounter_num = vd.encounter_num and obs.patient_num = vd.patient_num)
when matched then
  update set vd.inout_cd = obs.enc_type, upload_id = :upload_id
  where vd.inout_cd is null or vd.inout_cd != obs.enc_type
when not matched then
  insert (encounter_num, patient_num, providerid, start_date, end_date, inout_cd, upload_id)
  values (obs.encounter_num, obs.patient_num, obs.provider_id, obs.start_date, obs.end_date, obs.enc_type, :upload_id)
; -- 25,698 rows merged.
commit;


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
from "&&I2B2STAR".visit_dimension;
