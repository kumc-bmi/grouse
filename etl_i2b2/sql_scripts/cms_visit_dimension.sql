/** cms_visit_dimension - load visit_dimension from CMS

TODO: patient-day visits

We assume these edits to PCORNET_ENC:

  create or replace view cms_pcornet_enc as
  select * from cms_pcornet_terms e
  where e.c_fullname like '\PCORI\ENCOUNTER\ENC_TYPE\%'
  order by c_basecode
  ;


handy for dev:
truncate table "&&I2B2STAR".visit_dimension;

*/

select drg from "&&I2B2STAR".visit_dimension where 'dep' = 'vdim_add_cols.sql';
select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql';

-- Check for PCORNET ENCOUNTER metadata. ISSUE: parameterize metadata schema?
select c_fullname, c_basecode from grousemetadata.pcornet_enc where 1=0;


/** Make an encounter for each MSDRG:% fact

Every MEDPAR has a DRG_CD and every MAXDATA_IP has a DRG_REL_CD. TODO: pass/fail test.

select /*+ parallel(24) *-/ medpar_id, drg_cd
from cms_deid.medpar_all
where DRG_CD is null
;

select /*+ parallel(24) *-/ bene_id
from cms_deid.maxdata_ip
where DRG_REL_GROUP is null

Since DRG facts come from inpatient visits and we allocate an encounter_num
for each inpatient visit*, this is consistent with the primary key constraint
on visit_dimension.

* TODO: MAXDATA_IP encounter mappings

*/
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


/** ENC_TYPE
*/
merge /*+ parallel(vd, 8) append */ into "&&I2B2STAR".visit_dimension vd
using (
  select /*+ parallel(obs, 20) */
         encounter_num, patient_num, provider_id, start_date, end_date, concept_cd
       , e.pcori_basecode enc_type
  from "&&I2B2STAR".observation_fact obs
  join grousemetadata.pcornet_enc e on obs.concept_cd = e.c_basecode
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

/* TODO: discharge_status etc.
     , location_zip facility_location
     , facilityid
     , discharge_disposition
     , discharge_status
     , admitting_source
*/


select /*+ parallel(vd, 8) */ count(*) record_loaded
from "&&I2B2STAR".visit_dimension vd;
