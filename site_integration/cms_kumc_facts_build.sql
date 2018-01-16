-- cms_kumc_facts_build.sql
/*
create a blueherondata observation_fact with new patient_nums and encounter_nums

run against identified GROUSE server. 
NOTE: This code is yet to be completely run & tested. 
*/
drop table blueherondata_kumc.observation_fact_int purge;

CREATE TABLE BLUEHERONDATA_KUMC.OBSERVATION_FACT_INT as  
select 
  (ob.ENCOUNTER_NUM-(1)+400000000) ENCOUNTER_NUM
, coalesce(dp.bene_id_deid, to_char((ob.patient_num-(1))+20000000)) patient_num
, CONCEPT_CD 
-- , PROVIDER_ID -- Not using the KUMC provider_dimension
, ob.START_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) START_DATE
, ob.END_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) END_DATE
, ob.UPDATE_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) UPDATE_DATE
, MODIFIER_CD
, INSTANCE_NUM -- CONFIRM**
, VALTYPE_CD
-- , TVAL_CHAR -- removing TVAL_CHAR on purpose 
, NVAL_NUM 
, VALUEFLAG_CD 
, QUANTITY_NUM
, UNITS_CD 
, LOCATION_CD 
, OBSERVATION_BLOB
, CONFIDENCE_NUM
, DOWNLOAD_DATE 
, IMPORT_DATE 
, SOURCESYSTEM_CD
, upload_id*(-1) UPLOAD_ID
from 
blueherondata_kumc.observation_fact ob
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_kumc_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ob.patient_num;


-- ========== VERIFICATION
select count(patient_num) from 
blueherondata_kumc.OBSERVATION_FACT_INT;

select count(distinct patient_num) from 
blueherondata_kumc.OBSERVATION_FACT_INT;

select count(patient_num) from 
blueherondata_kumc.OBSERVATION_FACT_INT
where patient_num>=20000000; -- KUMC patients who do not have GROUSE data. 

select count(patient_num) from 
blueherondata_kumc.OBSERVATION_FACT_INT
where patient_num between 20000000 and 25000000; -- KUMC patients who do not have GROUSE data. 

select count(*) from 
blueherondata_kumc.visit_dimension_int;

select count(*) from 
blueherondata_kumc.visit_dimension;

select count(distinct encounter_num) from 
blueherondata_kumc.OBSERVATION_FACT_INT
where encounter_num>=400000000; -- KUMC patients who do not have GROUSE data. 

select count(encounter_num) from 
blueherondata_kumc.OBSERVATION_FACT_INT
where encounter_num between 400000000 and 460000000; -- KUMC patients who do not have GROUSE data. 

select count(distinct patient_num) from 
blueherondata_kumc.OBSERVATION_FACT_INT;

select count(distinct patient_num) from 
blueherondata_kumc.OBSERVATION_FACT_INT;

select count(distinct patient_num) from 
blueherondata_kumc.OBSERVATION_FACT_INT
where patient_num>=20000000; -- KUMC patients who do not have GROUSE data. 

select count(distinct patient_num) from 
blueherondata_kumc.OBSERVATION_FACT_INT
where patient_num between 1 and 19999999; -- KUMC patients who have GROUSE data. 