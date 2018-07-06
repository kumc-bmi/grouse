-- cms_cdm_kumc_dimensions_build.sql: create new cms tables
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against identified GROUSE server. 

-- ========== PATIENT_DIMENSION

select max(cast(patid as number)), min(cast(patid as number)) from cdm_kumc_calamus.demographic;

select max(cast(patient_num as number)), min(cast(patient_num as number)) 
from blueherondata_kumc_calamus.patient_dimension;

select max(cast(patid as number))-min(cast(patid as number)) from cdm_kumc_calamus.demographic;

select count(patid) from  cdm_kumc_calamus.demographic;

select count(patient_num) from blueherondata_kumc_calamus.patient_dimension;

select * from cms_id.patient_number_ranges;

-- KUMC patient_nums will start at 20000000 and run till 30000000 
drop table cdm_kumc_calamus.demographic_int purge;
create table 
cdm_kumc_calamus.demographic_int
as select 
coalesce(dp.bene_id_deid, to_char((pd.patid-(1))+20000000)) patid,
add_months(pd.birth_date - (nvl(bh_dob_date_shift,0)+ nvl(dp.bh_date_shift_days,0) - nvl(dp.cms_date_shift_days,0)),nvl(cms_dob_shift_months,0)) birth_date, 
birth_time,
sex,
sexual_orientation,
gender_identity,
hispanic,
biobank_flag,
race,
raw_sex,
raw_sexual_orientation,
raw_gender_identity,
raw_hispanic,
raw_race
from 
cdm_kumc_calamus.demographic pd
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, cms_dob_shift_months, 
bh_date_shift_days, bh_dob_date_shift 
from cms_id.cms_cdm_kumc_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0
and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null))
and ((patient_num=mp_patient_num) or (patient_num is null and mp_patient_num is null))
) dp
on dp.patient_num = pd.patid;

-- ========== VERIFICATION

select count(patid) from 
cdm_kumc_calamus.demographic_int;
-- This count should match the one below

select count(distinct patid) from 
cdm_kumc_calamus.demographic_int;

select count(patid) from 
cdm_kumc_calamus.demographic_int
where patid>=20000000; 
-- KUMC patients who do not have GROUSE data. 
-- The difference between the counts in the above two queries should match the intersection

select distinct patient_num, bene_id from cms_id.cms_cdm_kumc_mapping
where patient_num is not null and
bene_id is not null
and
dups_bene_id = 0 and 
dups_pat_num = 0
and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null))
and ((patient_num=mp_patient_num) or (patient_num is null and mp_patient_num is null));

select count(patid) from 
cdm_kumc_calamus.demographic_int
where patid between 20000000 and 30000000; 
-- KUMC patients who do not have GROUSE data. 

-- ========== VISIT_DIMENSION

select max(cast(encounterid as number)), min(cast(encounterid as number)) 
from cdm_kumc_calamus.encounter;

select max(cast(encounter_num as number)), min(cast(encounter_num as number)) 
from blueherondata_kumc_calamus.visit_dimension;
-- ** picked a couple of encounters to verify that the numbers match between cdm and i2b2.

select max(cast(encounterid as number))- min(cast(encounterid as number)) 
from cdm_kumc_calamus.encounter;

select count(*) from cdm_kumc_calamus.encounter;
-- This number should be less than the result of the previous query

select count(*) from cdm_kumc_calamus.encounter where encounterid < 0;
-- it should be 0 otherwise we need to look at the data once

select * from cms_id.visit_number_ranges;
-- This will give us an understanding of how many patient_nums have been used
-- Looks like we can use the KUMC CDM ENCOUNTERIDs as they are.

select count(*) from cdm_kumc_calamus.encounter;

select max(cast(encounterid as number))- min(cast(encounterid as number)) 
from cdm_kumc_calamus.encounter;

drop table cdm_kumc_calamus.encounter_int;
create table cdm_kumc_calamus.encounter_int
as select 
cast(ed.encounterid as number)-(1)+400000000 encounterid,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+20000000)) patid,
ed.admit_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) admit_date, 
admit_time,
ed.discharge_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) discharge_date,
discharge_time,
providerid,
facility_location,
enc_type,
facilityid,
discharge_disposition,
discharge_status,
drg,
drg_type,
admitting_source,
raw_siteid,
raw_enc_type,
raw_discharge_disposition,
raw_discharge_status,
raw_drg_type,
raw_admitting_source
from cdm_kumc_calamus.encounter ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id.cms_cdm_kumc_mapping  
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
cdm_kumc_calamus.encounter_int;
-- This count should match the one below

select count(*) from 
cdm_kumc_calamus.encounter;

select count(distinct patid) from 
cdm_kumc_calamus.encounter_int;
-- This count should match the one below

select count(distinct patid) from 
cdm_kumc_calamus.encounter;

select count(distinct patid) from 
cdm_kumc_calamus.encounter_int
where patid>=20000000; 
-- KUMC patients who do not have GROUSE data. 

select count(distinct patid) from 
cdm_kumc_calamus.encounter_int
where patid < 20000000; 
-- KUMC patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients
