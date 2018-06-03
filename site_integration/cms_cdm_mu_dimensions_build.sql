-- cms_cdm_mu_build.sql: create new cms tables
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against identified GROUSE server. 

-- ========== PATIENT_DIMENSION
select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.demographic;

select max(cast(patid as number))-min(cast(patid as number)) from mu_cdm.demographic;

-- insert into cms_id.patient_number_ranges values('MU',70000000,72969172);

select count(patid) from  mu_cdm.demographic;

-- This is not applicable as MU i2b2 data is not available
select max(cast(patient_num as number)), min(cast(patient_num as number)) from I2B2DEMODATAUTCRIS.patient_dimension;

select * from cms_id.patient_number_ranges;

select max_patient_number-min_patient_number, site from cms_id.patient_number_ranges;

select count(*) from mu_cdm.demographic;
-- This is to check how many patient_nums are needed for MU

select max(cast(patid as number))- min(cast(patid as number)) from mu_cdm.demographic;
-- This gives us an idea of the range of MU patient_nums

-- MU patient_nums will start at 30000000 and run till 59999999 
drop table mu_cdm.demographic_int purge;
create table 
mu_cdm.demographic_int
as select 
coalesce(dp.bene_id_deid, to_char((pd.patid-(1))+70000000)) patid,
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
mu_cdm.demographic pd
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, cms_dob_shift_months, 
bh_date_shift_days, bh_dob_date_shift 
from cms_id.cms_cdm_mu_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0
and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null))
and ((patient_num=mp_patient_num) or (patient_num is null and mp_patient_num is null))
) dp
on dp.patient_num = pd.patid;

-- ========== VERIFICATION
select count(patid) from 
mu_cdm.demographic_int;
-- This count should match the one below

select count(distinct patid) from 
mu_cdm.demographic_int;

select count(patid) from 
mu_cdm.demographic_int
where patid>=70000000; -- MU patients who do not have GROUSE data. 
-- The difference between the counts in the above two queries should match the intersection

select 608235-511900 from dual;
-- matches the result from cms_cdm_mu_mapping_dedup.sql

select count(patid) from 
mu_cdm.demographic_int
where patid between 70000000 and 80000000; -- MU patients who do not have GROUSE data. 

-- ========== VISIT_DIMENSION
select max(cast(encounterid as number)), min(cast(encounterid as number)) 
from mu_cdm.encounter;

select max(cast(encounterid as number))- min(cast(encounterid as number)) 
from mu_cdm.encounter;

select count(*) from mu_cdm.encounter where encounterid < 0;

select * from cms_id.visit_number_ranges;
-- This will give us an understanding of how many patient_nums have been used
-- Looks like we can use the MU CDM ENCOUNTERIDs as they are.

insert into cms_id.visit_number_ranges(site, min_encounter_number, max_encounter_number) values ('MU_CDM',600000000, 700000000);
-- MU patient_nums will start at 600,000,000 and run till 700,000,000 

select count(*) from mu_cdm.encounter;

select max(cast(encounterid as number))- min(cast(encounterid as number)) 
from mu_cdm.encounter;

drop table mu_cdm.encounter_int;
create table mu_cdm.encounter_int
as select 
cast(ed.encounterid as number)-114337562+600000000 encounterid,
-- encounters for MU patients will be from 600,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+70000000)) patid,
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
from mu_cdm.encounter ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id.cms_cdm_mu_mapping  
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
mu_cdm.encounter_int;
-- This count should match the one below

select count(*) from 
mu_cdm.encounter;

select count(distinct encounterid) from 
mu_cdm.encounter_int
where encounterid>=600000000; -- MU patients who do not have GROUSE data. 
-- This count should match the one below

select count(encounterid) from 
mu_cdm.encounter_int
where encounterid between 600000000 and 700000000; 

select count(distinct patid) from 
mu_cdm.encounter_int;
-- This count should match the one below

select count(distinct patid) from 
mu_cdm.encounter;

select count(distinct patid) from 
mu_cdm.encounter_int
where patid>=70000000; 
-- MU patients who do not have GROUSE data. 

select count(distinct patid) from 
mu_cdm.encounter_int
where patid < 70000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== CONDITION

select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.condition;
-- NULL NULL

create table mu_cdm.condition_int as
select * from mu_cdm.condition;

/*
-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.demographic;

select max(cast(encounterid as number)), min(cast(encounterid as number)) from mu_cdm.condition;
-- This should fall in the range below
select max(cast(encounterid as number)), min(cast(encounterid as number)) from mu_cdm.encounter;

drop table mu_cdm.condition_int;
create table mu_cdm.condition_int
as select 
conditionid,
cast(ed.encounterid as number)-114337562+600000000 encounterid,
-- encounters for mu patients will be from 600,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+70000000)) patid,
ed.report_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) report_date, 
ed.resolve_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) resolve_date,
ed.onset_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) onset_date,
condition_status,
condition,
condition_type,
condition_source,
raw_condition_status,
raw_condition,
raw_condition_type,
raw_condition_source
from mu_cdm.condition ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id.cms_cdm_mu_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
mu_cdm.condition_int;
-- This count should match the one below

select count(*) from 
mu_cdm.condition;

select count(distinct encounterid) from 
mu_cdm.condition
where encounterid>=1000000000; -- MU patients who do not have GROUSE data. 

select count(encounterid) from 
mu_cdm.condition_int
where encounterid between 1000000000 and 2000000000; 

select count(distinct patid) from 
mu_cdm.condition_int;
-- This count should match the one below

select count(distinct patid) from 
mu_cdm.condition;

select count(distinct patid) from 
mu_cdm.condition_int
where patid>=30000000; 
-- MU patients who do not have GROUSE data. 

select count(distinct patid) from 
mu_cdm.condition_int
where patid between 30000000 and 60000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
mu_cdm.condition_int
where patid < 30000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
mu_cdm.condition_int
where patid between 30000000 and 60000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients
*/

-- ========== DEATH
select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.death;

-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.demographic;

drop table mu_cdm.death_int;
create table mu_cdm.death_int
as select 
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+70000000)) patid,
ed.death_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) death_date, 
death_date_impute,
death_source,
death_match_confidence
from mu_cdm.death ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id.cms_cdm_mu_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
mu_cdm.death_int;
-- This count should match the one below

select count(*) from 
mu_cdm.death;

select count(distinct patid) from 
mu_cdm.death_int;
-- This count should match the one below

select count(distinct patid) from 
mu_cdm.death;

select count(distinct patid) from 
mu_cdm.death_int
where patid>=70000000; 
-- MU patients who do not have GROUSE data. 

select count(distinct patid) from 
mu_cdm.death_int
where patid <70000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
mu_cdm.death_int
where patid between 70000000 and 80000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== DEATH_CAUSE
select count(*) from mu_cdm.DEATH_CAUSE;
-- 0 rows

-- ========== DIAGNOSIS
select * from  mu_cdm.diagnosis;

select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.diagnosis;
-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.demographic;

select max(cast(encounterid as number)), min(cast(encounterid as number)) from mu_cdm.diagnosis;
-- This should fall in the range below
select max(cast(encounterid as number)), min(cast(encounterid as number)) from mu_cdm.encounter;

drop table mu_cdm.diagnosis_int;
create table mu_cdm.diagnosis_int
as select 
diagnosisid,
cast(ed.encounterid as number)-114337562+600000000 encounterid,
-- encounters for MU patients will be from 600,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+70000000)) patid,
enc_type,
ed.admit_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) admit_date, 
providerid,
dx,
dx_type,
dx_source,
dx_origin,
pdx,
raw_dx,
raw_dx_type,
raw_dx_source,
raw_origdx,
raw_pdx
from mu_cdm.diagnosis ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id.cms_cdm_mu_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
mu_cdm.diagnosis_int;
-- This count should match the one below

select count(*) from 
mu_cdm.diagnosis;

select count(distinct encounterid) from 
mu_cdm.diagnosis; -- MU patients who do not have GROUSE data. 

select count(distinct encounterid) from 
mu_cdm.diagnosis_int
where encounterid between 600000000 and 700000000; 

select count(distinct patid) from 
mu_cdm.diagnosis_int;
-- This count should match the one below

select count(distinct patid) from 
mu_cdm.diagnosis;

select count(distinct patid) from 
mu_cdm.diagnosis_int
where patid>=70000000; 
-- MU patients who do not have GROUSE data. 

select count(distinct patid) from 
mu_cdm.diagnosis_int
where patid < 70000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
mu_cdm.diagnosis_int
where patid between 70000000 and 80000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== DISPENSING
select * from  mu_cdm.dispensing;

select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.dispensing;
-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.demographic;

drop table mu_cdm.dispensing_int;
create table mu_cdm.dispensing_int
as select 
dispensingid,
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+70000000)) patid,
prescribingid,
ed.dispense_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) dispense_date,
ndc,
dispense_sup,
dispense_amt,
raw_ndc
from mu_cdm.dispensing ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id.cms_cdm_mu_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
mu_cdm.dispensing_int;
-- This count should match the one below

select count(*) from 
mu_cdm.dispensing;

select count(distinct patid) from 
mu_cdm.dispensing_int;
-- This count should match the one below

select count(distinct patid) from 
mu_cdm.dispensing;

select count(distinct patid) from 
mu_cdm.dispensing_int
where patid>=70000000; 
-- MU patients who do not have GROUSE data. 

select count(distinct patid) from 
mu_cdm.dispensing_int
where patid < 70000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== ENROLLMENT
select * from  mu_cdm.enrollment;

select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.enrollment;
-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.demographic;

drop table mu_cdm.enrollment_int;
create table mu_cdm.enrollment_int
as select 
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+70000000)) patid,
ed.enr_start_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) enr_start_date,
ed.enr_end_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) enr_end_date,
chart,
enr_basis,
raw_chart,
raw_basis
from mu_cdm.enrollment ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id.cms_cdm_mu_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
mu_cdm.enrollment_int;
-- This count should match the one below

select count(*) from 
mu_cdm.enrollment;

select count(distinct patid) from 
mu_cdm.enrollment_int;
-- This count should match the one below

select count(distinct patid) from 
mu_cdm.enrollment;

select count(distinct patid) from 
mu_cdm.enrollment_int
where patid>=20000000; 
-- MU patients who do not have GROUSE data. 

select count(distinct patid) from
mu_cdm.enrollment_int
where patid between 70000000 and 72969172; 

select count(distinct patid) from 
mu_cdm.enrollment_int
where patid < 20000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select 502315+96311 from dual;

-- ========== LAB_RESULT_CM  

select * from mu_cdm.lab_result_cm;

select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.lab_result_cm;
-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.demographic;

select max(cast(encounterid as number)), min(cast(encounterid as number)) from mu_cdm.lab_result_cm;
-- This should fall in the range below
select max(cast(encounterid as number)), min(cast(encounterid as number)) from mu_cdm.encounter;

drop table mu_cdm.lab_result_cm_int;
create table mu_cdm.lab_result_cm_int
as select 
lab_result_cm_id,
cast(ed.encounterid as number)-114337562+600000000 encounterid,
-- encounters for MU patients will be from 600,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+70000000)) patid,
lab_name,
specimen_source,
lab_loinc,
priority,
result_loc,
lab_px,
lab_px_type,
ed.lab_order_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) lab_order_date, 
ed.specimen_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) specimen_date, 
specimen_time,
ed.result_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) result_date, 
result_time,
result_qual,
result_num,
result_modifier,
result_unit,
norm_range_low,
norm_modifier_low,
norm_range_high,
norm_modifier_high,
abn_ind,
raw_lab_name,
raw_lab_code,
raw_panel,
raw_result,
raw_unit,
raw_order_dept,
raw_facility_code
from mu_cdm.lab_result_cm ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id.cms_cdm_mu_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select * from 
mu_cdm.lab_result_cm_int;

select count(*) from 
mu_cdm.lab_result_cm_int;
-- This count should match the one below

select count(*) from 
mu_cdm.lab_result_cm;

select min(encounterid), max(encounterid) from 
mu_cdm.lab_result_cm_int;
-- MU patients who do not have GROUSE data. 

select count(distinct encounterid) from 
mu_cdm.lab_result_cm; 
-- This count should match the one below

select count(distinct encounterid) from 
mu_cdm.lab_result_cm_int
where encounterid between 600000000 and 700000000; 
-- MU patients who do not have GROUSE data. 

select count(distinct patid) from 
mu_cdm.lab_result_cm_int;
-- This count should match the one below

select count(distinct patid) from 
mu_cdm.lab_result_cm;

select count(distinct patid) from 
mu_cdm.lab_result_cm_int
where patid>=70000000; 
-- MU patients who do not have GROUSE data. 

select count(distinct patid) from 
mu_cdm.lab_result_cm_int
where patid between 70000000 and 72969172; 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
mu_cdm.lab_result_cm_int
where patid < 20000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== PCORNET_TRIAL

select * from  mu_cdm.pcornet_trial;

select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.pcornet_trial;
-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.demographic;

drop table mu_cdm.pcornet_trial_int;
create table mu_cdm.pcornet_trial_int
as select 
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+70000000)) patid,
trialid,
participantid,
trial_siteid,
ed.trial_enroll_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) trial_enroll_date,
ed.trial_end_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) trial_end_date,
ed.trial_withdraw_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) trial_withdraw_date,
trial_invite_code
from mu_cdm.pcornet_trial ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id.cms_cdm_mu_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
mu_cdm.pcornet_trial_int;
-- This count should match the one below

select count(*) from 
mu_cdm.pcornet_trial;

select count(distinct patid) from 
mu_cdm.pcornet_trial_int;
-- This count should match the one below

select count(distinct patid) from 
mu_cdm.pcornet_trial;

select count(distinct patid) from 
mu_cdm.pcornet_trial_int
where patid>=70000000; 
-- MU patients who do not have GROUSE data. 

select count(distinct patid) from 
mu_cdm.pcornet_trial_int
where patid between 70000000 and 72969172; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
mu_cdm.pcornet_trial_int
where patid < 20000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== PRESCRIBING

select * from  mu_cdm.prescribing;

select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.prescribing;
-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.demographic;

select max(cast(encounterid as number)), min(cast(encounterid as number)) from mu_cdm.prescribing;
-- This should fall in the range below
select max(cast(encounterid as number)), min(cast(encounterid as number)) from mu_cdm.encounter;

drop table mu_cdm.prescribing_int;
create table mu_cdm.prescribing_int
as select 
prescribingid,
cast(ed.encounterid as number)-114337562+600000000 encounterid,
-- encounters for MU patients will be from 600,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+70000000)) patid,
rx_providerid,
ed.rx_order_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) rx_order_date, 
rx_order_time,
ed.rx_start_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) rx_start_date, 
ed.rx_end_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) rx_end_date, 
rx_quantity,
rx_quantity_unit,
rx_refills,
rx_days_supply,
rx_frequency,
rx_basis,
rxnorm_cui,
raw_rx_med_name,
raw_rx_frequency,
raw_rx_quantity,
raw_rx_ndc,
raw_rxnorm_cui
from mu_cdm.prescribing ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id.cms_cdm_mu_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
mu_cdm.prescribing_int;
-- This count should match the one below

select count(*) from 
mu_cdm.prescribing;

select count(distinct encounterid) from 
mu_cdm.prescribing; 
-- MU patients who do not have GROUSE data. 

select count(distinct encounterid) from 
mu_cdm.prescribing_int
where encounterid between 600000000 and 700000000; 

select count(distinct patid) from 
mu_cdm.prescribing_int;
-- This count should match the one below

select count(distinct patid) from 
mu_cdm.prescribing;

select count(distinct patid) from 
mu_cdm.prescribing_int;

select count(distinct patid) from 
mu_cdm.prescribing_int
where patid between 70000000 and 72969172; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
mu_cdm.prescribing_int
where patid < 20000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== PRO_CM

select * from mu_cdm.pro_cm;

drop table mu_cdm.pro_cm_int;
create table mu_cdm.pro_cm_int
as select * from mu_cdm.pro_cm;

/*
select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.pro_cm;
-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.demographic;

select max(cast(encounterid as number)), min(cast(encounterid as number)) from mu_cdm.pro_cm;
-- This should fall in the range below
select max(cast(encounterid as number)), min(cast(encounterid as number)) from mu_cdm.encounter;

drop table mu_cdm.pro_cm_int;
create table mu_cdm.pro_cm_int
as select 

pro_cm_id,
coalesce(dp.bene_id_deid, to_char((ed.patid-(-17199782))+30000000)) patid,
ed.encounterid encounterid,
pro_item,
pro_loinc,
ed.pro_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) pro_date, 
pro_time,
pro_response,
pro_method,
pro_mode,
pro_cat,
raw_pro_code,
raw_pro_response
from mu_cdm.pro_cm ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id.cms_mu_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
mu_cdm.pro_cm_int;
-- This count should match the one below

select count(*) from 
mu_cdm.pro_cm;

select count(distinct encounterid) from 
mu_cdm.pro_cm
where encounterid>=1000000000; -- MU patients who do not have GROUSE data. 

select count(distinct encounterid) from 
mu_cdm.pro_cm
where encounterid between 1000000000 and 2000000000; 

select count(distinct patid) from 
mu_cdm.pro_cm_int;
-- This count should match the one below

select count(distinct patid) from 
mu_cdm.pro_cm;

select count(distinct patid) from 
mu_cdm.pro_cm
where patid>=30000000; 
-- MU patients who do not have GROUSE data. 

select count(distinct patid) from 
mu_cdm.pro_cm_int
where patid between 30000000 and 60000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
mu_cdm.pro_cm_int
where patid < 30000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients
*/

-- ========== PRO_CM

select count(*) from  mu_cdm.procedures;

select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.procedures;
-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.demographic;

select max(cast(encounterid as number)), min(cast(encounterid as number)) from mu_cdm.procedures;
-- This should fall in the range below
select max(cast(encounterid as number)), min(cast(encounterid as number)) from mu_cdm.encounter;

drop table mu_cdm.procedures_int;
create table mu_cdm.procedures_int
as select 
proceduresid,
cast(ed.encounterid as number)-114337562+600000000 encounterid,
-- encounters for MU patients will be from 600,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+70000000)) patid,
enc_type,
ed.admit_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) admit_date, 
providerid,
ed.px_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) px_date, 
px,
px_type,
px_source,
raw_px,
raw_px_type
from mu_cdm.procedures ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id.cms_cdm_mu_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
mu_cdm.procedures;
-- This count should match the one below

select count(*) from 
mu_cdm.procedures_int;

select count(distinct encounterid) from 
mu_cdm.procedures_int;  

select count(distinct encounterid) from 
mu_cdm.procedures
where encounterid between 600000000 and 700000000; 

select count(distinct patid) from 
mu_cdm.procedures_int;
-- This count should match the one below

select count(distinct patid) from 
mu_cdm.procedures;

select count(distinct patid) from 
mu_cdm.procedures_int; 
-- MU patients who do not have GROUSE data. 

select count(distinct patid) from 
mu_cdm.procedures_int
where patid between 70000000 and 72969172; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
mu_cdm.procedures_int
where patid < 20000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== VITAL

select count(*) from  mu_cdm.vital;

select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.vital;
-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from mu_cdm.demographic;

select max(cast(encounterid as number)), min(cast(encounterid as number)) from mu_cdm.vital;
-- This should fall in the range below
select max(cast(encounterid as number)), min(cast(encounterid as number)) from mu_cdm.encounter;

drop table mu_cdm.vital_int;
create table mu_cdm.vital_int
as select 
vitalid,
cast(ed.encounterid as number)-114337562+600000000 encounterid,
-- encounters for MU patients will be from 600,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+70000000)) patid,
ed.measure_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) measure_date,
measure_time,
vital_source,
ht,
wt,
diastolic,
systolic,
original_bmi,
bp_position,
smoking,
tobacco,
tobacco_type,
raw_vital_source,
raw_ht,
raw_wt,
raw_diastolic,
raw_systolic,
raw_bp_position,
raw_smoking,
raw_tobacco,
raw_tobacco_type
from mu_cdm.vital ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id.cms_cdm_mu_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
mu_cdm.vital;
-- This count should match the one below

select count(*) from 
mu_cdm.vital_int;

select count(distinct encounterid) from 
mu_cdm.vital;

select count(distinct encounterid) from 
mu_cdm.vital_int
where encounterid between 600000000 and 700000000; 

select count(distinct patid) from 
mu_cdm.vital;
-- This count should match the one below

select count(distinct patid) from 
mu_cdm.vital_int;

select count(distinct patid) from 
mu_cdm.vital_int
where patid between 70000000 and 72969172; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
mu_cdm.vital_int
where patid < 20000000; 
-- MU patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== HARVEST

create table mu_cdm.harvest_int
as
select * from mu_cdm.harvest;

-- ========== PRO_CM
create table mu_cdm.pro_cm_int
as
select * from mu_cdm.pro_cm;

-- ========== DEATH_CAUSE
create table mu_cdm.death_cause_int
as
select * from mu_cdm.death_cause;
