-- cms_cdm_kumc_facts_build.sql: create new cms tables
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against identified GROUSE server. 

-- ========== CONDITION

drop table cdm_kumc_calamus.condition_int;
create table cdm_kumc_calamus.condition_int
as select 
conditionid,
cast(ed.encounterid as number)-(1)+400000000 encounterid,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+20000000)) patid,
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
from cdm_kumc_calamus.condition ed
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

-- ========== DEATH

drop table cdm_kumc_calamus.death_int;
create table cdm_kumc_calamus.death_int
as select 
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+20000000)) patid,
ed.death_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) death_date, 
death_date_impute,
death_source,
death_match_confidence
from cdm_kumc_calamus.death ed
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

-- ========== DEATH_CAUSE

create table cdm_kumc_calamus.death_cause_int
as select * from cdm_kumc_calamus.death_cause;

-- ========== DIAGNOSIS

drop table cdm_kumc_calamus.diagnosis_int;
create table cdm_kumc_calamus.diagnosis_int
as select 
diagnosisid,
cast(ed.encounterid as number)-(1)+400000000 encounterid,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+20000000)) patid,
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
from cdm_kumc_calamus.diagnosis ed
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

-- ========== DISPENSING

drop table cdm_kumc_calamus.dispensing_int;
create table cdm_kumc_calamus.dispensing_int
as select 
dispensingid,
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+20000000)) patid,
prescribingid,
ed.dispense_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) dispense_date,
ndc,
dispense_sup,
dispense_amt,
raw_ndc
from cdm_kumc_calamus.dispensing ed
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

-- ========== ENROLLMENT

drop table cdm_kumc_calamus.enrollment_int;
create table cdm_kumc_calamus.enrollment_int
as select 
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+20000000)) patid,
ed.enr_start_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) enr_start_date,
ed.enr_end_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) enr_end_date,
chart,
enr_basis,
raw_chart,
raw_basis
from cdm_kumc_calamus.enrollment ed
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

-- ========== LAB_RESULT_CM  

drop table cdm_kumc_calamus.lab_result_cm_int;
create table cdm_kumc_calamus.lab_result_cm_int
as select 
lab_result_cm_id,
cast(ed.encounterid as number)-(1)+400000000 encounterid,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+20000000)) patid,
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
from cdm_kumc_calamus.lab_result_cm ed
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

-- ========== PCORNET_TRIAL

drop table cdm_kumc_calamus.pcornet_trial_int;
create table cdm_kumc_calamus.pcornet_trial_int
as select 
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+20000000)) patid,
trialid,
participantid,
trial_siteid,
ed.trial_enroll_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) trial_enroll_date,
ed.trial_end_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) trial_end_date,
ed.trial_withdraw_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) trial_withdraw_date,
trial_invite_code
from cdm_kumc_calamus.pcornet_trial ed
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

-- ========== PRESCRIBING

drop table cdm_kumc_calamus.prescribing_int;
create table cdm_kumc_calamus.prescribing_int
as select 
prescribingid,
cast(ed.encounterid as number)-(1)+400000000 encounterid,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+20000000)) patid,
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
from cdm_kumc_calamus.prescribing ed
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

-- ========== PRO_CM

drop table cdm_kumc_calamus.pro_cm_int;
create table cdm_kumc_calamus.pro_cm_int
as select * from cdm_kumc_calamus.pro_cm;

-- ========== PRO_CM

drop table cdm_kumc_calamus.procedures_int;
create table cdm_kumc_calamus.procedures_int
as select 
proceduresid,
cast(ed.encounterid as number)-(1)+400000000 encounterid,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+20000000)) patid,
enc_type,
ed.admit_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) admit_date, 
providerid,
ed.px_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) px_date, 
px,
px_type,
px_source,
raw_px,
raw_px_type
from cdm_kumc_calamus.procedures ed
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

-- ========== VITAL

drop table cdm_kumc_calamus.vital_int;
create table cdm_kumc_calamus.vital_int
as select 
vitalid,
cast(ed.encounterid as number)-(1)+400000000 encounterid,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(1))+20000000)) patid,
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
from cdm_kumc_calamus.vital ed
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

-- ========== HARVEST

create table cdm_kumc_calamus.harvest_int
as
select * from cdm_kumc_calamus.harvest;

-- ========== DEATH_CAUSE

create table cdm_kumc_calamus.death_cause_int
as
select * from cdm_kumc_calamus.death_cause;

-- ========== PRO_CM

drop table cdm_kumc_calamus.pro_cm_int;
create table cdm_kumc_calamus.pro_cm_int
as select * from cdm_kumc_calamus.pro_cm;

-- ========== CONDITION VERIFICATION

select count(*) from 
cdm_kumc_calamus.condition_int;
-- This count should match the one below

select count(*) from 
cdm_kumc_calamus.condition;

select count(distinct patid) from 
cdm_kumc_calamus.condition_int;
-- This count should match the one below

select count(distinct patid) from 
cdm_kumc_calamus.condition;

select count(distinct patid) from 
cdm_kumc_calamus.condition_int
where patid < 20000000; 
-- KUMC patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
cdm_kumc_calamus.condition_int
where patid between 20000000 and 40000000; 
-- KUMC patients who do not have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== DEATH VERIFICATION

select count(*) from 
cdm_kumc_calamus.death_int;
-- This count should match the one below

select count(*) from 
cdm_kumc_calamus.death;

select count(distinct patid) from 
cdm_kumc_calamus.death_int;
-- This count should match the one below

select count(distinct patid) from 
cdm_kumc_calamus.death;

select count(distinct patid) from 
cdm_kumc_calamus.death_int
where patid>=20000000; 
-- KUMC patients who do not have GROUSE data. 

select count(distinct patid) from 
cdm_kumc_calamus.death_int
where patid <20000000; 
-- KUMC patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== DIAGNOSIS VERIFICATION

select count(*) from 
cdm_kumc_calamus.diagnosis_int;
-- This count should match the one below

select count(*) from 
cdm_kumc_calamus.diagnosis;

select count(distinct patid) from 
cdm_kumc_calamus.diagnosis_int;
-- This count should match the one below

select count(distinct patid) from 
cdm_kumc_calamus.diagnosis;

select count(distinct patid) from 
cdm_kumc_calamus.diagnosis_int
where patid>=20000000; 
-- KUMC patients who do not have GROUSE data. 

select count(distinct patid) from 
cdm_kumc_calamus.diagnosis_int
where patid < 20000000; 
-- KUMC patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== DISPENSING VERIFICATION

select count(*) from 
cdm_kumc_calamus.dispensing_int;
-- This count should match the one below

select count(*) from 
cdm_kumc_calamus.dispensing;

select count(distinct patid) from 
cdm_kumc_calamus.dispensing_int;
-- This count should match the one below

select count(distinct patid) from 
cdm_kumc_calamus.dispensing;

select count(distinct patid) from 
cdm_kumc_calamus.dispensing_int
where patid>=20000000; 
-- KUMC patients who do not have GROUSE data. 

select count(distinct patid) from 
cdm_kumc_calamus.dispensing_int
where patid < 20000000; 
-- KUMC patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== ENROLLMENT VERIFICATION

select count(*) from 
cdm_kumc_calamus.enrollment_int;
-- This count should match the one below

select count(*) from 
cdm_kumc_calamus.enrollment;

select count(distinct patid) from 
cdm_kumc_calamus.enrollment_int;
-- This count should match the one below

select count(distinct patid) from 
cdm_kumc_calamus.enrollment;

select count(distinct patid) from 
cdm_kumc_calamus.enrollment_int
where patid>=20000000; 
-- KUMC patients who do not have GROUSE data. 

select count(distinct patid) from
cdm_kumc_calamus.enrollment_int
where patid between 20000000 and 30000000; 

select count(distinct patid) from 
cdm_kumc_calamus.enrollment_int
where patid < 20000000; 
-- KUMC patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== LAB_RESULT_CM VERIFICATION

select * from 
cdm_kumc_calamus.lab_result_cm_int;

select count(*) from 
cdm_kumc_calamus.lab_result_cm_int;
-- This count should match the one below

select count(*) from 
cdm_kumc_calamus.lab_result_cm;

select count(distinct patid) from 
cdm_kumc_calamus.lab_result_cm_int;
-- This count should match the one below

select count(distinct patid) from 
cdm_kumc_calamus.lab_result_cm;

select count(distinct patid) from 
cdm_kumc_calamus.lab_result_cm_int
where patid>=20000000; 
-- KUMC patients who do not have GROUSE data.

select count(distinct patid) from 
cdm_kumc_calamus.lab_result_cm_int
where patid < 20000000; 
-- KUMC patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== PCORNET_TRIAL  VERIFICATION

select count(*) from 
cdm_kumc_calamus.pcornet_trial_int;
-- This count should match the one below

select count(*) from 
cdm_kumc_calamus.pcornet_trial;

select count(distinct patid) from 
cdm_kumc_calamus.pcornet_trial_int;
-- This count should match the one below

select count(distinct patid) from 
cdm_kumc_calamus.pcornet_trial;

select count(distinct patid) from 
cdm_kumc_calamus.pcornet_trial_int
where patid>=20000000; 
-- KUMC patients who do not have GROUSE data. 

select count(distinct patid) from 
cdm_kumc_calamus.pcornet_trial_int
where patid < 20000000; 
-- KUMC patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== PRESCRIBING VERIFICATION

select count(*) from 
cdm_kumc_calamus.prescribing_int;
-- This count should match the one below

select count(*) from 
cdm_kumc_calamus.prescribing;

select count(distinct patid) from 
cdm_kumc_calamus.prescribing_int;
-- This count should match the one below

select count(distinct patid) from 
cdm_kumc_calamus.prescribing;

select count(distinct patid) from 
cdm_kumc_calamus.prescribing_int
where patid between 20000000 and 30000000; 
-- KUMC patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
cdm_kumc_calamus.prescribing_int
where patid < 20000000; 
-- KUMC patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== PROCEDURES VERIFICATION

select count(*) from 
cdm_kumc_calamus.procedures;
-- This count should match the one below

select count(*) from 
cdm_kumc_calamus.procedures_int;

select count(distinct patid) from 
cdm_kumc_calamus.procedures;

select count(distinct patid) from 
cdm_kumc_calamus.procedures_int; 
-- KUMC patients who do not have GROUSE data. 

select count(distinct patid) from 
cdm_kumc_calamus.procedures_int
where patid between 20000000 and 30000000; 
-- KUMC patients who do not have GROUSE data. 

select count(distinct patid) from 
cdm_kumc_calamus.procedures_int
where patid < 20000000; 
-- KUMC patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== VITAL VERIFICATION

select count(*) from 
cdm_kumc_calamus.vital;
-- This count should match the one below

select count(*) from 
cdm_kumc_calamus.vital_int;

select count(distinct patid) from 
cdm_kumc_calamus.vital;
-- This count should match the one below

select count(distinct patid) from 
cdm_kumc_calamus.vital_int;

select count(distinct patid) from 
cdm_kumc_calamus.vital_int
where patid between 20000000 and 30000000; 
-- KUMC patients who do not have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
cdm_kumc_calamus.vital_int
where patid < 20000000; 
-- KUMC patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients
