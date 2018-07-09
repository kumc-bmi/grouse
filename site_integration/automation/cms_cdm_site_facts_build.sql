-- cms_cdm_site_facts_build.sql: create new cms tables
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against identified GROUSE server.
--==============================================================================
-- Vairables
--==============================================================================
/*
--undefine out_cms_site_mapping;
variables for KUMC
"&&I2B2_SITE_SCHEMA"     = BLUEHERONDATA_KUMC_CALAMUS
"&&CDM_SITE_SCHEMA"      = CDM_KUMC_CALAMUS_C4R3
"&&out_cms_site_mapping" = cms_cdm_kumc_mapping
--
&&SITE_PATDIM_PATNUM_MIN = 1
&&SITE_PATNUM_START      = 22000000 
&&SITE_PATNUM_END        = 29999999 
--
&&SITE_ENCDIM_ENCNUM_MIN = 1
&&SITE_ENCNUM_START      = 400000000
&&SITE_ENCNUM_END        = 460000000
*/
whenever sqlerror exit;
set echo on; 
--==============================================================================
-- CONDITION
--==============================================================================
whenever sqlerror continue;
drop table "&&CDM_SITE_SCHEMA".condition_int;
whenever sqlerror exit;
create table "&&CDM_SITE_SCHEMA".condition_int
as select 
conditionid,
cast(ed.encounterid as number)-(&&SITE_ENCDIM_ENCNUM_MIN)+&&SITE_ENCNUM_START encounterid,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(&&SITE_PATDIM_PATNUM_MIN))+&&SITE_PATNUM_START)) patid,
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
from "&&CDM_SITE_SCHEMA".condition ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id."&&out_cms_site_mapping" 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
--==============================================================================
-- DEATH
--==============================================================================
whenever sqlerror continue;
drop table "&&CDM_SITE_SCHEMA".death_int;
whenever sqlerror exit;
create table "&&CDM_SITE_SCHEMA".death_int
as select 
coalesce(dp.bene_id_deid, to_char((ed.patid-(&&SITE_PATDIM_PATNUM_MIN))+&&SITE_PATNUM_START)) patid,
ed.death_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) death_date, 
death_date_impute,
death_source,
death_match_confidence
from "&&CDM_SITE_SCHEMA".death ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id."&&out_cms_site_mapping" 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
--==============================================================================
-- DEATH_CAUSE
--==============================================================================
whenever sqlerror continue;
drop table "&&CDM_SITE_SCHEMA".death_cause_int;
whenever sqlerror exit;
create table "&&CDM_SITE_SCHEMA".death_cause_int
as select * from "&&CDM_SITE_SCHEMA".death_cause;
--==============================================================================
-- diagnosis_int
--==============================================================================
whenever sqlerror continue;
drop table "&&CDM_SITE_SCHEMA".diagnosis_int;
whenever sqlerror exit;
create table "&&CDM_SITE_SCHEMA".diagnosis_int
as select 
diagnosisid,
cast(ed.encounterid as number)-(&&SITE_ENCDIM_ENCNUM_MIN)+&&SITE_ENCNUM_START encounterid,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(&&SITE_PATDIM_PATNUM_MIN))+&&SITE_PATNUM_START)) patid,
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
from "&&CDM_SITE_SCHEMA".diagnosis ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id."&&out_cms_site_mapping" 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
--==============================================================================
-- DESPENSING_INT
--==============================================================================
whenever sqlerror continue;
drop table "&&CDM_SITE_SCHEMA".dispensing_int;
whenever sqlerror exit;
create table "&&CDM_SITE_SCHEMA".dispensing_int
as select 
dispensingid,
coalesce(dp.bene_id_deid, to_char((ed.patid-(&&SITE_PATDIM_PATNUM_MIN))+&&SITE_PATNUM_START)) patid,
prescribingid,
ed.dispense_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) dispense_date,
ndc,
dispense_sup,
dispense_amt,
raw_ndc
from "&&CDM_SITE_SCHEMA".dispensing ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id."&&out_cms_site_mapping" 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
--==============================================================================
-- enrollment
--==============================================================================
whenever sqlerror continue;
drop table "&&CDM_SITE_SCHEMA".enrollment_int;
whenever sqlerror exit;
create table "&&CDM_SITE_SCHEMA".enrollment_int
as select 
coalesce(dp.bene_id_deid, to_char((ed.patid-(&&SITE_PATDIM_PATNUM_MIN))+&&SITE_PATNUM_START)) patid,
ed.enr_start_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) enr_start_date,
ed.enr_end_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) enr_end_date,
chart,
enr_basis,
raw_chart,
raw_basis
from "&&CDM_SITE_SCHEMA".enrollment ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id."&&out_cms_site_mapping" 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
--==============================================================================
-- LAB_RESULT_CM
--==============================================================================
whenever sqlerror continue;
drop table "&&CDM_SITE_SCHEMA".lab_result_cm_int;
whenever sqlerror exit;
create table "&&CDM_SITE_SCHEMA".lab_result_cm_int
as select 
lab_result_cm_id,
cast(ed.encounterid as number)-(&&SITE_ENCDIM_ENCNUM_MIN)+&&SITE_ENCNUM_START encounterid,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(&&SITE_PATDIM_PATNUM_MIN))+&&SITE_PATNUM_START)) patid,
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
-- norm_modifier_high, -- Not for availble for KUMC
abn_ind,
raw_lab_name,
raw_lab_code,
raw_panel,
raw_result,
raw_unit,
raw_order_dept,
raw_facility_code
from "&&CDM_SITE_SCHEMA".lab_result_cm ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id."&&out_cms_site_mapping" 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
--==============================================================================
-- PCORNET_TRIAL
--==============================================================================
whenever sqlerror continue;
drop table "&&CDM_SITE_SCHEMA".pcornet_trial_int;
whenever sqlerror exit;
create table "&&CDM_SITE_SCHEMA".pcornet_trial_int
as select 
coalesce(dp.bene_id_deid, to_char((ed.patid-(&&SITE_PATDIM_PATNUM_MIN))+&&SITE_PATNUM_START)) patid,
trialid,
participantid,
trial_siteid,
ed.trial_enroll_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) trial_enroll_date,
ed.trial_end_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) trial_end_date,
ed.trial_withdraw_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) trial_withdraw_date,
trial_invite_code
from "&&CDM_SITE_SCHEMA".pcornet_trial ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id."&&out_cms_site_mapping" 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
--==============================================================================
-- PRESCRIBING
--==============================================================================
whenever sqlerror continue;
drop table "&&CDM_SITE_SCHEMA".prescribing_int;
whenever sqlerror exit;
create table "&&CDM_SITE_SCHEMA".prescribing_int
as select 
prescribingid,
cast(ed.encounterid as number)-(&&SITE_ENCDIM_ENCNUM_MIN)+&&SITE_ENCNUM_START encounterid,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(&&SITE_PATDIM_PATNUM_MIN))+&&SITE_PATNUM_START)) patid,
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
from "&&CDM_SITE_SCHEMA".prescribing ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id."&&out_cms_site_mapping" 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
--==============================================================================
-- PRO_CM
--==============================================================================
whenever sqlerror continue;
drop table "&&CDM_SITE_SCHEMA".pro_cm_int;
whenever sqlerror exit;
create table "&&CDM_SITE_SCHEMA".pro_cm_int
as select * from "&&CDM_SITE_SCHEMA".pro_cm;
--==============================================================================
-- procedures
--==============================================================================
whenever sqlerror continue;
drop table "&&CDM_SITE_SCHEMA".procedures_int;
whenever sqlerror exit;
create table "&&CDM_SITE_SCHEMA".procedures_int
as select 
proceduresid,
cast(ed.encounterid as number)-(&&SITE_ENCDIM_ENCNUM_MIN)+&&SITE_ENCNUM_START encounterid,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(&&SITE_PATDIM_PATNUM_MIN))+&&SITE_PATNUM_START)) patid,
enc_type,
ed.admit_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) admit_date, 
providerid,
ed.px_date - nvl(dp.bh_date_shift_days,0) + nvl(dp.cms_date_shift_days,0) px_date, 
px,
px_type,
px_source,
raw_px,
raw_px_type
from "&&CDM_SITE_SCHEMA".procedures ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id."&&out_cms_site_mapping" 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
--==============================================================================
-- vital
--==============================================================================
whenever sqlerror continue;
drop table "&&CDM_SITE_SCHEMA".vital_int;
whenever sqlerror exit;
create table "&&CDM_SITE_SCHEMA".vital_int
as select 
vitalid,
cast(ed.encounterid as number)-(&&SITE_ENCDIM_ENCNUM_MIN)+&&SITE_ENCNUM_START encounterid,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(&&SITE_PATDIM_PATNUM_MIN))+&&SITE_PATNUM_START)) patid,
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
from "&&CDM_SITE_SCHEMA".vital ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months, bh_dob_date_shift
from cms_id."&&out_cms_site_mapping" 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
--==============================================================================
-- harvest
--==============================================================================
whenever sqlerror continue;
drop table "&&CDM_SITE_SCHEMA".harvest_int;
whenever sqlerror exit;
create table "&&CDM_SITE_SCHEMA".harvest_int
as
select * from "&&CDM_SITE_SCHEMA".harvest;
--==============================================================================
-- VERIFICATION
--==============================================================================
--==============================================================================
-- CONDITION VERIFICATION
--==============================================================================
SELECT 
case 
  when ( select count(*) from "&&CDM_SITE_SCHEMA".condition_int )   
    =  ( select count(*) from "&&CDM_SITE_SCHEMA".condition) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
SELECT 
case 
  when ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".condition_int)   
    =  ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".condition) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
with pat_w_grouse
  As
  (
  	select count(distinct patid) w_grouse from 
	"&&CDM_SITE_SCHEMA".condition_int
	where patid < &&SITE_PATNUM_START
  ),
pat_wo_grouse
  As
  (
	select count(distinct patid) wo_grouse from 
	"&&CDM_SITE_SCHEMA".condition_int
	where patid between &&SITE_PATNUM_START and &&SITE_ENCNUM_START
  ),
total_raw_patient
  As
  (
  select count(distinct (patid)) total_raw_patient
  from "&&CDM_SITE_SCHEMA".condition_int
  )
select w_grouse, wo_grouse, w_grouse+wo_grouse total_patient, total_raw_patient,
case
  when  w_grouse+wo_grouse = total_raw_patient then 1 else 1/0
END PASS_FAILS
from pat_w_grouse,pat_wo_grouse,total_raw_patient
;
--==============================================================================
-- DEATH VERIFICATION
--==============================================================================
SELECT 
case 
  when ( select count(*) from "&&CDM_SITE_SCHEMA".death_int )   
    =  ( select count(*) from "&&CDM_SITE_SCHEMA".death) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
SELECT 
case 
  when ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".death_int)   
    =  ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".death) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
with pat_w_grouse
  As
  (
  	select count(distinct patid) w_grousefrom 
	"&&CDM_SITE_SCHEMA".death_int
	where patid < &&SITE_PATNUM_START
  ),
pat_wo_grouse
  As
  (
	select count(distinct patid) wo_grouse from 
	"&&CDM_SITE_SCHEMA".death_int
	where patid between &&SITE_PATNUM_START and &&SITE_ENCNUM_START
  ),
total_raw_patient
  As
  (
  select count(distinct (patid)) total_raw_patient
  from "&&CDM_SITE_SCHEMA".death_int
  )
select w_grouse, wo_grouse, w_grouse+wo_grouse total_patient, total_raw_patient,
case
  when  w_grouse+wo_grouse = total_raw_patient then 1 else 1/0
END PASS_FAILS
from pat_w_grouse,pat_wo_grouse,total_raw_patient
;
--==============================================================================
-- DIAGNOSIS VERIFICATION
--==============================================================================
SELECT 
case 
  when ( select count(*) from "&&CDM_SITE_SCHEMA".diagnosis_int )   
    =  ( select count(*) from "&&CDM_SITE_SCHEMA".diagnosis) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
SELECT 
case 
  when ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".diagnosis_int)   
    =  ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".diagnosis) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
with pat_w_grouse
  As
  (
  	select count(distinct patid) w_grouse from 
	"&&CDM_SITE_SCHEMA".diagnosis_int
	where patid < &&SITE_PATNUM_START
  ),
pat_wo_grouse
  As
  (
	select count(distinct patid) wo_grouse from 
	"&&CDM_SITE_SCHEMA".diagnosis_int
	where patid between &&SITE_PATNUM_START and &&SITE_ENCNUM_START
  ),
total_raw_patient
  As
  (
  select count(distinct (patid)) total_raw_patient
  from "&&CDM_SITE_SCHEMA".diagnosis_int
  )
select w_grouse, wo_grouse, w_grouse+wo_grouse total_patient, total_raw_patient,
case
  when  w_grouse+wo_grouse = total_raw_patient then 1 else 1/0
END PASS_FAILS
from pat_w_grouse,pat_wo_grouse,total_raw_patient
;
--==============================================================================
-- DISPENSING VERIFICATION
--==============================================================================
SELECT 
case 
  when ( select count(*) from "&&CDM_SITE_SCHEMA".dispensing_int )   
    =  ( select count(*) from "&&CDM_SITE_SCHEMA".dispensing) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
SELECT 
case 
  when ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".dispensing_int)   
    =  ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".dispensing) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
with pat_w_grouse
  As
  (
  	select count(distinct patid) w_grouse from 
	"&&CDM_SITE_SCHEMA".dispensing_int
	where patid < &&SITE_PATNUM_START
  ),
pat_wo_grouse
  As
  (
	select count(distinct patid) wo_grouse from 
	"&&CDM_SITE_SCHEMA".dispensing_int
	where patid between &&SITE_PATNUM_START and &&SITE_ENCNUM_START
  ),
total_raw_patient
  As
  (
  select count(distinct (patid)) total_raw_patient
  from "&&CDM_SITE_SCHEMA".dispensing_int
  )
select w_grouse, wo_grouse, w_grouse+wo_grouse total_patient, total_raw_patient,
case
  when  w_grouse+wo_grouse = total_raw_patient then 1 else 1/0
END PASS_FAILS
from pat_w_grouse,pat_wo_grouse,total_raw_patient
;
--==============================================================================
-- ENROLLMENT VERIFICATION
--==============================================================================
SELECT 
case 
  when ( select count(*) from "&&CDM_SITE_SCHEMA".enrollment_int )   
    =  ( select count(*) from "&&CDM_SITE_SCHEMA".enrollment) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
SELECT 
case 
  when ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".enrollment_int)   
    =  ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".enrollment) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
with pat_w_grouse
  As
  (
  	select count(distinct patid) w_grouse from 
	"&&CDM_SITE_SCHEMA".enrollment_int
	where patid < &&SITE_PATNUM_START
  ),
pat_wo_grouse
  As
  (
	select count(distinct patid) wo_grouse from 
	"&&CDM_SITE_SCHEMA".enrollment_int
	where patid between &&SITE_PATNUM_START and &&SITE_ENCNUM_START
  ),
total_raw_patient
  As
  (
  select count(distinct (patid)) total_raw_patient
  from "&&CDM_SITE_SCHEMA".enrollment_int
  )
select w_grouse, wo_grouse, w_grouse+wo_grouse total_patient, total_raw_patient,
case
  when  w_grouse+wo_grouse = total_raw_patient then 1 else 1/0
END PASS_FAILS
from pat_w_grouse,pat_wo_grouse,total_raw_patient
;
--==============================================================================
-- LAB_RESULT_CM VERIFICATION
--==============================================================================
SELECT 
case 
  when ( select count(*) from "&&CDM_SITE_SCHEMA".lab_result_cm_int )   
    =  ( select count(*) from "&&CDM_SITE_SCHEMA".lab_result_cm) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
SELECT 
case 
  when ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".lab_result_cm_int)   
    =  ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".lab_result_cm) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
with pat_w_grouse
  As
  (
  	select count(distinct patid) w_grouse from 
	"&&CDM_SITE_SCHEMA".lab_result_cm_int
	where patid < &&SITE_PATNUM_START
  ),
pat_wo_grouse
  As
  (
	select count(distinct patid) wo_grouse from 
	"&&CDM_SITE_SCHEMA".lab_result_cm_int
	where patid between &&SITE_PATNUM_START and &&SITE_ENCNUM_START
  ),
total_raw_patient
  As
  (
  select count(distinct (patid)) total_raw_patient
  from "&&CDM_SITE_SCHEMA".lab_result_cm_int
  )
select w_grouse, wo_grouse, w_grouse+wo_grouse total_patient, total_raw_patient,
case
  when  w_grouse+wo_grouse = total_raw_patient then 1 else 1/0
END PASS_FAILS
from pat_w_grouse,pat_wo_grouse,total_raw_patient
;
--==============================================================================
-- PCORNET_TRIAL  VERIFICATION
--==============================================================================
SELECT 
case 
  when ( select count(*) from "&&CDM_SITE_SCHEMA".pcornet_trial_int )   
    =  ( select count(*) from "&&CDM_SITE_SCHEMA".pcornet_trial) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
SELECT 
case 
  when ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".pcornet_trial_int)   
    =  ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".pcornet_trial) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
with pat_w_grouse
  As
  (
  	select count(distinct patid) w_grouse from 
	"&&CDM_SITE_SCHEMA".pcornet_trial_int
	where patid < &&SITE_PATNUM_START
  ),
pat_wo_grouse
  As
  (
	select count(distinct patid) wo_grouse from 
	"&&CDM_SITE_SCHEMA".pcornet_trial_int
	where patid between &&SITE_PATNUM_START and &&SITE_ENCNUM_START
  ),
total_raw_patient
  As
  (
  select count(distinct (patid)) total_raw_patient
  from "&&CDM_SITE_SCHEMA".pcornet_trial_int
  )
select w_grouse, wo_grouse, w_grouse+wo_grouse total_patient, total_raw_patient,
case
  when  w_grouse+wo_grouse = total_raw_patient then 1 else 1/0
END PASS_FAILS
from pat_w_grouse,pat_wo_grouse,total_raw_patient
;
--==============================================================================
-- PRESCRIBING VERIFICATION
--==============================================================================
SELECT 
case 
  when ( select count(*) from "&&CDM_SITE_SCHEMA".prescribing_int )   
    =  ( select count(*) from "&&CDM_SITE_SCHEMA".prescribing) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
SELECT 
case 
  when ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".prescribing_int)   
    =  ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".prescribing) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
with pat_w_grouse
  As
  (
  	select count(distinct patid) w_grouse from 
	"&&CDM_SITE_SCHEMA".prescribing_int
	where patid < &&SITE_PATNUM_START
  ),
pat_wo_grouse
  As
  (
	select count(distinct patid) wo_grouse from 
	"&&CDM_SITE_SCHEMA".prescribing_int
	where patid between &&SITE_PATNUM_START and &&SITE_ENCNUM_START
  ),
total_raw_patient
  As
  (
  select count(distinct (patid)) total_raw_patient
  from "&&CDM_SITE_SCHEMA".prescribing_int
  )
select w_grouse, wo_grouse, w_grouse+wo_grouse total_patient, total_raw_patient,
case
  when  w_grouse+wo_grouse = total_raw_patient then 1 else 1/0
END PASS_FAILS
from pat_w_grouse,pat_wo_grouse,total_raw_patient
;
--==============================================================================
-- PROCEDURES VERIFICATION
--==============================================================================
SELECT 
case 
  when ( select count(*) from "&&CDM_SITE_SCHEMA".procedures_int )   
    =  ( select count(*) from "&&CDM_SITE_SCHEMA".procedures) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
SELECT 
case 
  when ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".procedures_int)   
    =  ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".procedures) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
with pat_w_grouse
  As
  (
  	select count(distinct patid) w_grouse from 
	"&&CDM_SITE_SCHEMA".procedures_int
	where patid < &&SITE_PATNUM_START
  ),
pat_wo_grouse
  As
  (
	select count(distinct patid) wo_grouse from 
	"&&CDM_SITE_SCHEMA".procedures_int
	where patid between &&SITE_PATNUM_START and &&SITE_ENCNUM_START
  ),
total_raw_patient
  As
  (
  select count(distinct (patid)) total_raw_patient
  from "&&CDM_SITE_SCHEMA".procedures_int
  )
select w_grouse, wo_grouse, w_grouse+wo_grouse total_patient, total_raw_patient,
case
  when  w_grouse+wo_grouse = total_raw_patient then 1 else 1/0
END PASS_FAILS
from pat_w_grouse,pat_wo_grouse,total_raw_patient
;
--==============================================================================
-- VITAL VERIFICATION
--==============================================================================
SELECT 
case 
  when ( select count(*) from "&&CDM_SITE_SCHEMA".vital_int )   
    =  ( select count(*) from "&&CDM_SITE_SCHEMA".vital) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
SELECT 
case 
  when ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".vital_int)   
    =  ( select count(distinct patid) from "&&CDM_SITE_SCHEMA".vital) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
with pat_w_grouse
  As
  (
  	select count(distinct patid) w_grouse from 
	"&&CDM_SITE_SCHEMA".vital_int
	where patid < &&SITE_PATNUM_START
  ),
pat_wo_grouse
  As
  (
	select count(distinct patid) wo_grouse from 
	"&&CDM_SITE_SCHEMA".vital_int
	where patid between &&SITE_PATNUM_START and &&SITE_ENCNUM_START
  ),
total_raw_patient
  As
  (
  select count(distinct (patid)) total_raw_patient
  from "&&CDM_SITE_SCHEMA".vital_int
  )
select w_grouse, wo_grouse, w_grouse+wo_grouse total_patient, total_raw_patient,
case
  when  w_grouse+wo_grouse = total_raw_patient then 1 else 1/0
END PASS_FAILS
from pat_w_grouse,pat_wo_grouse,total_raw_patient
;
