-- cms_cdm_utsw_dimensions_build.sql: create new cms tables
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against identified GROUSE server. 

-- ========== PATIENT_DIMENSION
select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.demographic;

select max(cast(patid as number))-min(cast(patid as number)) from utsw_cdm.demographic;

select 11465187-(-17199782)+30000000 from dual;
-- 58664969 
-- < 60000000 and therefore still falls out of UNMC's range of patients.
-- May be I should have spaced them further apart.

select count(patid) from  utsw_cdm.demographic;

select max(cast(patient_num as number)), min(cast(patient_num as number)) from I2B2DEMODATAUTCRIS.patient_dimension;

select * from cms_id.patient_number_ranges;
-- UTSW	30000000	60000000

select max_patient_number-min_patient_number, site from cms_id.patient_number_ranges;

select count(*) from utsw_cdm.demographic;
-- This is to check how many patient_nums are needed for UTSW

select max(cast(patient_num as number))- min(cast(patient_num as number)) from utsw_cdm.demographic;
-- This gives us an idea of the range of UTSW patient_nums

-- UTSW patient_nums will start at 30000000 and run till 59999999 
drop table utsw_cdm.demographic_int purge;
create table 
utsw_cdm.demographic_int
as select 
coalesce(dp.bene_id_deid, to_char((pd.PATID-(-17199782))+30000000)) PATID,
add_months(pd.birth_date - (nvl(bh_dob_date_shift,0)+ nvl(dp.BH_DATE_SHIFT_DAYS,0) - nvl(dp.cms_date_shift_days,0)),nvl(cms_dob_shift_months,0)) birth_date, 
BIRTH_TIME,
SEX,
SEXUAL_ORIENTATION,
GENDER_IDENTITY,
HISPANIC,
BIOBANK_FLAG,
RACE,
RAW_SEX,
RAW_SEXUAL_ORIENTATION,
RAW_GENDER_IDENTITY,
RAW_HISPANIC,
RAW_RACE
from 
utsw_cdm.demographic pd
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, cms_dob_shift_months, 
bh_date_shift_days, bh_dob_date_shift 
from cms_id.cms_cdm_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0
and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null))
and ((patient_num=mp_patient_num) or (patient_num is null and mp_patient_num is null))
) dp
on dp.patient_num = pd.patid;

-- ========== VERIFICATION
select count(patid) from 
utsw_cdm.demographic_int;
-- This count should match the one below

select count(distinct patid) from 
utsw_cdm.demographic_int;

select count(patid) from 
utsw_cdm.demographic_int
where patid>=30000000; -- UTSW patients who do not have GROUSE data. 
-- The difference between the counts in the above two queries should match the intersection

select 2754144-2423711 from dual;
-- matches the result from cms_cdm_utsw_mapping_dedup.sql

select count(patid) from 
utsw_cdm.demographic_int
where patid between 30000000 and 60000000; -- UTSW patients who do not have GROUSE data. 

-- ========== VISIT_DIMENSION
select max(cast(encounterid as number)), min(cast(encounterid as number)) 
from utsw_cdm.encounter;

select max(cast(encounterid as number))- min(cast(encounterid as number)) 
from utsw_cdm.encounter;

select count(*) from utsw_cdm.encounter where encounterid < 0;

select * from cms_id.visit_number_ranges;
-- This will give us an understanding of how many patient_nums have been used
-- Looks like we can use the UTSW CDM ENCOUNTERIDs as they are.

-- insert into cms_id.visit_number_ranges(site, min_encounter_number, max_encounter_number) values ('UTSW_CDM',1000000000, 2000000000);

select count(*) from utsw_cdm.encounter;

select max(cast(encounterid as number))- min(cast(encounterid as number)) 
from utsw_cdm.encounter;

drop table utsw_cdm.encounter_int;
create table utsw_cdm.encounter_int
as select 
ed.encounterid ENCOUNTERID,
-- encounters for UTSW patients will be from 1,000,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.PATID-(-17199782))+30000000)) PATID,
ed.ADMIT_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) ADMIT_DATE, 
ADMIT_TIME,
ed.DISCHARGE_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) DISCHARGE_DATE,
DISCHARGE_TIME,
PROVIDERID,
FACILITY_LOCATION,
ENC_TYPE,
FACILITYID,
DISCHARGE_DISPOSITION,
DISCHARGE_STATUS,
DRG,
DRG_TYPE,
ADMITTING_SOURCE,
RAW_SITEID,
RAW_ENC_TYPE,
RAW_DISCHARGE_DISPOSITION,
RAW_DISCHARGE_STATUS,
RAW_DRG_TYPE,
RAW_ADMITTING_SOURCE
from utsw_cdm.encounter ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_cdm_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
utsw_cdm.encounter_int;
-- This count should match the one below

select count(*) from 
utsw_cdm.encounter;

select count(distinct encounterid) from 
utsw_cdm.encounter_int
where encounterid>=1000000000; -- UTSW patients who do not have GROUSE data. 
-- This count should match the one below

select count(encounterid) from 
utsw_cdm.encounter_int
where encounterid between 1000000000 and 2000000000; 

select count(distinct patid) from 
utsw_cdm.encounter_int;
-- This count should match the one below

select count(distinct patid) from 
utsw_cdm.encounter;

select count(distinct patid) from 
utsw_cdm.encounter_int
where patid>=30000000; 
-- UTSW patients who do not have GROUSE data. 

select count(distinct patid) from 
utsw_cdm.encounter_int
where patid < 30000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== CONDITION
select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.condition;
-- This should fall in the range below

select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.demographic;

select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.condition;

-- This should fall in the range below
select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.encounter;

drop table utsw_cdm.condition_int;
create table utsw_cdm.condition_int
as select 
CONDITIONID,
coalesce(dp.bene_id_deid, to_char((ed.PATID-(-17199782))+30000000)) PATID,
ed.encounterid ENCOUNTERID,
ed.REPORT_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) REPORT_DATE, 
ed.RESOLVE_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) RESOLVE_DATE,
ed.ONSET_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) ONSET_DATE,
CONDITION_STATUS,
CONDITION,
CONDITION_TYPE,
CONDITION_SOURCE,
RAW_CONDITION_STATUS,
RAW_CONDITION,
RAW_CONDITION_TYPE,
RAW_CONDITION_SOURCE
from utsw_cdm.condition ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_cdm_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
-- ========== VERIFICATION

select count(*) from 
utsw_cdm.condition_int;
-- This count should match the one below

select count(*) from 
utsw_cdm.condition;

select count(distinct encounterid) from 
utsw_cdm.condition
where encounterid>=1000000000; -- UTSW patients who do not have GROUSE data. 

select count(encounterid) from 
utsw_cdm.condition_int
where encounterid between 1000000000 and 2000000000; 

select count(distinct patid) from 
utsw_cdm.condition_int;
-- This count should match the one below

select count(distinct patid) from 
utsw_cdm.condition;

select count(distinct patid) from 
utsw_cdm.condition_int
where patid>=30000000; 
-- UTSW patients who do not have GROUSE data. 

select count(distinct patid) from 
utsw_cdm.condition_int
where patid < 30000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
utsw_cdm.condition_int
where patid < 30000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
utsw_cdm.condition_int
where patid between 30000000 and 60000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients


-- ========== DEATH
select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.DEATH;

-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.demographic;

select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.condition;

-- This should fall in the range below
select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.encounter;

drop table utsw_cdm.death_int;
create table utsw_cdm.death_int
as select 
coalesce(dp.bene_id_deid, to_char((ed.PATID-(-17199782))+30000000)) PATID,
ed.DEATH_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) DEATH_DATE, 
DEATH_DATE_IMPUTE,
DEATH_SOURCE,
DEATH_MATCH_CONFIDENCE
from utsw_cdm.death ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_cdm_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
-- ========== VERIFICATION

select count(*) from 
utsw_cdm.death_int;
-- This count should match the one below

select count(*) from 
utsw_cdm.death;

select count(distinct patid) from 
utsw_cdm.death_int;
-- This count should match the one below

select count(distinct patid) from 
utsw_cdm.death;

select count(distinct patid) from 
utsw_cdm.death_int
where patid>=30000000; 
-- UTSW patients who do not have GROUSE data. 

select count(distinct patid) from 
utsw_cdm.death_int
where patid < 30000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
utsw_cdm.death_int
where patid between 30000000 and 60000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== DEATH_CAUSE
select count(*) from utsw_cdm.DEATH_CAUSE;
-- 0 rows

-- ========== DIAGNOSIS
select * from  utsw_cdm.diagnosis;

select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.diagnosis;

-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.demographic;

select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.diagnosis;

-- This should fall in the range below
select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.encounter;

drop table utsw_cdm.diagnosis_int;
create table utsw_cdm.diagnosis_int
as select 
DIAGNOSISID,
coalesce(dp.bene_id_deid, to_char((ed.PATID-(-17199782))+30000000)) PATID,
ed.encounterid ENCOUNTERID,
ENC_TYPE,
ed.ADMIT_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) ADMIT_DATE, 
PROVIDERID,
DX,
DX_TYPE,
DX_SOURCE,
DX_ORIGIN,
PDX,
RAW_DX,
RAW_DX_TYPE,
RAW_DX_SOURCE,
RAW_ORIGDX,
RAW_PDX
from utsw_cdm.diagnosis ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_cdm_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
-- ========== VERIFICATION

select count(*) from 
utsw_cdm.diagnosis_int;
-- This count should match the one below

select count(*) from 
utsw_cdm.diagnosis;

select count(distinct encounterid) from 
utsw_cdm.diagnosis
where encounterid>=1000000000; -- UTSW patients who do not have GROUSE data. 

select count(distinct encounterid) from 
utsw_cdm.diagnosis_int
where encounterid between 1000000000 and 2000000000; 

select count(distinct patid) from 
utsw_cdm.diagnosis_int;
-- This count should match the one below

select count(distinct patid) from 
utsw_cdm.diagnosis;

select count(distinct patid) from 
utsw_cdm.diagnosis_int
where patid>=30000000; 
-- UTSW patients who do not have GROUSE data. 

select count(distinct patid) from 
utsw_cdm.diagnosis_int
where patid < 30000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
utsw_cdm.diagnosis_int
where patid between 30000000 and 60000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== DISPENSING
select * from  utsw_cdm.dispensing;

select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.dispensing;

-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.demographic;

drop table utsw_cdm.dispensing_int;
create table utsw_cdm.dispensing_int
as select 
DISPENSINGID,
coalesce(dp.bene_id_deid, to_char((ed.PATID-(-17199782))+30000000)) PATID,
PRESCRIBINGID,
ed.DISPENSE_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) DISPENSE_DATE,
NDC,
DISPENSE_SUP,
DISPENSE_AMT,
RAW_NDC
from utsw_cdm.dispensing ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_cdm_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
-- ========== VERIFICATION

select count(*) from 
utsw_cdm.dispensing_int;
-- This count should match the one below

select count(*) from 
utsw_cdm.dispensing;

select count(distinct patid) from 
utsw_cdm.dispensing_int;
-- This count should match the one below

select count(distinct patid) from 
utsw_cdm.dispensing;

select count(distinct patid) from 
utsw_cdm.dispensing_int
where patid>=30000000; 
-- UTSW patients who do not have GROUSE data. 

select count(distinct patid) from 
utsw_cdm.dispensing_int
where patid < 30000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
utsw_cdm.dispensing_int
where patid between 30000000 and 60000000; 


-- ========== ENROLLMENT
select * from  utsw_cdm.enrollment;

select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.enrollment;

-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.demographic;

drop table utsw_cdm.enrollment_int;
create table utsw_cdm.enrollment_int
as select 
coalesce(dp.bene_id_deid, to_char((ed.PATID-(-17199782))+30000000)) PATID,
ed.ENR_START_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) ENR_START_DATE,
ed.ENR_END_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) ENR_END_DATE,
CHART,
ENR_BASIS,
RAW_CHART,
RAW_BASIS
from utsw_cdm.enrollment ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_cdm_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
-- ========== VERIFICATION

select count(*) from 
utsw_cdm.enrollment_int;
-- This count should match the one below

select count(*) from 
utsw_cdm.enrollment;

select count(distinct patid) from 
utsw_cdm.enrollment_int;
-- This count should match the one below

select count(distinct patid) from 
utsw_cdm.enrollment;

select count(distinct patid) from 
utsw_cdm.enrollment_int
where patid>=30000000; 
-- UTSW patients who do not have GROUSE data. 

select count(distinct patid) from 
utsw_cdm.enrollment_int
where patid between 30000000 and 60000000; 

select count(distinct patid) from 
utsw_cdm.enrollment_int
where patid < 30000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients


-- ========== LAB_RESULT_CM
select * from  utsw_cdm.LAB_RESULT_CM;

select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.LAB_RESULT_CM;

-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.demographic;

select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.LAB_RESULT_CM;

-- This should fall in the range below
select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.encounter;

drop table utsw_cdm.lab_result_cm_int;
create table utsw_cdm.lab_result_cm_int
as select 
LAB_RESULT_CM_ID,
coalesce(dp.bene_id_deid, to_char((ed.PATID-(-17199782))+30000000)) PATID,
ed.encounterid ENCOUNTERID,
LAB_NAME,
SPECIMEN_SOURCE,
LAB_LOINC,
PRIORITY,
RESULT_LOC,
LAB_PX,
LAB_PX_TYPE,
ed.LAB_ORDER_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) LAB_ORDER_DATE, 
ed.SPECIMEN_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) SPECIMEN_DATE, 
SPECIMEN_TIME,
ed.RESULT_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) RESULT_DATE, 
RESULT_TIME,
RESULT_QUAL,
RESULT_NUM,
RESULT_MODIFIER,
RESULT_UNIT,
NORM_RANGE_LOW,
NORM_MODIFIER_LOW,
NORM_RANGE_HIGH,
NORM_MODIFIER_HIGH,
ABN_IND,
RAW_LAB_NAME,
RAW_LAB_CODE,
RAW_PANEL,
RAW_RESULT,
RAW_UNIT,
RAW_ORDER_DEPT,
RAW_FACILITY_CODE
from utsw_cdm.lab_result_cm ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_cdm_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
-- ========== VERIFICATION

select count(*) from 
utsw_cdm.lab_result_cm_int;
-- This count should match the one below

select count(*) from 
utsw_cdm.lab_result_cm;

select count(distinct encounterid) from 
utsw_cdm.lab_result_cm
where encounterid>=1000000000; -- UTSW patients who do not have GROUSE data. 

select count(distinct encounterid) from 
utsw_cdm.lab_result_cm_int
where encounterid between 1000000000 and 2000000000; 

select count(distinct patid) from 
utsw_cdm.lab_result_cm_int;
-- This count should match the one below

select count(distinct patid) from 
utsw_cdm.lab_result_cm;

select count(distinct patid) from 
utsw_cdm.lab_result_cm_int
where patid>=30000000; 
-- UTSW patients who do not have GROUSE data. 

select count(distinct patid) from 
utsw_cdm.lab_result_cm_int
where patid between 30000000 and 60000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
utsw_cdm.lab_result_cm_int
where patid < 30000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== PCORNET_TRIAL
select * from  utsw_cdm.PCORNET_TRIAL;

select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.PCORNET_TRIAL;

-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.demographic;

drop table utsw_cdm.pcornet_trial_int;
create table utsw_cdm.pcornet_trial_int
as select 
coalesce(dp.bene_id_deid, to_char((ed.PATID-(-17199782))+30000000)) PATID,
trialid,
participantid,
trial_siteid,
ed.trial_enroll_date - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) trial_enroll_date,
ed.trial_end_date - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) trial_end_date,
ed.trial_withdraw_date - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) trial_withdraw_date,
trial_invite_code
from utsw_cdm.pcornet_trial ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_cdm_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;
-- ========== VERIFICATION

select count(*) from 
utsw_cdm.pcornet_trial_int;
-- This count should match the one below

select count(*) from 
utsw_cdm.pcornet_trial;

select count(distinct patid) from 
utsw_cdm.pcornet_trial_int;
-- This count should match the one below

select count(distinct patid) from 
utsw_cdm.pcornet_trial;

select count(distinct patid) from 
utsw_cdm.pcornet_trial_int
where patid>=30000000; 
-- UTSW patients who do not have GROUSE data. 

select count(distinct patid) from 
utsw_cdm.pcornet_trial_int
where patid between 30000000 and 60000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
utsw_cdm.pcornet_trial_int
where patid < 30000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients


-- ========== PRESCRIBING
select * from  utsw_cdm.prescribing;

select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.prescribing;

-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.demographic;

select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.prescribing;

-- This should fall in the range below
select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.encounter;

drop table utsw_cdm.prescribing_int;
create table utsw_cdm.prescribing_int
as select 
prescribingid,
coalesce(dp.bene_id_deid, to_char((ed.PATID-(-17199782))+30000000)) patid,
ed.encounterid encounterid,
rx_providerid,
ed.rx_order_date - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) rx_order_date, 
rx_order_time,
ed.rx_start_date - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) rx_start_date, 
ed.rx_end_date - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) rx_end_date, 
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
from utsw_cdm.prescribing ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_cdm_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
utsw_cdm.prescribing_int;
-- This count should match the one below

select count(*) from 
utsw_cdm.prescribing;

select count(distinct encounterid) from 
utsw_cdm.prescribing
where encounterid>=1000000000; -- UTSW patients who do not have GROUSE data. 

select count(distinct encounterid) from 
utsw_cdm.prescribing_int
where encounterid between 1000000000 and 2000000000; 

select count(distinct patid) from 
utsw_cdm.prescribing_int;
-- This count should match the one below

select count(distinct patid) from 
utsw_cdm.prescribing;

select count(distinct patid) from 
utsw_cdm.prescribing_int
where patid>=30000000; 
-- UTSW patients who do not have GROUSE data. 

select count(distinct patid) from 
utsw_cdm.prescribing_int
where patid between 30000000 and 60000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
utsw_cdm.prescribing_int
where patid < 30000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== PRO_CM
select * from  utsw_cdm.pro_cm;
-- 0 rows
/*
select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.pro_cm;

-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.demographic;

select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.pro_cm;

-- This should fall in the range below
select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.encounter;

drop table utsw_cdm.pro_cm_int;
create table utsw_cdm.pro_cm_int
as select 
pro_cm_id,
coalesce(dp.bene_id_deid, to_char((ed.PATID-(-17199782))+30000000)) patid,
ed.encounterid encounterid,
pro_item,
pro_loinc,
ed.pro_date - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) pro_date, 
pro_time,
pro_response,
pro_method,
pro_mode,
pro_cat,
raw_pro_code,
raw_pro_response
from utsw_cdm.pro_cm ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_cdm_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
utsw_cdm.pro_cm_int;
-- This count should match the one below

select count(*) from 
utsw_cdm.pro_cm;

select count(distinct encounterid) from 
utsw_cdm.pro_cm
where encounterid>=1000000000; -- UTSW patients who do not have GROUSE data. 

select count(distinct encounterid) from 
utsw_cdm.pro_cm
where encounterid between 1000000000 and 2000000000; 

select count(distinct patid) from 
utsw_cdm.pro_cm_int;
-- This count should match the one below

select count(distinct patid) from 
utsw_cdm.pro_cm;

select count(distinct patid) from 
utsw_cdm.pro_cm
where patid>=30000000; 
-- UTSW patients who do not have GROUSE data. 

select count(distinct patid) from 
utsw_cdm.pro_cm_int
where patid between 30000000 and 60000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

select count(distinct patid) from 
utsw_cdm.pro_cm_int
where patid < 30000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

*/

-- ========== PRO_CM
select count(*) from  utsw_cdm.procedures;

select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.procedures;

-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.demographic;

select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.procedures;

-- This should fall in the range below
select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.encounter;


drop table utsw_cdm.procedures_int;
create table utsw_cdm.procedures_int
as select 
proceduresid,
coalesce(dp.bene_id_deid, to_char((ed.PATID-(-17199782))+30000000)) patid,
ed.encounterid encounterid,
enc_type,
ed.admit_date - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) admit_date, 
providerid,
ed.px_date - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) px_date, 
px,
px_type,
px_source,
raw_px,
raw_px_type
from utsw_cdm.procedures ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_cdm_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
utsw_cdm.procedures;
-- This count should match the one below

select count(*) from 
utsw_cdm.procedures_int;

select count(distinct encounterid) from 
utsw_cdm.procedures_int
where encounterid>=1000000000; -- UTSW patients who do not have GROUSE data. 

select count(distinct encounterid) from 
utsw_cdm.procedures
where encounterid between 1000000000 and 2000000000; 

select count(distinct patid) from 
utsw_cdm.procedures_int;
-- This count should match the one below

select count(distinct patid) from 
utsw_cdm.procedures;

select count(distinct patid) from 
utsw_cdm.procedures_int
where patid>=30000000; 
-- UTSW patients who do not have GROUSE data. 

select count(distinct patid) from 
utsw_cdm.procedures_int
where patid between 30000000 and 60000000; 

select count(distinct patid) from 
utsw_cdm.procedures_int
where patid < 30000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== VITAL
select count(*) from  utsw_cdm.vital;

select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.vital;

-- This should fall in the range below
select max(cast(patid as number)), min(cast(patid as number)) from utsw_cdm.demographic;

select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.vital;

-- This should fall in the range below
select max(cast(encounterid as number)), min(cast(encounterid as number)) from utsw_cdm.encounter;

drop table utsw_cdm.vital_int;
create table utsw_cdm.vital_int
as select 
vitalid,
coalesce(dp.bene_id_deid, to_char((ed.PATID-(-17199782))+30000000)) patid,
ed.encounterid encounterid,
ed.measure_date - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) measure_date,
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
from utsw_cdm.vital ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_cdm_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid;

-- ========== VERIFICATION

select count(*) from 
utsw_cdm.vital;
-- This count should match the one below

select count(*) from 
utsw_cdm.vital;

select count(distinct encounterid) from 
utsw_cdm.vital
where encounterid>=1000000000; -- UTSW patients who do not have GROUSE data. 

select count(distinct encounterid) from 
utsw_cdm.vital
where encounterid between 1000000000 and 2000000000; 

select count(distinct patid) from 
utsw_cdm.vital_int;
-- This count should match the one below

select count(distinct patid) from 
utsw_cdm.vital;

select count(distinct patid) from 
utsw_cdm.vital_int
where patid>=30000000; 
-- UTSW patients who do not have GROUSE data. 

select count(distinct patid) from 
utsw_cdm.vital_int
where patid between 30000000 and 60000000; 

select count(distinct patid) from 
utsw_cdm.vital_int
where patid < 30000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients

-- ========== HARVEST
drop table utsw_cdm.harvest_int;
create table utsw_cdm.harvest_int
as
select * from utsw_cdm.harvest;

-- ========== PRO_CM
drop table utsw_cdm.pro_cm_int;
create table utsw_cdm.pro_cm_int
as
select * from utsw_cdm.pro_cm;

-- ========== DEATH_CAUSE
drop table utsw_cdm.death_cause_int;
create table utsw_cdm.death_cause_int
as
select * from utsw_cdm.death_cause;