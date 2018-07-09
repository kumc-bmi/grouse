-- cms_cdm_site_dimensions_build.sql: create new cms tables
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against identified GROUSE server.
--==============================================================================
-- Vairables
--==============================================================================
/*
variables for KUMC
"&&I2B2_SITE_SCHEMA"     = BLUEHERONDATA_KUMC_CALAMUS
"&&CDM_SITE_SCHEMA"      = CDM_KUMC_CALAMUS_C4R3
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
-- comparing patient in i2b2 and cdm
--==============================================================================
with distinct_pat
  As
    (
    select count(distinct patid) d_cnt
    from "&&CDM_SITE_SCHEMA".DEMOGRAPHIC
    )
, shared_patid
  As
    (
    select 
    count(*) s_cnt
    from "&&CDM_SITE_SCHEMA" .DEMOGRAPHIC cdm
    join "&&I2B2_SITE_SCHEMA".PATIENT_DIMENSION i2b2
      on    to_char(i2b2.PATIENT_NUM) = cdm.patid
        and i2b2.BIRTH_DATE = cdm.BIRTH_DATE
        --and lower(i2b2.SEX_CD) = lower(cdm.SEX) 
    where lower(i2b2.sex_cd)||lower(cdm.sex) in ('mm','ff','23m','24f')
    )
select s_cnt/d_cnt ,
case 
  when s_cnt/d_cnt >= 0.8 then 1 else 1/0
END pass_fail
from distinct_pat,
     shared_patid
;
--==============================================================================
-- PATIENT_DIMENSION
--==============================================================================
select * from cms_id.patient_number_ranges
;
whenever sqlerror continue;
drop table "&&CDM_SITE_SCHEMA".demographic_int purge;
whenever sqlerror exit;
create table 
"&&CDM_SITE_SCHEMA".demographic_int
as select 
coalesce(dp.bene_id_deid, to_char((pd.patid-(&&SITE_PATDIM_PATNUM_MIN))+ &&SITE_PATNUM_START )) patid,
add_months(pd.birth_date - (
  -- nvl(bh_dob_date_shift,0)+ -- UTSW does not have dob_date_shift
  nvl(dp.bh_date_shift_days,0) - nvl(dp.cms_date_shift_days,0)),nvl(cms_dob_shift_months,0)) birth_date, 
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
"&&CDM_SITE_SCHEMA".demographic pd
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, cms_dob_shift_months, 
bh_date_shift_days
--, bh_dob_date_shift -- UTSW does not have dob_date_shift
from CMS_ID.CMS_KUMC_CALAMUS_MAPPING 
where 
dups_bene_id = 0 and 
dups_pat_num = 0
and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null))
and ((patient_num=mp_patient_num) or (patient_num is null and mp_patient_num is null))
) dp
on dp.patient_num = pd.patid
;
--==============================================================================
-- PATIENT_DIMENSION Verification
--==============================================================================
--check if all patid is unique.
select 
case
  when count(patid) = count(distinct(patid)) then 1 else 1/0
END pass_fail
from 
"&&CDM_SITE_SCHEMA".demographic
;
--check if all patid is unique.
select 
case
  when count(patid) = count(distinct(patid)) then 1 else 1/0
END pass_fail
from 
"&&CDM_SITE_SCHEMA".demographic_int
;
--==============================================================================
-- PATIENT_DIMENSION Counts
--==============================================================================
-- KUMC patients who do not have GROUSE data. 
select count(patid) from 
"&&CDM_SITE_SCHEMA".demographic_int
where patid between &&SITE_PATNUM_START  and &&SITE_PATNUM_END 
;
-- KUMC patients who have GROUSE data.
select count(patid) from 
"&&CDM_SITE_SCHEMA".demographic_int
where patid < &&SITE_PATNUM_START 
; 
--==============================================================================
-- VISIT_DIMENSION Counts
--==============================================================================
select max(cast(encounterid as number)), min(cast(encounterid as number)) 
from "&&CDM_SITE_SCHEMA".encounter
;
select max(cast(encounter_num as number)), min(cast(encounter_num as number)) 
from "&&I2B2_SITE_SCHEMA".visit_dimension
;
-- ** picked a couple of encounters to verify that the numbers match between cdm and i2b2.
select max(cast(encounterid as number))- min(cast(encounterid as number)) 
from "&&CDM_SITE_SCHEMA".encounter
;
select count(*) from "&&CDM_SITE_SCHEMA".encounter
;
-- This number should be less than the result of the previous query
select case when count(*) = 0 then 1 else 1/0 end pass_fail from "&&CDM_SITE_SCHEMA".encounter where encounterid < 0
;
-- it should be 0 otherwise we need to look at the data once
select * from cms_id.visit_number_ranges
;
-- This will give us an understanding of how many patient_nums have been used
-- Looks like we can use the KUMC CDM ENCOUNTERIDs as they are.
select count(*) from "&&CDM_SITE_SCHEMA".encounter
;
select max(cast(encounterid as number))- min(cast(encounterid as number)) 
from "&&CDM_SITE_SCHEMA".encounter
;
--==============================================================================
-- VISIT_DIMENSION
--==============================================================================
whenever sqlerror continue;
drop table "&&CDM_SITE_SCHEMA".encounter_int;
whenever sqlerror exit;
create table "&&CDM_SITE_SCHEMA".encounter_int
as select 
cast(ed.encounterid as number)-(&&SITE_ENCDIM_ENCNUM_MIN)+&&SITE_ENCNUM_START  encounterid,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patid-(&&SITE_PATDIM_PATNUM_MIN))+&&SITE_PATNUM_START)) patid,
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
from "&&CDM_SITE_SCHEMA".encounter ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, bh_date_shift_days,
cms_dob_shift_months
--, bh_dob_date_shift -- UTSW does not have dob_date_shift
from CMS_ID.CMS_KUMC_CALAMUS_MAPPING
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patid
;
--==============================================================================
-- VISIT_DIMENSION Verification
--==============================================================================
--check if encounter table count remains same
SELECT 
case 
  when ( SELECT COUNT(*) FROM   "&&CDM_SITE_SCHEMA".encounter )   
    =    ( SELECT COUNT(*) FROM   "&&CDM_SITE_SCHEMA".encounter_int ) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
SELECT 
case 
  when ( SELECT COUNT(distinct patid) FROM   "&&CDM_SITE_SCHEMA".encounter )   
    =    ( SELECT COUNT(distinct patid) FROM   "&&CDM_SITE_SCHEMA".encounter_int ) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
with pat_w_grouse
  As
  (
  select count(distinct patid) w_grouse from 
  "&&CDM_SITE_SCHEMA".encounter_int
  where patid between &&SITE_PATNUM_START  and &&SITE_PATNUM_END 
  ),
pat_wo_grouse
  As
  (
  select count(distinct patid) wo_grouse from 
  "&&CDM_SITE_SCHEMA".encounter_int
  where patid < &&SITE_PATNUM_START 
  ),
total_raw_patient
  As
  (
  select count(distinct (patid)) total_raw_patient
  from "&&CDM_SITE_SCHEMA".encounter
  )
select w_grouse, wo_grouse, w_grouse+wo_grouse total_patient, total_raw_patient,
case
  when  w_grouse+wo_grouse = total_raw_patient then 1 else 1/0
END PASS_FAILS
from pat_w_grouse,pat_wo_grouse,total_raw_patient
;  
