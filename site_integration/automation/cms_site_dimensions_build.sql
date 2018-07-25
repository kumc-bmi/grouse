-- cms_site_dimensions_build.sql: create new patient & visit dimensions that can be merged
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against identified GROUSE server. 
/*
variables and values for KUMC:
"&&I2B2_SITE_SCHEMA"     
"&&out_cms_site_mapping" 
--
&&SITE_PATDIM_PATNUM_MIN 
&&SITE_PATNUM_START      
&&SITE_PATNUM_END        
--
&&SITE_ENCDIM_ENCNUM_MIN 
&&SITE_ENCNUM_START      
&&SITE_ENCNUM_END        
--                           
&&CMS_PATNUM_START       
&&CMS_PATNUM_END         
*/
--undefine I2B2_SITE_SCHEMA;
--undefine SITE_PATDIM_PATNUM_MIN;
--undefine SITE_PATNUM_START;
--undefine SITE_ENCNUM_END;
set echo on;
whenever sqlerror exit;
select min(to_number(BENE_ID_DEID)), max(to_number(BENE_ID_DEID)) from CMS_ID.BENE_ID_MAPPING_11_15;
--CMS: 1, 21139822
select min(to_number(patient_num)), max(to_number(patient_num)) from "&&I2B2_SITE_SCHEMA".patient_dimension;
--KUMC:1, 4639068
select * from CMS_ID.PATIENT_NUMBER_RANGES;
select min(to_number(patient_num)), max(to_number(patient_num)) from "&&I2B2_SITE_SCHEMA".VISIT_DIMENSION;
--KUMC visit encounter:1, 4541667
--                        453288125
select * from CMS_ID.VISIT_NUMBER_RANGES;
select * from CMS_ID.VISIT_NUMBER_RANGES;
-- ========== PATIENT_DIMENSION
whenever sqlerror continue;
drop table "&&I2B2_SITE_SCHEMA".patient_dimension_int purge;
whenever sqlerror exit;
create table 
"&&I2B2_SITE_SCHEMA".patient_dimension_int
as select 
--dp.patient_num patient_num_old,
coalesce(dp.bene_id_deid, to_char((pd.patient_num-( &&SITE_PATDIM_PATNUM_MIN ))+ &&SITE_PATNUM_START )) patient_num,
-- 1 comes from min(patient_num) in patient_dimension
-- 22000000 is where blueheron patient_nums start
add_months(pd.birth_date - (
   --nvl(bh_dob_date_shift,0)+ -- UTSW does not have dob_date_shift
    nvl(dp.BH_DATE_SHIFT_DAYS,0) - nvl(dp.cms_date_shift_days,0)),nvl(cms_dob_shift_months,0)) birth_date, 
    pd.death_date - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) death_date,
    pd.update_date - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) update_date,
	VITAL_STATUS_CD,
	SEX_CD, 
	AGE_IN_YEARS_NUM, 
	LANGUAGE_CD, 
	RACE_CD, 
	MARITAL_STATUS_CD, 
	RELIGION_CD, 
	ZIP_CD, 
	STATECITYZIP_PATH, 
	INCOME_CD, 
	PATIENT_BLOB, 
	DOWNLOAD_DATE, 
	IMPORT_DATE, 
	SOURCESYSTEM_CD, 
	UPLOAD_ID*(-1) UPLOAD_ID, --??
	ETHNICITY_CD   
from 
"&&I2B2_SITE_SCHEMA".patient_dimension pd
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months
--, BH_DOB_DATE_SHIFT-- UTSW does not have dob_date_shift
from cms_id."&&out_cms_site_mapping" 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
--kumc_mapping dp
on dp.patient_num = pd.patient_num
;
-- ========== VERIFICATION
select count(patient_num) from 
"&&I2B2_SITE_SCHEMA".patient_dimension_int
;
-- This count should match the one below
select count(distinct patient_num) from 
"&&I2B2_SITE_SCHEMA".patient_dimension_int
;
--select count(patient_num) from 
--"&&I2B2_SITE_SCHEMA".patient_dimension_int
--where patient_num>=&&SITE_PATNUM_START
--; -- KUMC patients who do not have GROUSE data. 
-- The difference between the counts in the above two queries should match the intersection
select count(patient_num) from 
"&&I2B2_SITE_SCHEMA".patient_dimension_int
where patient_num between &&SITE_PATNUM_START and &&SITE_PATNUM_END
; 
-- KUMC patients who do not have GROUSE data. 
-- ========== VISIT_DIMENSION
whenever sqlerror continue;
drop table "&&I2B2_SITE_SCHEMA".visit_dimension_int;
whenever sqlerror exit;
create table "&&I2B2_SITE_SCHEMA".visit_dimension_int
as select 
(ed.ENCOUNTER_NUM-(&&SITE_ENCDIM_ENCNUM_MIN)+ &&SITE_ENCNUM_START) ENCOUNTER_NUM,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patient_num-(&&SITE_PATDIM_PATNUM_MIN))+&&SITE_PATNUM_START)) patient_num,
ACTIVE_STATUS_CD, 
ed.START_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) START_DATE,
ed.END_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) END_DATE,
INOUT_CD, 
LOCATION_CD, 
LOCATION_PATH, 
LENGTH_OF_STAY, 
VISIT_BLOB, 
UPDATE_DATE, 
DOWNLOAD_DATE, 
IMPORT_DATE, 
SOURCESYSTEM_CD,
UPLOAD_ID*(-1) UPLOAD_ID, --??
-- NULL as DRG, 
DISCHARGE_STATUS, 
DISCHARGE_DISPOSITION, 
-- NULL as LOCATION_ZIP, 
ADMITTING_SOURCE
-- NULL as FACILITYID, 
-- PROVIDERID
from "&&I2B2_SITE_SCHEMA".visit_dimension ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months
--, BH_DOB_DATE_SHIFT -- UTSW does not have dob_date_shift
from cms_id."&&out_cms_site_mapping" 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patient_num
;
-- ========== VERIFICATION
SELECT 
case 
  when ( select count(*) from "&&I2B2_SITE_SCHEMA".visit_dimension_int )   
    =  ( select count(*) from "&&I2B2_SITE_SCHEMA".visit_dimension ) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
SELECT 
case 
  when ( select count(distinct patient_num) from "&&I2B2_SITE_SCHEMA".visit_dimension_int )   
    =  ( select count(distinct patient_num) from "&&I2B2_SITE_SCHEMA".visit_dimension ) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
SELECT 
case 
  when ( select count(distinct encounter_num) from "&&I2B2_SITE_SCHEMA".visit_dimension_int )   
    =  ( select count(encounter_num) from 
        "&&I2B2_SITE_SCHEMA".visit_dimension_int
        where encounter_num between &&SITE_ENCNUM_START and &&SITE_ENCNUM_END
       ) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
-- ========== counts
select count(distinct patient_num) from 
"&&I2B2_SITE_SCHEMA".visit_dimension_int
where patient_num between &&SITE_PATNUM_START and &&SITE_PATNUM_END
;
-- KUMC patients who do not have GROUSE data. 
select count(distinct patient_num) from 
"&&I2B2_SITE_SCHEMA".visit_dimension_int
where patient_num between &&CMS_PATNUM_START  and &&CMS_PATNUM_END
;
-- KUMC patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients
-- ========== MODIFIER DIMENSION
whenever sqlerror continue;
drop table "&&I2B2_SITE_SCHEMA".modifier_dimension_int;
whenever sqlerror exit;
create table "&&I2B2_SITE_SCHEMA".modifier_dimension_int
as select * from "&&I2B2_SITE_SCHEMA".modifier_dimension
;
update "&&I2B2_SITE_SCHEMA".modifier_dimension_int
set upload_id=-1
;
commit;
-- ========== VERIFICATION
SELECT 
case 
  when ( select count(*) from "&&I2B2_SITE_SCHEMA".modifier_dimension )   
    =  ( select count(*) from "&&I2B2_SITE_SCHEMA".modifier_dimension_int ) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
-- ========== CONCEPT DIMENSION
whenever sqlerror continue;
drop table "&&I2B2_SITE_SCHEMA".concept_dimension_int;
whenever sqlerror exit;
create table "&&I2B2_SITE_SCHEMA".concept_dimension_int
as select * from "&&I2B2_SITE_SCHEMA".concept_dimension
;
update "&&I2B2_SITE_SCHEMA".concept_dimension_int
set upload_id=-1
;
commit;
-- ========== VERIFICATION
SELECT 
case 
  when ( select count(*) from "&&I2B2_SITE_SCHEMA".concept_dimension)   
    =  ( select count(*) from "&&I2B2_SITE_SCHEMA".concept_dimension_int ) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
