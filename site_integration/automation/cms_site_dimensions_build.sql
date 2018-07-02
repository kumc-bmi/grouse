-- cms_kumc_dimensions_build.sql: create new patient & visit dimensions that can be merged
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against identified GROUSE server. 
/*
variables and values for KUMC:
"&&I2B2_SITE_SCHEMA"     = BLUEHERONDATA_KUMC_CALAMUS
"&&out_cms_site_mapping" = CMS_KUMC_CALAMUS_MAPPING
--
&&SITE_PATDIM_PATNUM_MIN = 1
&&SITE_PATNUM_START      = 22000000 
&&SITE_PATNUM_END        = 25000000 
--
&&SITE_ENCDIM_ENCNUM_MIN = 1
&&SITE_ENCNUM_START      = 400000000
&&SITE_ENCNUM_END        = 460000000
--                           
&&CMS_PATNUM_START       = 1
&&CMS_PATNUM_END         = 21999999
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
add_months(pd.birth_date - (nvl(bh_dob_date_shift,0)+ nvl(dp.BH_DATE_SHIFT_DAYS,0) - nvl(dp.cms_date_shift_days,0)),nvl(cms_dob_shift_months,0)) birth_date, 
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
cms_dob_shift_months, BH_DOB_DATE_SHIFT
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
; -- KUMC patients who do not have GROUSE data. 
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
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id."&&out_cms_site_mapping" 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patient_num
;
-- ========== VERIFICATION
select count(*) from 
"&&I2B2_SITE_SCHEMA".visit_dimension_int
;
-- This count should match the one below
select count(*) from 
"&&I2B2_SITE_SCHEMA".visit_dimension
;
--select count(distinct encounter_num) from 
--"&&I2B2_SITE_SCHEMA".visit_dimension_int
--where encounter_num>=400000000
--; -- KUMC patients who do not have GROUSE data. 
-- This count should match the one below
select count(encounter_num) from 
"&&I2B2_SITE_SCHEMA".visit_dimension_int
where encounter_num between &&SITE_ENCNUM_START and &&SITE_ENCNUM_END
; -- KUMC patients who do not have GROUSE data. 
select count(distinct patient_num) from 
"&&I2B2_SITE_SCHEMA".visit_dimension_int;
-- This count should match the one below
select count(distinct patient_num) from 
"&&I2B2_SITE_SCHEMA".visit_dimension
;
--select count(distinct patient_num) from 
--"&&I2B2_SITE_SCHEMA".visit_dimension_int
--where patient_num>=&&SITE_PATNUM_START
--; -- KUMC patients who do not have GROUSE data. 
select count(distinct patient_num) from 
"&&I2B2_SITE_SCHEMA".visit_dimension_int
where patient_num between &&CMS_PATNUM_START  and &&CMS_PATNUM_END
; -- KUMC patients who have GROUSE data. 
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
select count(*) from "&&I2B2_SITE_SCHEMA".modifier_dimension
;
select count(*) from "&&I2B2_SITE_SCHEMA".modifier_dimension_int
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
select count(*) from "&&I2B2_SITE_SCHEMA".concept_dimension;
select count(*) from "&&I2B2_SITE_SCHEMA".concept_dimension_int;
-- ========== INDEXES
whenever sqlerror continue;
drop index "&&I2B2_SITE_SCHEMA"."PATD_UPLOADID_IDX_INT";
whenever sqlerror exit;
CREATE INDEX "&&I2B2_SITE_SCHEMA"."PATD_UPLOADID_IDX_INT" ON "&&I2B2_SITE_SCHEMA"."PATIENT_DIMENSION_INT" ("UPLOAD_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;
whenever sqlerror continue;
drop index "&&I2B2_SITE_SCHEMA"."PD_IDX_ALLPATIENTDIM_INT;
whenever sqlerror exit;  
CREATE INDEX "&&I2B2_SITE_SCHEMA"."PD_IDX_ALLPATIENTDIM_INT" ON "&&I2B2_SITE_SCHEMA"."PATIENT_DIMENSION_INT" ("PATIENT_NUM", "VITAL_STATUS_CD", "BIRTH_DATE", "DEATH_DATE", "SEX_CD", "AGE_IN_YEARS_NUM", "LANGUAGE_CD", "RACE_CD", "MARITAL_STATUS_CD", "RELIGION_CD", "ZIP_CD", "INCOME_CD") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;
whenever sqlerror continue;
drop index "&&I2B2_SITE_SCHEMA"."PD_IDX_DATES_INT";
whenever sqlerror exit;  
CREATE INDEX "&&I2B2_SITE_SCHEMA"."PD_IDX_DATES_INT" ON "&&I2B2_SITE_SCHEMA"."PATIENT_DIMENSION_INT" ("PATIENT_NUM", "VITAL_STATUS_CD", "BIRTH_DATE", "DEATH_DATE") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;
whenever sqlerror continue;
drop index "&&I2B2_SITE_SCHEMA"."PD_IDX_STATECITYZIP_INT";
whenever sqlerror exit;  
CREATE INDEX "&&I2B2_SITE_SCHEMA"."PD_IDX_STATECITYZIP_INT" ON "&&I2B2_SITE_SCHEMA"."PATIENT_DIMENSION_INT" ("STATECITYZIP_PATH", "PATIENT_NUM") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;
-------
whenever sqlerror continue;
drop index "&&I2B2_SITE_SCHEMA"."VD_UPLOADID_IDX_INT";
whenever sqlerror exit;  
CREATE INDEX "&&I2B2_SITE_SCHEMA"."VD_UPLOADID_IDX_INT" ON "&&I2B2_SITE_SCHEMA"."VISIT_DIMENSION_INT" ("UPLOAD_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;
whenever sqlerror continue;
drop index "&&I2B2_SITE_SCHEMA"."VISITDIM_EN_PN_LP_INT";
whenever sqlerror exit;  
CREATE INDEX "&&I2B2_SITE_SCHEMA"."VISITDIM_EN_PN_LP_INT" ON "&&I2B2_SITE_SCHEMA"."VISIT_DIMENSION_INT" ("ENCOUNTER_NUM", "PATIENT_NUM", "LOCATION_PATH", "INOUT_CD", "START_DATE", "END_DATE", "LENGTH_OF_STAY") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;
whenever sqlerror continue;
drop index "&&I2B2_SITE_SCHEMA"."VISITDIM_STD_EDD_IDX_INT";
whenever sqlerror exit;  
CREATE INDEX "&&I2B2_SITE_SCHEMA"."VISITDIM_STD_EDD_IDX_INT" ON "&&I2B2_SITE_SCHEMA"."VISIT_DIMENSION_INT" ("START_DATE", "END_DATE") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;
--------
whenever sqlerror continue;
drop index "&&I2B2_SITE_SCHEMA"."MD_IDX_UPLOADID_INT";
whenever sqlerror exit;  
CREATE INDEX "&&I2B2_SITE_SCHEMA"."MD_IDX_UPLOADID_INT" ON "&&I2B2_SITE_SCHEMA"."MODIFIER_DIMENSION_INT" ("UPLOAD_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;
----------
whenever sqlerror continue;
drop index "&&I2B2_SITE_SCHEMA"."CD_UPLOADID_IDX_INT";
whenever sqlerror exit;  
CREATE INDEX "&&I2B2_SITE_SCHEMA"."CD_UPLOADID_IDX_INT" ON "&&I2B2_SITE_SCHEMA"."CONCEPT_DIMENSION_INT" ("UPLOAD_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;