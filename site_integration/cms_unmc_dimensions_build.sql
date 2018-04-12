-- cms_unmc_dimensions_build.sql: create new patient & visit dimensions that can be merged
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against identified GROUSE server. 

-- ========== PATIENT_DIMENSION
select max(cast(patient_num as number)), min(cast(patient_num as number)) 
from blueherondata_unmc.patient_dimension;
-- 26,085,876	22,551,926

select max(cast(patient_num as number)), min(cast(patient_num as number)) from blueherondata_kumc.patient_dimension_int;
-- 24595581 2
select max(cast(patient_num as number)),min(cast(patient_num as number)) from i2b2demodatautcris.patient_dimension_int;
-- 58,030,331 16
-- This will give us an understanding of how many patient_nums have been used


select count(*) from blueherondata_unmc.patient_dimension;
-- This is to check how many patient_nums are needed for unmc
-- 2035206

select max(cast(patient_num as number))- min(cast(patient_num as number)) from blueherondata_unmc.patient_dimension;
-- This gives us an idea of the range of unmc patient_nums
-- 3,533,950

-- unmc patient_nums will start at 60000000 and run till 64999999 
drop table blueherondata_unmc.patient_dimension_int purge;

create table 
blueherondata_unmc.patient_dimension_int
as select 
coalesce(dp.bene_id_deid, to_char((pd.patient_num-(22551926))+60000000)) patient_num,
-- 22551926 comes from min(patient_num) in patient_dimension
-- 60000000 is where blueheron patient_nums start
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
	UPLOAD_ID*(-1) UPLOAD_ID --, 
    -- ETHNICITY_CD   
from 
blueherondata_unmc.patient_dimension pd
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_unmc_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = pd.patient_num;

-- ========== VERIFICATION
select count(patient_num) from 
blueherondata_unmc.patient_dimension_int;
-- This count should match the one below
-- 2035206

select count(distinct patient_num) from 
blueherondata_unmc.patient_dimension_int;
-- 2035206

select count(patient_num) from 
blueherondata_unmc.patient_dimension_int
where patient_num<20000000; -- unmc patients who do not have GROUSE data. 
-- The difference between the counts in the above two queries should match the intersection
-- 120773

select count(patient_num) from 
blueherondata_unmc.patient_dimension_int
where patient_num between 60000000 and 65000000; -- unmc patients who do not have GROUSE data. 
-- 1914433

select 1914433+120773 from dual; 
-- 2035206

-- ========== VISIT_DIMENSION
select max(cast(encounter_num as number)), min(cast(encounter_num as number)) 
from blueherondata_unmc.visit_dimension;
-- 780844876	731679021

select max(cast(encounter_num as number))- min(cast(encounter_num as number)) 
from blueherondata_unmc.visit_dimension;
-- 49,165,855

select max(cast(encounter_num as number)) from 
blueherondata_kumc.visit_dimension_int;
-- 453,288,125

select count(*) from
blueherondata_unmc.new_visit_dimension;

select max(cast(encounter_num as number))- min(cast(encounter_num as number)) 
from blueherondata_unmc.new_visit_dimension;

-- unmc patient_nums will start at 500000000 and run till 599999999 

drop table blueherondata_unmc.visit_dimension_int;

create table blueherondata_unmc.visit_dimension_int
as select 
(ed.ENCOUNTER_NUM-(731679021)+500000000) ENCOUNTER_NUM,
-- encounters for unmc patients will be from 500000000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patient_num-(22551926))+60000000)) patient_num,
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
UPLOAD_ID*(-1) UPLOAD_ID,
DRG, 
DISCHARGE_STATUS, 
DISCHARGE_DISPOSITION, 
-- LOCATION_ZIP, 
ADMITTING_SOURCE,
FACILITYID, 
PROVIDERID
from blueherondata_unmc.visit_dimension ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_unmc_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patient_num;

-- ========== VERIFICATION

select count(*) from 
blueherondata_unmc.visit_dimension_int;
-- This count should match the one below
-- 49143838

select count(*) from 
blueherondata_unmc.visit_dimension;
-- 49143838

select count(distinct encounter_num) from 
blueherondata_unmc.visit_dimension_int
where encounter_num>=200000000000000000000000; -- unmc patients who do not have GROUSE data. 
-- This count should match the one below

select count(encounter_num) from 
blueherondata_unmc.visit_dimension_int
where encounter_num between 500000000 and 600000000; 
-- unmc patients who do not have GROUSE data.
-- 49143838

select count(distinct patient_num) from 
blueherondata_unmc.visit_dimension_int;
-- This count should match the one below
-- 2035180

select count(distinct patient_num) from 
blueherondata_unmc.visit_dimension;
-- 2035180


-- ========== MODIFIER DIMENSION
drop table blueherondata_unmc.modifier_dimension_int;

create table blueherondata_unmc.modifier_dimension_int
as select * from blueherondata_unmc.modifier_dimension;

update blueherondata_unmc.modifier_dimension_int
set upload_id=-2;

-- ========== VERIFICATION
select count(*) from blueherondata_unmc.modifier_dimension;

select count(*) from blueherondata_unmc.modifier_dimension_int;

-- ========== CONCEPT DIMENSION
drop table blueherondata_unmc.concept_dimension_int;

create table blueherondata_unmc.concept_dimension_int
as select * from blueherondata_unmc.concept_dimension;

update blueherondata_unmc.concept_dimension_int
set upload_id=-2;

-- ========== VERIFICATION

select count(*) from blueherondata_unmc.concept_dimension;
-- 20235660

select count(*) from blueherondata_unmc.concept_dimension_int;

-- ========== INDEXES

CREATE INDEX "BLUEHERONDATA_UNMC"."PATD_UPLOADID_IDX_INT" ON "BLUEHERONDATA_UNMC"."PATIENT_DIMENSION_INT" ("UPLOAD_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

  CREATE INDEX "BLUEHERONDATA_UNMC"."PD_IDX_ALLPATIENTDIM_INT" ON "BLUEHERONDATA_UNMC"."PATIENT_DIMENSION_INT" ("PATIENT_NUM", "VITAL_STATUS_CD", "BIRTH_DATE", "DEATH_DATE", "SEX_CD", "AGE_IN_YEARS_NUM", "LANGUAGE_CD", "RACE_CD", "MARITAL_STATUS_CD", "RELIGION_CD", "ZIP_CD", "INCOME_CD") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

  CREATE INDEX "BLUEHERONDATA_UNMC"."PD_IDX_DATES_INT" ON "BLUEHERONDATA_UNMC"."PATIENT_DIMENSION_INT" ("PATIENT_NUM", "VITAL_STATUS_CD", "BIRTH_DATE", "DEATH_DATE") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

  CREATE INDEX "BLUEHERONDATA_UNMC"."PD_IDX_STATECITYZIP_INT" ON "BLUEHERONDATA_UNMC"."PATIENT_DIMENSION_INT" ("STATECITYZIP_PATH", "PATIENT_NUM") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

-------

  CREATE INDEX "BLUEHERONDATA_UNMC"."VD_UPLOADID_IDX_INT" ON "BLUEHERONDATA_UNMC"."VISIT_DIMENSION_INT" ("UPLOAD_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

  CREATE INDEX "BLUEHERONDATA_UNMC"."VISITDIM_EN_PN_LP_INT" ON "BLUEHERONDATA_UNMC"."VISIT_DIMENSION_INT" ("ENCOUNTER_NUM", "PATIENT_NUM", "LOCATION_PATH", "INOUT_CD", "START_DATE", "END_DATE", "LENGTH_OF_STAY") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

  CREATE INDEX "BLUEHERONDATA_UNMC"."VISITDIM_STD_EDD_IDX_INT" ON "BLUEHERONDATA_UNMC"."VISIT_DIMENSION_INT" ("START_DATE", "END_DATE") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

--------

  CREATE UNIQUE INDEX "BLUEHERONDATA_UNMC"."MODIFIER_DIMENSION_PK" ON "BLUEHERONDATA_UNMC"."MODIFIER_DIMENSION_INT" ("MODIFIER_PATH") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

  CREATE INDEX "BLUEHERONDATA_UNMC"."MD_IDX_UPLOADID_INT" ON "BLUEHERONDATA_UNMC"."MODIFIER_DIMENSION_INT" ("UPLOAD_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

----------

  CREATE INDEX "BLUEHERONDATA_UNMC"."CD_UPLOADID_IDX_INT" ON "BLUEHERONDATA_UNMC"."CONCEPT_DIMENSION_INT" ("UPLOAD_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;
