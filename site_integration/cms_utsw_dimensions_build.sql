-- cms_utsw_dimensions_build.sql: create new patient & visit dimensions that can be merged
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against identified GROUSE server. 
-- NOTE: CODE HAS NOT BEEN TESTED YET.

-- ========== PATIENT_DIMENSION
select max(cast(patient_num as number)), min(cast(patient_num as number)) from I2B2DEMODATAUTCRIS.patient_dimension;

select max(cast(patient_num as number)) from blueherondata_kumc.patient_dimension_int;
-- This will give us an understanding of how many patient_nums have been used

select count(*) from i2b2demodatautcris.patient_dimension;
-- This is to check how many patient_nums are needed for UTSW

select max(cast(patient_num as number))+ min(cast(patient_num as number)) from I2B2DEMODATAUTCRIS.patient_dimension;
-- This gives us an idea of the range of UTSW patient_nums

-- UTSW patient_nums will start at 30000000 and run till 59999999 
drop table i2b2demodatautcris.patient_dimension_int purge;

create table 
i2b2demodatautcris.patient_dimension_int
as select 
coalesce(dp.bene_id_deid, to_char((pd.patient_num-(-17199782))+30000000)) patient_num,
-- -17199782 comes from min(patient_num) in patient_dimension
-- 30000000 is where blueheron patient_nums start
-- 59000000 is where blueheron patient_nums will end
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
	UPLOAD_ID*(-1) UPLOAD_ID, 
	ETHNICITY_CD   
from 
i2b2demodatautcris.patient_dimension pd
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = pd.patient_num;

-- ========== VERIFICATION
select count(patient_num) from 
i2b2demodatautcris.patient_dimension_int;
-- This count should match the one below

select count(distinct patient_num) from 
i2b2demodatautcris.patient_dimension_int;

select count(patient_num) from 
i2b2demodatautcris.patient_dimension_int
where patient_num>=30000000; -- UTSW patients who do not have GROUSE data. 
-- The difference between the counts in the above two queries should match the intersection

select count(patient_num) from 
i2b2demodatautcris.patient_dimension_int
where patient_num between 30000000 and 60000000; -- UTSW patients who do not have GROUSE data. 


-- ========== VISIT_DIMENSION
select max(cast(encounter_num as number)), min(cast(encounter_num as number)) from I2B2DEMODATAUTCRIS.new_visit_dimension;
-- 1090763837	-229152720030522111800009

select count(*) from I2B2DEMODATAUTCRIS.new_visit_dimension
where encounter_num < 0;

select max(cast(encounter_num as number)) from 
blueherondata_kumc.visit_dimension_int;
-- 453,288,125

select count(*) from
i2b2demodatautcris.new_visit_dimension;
-- 83,974,671

select max(cast(encounter_num as number))- min(cast(encounter_num as number)) from I2B2DEMODATAUTCRIS.new_visit_dimension;
229,152,720,030,523,202,563,846

-- UTSW patient_nums will start at 500000000 and run till 599999999 

drop table i2b2demodatautcris.visit_dimension_int;

create table i2b2demodatautcris.visit_dimension_int
as select 
(ed.ENCOUNTER_NUM-(-229152720030522111800009)+200000000000000000000000) ENCOUNTER_NUM,
-- #HOTMESS
-- encounters for UTSW patients will be from 200000000000000000000000 onwards
coalesce(dp.bene_id_deid, to_char((pd.patient_num-(-17199782))+30000000)) patient_num,
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
-- NULL as DRG, 
DISCHARGE_STATUS, 
DISCHARGE_DISPOSITION, 
-- NULL as LOCATION_ZIP, 
ADMITTING_SOURCE
-- NULL as FACILITYID, 
-- PROVIDERID
from i2b2demodatautcris.new_visit_dimension ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patient_num;

-- ========== VERIFICATION

select count(*) from 
i2b2demodatautcris.visit_dimension_int;
-- This count should match the one below

select count(*) from 
i2b2demodatautcris.new_visit_dimension;

select count(distinct encounter_num) from 
i2b2demodatautcris.visit_dimension_int
where encounter_num>=200000000000000000000000; -- UTSW patients who do not have GROUSE data. 
-- This count should match the one below

select count(encounter_num) from 
i2b2demodatautcris.visit_dimension_int
where encounter_num between 200000000000000000000000 and 500000000000000000000000; 
-- UTSW patients who do not have GROUSE data. 

select count(distinct patient_num) from 
i2b2demodatautcris.visit_dimension_int;
-- This count should match the one below

select count(distinct patient_num) from 
i2b2demodatautcris.visit_dimension;

select count(distinct patient_num) from 
i2b2demodatautcris.visit_dimension_int
where patient_num>=30000000; 
-- UTSW patients who do not have GROUSE data. 

select count(distinct patient_num) from 
i2b2demodatautcris.visit_dimension_int
where patient_num between 30000000 and 40000000; 
-- UTSW patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients


-- ========== MODIFIER DIMENSION
drop table i2b2demodatautcris.modifier_dimension_int;

create table i2b2demodatautcris.modifier_dimension_int
as select * from i2b2demodatautcris.modifier_dimension;

update i2b2demodatautcris.modifier_dimension_int
set upload_id=-1;

-- ========== VERIFICATION
select count(*) from i2b2demodatautcris.modifier_dimension;

select count(*) from i2b2demodatautcris.modifier_dimension_int;

-- ========== CONCEPT DIMENSION
drop table i2b2demodatautcris.concept_dimension_int;

create table i2b2demodatautcris.concept_dimension_int
as select * from i2b2demodatautcris.concept_dimension;

update i2b2demodatautcris.concept_dimension_int
set upload_id=-1;

-- ========== VERIFICATION

select * from i2b2demodatautcris.concept_dimension;

select * from i2b2demodatautcris.concept_dimension_int;

select count(*) from i2b2demodatautcris.concept_dimension;

select count(*) from i2b2demodatautcris.concept_dimension_int;

-- ========== INDEXES

CREATE INDEX "i2b2demodatautcris"."PATD_UPLOADID_IDX_INT" ON "i2b2demodatautcris"."PATIENT_DIMENSION_INT" ("UPLOAD_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

  CREATE INDEX "i2b2demodatautcris"."PD_IDX_ALLPATIENTDIM_INT" ON "i2b2demodatautcris"."PATIENT_DIMENSION_INT" ("PATIENT_NUM", "VITAL_STATUS_CD", "BIRTH_DATE", "DEATH_DATE", "SEX_CD", "AGE_IN_YEARS_NUM", "LANGUAGE_CD", "RACE_CD", "MARITAL_STATUS_CD", "RELIGION_CD", "ZIP_CD", "INCOME_CD") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

  CREATE INDEX "i2b2demodatautcris"."PD_IDX_DATES_INT" ON "i2b2demodatautcris"."PATIENT_DIMENSION_INT" ("PATIENT_NUM", "VITAL_STATUS_CD", "BIRTH_DATE", "DEATH_DATE") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

  CREATE INDEX "i2b2demodatautcris"."PD_IDX_STATECITYZIP_INT" ON "i2b2demodatautcris"."PATIENT_DIMENSION_INT" ("STATECITYZIP_PATH", "PATIENT_NUM") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

-------

  CREATE INDEX "i2b2demodatautcris"."VD_UPLOADID_IDX_INT" ON "i2b2demodatautcris"."VISIT_DIMENSION_INT" ("UPLOAD_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

  CREATE INDEX "i2b2demodatautcris"."VISITDIM_EN_PN_LP_INT" ON "i2b2demodatautcris"."VISIT_DIMENSION_INT" ("ENCOUNTER_NUM", "PATIENT_NUM", "LOCATION_PATH", "INOUT_CD", "START_DATE", "END_DATE", "LENGTH_OF_STAY") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

  CREATE INDEX "i2b2demodatautcris"."VISITDIM_STD_EDD_IDX_INT" ON "i2b2demodatautcris"."VISIT_DIMENSION_INT" ("START_DATE", "END_DATE") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

--------

  CREATE UNIQUE INDEX "I2B2DEMODATAUTCRIS"."MODIFIER_DIMENSION_PK" ON "I2B2DEMODATAUTCRIS"."MODIFIER_DIMENSION_INT" ("MODIFIER_PATH") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

  CREATE INDEX "i2b2demodatautcris"."MD_IDX_UPLOADID_INT" ON "i2b2demodatautcris"."MODIFIER_DIMENSION_INT" ("UPLOAD_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;

----------

  CREATE INDEX "i2b2demodatautcris"."CD_UPLOADID_IDX_INT" ON "i2b2demodatautcris"."CONCEPT_DIMENSION_INT" ("UPLOAD_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 163840 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "I2B2_DATAMARTS" ;
