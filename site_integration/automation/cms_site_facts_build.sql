-- cms_kumc_facts_build.sql: create a new blueherondata observation_fact
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against identified GROUSE server
/*
undefine I2B2_SITE_SCHEMA;
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
set echo on;
whenever sqlerror continue;
drop table "&&I2B2_SITE_SCHEMA".observation_fact_int purge;
whenever sqlerror exit;
CREATE TABLE "&&I2B2_SITE_SCHEMA".OBSERVATION_FACT_INT as  
select 
  (ob.ENCOUNTER_NUM-(&&SITE_ENCDIM_ENCNUM_MIN)+&&SITE_ENCNUM_START ) ENCOUNTER_NUM
  /*+ index(observation_fact OBS_FACT_PAT_NUM_BI) */
, ob.patient_num as patient_num_bh
, coalesce(dp.bene_id_deid, to_char((ob.patient_num-(&&SITE_PATDIM_PATNUM_MIN))+&&SITE_PATNUM_START)) patient_num
, CONCEPT_CD 
-- , PROVIDER_ID -- Not using the KUMC provider_dimension
, ob.START_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) START_DATE
, ob.END_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) END_DATE
, ob.UPDATE_DATE - nvl(dp.BH_DATE_SHIFT_DAYS,0) + nvl(dp.cms_date_shift_days,0) UPDATE_DATE
, MODIFIER_CD
, INSTANCE_NUM -- CONFIRM**
, VALTYPE_CD
-- , TVAL_CHAR -- removing TVAL_CHAR on purpose 
, NVAL_NUM 
, VALUEFLAG_CD 
, QUANTITY_NUM
, UNITS_CD 
, LOCATION_CD 
, OBSERVATION_BLOB
, CONFIDENCE_NUM
, DOWNLOAD_DATE 
, IMPORT_DATE 
, SOURCESYSTEM_CD
, upload_id*(-1) UPLOAD_ID
from 
"&&I2B2_SITE_SCHEMA".observation_fact ob
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
and patient_num is not null and bene_id_deid is not null
) dp
on dp.patient_num = ob.patient_num
;
CREATE BITMAP INDEX "&&I2B2_SITE_SCHEMA"."OBS_FACT_ENC_NUM_BI_INT" ON "&&I2B2_SITE_SCHEMA"."OBSERVATION_FACT_INT" ("ENCOUNTER_NUM") 
PCTFREE 10 INITRANS 2 MAXTRANS 255 NOLOGGING 
STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
TABLESPACE "I2B2_DATAMARTS" 
PARALLEL 2 
;
CREATE BITMAP INDEX "&&I2B2_SITE_SCHEMA"."OBS_FACT_PAT_NUM_BI_INT" ON "&&I2B2_SITE_SCHEMA"."OBSERVATION_FACT_INT" ("PATIENT_NUM") 
PCTFREE 10 INITRANS 2 MAXTRANS 255 NOLOGGING 
STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
TABLESPACE "I2B2_DATAMARTS" 
PARALLEL 2 
;
-- ========== counts
select count(patient_num) from 
"&&I2B2_SITE_SCHEMA".OBSERVATION_FACT_INT
;
select count(distinct patient_num) from 
"&&I2B2_SITE_SCHEMA".OBSERVATION_FACT_INT
;
select count(distinct patient_num) from 
"&&I2B2_SITE_SCHEMA".OBSERVATION_FACT_INT
where patient_num between &&SITE_PATNUM_START and &&SITE_PATNUM_END  
;
-- KUMC patients who do not have GROUSE data.
select count(*) from 
"&&I2B2_SITE_SCHEMA".visit_dimension_int
;
select count(*) from 
"&&I2B2_SITE_SCHEMA".visit_dimension
;
select count(encounter_num) from 
"&&I2B2_SITE_SCHEMA".OBSERVATION_FACT_INT
where encounter_num between &&SITE_ENCNUM_START and &&SITE_ENCNUM_END  
;
-- KUMC patients who do not have GROUSE data.
select count(distinct encounter_num) from 
"&&I2B2_SITE_SCHEMA".OBSERVATION_FACT_INT
where encounter_num between &&SITE_ENCNUM_START and &&SITE_ENCNUM_END  
;
-- KUMC patients who do not have GROUSE data.
