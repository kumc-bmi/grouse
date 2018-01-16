-- cms_kumc_dimensions_build.sql
/*
map blueherondata patient and visit dimensions and facts so that they can be merged with CMS i2b2

run against identified GROUSE server. 
*/

-- ========== PATIENT_DIMENSION

drop table blueherondata_kumc.patient_dimension_int purge;

create table 
blueherondata_kumc.patient_dimension_int
as select 
coalesce(dp.bene_id_deid, to_char((pd.patient_num-(1))+20000000)) patient_num,
-- 1 comes from min(patient_num) in patient_dimension
-- 200000000 is where blueheron patient_nums start
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
	UPLOAD_ID, 
	ETHNICITY_CD   
from 
blueherondata_kumc.patient_dimension pd
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_kumc_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = pd.patient_num;

-- ========== VERIFICATION
select count(patient_num) from 
blueherondata_kumc.patient_dimension_int;
-- This count should match the one below

select count(distinct patient_num) from 
blueherondata_kumc.patient_dimension_int;

select count(patient_num) from 
blueherondata_kumc.patient_dimension_int
where patient_num>=20000000; -- KUMC patients who do not have GROUSE data. 
-- The difference between the counts in the above two queries should match the intersection

select count(patient_num) from 
blueherondata_kumc.patient_dimension_int
where patient_num between 20000000 and 25000000; -- KUMC patients who do not have GROUSE data. 


-- ========== VISIT_DIMENSION
drop table blueherondata_kumc.visit_dimension_int;

create table blueherondata_kumc.visit_dimension_int
as select 
(ed.ENCOUNTER_NUM-(1)+400000000) ENCOUNTER_NUM,
-- encounters for KUMC patients will be from 400,000,000 onwards
coalesce(dp.bene_id_deid, to_char((ed.patient_num-(1))+20000000)) patient_num,
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
UPLOAD_ID, 
-- NULL as DRG, 
DISCHARGE_STATUS, 
DISCHARGE_DISPOSITION, 
-- NULL as LOCATION_ZIP, 
ADMITTING_SOURCE, 
-- NULL as FACILITYID, 
PROVIDERID
from blueherondata_kumc.visit_dimension ed
left join
(
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_kumc_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
(dups_missing_map =0 or (bene_id is not null and xw_bene_id is not null))
) dp
on dp.patient_num = ed.patient_num;


-- ========== VERIFICATION
select count(*) from 
blueherondata_kumc.visit_dimension_int;
-- This count should match the one below

select count(*) from 
blueherondata_kumc.visit_dimension;

select count(distinct encounter_num) from 
blueherondata_kumc.visit_dimension_int
where encounter_num>=400000000; -- KUMC patients who do not have GROUSE data. 
-- This count should match the one below

select count(encounter_num) from 
blueherondata_kumc.visit_dimension_int
where encounter_num between 400000000 and 460000000; -- KUMC patients who do not have GROUSE data. 

select count(distinct patient_num) from 
blueherondata_kumc.visit_dimension_int;
-- This count should match the one below

select count(distinct patient_num) from 
blueherondata_kumc.visit_dimension;

select count(distinct patient_num) from 
blueherondata_kumc.visit_dimension_int
where patient_num>=20000000; -- KUMC patients who do not have GROUSE data. 

select count(distinct patient_num) from 
blueherondata_kumc.visit_dimension_int
where patient_num between 1 and 19999999; -- KUMC patients who have GROUSE data. 
-- The sum of the counts from the above two queries should equal the total patients