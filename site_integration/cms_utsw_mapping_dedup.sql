-- cms_kumc_mapping_dedup.sql: explore the duplicates in the data and remove them from consideration
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against identified GROUSE server. 

-- 1. How many bene_id maps to multiple patient_nums
select count(patient_num) ct, bene_id from (
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS
from cms_id.cms_utsw_mapping
where patient_num is not null and bene_id is not null)
group by bene_id
having count(patient_num)>1;
-- 1500 rows

-- 2. How many patient_num maps to multiple bene_ids
select count(bene_id) ct, patient_num from (
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS
from cms_id.cms_utsw_mapping
where patient_num is not null and bene_id is not null)
group by patient_num
having count(bene_id)>1;
-- 1788


-- 3. Duplicates caused by bene_ids in crosswalk that are missing in cms_id.bene_id_mapping
select count(*) from cms_id.cms_utsw_mapping
where bene_id is null and xw_bene_id is not null;
-- 96,782

select count(*) ct, patient_num from (
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS  from cms_id.cms_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
dups_missing_map = 0)
group by patient_num
having count(*) > 1;
-- 4718 rows


-- De-duplication

-- 1. dups_pat_num will be set to the number of patient_nums the bene_id maps to
-- if it maps to more than one
MERGE
INTO    cms_id.cms_utsw_mapping mp
USING   (
        select count(patient_num) ct, bene_id from (
        select distinct patient_num, bene_id, bene_id_deid, 
        cms_date_shift_days, BH_DATE_SHIFT_DAYS
        from cms_id.cms_utsw_mapping
        where patient_num is not null and bene_id is not null)
        group by bene_id
        having count(patient_num)>1
        ) dup
ON      (dup.bene_id = mp.bene_id)
WHEN MATCHED THEN UPDATE
    SET mp.dups_pat_num = dup.ct;
-- 3,020 rows merged

select count(*) from cms_id.cms_utsw_mapping
where dups_pat_num<>0;
-- 3,020

-- 2. dups_bene_id will be set to the number of bene_ids the patient_num maps to
-- if it maps more than one
MERGE
INTO    cms_id.cms_utsw_mapping mp
USING   (
        select count(bene_id) ct, patient_num from (
        select distinct patient_num, bene_id, bene_id_deid, 
        cms_date_shift_days, BH_DATE_SHIFT_DAYS
        from cms_id.cms_utsw_mapping
        where patient_num is not null and bene_id is not null)
        group by patient_num
        having count(bene_id)>1
        ) dup
ON      (dup.patient_num = mp.patient_num)
WHEN MATCHED THEN UPDATE
    SET mp.dups_bene_id = dup.ct;
-- 3,657 rows merged.

select count(*) from cms_id.cms_utsw_mapping
where dups_bene_id<>0;
-- 3,657

-- 3. dups_missing_map will be set to the number of rows a patient_num maps to 
-- when the issue is because bene_ids in crosswalk are missing in cms_id.bene_id_mapping
MERGE
INTO    cms_id.cms_utsw_mapping mp
USING   (
        select count(*) ct, patient_num from (
        select distinct patient_num, bene_id, bene_id_deid, 
        cms_date_shift_days, BH_DATE_SHIFT_DAYS  from cms_id.cms_utsw_mapping 
        where 
        dups_bene_id = 0 and 
        dups_pat_num = 0)
        group by patient_num
        having count(*) > 1
        ) dup
ON      (dup.patient_num = mp.patient_num)
WHEN MATCHED THEN UPDATE
    SET mp.DUPS_MISSING_MAP = dup.ct;
-- 5,856 rows merged.

select count(*) from cms_id.cms_utsw_mapping
where DUPS_MISSING_MAP<>0;
-- 5,856

commit;

-- ======== FINAL PATIENT MAPPING OF INTEREST
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, cms_dob_shift_months, 
bh_date_shift_days, bh_dob_date_shift 
from cms_id.cms_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0
and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null));
-- 214,

-- ========= PATIENTS WHO ARE IN BOTH THE DATA SETS
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS  from cms_id.cms_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0
and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null))
and patient_num is not null and bene_id_deid is not null;
-- 370,811

-- ========== NOW VERIFICATION OF THIS SET
-- Verifying some counts
select count(distinct patient_num) from cms_id.cms_utsw_mapping;
-- 5,268,404

select count(distinct bene_id) from cms_id.cms_utsw_mapping;
-- 19,725,582

select count(*) from cms_id.bene_id_mapping;
-- 19725583

select count(distinct bene_id) from cms_id.bene_id_mapping;
-- 19725582

-- Verification of de-duplication 
select count(bene_id_deid), patient_num from (
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months, BH_DOB_DATE_SHIFT
from cms_id.cms_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
((dups_missing_map =0) or (bene_id is not null and xw_bene_id is not null))
) group by patient_num
having count(bene_id_deid)>1;

select count(distinct patient_num) 
from (
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, cms_dob_shift_months, 
bh_date_shift_days, bh_dob_date_shift 
from cms_id.cms_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0
and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null))
);
-- This count should match the one below 
-- 5,263,737

select count(patient_num) 
from (
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, cms_dob_shift_months, 
bh_date_shift_days, bh_dob_date_shift 
from cms_id.cms_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0
and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null))
);
-- 5,263,737

select count(distinct BENE_ID_DEID) 
from (
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, cms_dob_shift_months, 
bh_date_shift_days, bh_dob_date_shift 
from cms_id.cms_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0
and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null))
);
-- This count should match the one below 
-- 19720602

select count(BENE_ID_DEID) 
from (
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, cms_dob_shift_months, 
bh_date_shift_days, bh_dob_date_shift 
from cms_id.cms_utsw_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0
and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null))
);
-- 19720602