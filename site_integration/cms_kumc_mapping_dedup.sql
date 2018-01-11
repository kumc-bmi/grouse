-- cms_kumc_mapping_dedup.sql
/*
explore the duplicates in the data and remove them from consideration

run against identified GROUSE server. 
*/

-- 1. How many bene_id maps to multiple patient_nums
select count(patient_num) ct, bene_id from (
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS
from cms_id.cms_kumc_mapping
where patient_num is not null and bene_id is not null)
group by bene_id
having count(patient_num)>1;

-- 2. How many patient_num maps to multiple bene_ids
select count(bene_id) ct, patient_num from (
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS
from cms_id.cms_kumc_mapping
where patient_num is not null and bene_id is not null)
group by patient_num
having count(bene_id)>1; 

-- 3. Duplicates caused by bene_ids in crosswalk that are missing in cms_id.bene_id_mapping


-- De-duplication

-- 1. dups_pat_num will be set to the number of patient_nums the bene_id maps to
-- if it maps to more than one
MERGE
INTO    cms_id.cms_kumc_mapping mp
USING   (
        select count(patient_num) ct, bene_id from (
        select distinct patient_num, bene_id, bene_id_deid, 
        cms_date_shift_days, BH_DATE_SHIFT_DAYS
        from cms_id.cms_kumc_mapping
        where patient_num is not null and bene_id is not null)
        group by bene_id
        having count(patient_num)>1
        ) dup
ON      (dup.bene_id = mp.bene_id)
WHEN MATCHED THEN UPDATE
    SET mp.dups_pat_num = dup.ct;


-- 2. dups_bene_id will be set to the number of bene_ids the patient_num maps to
-- if it maps more than one
MERGE
INTO    cms_id.cms_kumc_mapping mp
USING   (
        select count(bene_id) ct, patient_num from (
        select distinct patient_num, bene_id, bene_id_deid, 
        cms_date_shift_days, BH_DATE_SHIFT_DAYS
        from cms_id.cms_kumc_mapping
        where patient_num is not null and bene_id is not null)
        group by patient_num
        having count(bene_id)>1
        ) dup
ON      (dup.patient_num = mp.patient_num)
WHEN MATCHED THEN UPDATE
    SET mp.dups_bene_id = dup.ct;


-- 3. dups_missing_map will be set to the number of rows a patient_num maps to 
-- when the issue is because bene_ids in crosswalk are missing in cms_id.bene_id_mapping
MERGE
INTO    cms_id.cms_kumc_mapping mp
USING   (
        select count(*) ct, patient_num from (
        select distinct patient_num, bene_id, bene_id_deid, 
        cms_date_shift_days, BH_DATE_SHIFT_DAYS  from cms_id.cms_kumc_mapping 
        where 
        dups_bene_id = 0 and 
        dups_pat_num = 0)
        group by patient_num
        having count(*) > 1
        ) dup
ON      (dup.patient_num = mp.patient_num)
WHEN MATCHED THEN UPDATE
    SET mp.DUPS_MISSING_MAP = dup.ct;


-- final patient mapping of interest
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS  from cms_id.cms_kumc_mapping 
where 
dups_bene_id = 0 and 
dups_pat_num = 0
and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null));

-- Verifying some counts
select count(distinct patient_num) from cms_id.cms_kumc_mapping;
select count(distinct bene_id) from cms_id.cms_kumc_mapping;
select count(*) from cms_id.bene_id_mapping;
select count(distinct bene_id) from cms_id.bene_id_mapping;