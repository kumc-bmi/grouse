-- cms_kumc_mapping_dedup.sql: explore the duplicates in the data and remove them from consideration
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against identified GROUSE server. 
-- De-duplication
-- 1. dups_pat_num will be set to the number of patient_nums the bene_id maps to
-- if it maps to more than one
/*
variables
"&&out_cms_site_mapping"

*/
set echo on;
MERGE
INTO    cms_id."&&out_cms_site_mapping" mp
USING   (
        select count(patient_num) ct, bene_id from (
        select distinct patient_num, bene_id, bene_id_deid, 
        cms_date_shift_days, BH_DATE_SHIFT_DAYS
        from cms_id."&&out_cms_site_mapping"
        where patient_num is not null and bene_id is not null)
        group by bene_id
        having count(patient_num)>1
        ) dup
ON      (dup.bene_id = mp.bene_id)
WHEN MATCHED THEN UPDATE
    SET mp.dups_pat_num = dup.ct
;
commit;
-- 2. dups_bene_id will be set to the number of bene_ids the patient_num maps to
-- if it maps more than one
MERGE
INTO    cms_id."&&out_cms_site_mapping" mp
USING   (
        select count(bene_id) ct, patient_num from (
        select distinct patient_num, bene_id, bene_id_deid, 
        cms_date_shift_days, BH_DATE_SHIFT_DAYS
        from cms_id."&&out_cms_site_mapping"
        where patient_num is not null and bene_id is not null)
        group by patient_num
        having count(bene_id)>1
        ) dup
ON      (dup.patient_num = mp.patient_num)
WHEN MATCHED THEN UPDATE
    SET mp.dups_bene_id = dup.ct
;
commit;
-- 3. dups_missing_map will be set to the number of rows a patient_num maps to 
-- when the issue is because bene_ids in crosswalk are missing in cms_id.bene_id_mapping_11_15
MERGE
INTO    cms_id."&&out_cms_site_mapping" mp
USING   (
        select count(*) ct, patient_num from (
        select distinct patient_num, bene_id, bene_id_deid, 
        cms_date_shift_days, BH_DATE_SHIFT_DAYS  from cms_id."&&out_cms_site_mapping" 
        where 
        dups_bene_id = 0 and 
        dups_pat_num = 0)
        group by patient_num
        having count(*) > 1
        ) dup
ON      (dup.patient_num = mp.patient_num)
WHEN MATCHED THEN UPDATE
    SET mp.DUPS_MISSING_MAP = dup.ct
;
commit;
-- -- ======== FINAL PATIENT MAPPING OF INTEREST
-- select distinct patient_num, bene_id, bene_id_deid, 
-- cms_date_shift_days, cms_dob_shift_months, 
-- bh_date_shift_days
-- --, bh_dob_date_shift -- UTSW does not have dob_date_shift
-- from cms_id."&&out_cms_site_mapping" 
-- where 
-- dups_bene_id = 0 and 
-- dups_pat_num = 0
-- and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null));
-- -- ========= PATIENTS WHO ARE IN BOTH THE DATA SETS
-- select distinct patient_num, bene_id, bene_id_deid, 
-- cms_date_shift_days, BH_DATE_SHIFT_DAYS  from cms_id."&&out_cms_site_mapping" 
-- where 
-- dups_bene_id = 0 and 
-- dups_pat_num = 0
-- and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null))
-- and patient_num is not null and bene_id_deid is not null;
-- ========== NOW VERIFICATION OF THIS SET
-- Verifying some counts
select count(distinct patient_num) from cms_id."&&out_cms_site_mapping";
select count(distinct bene_id) from cms_id."&&out_cms_site_mapping";
select count(*) from cms_id.bene_id_mapping_11_15;
select count(distinct bene_id) from cms_id.bene_id_mapping_11_15;
-- Verification of de-duplication 
select count(bene_id_deid), patient_num from (
select distinct patient_num, bene_id, bene_id_deid, 
cms_date_shift_days, BH_DATE_SHIFT_DAYS,
cms_dob_shift_months
--, BH_DOB_DATE_SHIFT -- UTSW does not have dob_date_shift
from cms_id."&&out_cms_site_mapping" 
where 
dups_bene_id = 0 and 
dups_pat_num = 0 and 
((dups_missing_map =0) or (bene_id is not null and xw_bene_id is not null))
) group by patient_num
having count(bene_id_deid)>1
;
SELECT 
case 
  when 
    ( 
        select count(distinct patient_num) 
        from (
        select distinct patient_num, bene_id, bene_id_deid, 
        cms_date_shift_days, cms_dob_shift_months, 
        bh_date_shift_days
        --, bh_dob_date_shift -- UTSW does not have dob_date_shift
        from cms_id."&&out_cms_site_mapping" 
        where 
        dups_bene_id = 0 and 
        dups_pat_num = 0
        and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null))
        )
    )   
=
    (
        select count(patient_num) 
        from (
        select distinct patient_num, bene_id, bene_id_deid, 
        cms_date_shift_days, cms_dob_shift_months, 
        bh_date_shift_days
        --, bh_dob_date_shift ---- UTSW does not have dob_date_shift
        from cms_id."&&out_cms_site_mapping" 
        where 
        dups_bene_id = 0 and 
        dups_pat_num = 0
        and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null))
        )    
    ) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
SELECT 
case 
  when 
    (
        select count(distinct BENE_ID_DEID) 
        from (
        select distinct patient_num, bene_id, bene_id_deid, 
        cms_date_shift_days, cms_dob_shift_months, 
        bh_date_shift_days
        --, bh_dob_date_shift ---- UTSW does not have dob_date_shift
        from cms_id."&&out_cms_site_mapping" 
        where 
        dups_bene_id = 0 and 
        dups_pat_num = 0
        and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null))
        )
    )   
=
    (
        select count(BENE_ID_DEID) 
        from (
        select distinct patient_num, bene_id, bene_id_deid, 
        cms_date_shift_days, cms_dob_shift_months, 
        bh_date_shift_days
        --, bh_dob_date_shift -- UTSW does not have dob_date_shift
        from cms_id."&&out_cms_site_mapping" 
        where 
        dups_bene_id = 0 and 
        dups_pat_num = 0
        and (dups_missing_map = 0 or (bene_id is not null and xw_bene_id is not null))
        )    
    ) 
  then 1
  else 1/0
END PASS_FAIL
FROM    dual
;
