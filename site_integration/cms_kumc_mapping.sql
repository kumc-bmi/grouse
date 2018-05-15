-- cms_kumc_mapping.sql: create a mapping to integrate CMS data and blueheron data
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against the identified GROUSE data. 

-- verify that this table has been created by patient_hash_map.sql and imported
select count(*) from blueherondata_kumc.patient_hash_map;

select * from blueherondata_kumc.patient_hash_map;
select * from cms_id.unique_bene_xwalk xw;
select * from cms_id.bene_id_mapping gpd;

-- creating the mapping table that will have all the KUMC patients 
-- and CMS cohort along with the links between bene_ids and patient_nums
-- Note: takes a while to run
drop table cms_id.cms_kumc_mapping;
create table cms_id.cms_kumc_mapping as 
select distinct pd.patient_num,
gpd.bene_id, gpd.bene_id_deid, 
gpd.date_shift_days cms_date_shift_days, 
gpd.dob_shift_months cms_dob_shift_months, 
0 dups_pat_num,
0 dups_bene_id,
0 dups_missing_map,
bpd.*
from
cms_id.bene_id_mapping gpd

full outer join
(
  select bene_id xw_bene_id, pat_match, sex_match, dob_match,
  patient_num mp_patient_num, date_shift bh_date_shift_days, 
  dob_date_shift bh_dob_date_shift
  from cms_id.unique_bene_xwalk xw
  join blueherondata_kumc.patient_hash_map mp 
  on mp.patient_num_hash=xw.patientnum_hash
) bpd
on gpd.bene_id = bpd.xw_bene_id

full outer join

blueherondata_kumc.patient_dimension pd
on bpd.mp_patient_num = pd.patient_num;

select count(*) from 
cms_id.cms_kumc_mapping;

-- if patient_num and bene_id_deid are not null 
-- the row represents a patient who is part of both the site data and CMS data
select distinct patient_num, bene_id from cms_id.cms_kumc_mapping
where patient_num is not null and
bene_id is not null;