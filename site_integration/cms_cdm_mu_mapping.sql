-- cms_mu_mapping.sql: create a mapping to integrate CMS data and blueheron data
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against the identified GROUSE data. 

-- verify that the hashmapcrosswalk table exists for MU
-- Was staged in ticket:4743#comment:21
select count(*) from mu_cdm.hashmapcrosswalk;

-- Looking at the characteristics of the table
-- In comparison to blueherondata_kumc.hashmapcrosswalk it is missing dob_date_shift
select * from mu_cdm.hashmapcrosswalk;
-- sigh, patient_num column is named i2b2_cdm_patient_id
-- the date_shift field can be negative or positive
-- the date_shift field has newlines and needs to_number(REPLACE(REPLACE(date_shift, CHR(10), ''), CHR(13), ''))

-- creating the mapping table that will have all the MU patients 
-- and CMS cohort along with the links between bene_ids and patient_nums
-- Note: takes a while to run (~30 minutes)
drop table cms_id.cms_cdm_mu_mapping;
create table cms_id.cms_cdm_mu_mapping as 
select distinct pd.patid patient_num,
gpd.bene_id, gpd.bene_id_deid, 
gpd.date_shift_days cms_date_shift_days, 
0 cms_dob_shift_months, 
0 dups_pat_num,
0 dups_bene_id,
0 dups_missing_map,
bpd.*
from
cms_id.bene_id_mapping gpd

full outer join
(
  select bene_id xw_bene_id, pat_match, sex_match, dob_match,
  i2b2_cdm_patient_id mp_patient_num, 
  to_number(REPLACE(REPLACE(date_shift, CHR(10), ''), CHR(13), '')) bh_date_shift_days, 
  0 bh_dob_date_shift -- we did not receive dob shift from other sites
  from cms_id.unique_bene_xwalk xw
  join mu_cdm.hashmapcrosswalk mp 
  on mp.patient_num_hash=xw.patientnum_hash
) bpd
on gpd.bene_id = bpd.xw_bene_id

full outer join

mu_cdm.demographic pd
on bpd.mp_patient_num = pd.patid;

select count(*) from 
cms_id.cms_cdm_mu_mapping;

-- if patient_num and bene_id_deid are not null 
-- the row represents a patient who is part of both the site data and CMS data
select distinct patient_num, bene_id from cms_id.cms_cdm_mu_mapping
where patient_num is not null and
bene_id is not null;