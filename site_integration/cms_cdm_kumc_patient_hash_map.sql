-- cms_cdm_kumc_patient_hash_map.sql: create cdm_patient_hash_map table needed to link CMS bene_ids with CDM patids
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run against a nighheron sid on whaleshark


-- To verify that the data is from the Calamus release
select * from i2b2pm.pm_project_data;

-- creating the cdm_patient_hash_map table
-- Using i2b2 patient_nums should work as CDM patids are the same as deidentified blueheron patient_nums
alter session set nls_date_format='mm/dd/yyyy';
drop table grouse.cdm_patient_hash_map;
create table grouse.cdm_patient_hash_map as
select nh.patient_num, 
nh.date_shift, 
to_date(nh.birth_date_hipaa,'mm/dd/yyyy') - to_date(nh.birth_date,'mm/dd/yyyy') dob_date_shift,
-- Looks funny but the date of birth is moved forward in time so the person is younger. 
-- Refer to https://bmi-work.kumc.edu/work/browser/heron_load/epic_visit_transform.sql#L86 
fi.patient_num_hash
from nightherondata.patient_dimension nh
join 
nightherondata.patient_mapping pm
on nh.patient_num=pm.patient_num
join
grouse.kumc_finderfile fi
on fi.patient_num = pm.patient_ide;

-- VERIFICATION QUERIES

-- how many patients did we send to CMS in the finderfile
-- based on https://bmi-work.kumc.edu/work/ticket/4292
select count(*) from grouse.kumc_finderfile;
select count(*) from grouse.patient_hash_map;
select count(*) from grouse.cdm_patient_hash_map;

select pm.patient_num, 
fi.patient_num_hash
from grouse.kumc_finderfile fi
join 
nightherondata.patient_mapping pm
on fi.patient_num = pm.patient_ide;

-- is the mapping accurate
-- can be verified by using SSNs which are documented for 99% of the patients according to #4292
select count(*)
from nightherondata.patient_dimension nh
join 
nightherondata.patient_mapping pm
on nh.patient_num=pm.patient_num
join
grouse.kumc_finderfile fi
on fi.patient_num = pm.patient_ide
and trim(REPLACE(nh.ssn, '-', '')) = fi.ssn;

-- verifying that we understand how dateshifts are used
drop table grouse_mapping_alt;
create table grouse_mapping_alt as
select bh.patient_num, 
nh.birth_date start_dob,
bh.birth_date final_dob,
nh.birth_date + ((to_date(nh.birth_date_hipaa,'mm/dd/yyyy')-to_date(nh.birth_date,'mm/dd/yyyy')) + nh.date_shift) verify_dob
from nightherondata.patient_dimension nh
join
blueherondata.patient_dimension@deid bh
on bh.patient_num = nh.patient_num
join
nightherondata.patient_mapping pm
on pm.patient_num = nh.patient_num;

select count(*) from grouse_mapping_alt
where start_dob is not null;

select count(*) from grouse_mapping_alt
where verify_dob = final_dob;