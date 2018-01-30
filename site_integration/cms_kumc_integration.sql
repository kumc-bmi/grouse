-- cms_kumc_integration.sql: Integrate site dimensions & facts with grouse
-- Copyright (c) 2017 University of Kansas Medical Center
-- Run on de-identified GROUSE server. 

-- *** TODO: Port this to code that runs on GROUSE identified server
-- *** TODO: This could be fancier. Blueherondata_kumc is still site specific
-- But this table can some what explain the numbers we are using here as upload_ids
drop table site_dimensions;
create table site_dimensions as 
select 1 as site_id, 'kumc' site_name, 
'blueherondata_kumc' as site_schema,
20000000 patient_num_min, 25000000 patient_num_max, 
400000000 encounter_num_min, 460000000 encounter_num_max
from dual;


-- ========== PATIENT_DIMENSION
-- ***TODO: Implications of giving grousedata.patient_dimension precendence (DOB for example)
-- When the patient exists in both cohorts we are keeping the GROUSE version of the patients

merge into grousedata.patient_dimension pd
using   
blueherondata_kumc.patient_dimension_int pdi
on      (pdi.patient_num = pd.patient_num)
when not matched then 
    insert (patient_num, vital_status_cd,birth_date, death_date, sex_cd, 
    age_in_years_num, 
    language_cd, race_cd, marital_status_cd, religion_cd, 
    zip_cd, statecityzip_path, income_cd, patient_blob, 
    update_date, 
    download_date, 
    import_date, 
    sourcesystem_cd, 
    upload_id, 
    ethnicity_cd)
values (
pdi.patient_num, pdi.vital_status_cd, pdi.birth_date, pdi.death_date, pdi.sex_cd, 
pdi.age_in_years_num,-- ***todo - check if this requires extra work
pdi.language_cd, pdi.race_cd, pdi.marital_status_cd, pdi.religion_cd, 
pdi.zip_cd, pdi.statecityzip_path, pdi.income_cd, pdi.patient_blob, 
pdi.update_date, 
pdi.download_date, 
sysdate, -- import_date
pdi.sourcesystem_cd,
(select -1*patient_num_min from site_dimensions), -- upload_id
pdi.ethnicity_cd);

commit;


-- ========== VISIT_DIMENSION
insert into grousedata.visit_dimension 
 (encounter_num, patient_num, active_status_cd, start_date, end_date, 
inout_cd, location_cd, location_path, length_of_stay, visit_blob, 
update_date, download_date, 
import_date, 
sourcesystem_cd, 
upload_id, 
drg, 
discharge_status, 
discharge_disposition, 
location_zip, 
admitting_source, 
facilityid, 
providerid
)
select 
encounter_num, patient_num, active_status_cd, start_date, end_date, 
inout_cd, location_cd, location_path, length_of_stay, visit_blob, 
update_date, download_date, 
sysdate, -- import_date 
sourcesystem_cd, 
(select -1*patient_num_min from site_dimensions), -- upload_id
 null, -- drg 
discharge_status, 
discharge_disposition, 
null, -- location_zip
admitting_source, 
null, -- facilityid, 
'@' -- providerid -- ticket:5054#comment:5 
from blueherondata_kumc.visit_dimension_int;

commit;


-- ========== CONCEPT DIMENSION
insert into grousedata.concept_dimension 
(concept_path, 
concept_cd, name_char, concept_blob, update_date, download_date, 
import_date, 
sourcesystem_cd, 
upload_id)
select 
'\' || (select site_name from site_dimensions) || concept_path,
concept_cd, name_char, concept_blob, update_date, download_date, 
sysdate, -- import_date
sourcesystem_cd, 
(select -1*patient_num_min from site_dimensions) -- upload_id 
from
blueherondata_kumc.concept_dimension_int;

commit;


-- ========== MODIFIER DIMENSION
insert into grousedata.modifier_dimension
(modifier_path, 
modifier_cd, 
name_char, 
modifier_blob, 
update_date, 
download_date, 
import_date, 
sourcesystem_cd, 
upload_id 
)
select
'\' || (select site_name from site_dimensions) || modifier_path,
modifier_cd, 
name_char, 
modifier_blob, 
update_date, 
download_date, 
sysdate, -- import_date, 
sourcesystem_cd, 
(select -1*patient_num_min from site_dimensions) -- upload_id 
from
blueherondata_kumc.modifier_dimension_int;

commit;


-- ========== OBSERVATION_FACT
insert into grousedata.observation_fact 
(encounter_num, 
    patient_num, 
    concept_cd, 
    provider_id, 
    start_date, modifier_cd, instance_num, valtype_cd, 
    tval_char, nval_num, valueflag_cd, quantity_num, 
    units_cd, end_date, location_cd, observation_blob, 
    confidence_num, update_date, download_date, 
    import_date, 
    sourcesystem_cd, 
    upload_id)
select 
encounter_num, 
    patient_num,
    concept_cd, 
    '@', -- providerid 
    start_date, modifier_cd, instance_num, valtype_cd, 
    null, nval_num, valueflag_cd, quantity_num, 
    units_cd, end_date, location_cd, observation_blob, 
    confidence_num, update_date, download_date, 
    sysdate, -- obi.import_date, 
    sourcesystem_cd, 
    (select -1*patient_num_min from site_dimensions) -- upload_id 
from blueherondata_kumc.observation_fact_int obi;

commit;


-- ========== PATIENT_DIMENSION VERIFICATION
-- before running the above merge
select count(*) from blueherondata_kumc.patient_dimension_int;

select max(patient_num), min(patient_num) from grousedata.patient_dimension;

select count(*) from grousedata.patient_dimension;

select count(*) from grousedata.patient_dimension
where upload_id=(select -1*patient_num_min from site_dimensions);

-- after running the above merge
select count(*) from grousedata.patient_dimension;

-- after running the above merge
select count(*) from grousedata.patient_dimension
where upload_id=(select -1*patient_num_min from site_dimensions);
-- patient records common between the two cohorts should match earlier counts.

select count(*) from grousedata.patient_dimension gh
join blueherondata_kumc.patient_dimension_int bh
on bh.patient_num = gh.patient_num
where gh.patient_num < (select patient_num_min from site_dimensions);
-- should match the above count

select count(*) from grousedata.patient_dimension gh
join blueherondata_kumc.patient_dimension_int bh
on bh.patient_num = gh.patient_num;
-- should match the number of inserts


-- ========== VISIT_DIMENSION VERIFICATION
-- before running the merge
select count(*) from blueherondata_kumc.visit_dimension_int;

select max(encounter_num), min(encounter_num) 
from blueherondata_kumc.visit_dimension_int;

select max(encounter_num), min(encounter_num) from grousedata.visit_dimension;

select count(*) from grousedata.visit_dimension; 

-- after running the above merge
select count(*) from grousedata.visit_dimension
where upload_id=(select -1*patient_num_min from site_dimensions);

select count(*) from blueherondata_kumc.visit_dimension_int;

select max(encounter_num), min(encounter_num) 
from blueherondata_kumc.visit_dimension_int;

select max(encounter_num), min(encounter_num) from grousedata.visit_dimension;

select count(*) from grousedata.visit_dimension; 

select count(*) from grousedata.visit_dimension
where upload_id=(select -1*patient_num_min from site_dimensions);


-- ========== CONCEPT_DIMENSION VERIFICATION
-- before the merge
select * from grousedata.concept_dimension;

select count(*) from grousedata.concept_dimension;

select count(*) from (
select distinct concept_path, concept_cd 
from grousedata.concept_dimension);

select count(*) from grousedata.concept_dimension
where concept_path like '\kumc%';

select count(*) from blueherondata_kumc.concept_dimension_int;

select count(*) from grousedata.concept_dimension
where upload_id=(select -1*patient_num_min from site_dimensions);

select count(concept_cd), concept_path
from grousedata.concept_dimension
group by concept_path
having count(concept_cd) > 1;

-- after the merge
select * from grousedata.concept_dimension;

select count(*) from grousedata.concept_dimension;

select 2243843+3515460 from dual;

select distinct concept_path from grousedata.concept_dimension;

select count(*) from grousedata.concept_dimension
where concept_path like '\kumc\%';

select count(*) from blueherondata_kumc.concept_dimension_int;

select * from grousedata.concept_dimension
where upload_id=(select -1*patient_num_min from site_dimensions)
and concept_path  not like '\kumc\%';

select * from blueherondata_kumc.concept_dimension_int 
where concept_cd='ORIGPX:NI';

select * from grousedata.concept_dimension 
where concept_cd='ORIGPX:NI' or concept_path='1';
-- may be we can ignore and just caveat it.

select * from grousedata.concept_dimension
where upload_id=(select -1*patient_num_min from site_dimensions)
and concept_path like '1%';

select count(*) from (
select distinct concept_path, concept_cd from grousedata.concept_dimension);

select count(concept_cd), concept_path
from grousedata.concept_dimension
group by concept_path
having count(concept_cd) > 1;


-- ========== MODIFIER_DIMENSION VERIFICATION
-- before the merge
select count(*) from grousedata.modifier_dimension;

select count(*) from (
select distinct modifier_path, modifier_cd 
from grousedata.modifier_dimension);

select count(*) from grousedata.modifier_dimension
where modifier_path like '\kumc%';

select count(*) from blueherondata_kumc.modifier_dimension_int;

select count(*) from grousedata.modifier_dimension
where upload_id=(select -1*patient_num_min from site_dimensions);

select count(modifier_cd), modifier_path
from grousedata.modifier_dimension
group by modifier_path
having count(modifier_cd) > 1;

-- after the merge
select count(*) from grousedata.modifier_dimension;

select count(*) from (
select distinct modifier_path, modifier_cd 
from grousedata.modifier_dimension);

select count(*) from grousedata.modifier_dimension
where modifier_path like '\kumc%';

select count(*) from blueherondata_kumc.modifier_dimension_int;

select count(*) from grousedata.modifier_dimension
where upload_id=(select -1*patient_num_min from site_dimensions);

select count(modifier_cd), modifier_path
from grousedata.modifier_dimension
group by modifier_path
having count(modifier_cd) > 1;


-- ========== OBSERVATION_FACT VERIFICATION
-- before the merge
select * from grousedata.observation_fact;

select count(*) from grousedata.observation_fact;

select count(*) from blueherondata_kumc.observation_fact_int;

select count(*) from 
grousedata.observation_fact
where upload_id=-20000000;

-- after the merge
select * from grousedata.observation_fact;

select count(*) from grousedata.observation_fact;

select count(*) from blueherondata_kumc.observation_fact_int;

select count(*) from 
grousedata.observation_fact
where upload_id=-20000000;
