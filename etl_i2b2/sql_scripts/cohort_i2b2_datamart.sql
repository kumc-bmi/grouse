/** cohort_i2b2_datamart - build an i2b2 datamart for a cohort

ISSUE: combine multiple cohorts?
*/

-- dependencies
select :cohort_id, &&upload_id
from dual;


create table patient_dimension_&&upload_id
as select * from "&&I2B2_STAR".patient_dimension
where 1=0;

insert /*+ append */ into patient_dimension_&&upload_id
select *
from grousedata.patient_dimension gpd
where exists
  (select psc.patient_num
  from "&&I2B2_STAR".qt_patient_set_collection psc
  where psc.result_instance_id = :cohort_id
    and psc.patient_num = gpd.patient_num
  ) ;
commit;


insert /*+ append */ into patient_dimension_&&upload_id
  (
    patient_num  , vital_status_cd  , birth_date  , death_date  , sex_cd  , age_in_years_num
  , language_cd  , race_cd
--  , ethnicity_cd
  , marital_status_cd  , religion_cd
  , zip_cd  , statecityzip_path
-- , income_cd
--  , patient_blob
  , update_date  , download_date  , import_date  , sourcesystem_cd  , upload_id
  )

with cohort as (
  select patient_num
  from qt_patient_set_collection psc
  where psc.result_instance_id = :cohort_id
)
select upd.patient_num, upd.vital_status_cd, upd.birth_date, upd.death_date
, upd.sex_cd, upd.age_in_years_num
, upd.language_cd, upd.race_cd
, upd.marital_status_cd, upd.religion_cd
, upd.zip_cd, upd.statecityzip_path
-- ISSUE: missing? , upd.income_cd
--, patient_blob
, upd.update_date, upd.download_date, upd.import_date, upd.sourcesystem_cd
, upd.upload_id
from "&&I2B2_STAR_SITE".patient_dimension upd
join cohort on cohort.patient_num = upd.patient_num
left join patient_dimension pd on pd.patient_num = cohort.patient_num
where pd.patient_num is null;


commit;

/* cdm_i2p assumes race_cd starts with 01, 02 etc.
   BlueHeron patient_dimension records that do not follow these conventions have been mixed in. */
update patient_dimension_&&upload_id pd
set race_cd = coalesce(decode(pd.race_cd, 'other', 'OT=Other'
                   , 'asian', '02=Asian'
                   , 'white', '05=White'
                   , 'pac islander', '04=Native Hawaiian or Other Pacific Islander'
                   , 'black', '03=Black'
                   , 'two races', '06=Multiple race'
                   , 'amerian ind', '01=American Indian or Alaska Native'
                   , 'declined', '07=Refuse to answer'
                   , '@', 'NI=No information'
                   , pd.race_cd), 'NI=No information')
where race_cd is null
   or substr(race_cd, 1, 2) not in ('01', '02', '03', '04', '05', '06', '07', 'NI', 'UN', 'OT');


update patient_dimension_&&upload_id pd
set sex_cd = decode(pd.sex_cd, 'Male', 'M=Male'
                   , 'Female', 'F=Female'
                   , 'f', 'F=Female'
                   , 'm', 'M=Male'
                   , pd.sex_cd)
where sex_cd not in ('M=Male', 'F=Female');
commit;

-- select sex_cd, count(*) from patient_dimension_&&upload_id group by sex_cd;
-- select race_cd, count(*) from patient_dimension_&&upload_id group by race_cd;


create table visit_dimension_&&upload_id
as select * from "&&I2B2_STAR".visit_dimension
where 1=0;

insert /*+ append */ into visit_dimension_&&upload_id
select * from grousedata.visit_dimension gvd
where exists (
  select 1
  from qt_patient_set_collection psc
  where psc.result_instance_id = :cohort_id
  and psc.patient_num = gvd.patient_num
)
;
commit;

-- select count(distinct patient_num) from visit_dimension_&&upload_id; -- 1048
-- select inout_cd, count(*) from visit_dimension_&&upload_id group by inout_cd;


create table observation_fact_&&upload_id
as select * from "&&I2B2_STAR".observation_fact
where 1=0;

insert /*+ append */ into observation_fact_&&upload_id
select * from grousedata.observation_fact gobs
where gobs.patient_num in (
  select patient_num
  from qt_patient_set_collection psc
  where psc.result_instance_id = :cohort_id
)
;
commit;


-- done already?
select 1 complete
from "&&I2B2_STAR".upload_status
where transform_name = :task_id and load_status='OK'
;
