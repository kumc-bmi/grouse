/** cohort_i2b2_datamart - build an i2b2 datamart for a cohort

ISSUE: combine multiple cohorts?
*/

-- dependencies
select patient_num from grousedata.observation_fact where 1=0;
select :cohort_id_list from dual;
select patient_num from "&&I2B2_STAR".qt_patient_set_collection where 1=0; -- ref build_cohort.sql


delete from "&&I2B2_STAR".observation_fact; commit;

insert /*+ append */ into "&&I2B2_STAR".observation_fact
select * from grousedata.observation_fact gobs
where gobs.patient_num in (
  select patient_num  -- 9,803
  from "&&I2B2_STAR".qt_patient_set_collection psc
  where psc.result_instance_id in (
    select to_number(column_value)
    from xmltable(:cohort_id_list)
  )
)
;
commit;


delete from "&&I2B2_STAR".patient_dimension; commit;

insert /*+ append */ into "&&I2B2_STAR".patient_dimension
select *
from grousedata.patient_dimension gpd
where exists
  (select psc.patient_num
  from "&&I2B2_STAR".qt_patient_set_collection psc
  where psc.result_instance_id  in (
    select to_number(column_value)
    from xmltable(:cohort_id_list)
    )
    and psc.patient_num = gpd.patient_num
  ) ;
commit;

/* cdm_i2p assumes race_cd starts with 01, 02 etc.
   BlueHeron patient_dimension records that do not follow these conventions have been mixed in. */
update "&&I2B2_STAR".patient_dimension pd
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


update "&&I2B2_STAR".patient_dimension pd
set sex_cd = decode(pd.sex_cd, 'Male', 'M=Male'
                   , 'Female', 'F=Female'
                   , 'f', 'F=Female'
                   , 'm', 'M=Male'
                   , pd.sex_cd)
where sex_cd not in ('M=Male', 'F=Female');
commit;

-- select sex_cd, count(*) from patient_dimension group by sex_cd;
-- select race_cd, count(*) from patient_dimension group by race_cd;


delete from "&&I2B2_STAR".visit_dimension; commit;

insert /*+ append */ into "&&I2B2_STAR".visit_dimension
select * from grousedata.visit_dimension gvd
where exists (
  select 1
  from "&&I2B2_STAR".qt_patient_set_collection psc
  where psc.result_instance_id  in (
    select to_number(column_value)
    from xmltable(:cohort_id_list)
  )
  and psc.patient_num = gvd.patient_num
)
;
commit;

-- select count(distinct patient_num) from visit_dimension;
-- select inout_cd, count(*) from visit_dimension group by inout_cd;


-- done already?
select 1 complete
from "&&I2B2_STAR".upload_status
where transform_name = :task_id and load_status='OK'
;
