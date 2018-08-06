/** cms_patient_dimension - Load patient dimension from observation_fact.

handy for development:
  truncate table "&&I2B2STAR".patient_dimension;
  drop table cms_pcornet_map;
*/

select cms_table from cms_pcornet_map where 'check' = 'see LoadDataFile';
select ethnicity_cd from "&&I2B2STAR".patient_dimension where 'dep' = 'pdim_add_cols.sql';

/***
 * Birth Date from BENE_BIRTH_DT:, 'EL_DOB: facts
 *
 * In case multiple birth dates are recorded, use latest.
 */
merge /*+ parallel(pd, 8) */into "&&I2B2STAR".patient_dimension pd
using (
  select /*+ parallel(obs, 20) */patient_num
       , max(start_date) birth_date
  from "&&I2B2STAR".observation_fact obs
  where obs.concept_cd in ('BENE_BIRTH_DT:', 'EL_DOB:')
  group by patient_num
) obs on (obs.patient_num = pd.patient_num)
when matched then
  update set pd.birth_date = obs.birth_date, upload_id = :upload_id
  where pd.birth_date is null or pd.birth_date != obs.birth_date
when not matched then
  insert (patient_num, birth_date, upload_id) values (obs.patient_num, obs.birth_date, :upload_id)
;
commit;


/***
 * Death Date
 *
 * TODO: MDCR_DEATH_DAY_SW 	Day of death verified (from Mcare EDB)
 */
merge /*+ parallel(pd, 8) */into "&&I2B2STAR".patient_dimension pd
using (
  select /*+ parallel(obs, 20) */patient_num, max(start_date) death_date
  from "&&I2B2STAR".observation_fact obs
  where obs.concept_cd in ('BENE_DEATH_DT:', 'NDI_DEATH_DT:', 'EL_DOD:', 'MDCR_DOD:')
  group by patient_num
) obs on (obs.patient_num = pd.patient_num)
when matched then
  update set pd.death_date = obs.death_date, upload_id = :upload_id
  where pd.death_date is null or pd.death_date != obs.death_date
when not matched then
  insert (patient_num, death_date, upload_id) values (obs.patient_num, obs.death_date, :upload_id)
;
commit;


/***
 * Age
 *
 * this should be just an update...
 */
merge /*+ parallel(pd, 8) */into "&&I2B2STAR".patient_dimension pd
using (
  select patient_num
       , trunc((least(sysdate, nvl( death_date, sysdate)) - birth_date) / 365.25) age_in_years_num
  from "&&I2B2STAR".patient_dimension
  where birth_date is not null
) age_calc
on (age_calc.patient_num = pd.patient_num)
when matched then
  update set pd.age_in_years_num = age_calc.age_in_years_num
  where pd.age_in_years_num is null or pd.age_in_years_num != age_calc.age_in_years_num
;
commit;


/***
 * Sex
 */
merge /*+ parallel(pd, 8) */into "&&I2B2STAR".patient_dimension pd
using (
  with ea as (
    select column_name || ':' || "Code" concept_cd
         , valueset_item_descriptor sex_cd
    from cms_pcornet_map where field_name = 'SEX' and pcornet_table = 'DEMOGRAPHIC'
  )
  , dem as (
    select /*+ parallel(obs, 20) */ obs.patient_num, obs.concept_cd, ea.sex_cd
    from "&&I2B2STAR".observation_fact obs
    join ea on ea.concept_cd = obs.concept_cd
  )
  -- select sex_cd, count(distinct patient_num) from dem group by sex_cd
  select patient_num, min(sex_cd) sex_cd
  from dem
  group by patient_num
) obs on (obs.patient_num = pd.patient_num)
when matched then
  update set pd.sex_cd = obs.sex_cd, upload_id = :upload_id
  where pd.sex_cd is null or pd.sex_cd != obs.sex_cd
when not matched then
  insert (patient_num, sex_cd, upload_id) values (obs.patient_num, obs.sex_cd, :upload_id)
;
commit;


/***
 * Race
 *
 * Decode to PCORNet CDM values
 */
merge /*+ parallel(pd, 8) */into "&&I2B2STAR".patient_dimension pd
using (
  with ea as (
    select column_name || ':' || "Code" concept_cd
         , valueset_item_descriptor race_cd
    from cms_pcornet_map where field_name = 'RACE' and pcornet_table = 'DEMOGRAPHIC'
  )
  , dem as (
    select /*+ parallel(obs, 20) */ obs.patient_num, obs.concept_cd, ea.race_cd
    from "&&I2B2STAR".observation_fact obs
    join ea on ea.concept_cd = obs.concept_cd
  )
  -- select race_cd, concept_cd, count(*) from dem group by race_cd, concept_cd order by race_cd, concept_cd
  select patient_num, min(race_cd) race_cd
  from dem
  group by patient_num
) obs on (obs.patient_num = pd.patient_num)
when matched then
  update set pd.race_cd = obs.race_cd, upload_id = :upload_id
  where pd.race_cd is null or pd.race_cd != obs.race_cd
when not matched then
  insert (patient_num, race_cd, upload_id) values (obs.patient_num, obs.race_cd, :upload_id)
;
commit;


/***
 * Ethnicity / Hispanic
 *
 * Decode to PCORNet CDM values
 */
merge /*+ parallel(pd, 8) */into "&&I2B2STAR".patient_dimension pd
using (
  with ea as (
    select column_name || ':' || "Code" concept_cd
         , valueset_item_descriptor ethnicity_cd
    from cms_pcornet_map where field_name = 'HISPANIC' and pcornet_table = 'DEMOGRAPHIC'
  )
  , dem as (
    select /*+ parallel(obs, 20) */ obs.patient_num, obs.concept_cd, ea.ethnicity_cd
    from "&&I2B2STAR".observation_fact obs
    join ea on ea.concept_cd = obs.concept_cd
  )
  select patient_num, min(ethnicity_cd) ethnicity_cd
  from dem
  group by patient_num
) obs on (obs.patient_num = pd.patient_num)
when matched then
  update set pd.ethnicity_cd = obs.ethnicity_cd, upload_id = :upload_id
  where pd.ethnicity_cd is null or pd.ethnicity_cd != obs.ethnicity_cd
when not matched then
  insert (patient_num, ethnicity_cd, upload_id) values (obs.patient_num, obs.ethnicity_cd, :upload_id)
;

commit;


select 1 complete
from "&&I2B2STAR".patient_dimension pd
where pd.upload_id =
  (select max(upload_id)
  from "&&I2B2STAR".upload_status up
  where up.transform_name = :task_id
  and load_status = 'OK'
  )
  and rownum = 1;
