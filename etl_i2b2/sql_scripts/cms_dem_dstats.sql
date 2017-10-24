/** cms_dem_dstats - Descriptive statistics for CMS Demographics.

This is an initial quality check on the transformation of CMS demographics
into i2b2.

*/

select ethnicity_cd from "&&I2B2STAR".patient_dimension where 'dep' = 'pdim_add_cols.sql';


create or replace view pcornet_demographic as
select patient_num patid
     , trunc(birth_date) birth_date
     , to_char(birth_date, 'HH24:MI') birth_time
     , nvl(substr(sex_cd, 1, 1), 'NI') sex
     , 'NI' sexual_orientation
     , 'NI' gender_identity
     , nvl(substr(ethnicity_cd, 1, 1), 'NI') hispanic
     , 'N' biobank_flag -- ISSUE: biobank_flag is after race in the parseable PCORNet CDM spec
     , nvl(substr(race_cd, 1, 2), 'NI') race
     , sex_cd raw_sex
     , null raw_sexual_orientation
     , null RAW_GENDER_IDENTITY
     , ethnicity_cd RAW_HISPANIC
     , race_cd RAW_RACE
from "&&I2B2STAR".patient_dimension
;

/** demographic_summary - based on PCORNet CDM EDC Table IA
*/
create or replace view demographic_summary
as
with
pat_age as (
select dem.*
     , trunc((current_date - birth_date) / 365.25) age_in_years_num
from pcornet_demographic dem
),
pat as (
  select sex
    , race
    , hispanic
    , age_in_years_num
    , case
      when age_in_years_num between 0 and 4   then ' 0-4'
      when age_in_years_num between 5 and 14  then ' 5-14'
      when age_in_years_num between 15 and 21 then '15-21'
      when age_in_years_num between 22 and 64 then '22-64'
      when age_in_years_num >= 65             then '65+'
      else 'Missing'
    end as age_group
  from pat_age dem
  ), denominator as
  (select count( *) qty from pat
  )

select *
from
  (select 'a' row_order, 'Patients' statistic, null category, (select qty from denominator
    ) n, null "%", null source_table
  from dual

  union all
  select 'b', 'Age' statistic, null, null, null, null from dual
  union all
  select 'b1', null, 'Mean' statistic,(select round(avg(age_in_years_num)) from pat
    ), null, null
  from dual
  union all
  select 'b2', null, 'Median' statistic,(select round(median(age_in_years_num)) from pat
    ), null, null
  from dual

  union all
  select 'c', 'Age group' statistic, null, null
  , null, null
  from dual
  union all
  select 'c'
    || age_group, null, age_group, count( *) n
  , round(100 * count( *) / qty, 1), null
  from pat
  cross join denominator
  group by age_group, qty, 'c', null

  union all
  select 'd', 'Hispanic' statistic, null, null, null, null from dual
  union all
  select 'd'
    || hispanic, null, hispanic, count( *) n
  , round(100 * count( *) / qty, 1), null
  from pat
  cross join denominator
  group by hispanic, qty, 'd', null

  union all
  select 'e', 'Sex' statistic, null, null, null, null from dual
  union all
  select 'e'
    || sex, null, sex, count( *) n
  , round(100 * count( *) / qty, 1), null
  from pat
  cross join denominator
  group by sex, qty, 'e', null

  union all
  select 'f', 'Race' statistic, null, null, null, null from dual
  union all
  select 'f'
    || race, null, race, count( *) n
  , round(100 * count( *) / qty, 1), null
  from pat
  cross join denominator
  group by race, qty, 'f', null
  )
order by row_order ;

-- select * from demographic_summary;

create or replace view cms_dem_stats as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_dem_stats where design_digest = &&design_digest;
