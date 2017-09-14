/** cms_dem_dstats - Descriptive statistics for CMS Demographics.

This is an initial quality check on the transformation of CMS demographics
into i2b2.

ISSUE: pass/fail testing?

*/

select ethnicity_cd from "&&I2B2STAR".patient_dimension where 'dep' = 'pdim_add_cols.sql';


/** demographic_summary - based on PCORNet CDM EDC Table IA
*/
create or replace view demographic_summary
as
with pat as (
  select coalesce(pd.sex_cd, 'NI') sex_cd
    , coalesce(pd.race_cd, 'NI') race_cd
    , coalesce(pd.ethnicity_cd, 'NI') ethnicity_cd
    , coalesce(vital_status_cd, 'NI') vital_status_cd
    , age_in_years_num
    , case
      when age_in_years_num between 0 and 4   then ' 0-4'
      when age_in_years_num between 5 and 14  then ' 5-14'
      when age_in_years_num between 15 and 21 then '15-21'
      when age_in_years_num between 22 and 64 then '22-64'
      when age_in_years_num >= 65             then '65+'
      else 'Missing'
    end as age_group
  from "&&I2B2STAR".patient_dimension pd
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
    || ethnicity_cd, null, ethnicity_cd, count( *) n
  , round(100 * count( *) / qty, 1), null
  from pat
  cross join denominator
  group by ethnicity_cd, qty, 'd', null

  union all
  select 'e', 'Sex' statistic, null, null, null, null from dual
  union all
  select 'e'
    || sex_cd, null, sex_cd, count( *) n
  , round(100 * count( *) / qty, 1), null
  from pat
  cross join denominator
  group by sex_cd, qty, 'e', null

  union all
  select 'f', 'Race' statistic, null, null, null, null from dual
  union all
  select 'f'
    || race_cd, null, race_cd, count( *) n
  , round(100 * count( *) / qty, 1), null
  from pat
  cross join denominator
  group by race_cd, qty, 'f', null

  -- TODO: separate death stuff
  union all
  select 'g', 'Vital Status' category, null, null
  , null, null
  from dual
  union all
  select 'g'
    || vital_status_cd, null, vital_status_cd, count( *) n
  , round(100 * count( *) / qty, 1), null
  from pat
  cross join denominator
  group by vital_status_cd, qty, 'g', null
  )
order by row_order ;

-- select * from demographic_summary;

create or replace view cms_dem_stats as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_dem_stats where design_digest = &&design_digest;
