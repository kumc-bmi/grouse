/** cms_dem_dstats - Descriptive statistics for CMS Demographics.

This is an initial quality check on the transformation of CMS demographics
into i2b2.

ISSUE: pass/fail testing?

*/

-- This dependency is somewhat indirect.
select sex_cd from cms_patient_dimension where 'dep' = 'cms_dem_txform.sql';

-- ISSUE: how to express dependency on "&&I2B2STAR".patient_dimension?
select birth_date from "&&I2B2STAR".patient_dimension
where 'variable' = 'I2B2STAR';

/** demographic_summary - based on PCORNet CDM EDC Table IA
*/
create or replace view demographic_summary
as
with pat as
  (select pd.*, case
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
  (select 'a' row_order, 'Patients' statistic,(select qty from denominator
    ) n, null "%", &&design_digest source_table
  from dual

  union all
  select 'b', 'Age' statistic, null, null, null from dual
  union all
  select 'b1', 'Mean' statistic,(select round(avg(age_in_years_num)) from pat
    ), null, null
  from dual
  union all
  select 'b2', 'Median' statistic,(select round(median(age_in_years_num)) from pat
    ), null, null
  from dual

  union all
  select 'c', 'Age group' category, null
  , null, null
  from dual
  union all
  select 'c'
    || age_group, age_group, count( *) n
  , round(100 * count( *) / qty, 1), null
  from pat
  cross join denominator
  group by age_group, qty, 'c', null

  -- TODO: Hispanic

  union all
  select 'd', 'Sex' category, null, null, null from dual
  union all
  select 'd'
    || sex_cd, sex_cd, count( *) n
  , round(100 * count( *) / qty, 1), null
  from pat
  cross join denominator
  group by sex_cd, qty, 'd', null

  union all
  select 'e', 'Race' category, null, null, null from dual
  union all
  select 'e'
    || race_cd, race_cd, count( *) n
  , round(100 * count( *) / qty, 1), null
  from pat
  cross join denominator
  group by race_cd, qty, 'e', null

  -- TODO: separate death stuff
  union all
  select 'f', 'Vital Status' category, null
  , null, null
  from dual
  union all
  select 'f'
    || vital_status_cd, vital_status_cd, count( *) n
  , round(100 * count( *) / qty, 1), null
  from pat
  cross join denominator
  group by vital_status_cd, qty, 'f', null
  )
order by row_order ;


select 1 complete
from demographic_summary ds
where ds.source_table =
  &&design_digest
  and rownum = 1;
