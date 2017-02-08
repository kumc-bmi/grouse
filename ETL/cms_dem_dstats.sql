/** cms_dem_dstats -- Descriptive statistics for CMS Demographics.

This is an initial quality check on the transformation of CMS demographics
into i2b2.

TODO: arrange for Luigi to save its output.

TODO: pass/fail testing?
*/

select birth_date from cms_patient_dimension where 1=0; -- prereq check. TODO: integrate with Luigi?

-- ISSUE: what does the analagous PCORNet CDM EDC table look like?
create or replace view age_dist_by_decade
as
  select decade
  , vital_status_cd
  , count(*) freq
  from
    (select round(age_in_years_num / 10) * 10 decade
    , vital_status_cd
    from
      (select age_in_years_num, vital_status_cd
      from cms_patient_dimension
      )
    )
  group by decade
  , vital_status_cd
  order by decade
  , vital_status_cd ;

select * from age_dist_by_decade;  -- TODO: get Luigi to save this

/** Sex distribution
TODO: test that it's roughly 1/2 and 1/2?
*/
select sex_cd
, count(*) freq
from cms_patient_dimension
group by sex_cd
order by 1 ;


-- Race distribution
-- TODO: decode into race, hispanic, as
-- CMS data seems to pre-date separation of ethnicity from race.
select race_cd
, count(*) freq
from cms_patient_dimension
group by race_cd
order by 1 ;

-- TODO: DEATH stats


