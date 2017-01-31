-- hipaa_dob_shift.sql: DOB shift to satisfy HIPAA "Safe Harbor" requirements
-- https://www.hhs.gov/hipaa/for-professionals/privacy/special-topics/de-identification/
-- Copyright (c) 2017 University of Kansas Medical Center

-- First, insert min/max date for every bene_id
insert /*+ APPEND */ into min_max_date_events
select 
  mm.bene_id, null msis_id, null state_cd, mindt, maxdt from (
    select /*+ PARALLEL(date_events,12) */
      bene_id, min(dt) mindt, max(dt) maxdt
    from date_events
    where bene_id is not null
    group by bene_id
    ) mm;
commit;


-- Next, insert min/max date for every msis_id + state_cd where bene_id is null
insert /*+ APPEND */ into min_max_date_events
select /*+ PARALLEL(date_events,12) */
  null bene_id, msis_id, state_cd, mindt, maxdt from (
    select
      msis_id, state_cd, min(dt) mindt, max(dt) maxdt
    from date_events
    where bene_id is null
    group by msis_id, state_cd
    ) mm;
commit;


-- Finally, insert the per-person dob shift
insert /*+ APPEND */ into dob_shift
with hipaa as (
  select 89 * 12 max_age_months from dual
  ),
age_months as (
  select /*+ PARALLEL(min_max_date_events,12) */
    bene_id, msis_id, state_cd, round(months_between(max_dt, min_dt)) age_months
  from min_max_date_events
  )
select
  am.bene_id, am.msis_id, am.state_cd,
  case
    when am.age_months > hipaa.max_age_months
    then am.age_months - hipaa.max_age_months
    else null
  end dob_shift_months
from age_months am cross join hipaa
;
commit;
