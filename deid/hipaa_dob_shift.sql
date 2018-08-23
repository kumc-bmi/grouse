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

whenever sqlerror continue;
drop table min_max_date_events_up;
whenever sqlerror exit;

create table min_max_date_events_up as
(
select
    cmm.bene_id bene_id,
    cmm.msis_id,
    cmm.state_cd,
    least(cmm.min_dt, coalesce(pmm.min_dt,cmm.min_dt), coalesce(p2mm.min_dt,cmm.min_dt) ) min_dt,
    greatest(cmm.max_dt, coalesce(pmm.max_dt,cmm.max_dt), coalesce(p2mm.max_dt,cmm.max_dt) ) max_dt
from
    min_max_date_events cmm
left join
    "&&prev_cms_id_schema".min_max_date_events pmm
on
    cmm.bene_id = pmm.bene_id
left join
    "&&prev_cms_id_schema2".min_max_date_events p2mm
on
    cmm.bene_id = p2mm.bene_id
);

-- Finally, insert the per-person dob shift
insert /*+ APPEND */ into dob_shift
with hipaa as (
  select 89 * 12 max_age_months from dual
  ),
age_months as (
  select /*+ PARALLEL(min_max_date_events_up,12) */
    bene_id, msis_id, state_cd, ceil(months_between(max_dt, min_dt)) age_months
  from min_max_date_events_up
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

/*
TODO:
Does 2011-13 data dates need to be shift as some people 
age would have cross 89 after 2014-15 data ?
*/
