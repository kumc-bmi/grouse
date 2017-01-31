-- deid_tests.sql: Tests to help make sure de-identification was done properly
-- Copyright (c) 2017 University of Kansas Medical Center

-- Verify that we don't report individual ages over 89
with ages_over_89 as (
  select bene_age_at_end_ref_yr age 
  from mbsf_ab_summary 
  where bene_age_at_end_ref_yr > 89
  union all
  select bene_age_cnt age
  from medpar_all where bene_age_cnt > 89
  )
select case when qty > 0 then 1/0 else 1 end ages_over_89 from (
  select count(*) qty from ages_over_89
  );


-- There shouldn't be any birth dates more than 89 years prior to the extract
-- date unless there is a death date.  There should be no birth dates more than
-- 89 years prior to a death date.
with dob_dod as (
  select bene_id, null msis_id, null state_cd, bene_birth_dt dob, bene_death_dt dod, extract_dt from mbsf_ab_summary
  union all
  select bene_id, null msis_id, null state_cd, dob_dt dob, null dod, extract_dt from outpatient_base_claims
  union all
  select bene_id, null msis_id, null state_cd, dob_dt dob, null dod, extract_dt from hha_base_claims
  union all
  select bene_id, null msis_id, null state_cd, dob_dt dob, null dod, extract_dt from bcarrier_claims
  union all
  select bene_id, null msis_id, null state_cd, dob_dt dob, null dod, extract_dt from hospice_base_claims
  union all
  select null bene_id, msis_id, state_cd, el_dob dob, el_dod dod, extract_dt from maxdata_ps
  union all
  select null bene_id, msis_id, state_cd, el_dob dob, mdcr_dod dod, extract_dt from maxdata_ps
  union all
  select null bene_id, msis_id, state_cd, el_dob dob, ssa_dod dod, extract_dt from maxdata_ps
  union all
  select null bene_id, msis_id, state_cd, el_dob dob, null dod, extract_dt from maxdata_ot
  union all
  select null bene_id, msis_id, state_cd, el_dob dob, null dod, extract_dt from maxdata_ip
  union all
  select null bene_id, msis_id, state_cd, el_dob dob, null dod, extract_dt from maxdata_rx
  union all
  select null bene_id, msis_id, state_cd, el_dob dob, null dod, extract_dt from maxdata_lt
  )
select case when count(*) > 0 then 1/0 else 1 end apparent_age_over_89 from (
  select * from (
    select 
      bene_id, msis_id, state_cd, dob, dod, extract_dt, 
      floor(months_between(coalesce(dod, extract_dt), dob)/12) apparent_age
    from dob_dod
    )
  where apparent_age > 89
  )
;
