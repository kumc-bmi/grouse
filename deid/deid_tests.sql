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
