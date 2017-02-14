-- simulate MBSF_AB_SUMMARY using desynpuf CMS.BEN_SUMMARY
create or replace view mbsf_ab_summary as
select desynpuf_id bene_id
    , to_date(bene_birth_dt, 'YYYYMMDD') bene_birth_dt
    , to_date(bene_death_dt, 'YYYYMMDD') bene_death_dt
    , bene_sex_ident_cd
    , bene_race_cd
from cms.ben_summary
    ;

-- simulate bcarrier_claims using desynpuf carrier_claims_1a
-- ISSUE: carrier_claims_1b?
create or replace view bcarrier_claims
as
  select desynpuf_id bene_id
  , clm_id
  , to_date(clm_from_dt, 'YYYYMMDD') clm_from_dt
  , to_date(clm_thru_dt, 'YYYYMMDD') clm_thru_dt
  , date '2014-12-10' nch_wkly_proc_dt -- arbitrary
  from cms.carrier_claims_1a ;


create index mbsf_bene on cms.ben_summary
  (
    desynpuf_id
  ) ;

create index bclaim_clm on cms.carrier_claims_1a
  (
    clm_id
  ) ;

create index bclaim_bene on cms.carrier_claims_1a
  (
    desynpuf_id
  ) ;

select 0 not_complete_always_run
from mbsf_ab_summary
, bcarrier_claims
where rownum <= 1 ;

