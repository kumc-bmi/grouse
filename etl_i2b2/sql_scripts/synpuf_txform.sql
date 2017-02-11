-- simulate MBSF_AB_SUMMARY using desynpuf CMS.BEN_SUMMARY
create or replace view mbsf_ab_summary as
select desynpuf_id bene_id
    , to_date(bene_birth_dt, 'YYYYMMDD') bene_birth_dt
    , to_date(bene_death_dt, 'YYYYMMDD') bene_death_dt
    , bene_sex_ident_cd
    , bene_race_cd
from cms.ben_summary
    ;

create or replace view bcarrier_claims as
select
    desynpuf_id bene_id
    , clm_id
    , clm_from_dt
    , clm_thru_dt
    , date '2014-12-10' nch_wkly_proc_dt  -- arbitrary
from cms.carrier_claims_1a
-- carrier_claims_1b?
;

select 0 not_complete_always_run
from mbsf_ab_summary, bcarrier_claims
where rownum <= 1
    ;
