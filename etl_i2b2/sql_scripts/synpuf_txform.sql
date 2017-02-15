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
  , icd9_dgns_cd_1 icd_dgns_cd1, case when icd9_dgns_cd_1 is not null then '9' end icd_dgns_vrsn_cd1
  , icd9_dgns_cd_2 icd_dgns_cd2, case when icd9_dgns_cd_2 is not null then '9' end icd_dgns_vrsn_cd2
  , icd9_dgns_cd_3 icd_dgns_cd3, case when icd9_dgns_cd_3 is not null then '9' end icd_dgns_vrsn_cd3
  , icd9_dgns_cd_4 icd_dgns_cd4, case when icd9_dgns_cd_4 is not null then '9' end icd_dgns_vrsn_cd4
  , icd9_dgns_cd_5 icd_dgns_cd5, case when icd9_dgns_cd_5 is not null then '9' end icd_dgns_vrsn_cd5
  , icd9_dgns_cd_6 icd_dgns_cd6, case when icd9_dgns_cd_6 is not null then '9' end icd_dgns_vrsn_cd6
  , icd9_dgns_cd_7 icd_dgns_cd7, case when icd9_dgns_cd_7 is not null then '9' end icd_dgns_vrsn_cd7
  , icd9_dgns_cd_8 icd_dgns_cd8, case when icd9_dgns_cd_8 is not null then '9' end icd_dgns_vrsn_cd8
  , null icd_dgns_cd9, null icd_dgns_vrsn_cd9
  , null icd_dgns_cd10, null icd_dgns_vrsn_cd10
  , null icd_dgns_cd11, null icd_dgns_vrsn_cd11
  , null icd_dgns_cd12, null icd_dgns_vrsn_cd12
from
  cms.carrier_claims_1a ;
-- TODO: CMS_TABLE_SPEC,BCARRIER_CLAIMS,24,PRNCPAL_DGNS_CD,VARCHAR2
-- ISSUE: segment?

create or replace view medpar_all
as
  select
    desynpuf_id bene_id
  , clm_id || '.' || segment medpar_id
  , prvdr_num
  , to_date(clm_admsn_dt, 'YYYYMMDD') admsn_dt
  , to_date(nch_bene_dschrg_dt, 'YYYYMMDD') dschrg_dt
  , to_number(clm_utlztn_day_cnt) los_day_cnt -- guessing, here. But for test data, should be good enough.  
  , clm_drg_cd
  , icd9_dgns_cd_1 dgns_1_cd, case when icd9_dgns_cd_1 is not null then '9' end dgns_vrsn_cd_1  -- TODO: thru 25
  -- procedures: SRGCL_PRCDR_1_CD etc.
  from
    cms.inpatient ;

/* Simulate BCARRIER_LINE for place of service,
whence comes PCORNet ENC_TYPE via i2b2 INOUT_CD
*/
create or replace view bcarrier_line as
with place_of_service as
(
-- ref https://www.resdac.org/cms-data/variables/line-place-service-code,
--     https://www.resdac.org/sites/resdac.umn.edu/files/Place%20of%20Service%20Table.txt 
 select '11' office from dual
)
select desynpuf_id bene_id
, cc1.clm_id
, 1 line_num
, place_of_service.office line_place_of_srvc_cd
from
cms.carrier_claims_1a cc1, place_of_service
;


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

