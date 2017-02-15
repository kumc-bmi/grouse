/** cms_dx_txform - Make i2b2 facts from CMS diagnoses
*/


create or replace view observation_fact_cms_dx
as
  select
  -- TODO: join with bcarrier_line to get real line_num
    'LINE:' || lpad('1', 4) || ' CLM_ID:' || detail.clm_id encounter_ide
  , &&cms_source_cd encounter_ide_source
  , bene_id, case
      when dgns_vrsn = '9' then 'ICD9:'
        || substr(dgns_cd, 1, 3)
        ||
        case
          when length(dgns_cd) > 3 then '.'
            || substr(dgns_cd, 4)
          else ''
        end
      when dgns_vrsn = '10' then 'ICD10:'
        || dgns_cd -- ISSUE: IDC10 code formatting?
    end concept_cd, '@' provider_id -- TODO: providerID
  , clm_from_dt start_date
  , modifier_cd -- ISSUE: ADMIT_DIAG???
  , detail.i instance_num
  , '@' valtype_cd, null tval_char, to_number(null) nval_num, null valueflag_cd
  , null quantity_num
  , null units_cd
  , clm_thru_dt end_date
  , null location_cd, to_number(null) confidence_num
    -- ISSUE: split into years or groups of patients?     , 1 as part
  , sysdate update_date  -- TODO
  , &&cms_source_cd sourcesystem_cd
  from
    (
    -- KLUDGE: we're using instance_num for uniqueness where we probably shouldn't.
    -- TODO: re-think instance_num vs modifier for uniqueness
    select
  bene_id, clm_id, null line_num, clm_from_dt, clm_thru_dt, dgns_cd, dgns_vrsn, 'DiagObs:Carrier' modifier_cd, dgns_ix i
from
  bcarrier_claims unpivot( (dgns_cd, dgns_vrsn) for dgns_ix in(
    (icd_dgns_cd1, icd_dgns_vrsn_cd1) as 1
  , (icd_dgns_cd2, icd_dgns_vrsn_cd2) as 2
  , (icd_dgns_cd3, icd_dgns_vrsn_cd3) as 3
  , (icd_dgns_cd4, icd_dgns_vrsn_cd4) as 4
  , (icd_dgns_cd5, icd_dgns_vrsn_cd5) as 5
  , (icd_dgns_cd6, icd_dgns_vrsn_cd6) as 6
  , (icd_dgns_cd7, icd_dgns_vrsn_cd7) as 7
  , (icd_dgns_cd8, icd_dgns_vrsn_cd8) as 8
  , (icd_dgns_cd9, icd_dgns_vrsn_cd9) as 9
  , (icd_dgns_cd10, icd_dgns_vrsn_cd10) as 10
  , (icd_dgns_cd11, icd_dgns_vrsn_cd11) as 11
  , (icd_dgns_cd12, icd_dgns_vrsn_cd12) as 1
  ))
      -- TODO: 'DiagObs:Inpatient'
      -- TODO: 'DiagObs:Inpatient'
      -- TODO:
      -- Need to add the DX icd9 codes from carrier_claims_1b table as well since
      --    they both are needed to have all data... reference:
      -- http://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DESample01.html

    ) detail
    -- TODO: FROM_DT is NULL sometimes with segment = 2 - confirm that this is expected.
  where
    detail.dgns_cd is not null
    -- ISSUE: and segment = 1
    /*
    ISSUE: In SamTheEagle, there were numerous codes that do not have an actual number
    TODO: Handle the codes (outp_claims' ICD9_DGNS_CD_1) that say 'OTHER'
    Reference: select ICD9_DGNS_CD_1 from cms.outp_claims
    where ICD9_DGNS_CD_1 like 'OTHER';
    and detail.dgns_cd not like '%OTHER%'
    */
    ;

-- eyeball it: select * from observation_fact_cms_dx;

-- TODO:
create or replace view observation_fact_cms_drg as
select CLM_ID || '.' || segment encounter_ide
     , DESYNPUF_ID as patient_ide
     , 'CMS|MSDRG:' || clm_drg_cd concept_cd
     , '@' provider_id
     , to_date(CLM_FROM_DT, 'YYYYMMDD') start_date
     , 'DiagObs:Inpatient' modifier_cd
     , 1 instance_num
     , 'T' valtype_cd
     , '@' tval_char
     , to_number(null) nval_num
     , null valueflag_cd
     , null units_cd
     , to_date(CLM_THRU_DT, 'YYYYMMDD') end_date
     , null location_cd 
     , to_number(null) confidence_num
     , to_date(null) update_date
     , 1 as part
from cms.inpatient
where clm_drg_cd is not null
and clm_drg_cd not like '%OTH%'
and segment = 1
;

create or replace view cms_dx_txform as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_dx_txform where design_digest = &&design_digest;
