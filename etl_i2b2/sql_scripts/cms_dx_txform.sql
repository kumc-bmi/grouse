/** cms_dx_txform - Make i2b2 facts from CMS diagnoses

for DRGs from MAXDATA_IP, ref:
  - Medicaid Analytic Extract Inpatient (IP) Record Layout and Description 2013
    https://www.cms.gov/Research-Statistics-Data-and-Systems/Computer-Data-and-Systems/MedicaidDataSourcesGenInfo/Downloads/geninfomax2013.zip
    https://www.cms.gov/Research-Statistics-Data-and-Systems/Computer-Data-and-Systems/MedicaidDataSourcesGenInfo/MAXGeneralInformation.html
*/

select clm_line_cd from cms_key_sources where 'dep' = 'cms_keys.pls';

  
create or replace view cms_dx_design
as
with cms_schema as
  (select *
  from all_tab_columns
  where owner = '&&CMS_RIF'
    and table_name not like 'SYS_%'
  )
select dgns.table_name
, dgns.column_id
, dgns.column_name
, dgns.data_type
, ', '
  || '('
  || dgns.column_name
  || ', '
  || dgns_vrsn.column_name
  || ') as '''
  || dgns.column_name
  || '''' sql_snippet
from cms_schema dgns_vrsn
join cms_schema dgns
on dgns.table_name                         = dgns_vrsn.table_name
  and replace(dgns.column_name, '_CD', '') = replace(replace(dgns_vrsn.column_name, '_CD', ''), '_VRSN', '')

where dgns_vrsn.column_name like '%DGNS%'
  and dgns_vrsn.column_name like '%VRSN%'
  and dgns.column_name like '%DGNS%'
  and dgns.column_name not like '%VRSN%'
order by table_name
, column_id ;


create or replace view cms_carrier_outpatient_dx
as
with bcarrier_dx as
  (select 'BCARRIER_CLAIMS' table_name
  , bene_id
  , clm_id
  , clm_from_dt start_date
  , clm_thru_dt
  , null provider_id
  , nch_wkly_proc_dt update_date
  , dgns_cd
  , dgns_vrsn
  , dgns_label
  from "&&CMS_RIF".bcarrier_claims unpivot((dgns_cd, dgns_vrsn) for dgns_label in(

  (PRNCPAL_DGNS_CD, PRNCPAL_DGNS_VRSN_CD) as 'PRNCPAL_DGNS_CD'
, (ICD_DGNS_CD1, ICD_DGNS_VRSN_CD1) as 'ICD_DGNS_CD1'
, (ICD_DGNS_CD2, ICD_DGNS_VRSN_CD2) as 'ICD_DGNS_CD2'
, (ICD_DGNS_CD3, ICD_DGNS_VRSN_CD3) as 'ICD_DGNS_CD3'
, (ICD_DGNS_CD4, ICD_DGNS_VRSN_CD4) as 'ICD_DGNS_CD4'
, (ICD_DGNS_CD5, ICD_DGNS_VRSN_CD5) as 'ICD_DGNS_CD5'
, (ICD_DGNS_CD6, ICD_DGNS_VRSN_CD6) as 'ICD_DGNS_CD6'
, (ICD_DGNS_CD7, ICD_DGNS_VRSN_CD7) as 'ICD_DGNS_CD7'
, (ICD_DGNS_CD8, ICD_DGNS_VRSN_CD8) as 'ICD_DGNS_CD8'
, (ICD_DGNS_CD9, ICD_DGNS_VRSN_CD9) as 'ICD_DGNS_CD9'
, (ICD_DGNS_CD10, ICD_DGNS_VRSN_CD10) as 'ICD_DGNS_CD10'
, (ICD_DGNS_CD11, ICD_DGNS_VRSN_CD11) as 'ICD_DGNS_CD11'
, (ICD_DGNS_CD12, ICD_DGNS_VRSN_CD12) as 'ICD_DGNS_CD12'
    
    )
  ))
, bcarrier_line_dx as
  (select 'BCARRIER_LINE' table_name
  , bene_id
  , clm_id
  , clm_thru_dt start_date
  , clm_thru_dt
  , PRF_PHYSN_NPI provider_id
  , line_last_expns_dt update_date
  , line_icd_dgns_cd dgns_cd
  , line_icd_dgns_vrsn_cd dgns_vrsn
  , to_char(line_num) ix
  from "&&CMS_RIF".bcarrier_line
  )
, detail as (
select * from bcarrier_dx
union all
select * from bcarrier_line_dx
)
, outpatient_claims_dx as
  (select 'OUTPATIENT_BASE_CLAIMS' table_name
  , bene_id
  , clm_id
  , clm_from_dt start_date
  , clm_thru_dt
  , PRVDR_NUM provider_id  -- ISSUE: ORG_NPI_NUM? PRVDR_STATE_CD?
  , nch_wkly_proc_dt update_date
  , dgns_cd
  , dgns_vrsn
  , dgns_label
  from "&&CMS_RIF".outpatient_base_claims unpivot((dgns_cd, dgns_vrsn) for dgns_label in(
  (PRNCPAL_DGNS_CD, PRNCPAL_DGNS_VRSN_CD) as 'PRNCPAL_DGNS_CD'
, (ICD_DGNS_CD1, ICD_DGNS_VRSN_CD1) as 'ICD_DGNS_CD1'
, (ICD_DGNS_CD2, ICD_DGNS_VRSN_CD2) as 'ICD_DGNS_CD2'
, (ICD_DGNS_CD3, ICD_DGNS_VRSN_CD3) as 'ICD_DGNS_CD3'
, (ICD_DGNS_CD4, ICD_DGNS_VRSN_CD4) as 'ICD_DGNS_CD4'
, (ICD_DGNS_CD5, ICD_DGNS_VRSN_CD5) as 'ICD_DGNS_CD5'
, (ICD_DGNS_CD6, ICD_DGNS_VRSN_CD6) as 'ICD_DGNS_CD6'
, (ICD_DGNS_CD7, ICD_DGNS_VRSN_CD7) as 'ICD_DGNS_CD7'
, (ICD_DGNS_CD8, ICD_DGNS_VRSN_CD8) as 'ICD_DGNS_CD8'
, (ICD_DGNS_CD9, ICD_DGNS_VRSN_CD9) as 'ICD_DGNS_CD9'
, (ICD_DGNS_CD10, ICD_DGNS_VRSN_CD10) as 'ICD_DGNS_CD10'
, (ICD_DGNS_CD11, ICD_DGNS_VRSN_CD11) as 'ICD_DGNS_CD11'
, (ICD_DGNS_CD12, ICD_DGNS_VRSN_CD12) as 'ICD_DGNS_CD12'
, (ICD_DGNS_CD13, ICD_DGNS_VRSN_CD13) as 'ICD_DGNS_CD13'
, (ICD_DGNS_CD14, ICD_DGNS_VRSN_CD14) as 'ICD_DGNS_CD14'
, (ICD_DGNS_CD15, ICD_DGNS_VRSN_CD15) as 'ICD_DGNS_CD15'
, (ICD_DGNS_CD16, ICD_DGNS_VRSN_CD16) as 'ICD_DGNS_CD16'
, (ICD_DGNS_CD17, ICD_DGNS_VRSN_CD17) as 'ICD_DGNS_CD17'
, (ICD_DGNS_CD18, ICD_DGNS_VRSN_CD18) as 'ICD_DGNS_CD18'
, (ICD_DGNS_CD19, ICD_DGNS_VRSN_CD19) as 'ICD_DGNS_CD19'
, (ICD_DGNS_CD20, ICD_DGNS_VRSN_CD20) as 'ICD_DGNS_CD20'
, (ICD_DGNS_CD21, ICD_DGNS_VRSN_CD21) as 'ICD_DGNS_CD21'
, (ICD_DGNS_CD22, ICD_DGNS_VRSN_CD22) as 'ICD_DGNS_CD22'
, (ICD_DGNS_CD23, ICD_DGNS_VRSN_CD23) as 'ICD_DGNS_CD23'
, (ICD_DGNS_CD24, ICD_DGNS_VRSN_CD24) as 'ICD_DGNS_CD24'
, (ICD_DGNS_CD25, ICD_DGNS_VRSN_CD25) as 'ICD_DGNS_CD25'
, (FST_DGNS_E_CD, FST_DGNS_E_VRSN_CD) as 'FST_DGNS_E_CD'
, (ICD_DGNS_E_CD1, ICD_DGNS_E_VRSN_CD1) as 'ICD_DGNS_E_CD1'
, (ICD_DGNS_E_CD2, ICD_DGNS_E_VRSN_CD2) as 'ICD_DGNS_E_CD2'
, (ICD_DGNS_E_CD3, ICD_DGNS_E_VRSN_CD3) as 'ICD_DGNS_E_CD3'
, (ICD_DGNS_E_CD4, ICD_DGNS_E_VRSN_CD4) as 'ICD_DGNS_E_CD4'
, (ICD_DGNS_E_CD5, ICD_DGNS_E_VRSN_CD5) as 'ICD_DGNS_E_CD5'
, (ICD_DGNS_E_CD6, ICD_DGNS_E_VRSN_CD6) as 'ICD_DGNS_E_CD6'
, (ICD_DGNS_E_CD7, ICD_DGNS_E_VRSN_CD7) as 'ICD_DGNS_E_CD7'
, (ICD_DGNS_E_CD8, ICD_DGNS_E_VRSN_CD8) as 'ICD_DGNS_E_CD8'
, (ICD_DGNS_E_CD9, ICD_DGNS_E_VRSN_CD9) as 'ICD_DGNS_E_CD9'
, (ICD_DGNS_E_CD10, ICD_DGNS_E_VRSN_CD10) as 'ICD_DGNS_E_CD10'
, (ICD_DGNS_E_CD11, ICD_DGNS_E_VRSN_CD11) as 'ICD_DGNS_E_CD11'
, (ICD_DGNS_E_CD12, ICD_DGNS_E_VRSN_CD12) as 'ICD_DGNS_E_CD12'
)))

, no_info as (
select
 null tval_char
, to_number(null) nval_num
, null valueflag_cd
, null quantity_num
, null units_cd
, null location_cd
, to_number(null) confidence_num
from dual)

select
  -- TODO: join with bcarrier_line to get provider?
  fmt_patient_day(bene_id, start_date) encounter_ide
, key_sources.patient_day_cd encounter_ide_source
, bene_id
, dx_code(dgns_cd, dgns_vrsn) concept_cd
, provider_id -- TODO: providerID
, start_date
, 'CMS_RIF:' || table_name modifier_cd -- ISSUE: PRNCPAL_DGNS_CD?
, ora_hash(clm_id || detail.dgns_label) instance_num -- ISSUE: collision could violate primary key
, '@' valtype_cd
, clm_thru_dt end_date
, update_date -- TODO
,
  &&cms_source_cd sourcesystem_cd
, no_info.*
from detail
cross join no_info
cross join cms_key_sources key_sources
  where detail.dgns_cd is not null ;

-- eyeball it: select * from cms_carrier_outpatient_dx;


create or replace view cms_medpar_dx
as
with detail as (
select 'MEDPAR_ALL' table_name
  , bene_id, medpar_id, ADMSN_DT start_date, DSCHRG_DT end_date
  , PRVDR_NUM provider_num
  , LTST_CLM_ACRTN_DT update_date
  , dgns_cd, dgns_vrsn, dgns_label
  from
  "&&CMS_RIF".medpar_all unpivot( (dgns_cd, dgns_vrsn) for dgns_label in(
  (ADMTG_DGNS_CD, ADMTG_DGNS_VRSN_CD) as 'ADMTG_DGNS_CD'
, (DGNS_1_CD, DGNS_VRSN_CD_1) as 'DGNS_1_CD'
, (DGNS_2_CD, DGNS_VRSN_CD_2) as 'DGNS_2_CD'
, (DGNS_3_CD, DGNS_VRSN_CD_3) as 'DGNS_3_CD'
, (DGNS_4_CD, DGNS_VRSN_CD_4) as 'DGNS_4_CD'
, (DGNS_5_CD, DGNS_VRSN_CD_5) as 'DGNS_5_CD'
, (DGNS_6_CD, DGNS_VRSN_CD_6) as 'DGNS_6_CD'
, (DGNS_7_CD, DGNS_VRSN_CD_7) as 'DGNS_7_CD'
, (DGNS_8_CD, DGNS_VRSN_CD_8) as 'DGNS_8_CD'
, (DGNS_9_CD, DGNS_VRSN_CD_9) as 'DGNS_9_CD'
, (DGNS_10_CD, DGNS_VRSN_CD_10) as 'DGNS_10_CD'
, (DGNS_11_CD, DGNS_VRSN_CD_11) as 'DGNS_11_CD'
, (DGNS_12_CD, DGNS_VRSN_CD_12) as 'DGNS_12_CD'
, (DGNS_13_CD, DGNS_VRSN_CD_13) as 'DGNS_13_CD'
, (DGNS_14_CD, DGNS_VRSN_CD_14) as 'DGNS_14_CD'
, (DGNS_15_CD, DGNS_VRSN_CD_15) as 'DGNS_15_CD'
, (DGNS_16_CD, DGNS_VRSN_CD_16) as 'DGNS_16_CD'
, (DGNS_17_CD, DGNS_VRSN_CD_17) as 'DGNS_17_CD'
, (DGNS_18_CD, DGNS_VRSN_CD_18) as 'DGNS_18_CD'
, (DGNS_19_CD, DGNS_VRSN_CD_19) as 'DGNS_19_CD'
, (DGNS_20_CD, DGNS_VRSN_CD_20) as 'DGNS_20_CD'
, (DGNS_21_CD, DGNS_VRSN_CD_21) as 'DGNS_21_CD'
, (DGNS_22_CD, DGNS_VRSN_CD_22) as 'DGNS_22_CD'
, (DGNS_23_CD, DGNS_VRSN_CD_23) as 'DGNS_23_CD'
, (DGNS_24_CD, DGNS_VRSN_CD_24) as 'DGNS_24_CD'
, (DGNS_25_CD, DGNS_VRSN_CD_25) as 'DGNS_25_CD'
, (DGNS_E_1_CD, DGNS_E_VRSN_CD_1) as 'DGNS_E_1_CD'
, (DGNS_E_2_CD, DGNS_E_VRSN_CD_2) as 'DGNS_E_2_CD'
, (DGNS_E_3_CD, DGNS_E_VRSN_CD_3) as 'DGNS_E_3_CD'
, (DGNS_E_4_CD, DGNS_E_VRSN_CD_4) as 'DGNS_E_4_CD'
, (DGNS_E_5_CD, DGNS_E_VRSN_CD_5) as 'DGNS_E_5_CD'
, (DGNS_E_6_CD, DGNS_E_VRSN_CD_6) as 'DGNS_E_6_CD'
, (DGNS_E_7_CD, DGNS_E_VRSN_CD_7) as 'DGNS_E_7_CD'
, (DGNS_E_8_CD, DGNS_E_VRSN_CD_8) as 'DGNS_E_8_CD'
, (DGNS_E_9_CD, DGNS_E_VRSN_CD_9) as 'DGNS_E_9_CD'
, (DGNS_E_10_CD, DGNS_E_VRSN_CD_10) as 'DGNS_E_10_CD'
, (DGNS_E_11_CD, DGNS_E_VRSN_CD_11) as 'DGNS_E_11_CD'
, (DGNS_E_12_CD, DGNS_E_VRSN_CD_12) as 'DGNS_E_12_CD'
))
)
, no_info as (
select
 null tval_char
, to_number(null) nval_num
, null valueflag_cd
, null quantity_num
, null units_cd
, null location_cd  -- ISSUE: provider state code?
, to_number(null) confidence_num
from dual)

select
  medpar_id encounter_ide
, key_sources.medpar_cd encounter_ide_source
, bene_id
, dx_code(dgns_cd, dgns_vrsn) concept_cd
, provider_num
, start_date
, 'CMS_RIF:' || table_name modifier_cd -- ISSUE: ADMIT_DIAG???
, ora_hash(medpar_id || detail.dgns_label) instance_num -- ISSUE: collision could violate primary key
, '@' valtype_cd
, end_date
, update_date
,
  &&cms_source_cd sourcesystem_cd
, no_info.*
from detail
cross join no_info
cross join cms_key_sources key_sources
  where detail.dgns_cd is not null ;


create or replace view cms_medpar_drg
as
with detail as (
select
'MEDPAR_ALL' table_name
  , bene_id, medpar_id, ADMSN_DT start_date, DSCHRG_DT end_date
  , PRVDR_NUM provider_num
  , LTST_CLM_ACRTN_DT update_date
  , DRG_CD from "&&CMS_RIF".medpar_all
)
, no_info as (
select
 null tval_char
, to_number(null) nval_num
, null valueflag_cd
, null quantity_num
, null units_cd
, null location_cd  -- ISSUE: provider state code?
, to_number(null) confidence_num
from dual)
select
  detail.medpar_id encounter_ide
, key_sources.medpar_cd encounter_ide_source
, bene_id
, 'DRG:' || detail.DRG_CD concept_cd  -- @@magic string
, detail.provider_num
, detail.start_date
, 'CMS_RIF:' || detail.table_name modifier_cd
, 1 instance_num -- @@ magic number?
, '@' valtype_cd
, end_date
, update_date
,
  &&cms_source_cd sourcesystem_cd
, no_info.*
from detail
cross join no_info
cross join cms_key_sources key_sources
  where detail.DRG_CD is not null ;


select *
from "&&CMS_RIF".MAXDATA_IP
;


create or replace view cms_max_ip_drg
as
with detail as (
select
'MAXDATA_IP' table_name
  , bene_id, '@@TODO' encounter_ide, srvc_bgn_dt start_date, srvc_end_dt end_date
  , PRVDR_ID_NMBR -- ISSUE: NPI?
  , yr_num
  , DRG_REL_GROUP from "&&CMS_RIF".MAXDATA_IP
)
, no_info as (
select
 null tval_char
, to_number(null) nval_num
, null valueflag_cd
, null quantity_num
, null units_cd
, null location_cd  -- ISSUE: provider state code?
, to_number(null) confidence_num
from dual)
select
  detail.encounter_ide
, '@@TODO key_sources.maxdata_ip' encounter_ide_source
, bene_id
, 'DRG:' || lpad(detail.DRG_REL_GROUP, 3, '0')  concept_cd  -- @@magic string. function?
, detail.PRVDR_ID_NMBR provider_num
, detail.start_date
, 'CMS_RIF:' || detail.table_name modifier_cd
, 1 instance_num -- @@ magic number?
, '@' valtype_cd
, end_date
, to_date(yr_num || '1231', 'YYYYMMDD') update_date
,
  &&cms_source_cd sourcesystem_cd
, no_info.*
from detail
cross join no_info
cross join cms_key_sources key_sources
  where detail.DRG_REL_GROUP is not null
-- IF DRGs ARE NOT USED, THIS DATA ELEMENT IS 8 -FILLED. IF DRGs ARE USED BUT THE DRG VALUE IS UNKNOWN, THIS DATA ELEMENT IS 9 -FILLED
and DRG_REL_GROUP not in (8888, 9999)
;


create or replace view cms_dx_txform as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_dx_txform where design_digest = &&design_digest;
