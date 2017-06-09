/** medpar_pivot - (un)pivot CMS MEDPAR file into i2b2 fact table shape

Refs:

Chronic Conditions Data Warehouse Data Dictionaries 
https://www.ccwdata.org/web/guest/data-dictionaries

MedPAR RIF
https://www.resdac.org/cms-data/files/medpar-rif/data-documentation

@@i2b2 CRC design

*/

select bene_cd from cms_key_sources where 'dep' = 'cms_keys.pls';
select no_value from valtype_cd where 'dep' = 'i2b2_crc_design.sql';


/* cms_medpar_dx - pivot diagnosis columns
  - DGNSCD{x}	MEDPAR Diagnosis Code
    - DGNS_VRSN_CD_{x} MEDPAR Diagnosis Version Code
    - POA_DGNS_{x}_IND_CD MEDPAR Diagnosis Present on Admission Indicator Code
  - DGNS_E_VRSN_CD_{x} MEDPAR Diagnosis E Version Code
  - POA_DGNS_E_{x}_IND_CD MEDPAR Diagnosis E Code Present on Admission Indicator

ISSUE: we're throwing away {x}; do we need it? for primary diagnosis, for example?
*/
create or replace view cms_medpar_dx as
with detail as (
select 'MEDPAR_ALL' table_name
  , bene_id, medpar_id, ADMSN_DT start_date, DSCHRG_DT end_date
  , ORG_NPI_NUM
  , LTST_CLM_ACRTN_DT update_date
  , dgns_cd, dgns_vrsn, poa_ind, dgns_label
  from
  "&&CMS_RIF".medpar_all unpivot( (dgns_cd, dgns_vrsn, poa_ind) for dgns_label in (
  (ADMTG_DGNS_CD, ADMTG_DGNS_VRSN_CD, BENE_SEX_CD /*filler*/) as 'ADMTG_DGNS_CD'
, (DGNS_1_CD, DGNS_VRSN_CD_1, POA_DGNS_1_IND_CD) as 'DGNS_1_CD'
, (DGNS_2_CD, DGNS_VRSN_CD_2, POA_DGNS_2_IND_CD) as 'DGNS_2_CD'
, (DGNS_3_CD, DGNS_VRSN_CD_3, POA_DGNS_3_IND_CD) as 'DGNS_3_CD'
, (DGNS_4_CD, DGNS_VRSN_CD_4, POA_DGNS_4_IND_CD) as 'DGNS_4_CD'
, (DGNS_5_CD, DGNS_VRSN_CD_5, POA_DGNS_5_IND_CD) as 'DGNS_5_CD'
, (DGNS_6_CD, DGNS_VRSN_CD_6, POA_DGNS_6_IND_CD) as 'DGNS_6_CD'
, (DGNS_7_CD, DGNS_VRSN_CD_7, POA_DGNS_7_IND_CD) as 'DGNS_7_CD'
, (DGNS_8_CD, DGNS_VRSN_CD_8, POA_DGNS_8_IND_CD) as 'DGNS_8_CD'
, (DGNS_9_CD, DGNS_VRSN_CD_9, POA_DGNS_9_IND_CD) as 'DGNS_9_CD'
, (DGNS_10_CD, DGNS_VRSN_CD_10, POA_DGNS_10_IND_CD) as 'DGNS_10_CD'
, (DGNS_11_CD, DGNS_VRSN_CD_11, POA_DGNS_11_IND_CD) as 'DGNS_11_CD'
, (DGNS_12_CD, DGNS_VRSN_CD_12, POA_DGNS_12_IND_CD) as 'DGNS_12_CD'
, (DGNS_13_CD, DGNS_VRSN_CD_13, POA_DGNS_13_IND_CD) as 'DGNS_13_CD'
, (DGNS_14_CD, DGNS_VRSN_CD_14, POA_DGNS_14_IND_CD) as 'DGNS_14_CD'
, (DGNS_15_CD, DGNS_VRSN_CD_15, POA_DGNS_15_IND_CD) as 'DGNS_15_CD'
, (DGNS_16_CD, DGNS_VRSN_CD_16, POA_DGNS_16_IND_CD) as 'DGNS_16_CD'
, (DGNS_17_CD, DGNS_VRSN_CD_17, POA_DGNS_17_IND_CD) as 'DGNS_17_CD'
, (DGNS_18_CD, DGNS_VRSN_CD_18, POA_DGNS_18_IND_CD) as 'DGNS_18_CD'
, (DGNS_19_CD, DGNS_VRSN_CD_19, POA_DGNS_19_IND_CD) as 'DGNS_19_CD'
, (DGNS_20_CD, DGNS_VRSN_CD_20, POA_DGNS_20_IND_CD) as 'DGNS_20_CD'
, (DGNS_21_CD, DGNS_VRSN_CD_21, POA_DGNS_21_IND_CD) as 'DGNS_21_CD'
, (DGNS_22_CD, DGNS_VRSN_CD_22, POA_DGNS_22_IND_CD) as 'DGNS_22_CD'
, (DGNS_23_CD, DGNS_VRSN_CD_23, POA_DGNS_23_IND_CD) as 'DGNS_23_CD'
, (DGNS_24_CD, DGNS_VRSN_CD_24, POA_DGNS_24_IND_CD) as 'DGNS_24_CD'
, (DGNS_25_CD, DGNS_VRSN_CD_25, POA_DGNS_25_IND_CD) as 'DGNS_25_CD'
, (DGNS_E_1_CD, DGNS_E_VRSN_CD_1, POA_DGNS_E_1_IND_CD) as 'DGNS_E_1_CD'
, (DGNS_E_2_CD, DGNS_E_VRSN_CD_2, POA_DGNS_E_2_IND_CD) as 'DGNS_E_2_CD'
, (DGNS_E_3_CD, DGNS_E_VRSN_CD_3, POA_DGNS_E_3_IND_CD) as 'DGNS_E_3_CD'
, (DGNS_E_4_CD, DGNS_E_VRSN_CD_4, POA_DGNS_E_4_IND_CD) as 'DGNS_E_4_CD'
, (DGNS_E_5_CD, DGNS_E_VRSN_CD_5, POA_DGNS_E_5_IND_CD) as 'DGNS_E_5_CD'
, (DGNS_E_6_CD, DGNS_E_VRSN_CD_6, POA_DGNS_E_6_IND_CD) as 'DGNS_E_6_CD'
, (DGNS_E_7_CD, DGNS_E_VRSN_CD_7, POA_DGNS_E_7_IND_CD) as 'DGNS_E_7_CD'
, (DGNS_E_8_CD, DGNS_E_VRSN_CD_8, POA_DGNS_E_8_IND_CD) as 'DGNS_E_8_CD'
, (DGNS_E_9_CD, DGNS_E_VRSN_CD_9, POA_DGNS_E_9_IND_CD) as 'DGNS_E_9_CD'
, (DGNS_E_10_CD, DGNS_E_VRSN_CD_10, POA_DGNS_E_10_IND_CD) as 'DGNS_E_10_CD'
, (DGNS_E_11_CD, DGNS_E_VRSN_CD_11, POA_DGNS_E_11_IND_CD) as 'DGNS_E_11_CD'
, (DGNS_E_12_CD, DGNS_E_VRSN_CD_12, POA_DGNS_E_12_IND_CD) as 'DGNS_E_12_CD'
))
)
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
  medpar_id
, bene_id
, dx_code(dgns_cd, dgns_vrsn) concept_cd
, coalesce(ORG_NPI_NUM, no_value.not_recorded) provider_id-- ISSUE: ORG_NPI_NUM?
, start_date
, case
  when dgns_label = 'ADMTG_DGNS_CD' then dgns_label || ':'
  when poa_ind is not null
  then poa_cd(poa_ind)
  else rif_modifier(table_name)
  end modifier_cd -- ISSUE: ADMIT_DIAG???
, ora_hash(medpar_id || detail.dgns_label) instance_num
, valtype_cd.no_value valtype_cd
, end_date
, update_date
,
  &&cms_source_cd sourcesystem_cd
, no_info.*
from detail
cross join no_info
cross join valtype_cd
cross join no_value
cross join cms_key_sources key_sources
  where detail.dgns_cd is not null ;


create or replace view cms_srgcl_prcdr_design as
with information_schema as
  (select *
  from all_tab_columns
  where owner = '&&CMS_RIF'
    and table_name not like 'SYS_%'
  )
select prcdr.table_name
, prcdr.column_id
, prcdr.column_name
, prcdr.data_type
, ', '
  || '('
  || prcdr.column_name
  || ', '
  || prcdr_vrsn.column_name
  || ', '
  || prcdr_dt.column_name
  || ') as '''
  || prcdr.column_name
  || '''' sql_snippet
from information_schema prcdr
join information_schema prcdr_vrsn
on prcdr.table_name                         = prcdr_vrsn.table_name
  -- SRGCL_PRCDR_1_CD -> SRGCL_PRCDR_1        SRGCL_PRCDR_VRSN_CD_1 -> SRGCL_PRCDR_1
  and replace(prcdr.column_name, '_CD', '') = replace(prcdr_vrsn.column_name, '_VRSN_CD', '')
join information_schema prcdr_dt
on prcdr.table_name                         = prcdr_dt.table_name
  -- SRGCL_PRCDR_1_CD -> SRGCL_PRCDR_1        SRGCL_PRCDR_PRFRM_1_DT -> SRGCL_PRCDR_1
  and replace(prcdr.column_name, '_CD', '') = replace(replace(prcdr_dt.column_name, '_PRFRM', ''), '_DT', '')
where prcdr_vrsn.column_name like '%PRCDR%'
  and prcdr_vrsn.column_name like '%VRSN%'
  and prcdr_dt.column_name like '%PRCDR%'
  and prcdr_dt.column_name like '%PRFRM%'
  and prcdr.column_name like '%PRCDR%'
  and prcdr.column_name not like '%VRSN%'
  and prcdr.column_name not like '%PRFRM%'
order by table_name
, column_id ;


create or replace view cms_medpar_px as
with
detail as (
select 'MEDPAR_ALL' table_name
  , bene_id, medpar_id, admsn_dt, dschrg_dt
  , ORG_NPI_NUM
  , LTST_CLM_ACRTN_DT
  , prcdr_cd, vrsn, prfrm_dt, prcdr_label
from "&&CMS_RIF".medpar_all unpivot( (prcdr_cd, vrsn, prfrm_dt) for prcdr_label in (
  (SRGCL_PRCDR_1_CD, SRGCL_PRCDR_VRSN_CD_1, SRGCL_PRCDR_PRFRM_1_DT) as 'SRGCL_PRCDR_1_CD'
, (SRGCL_PRCDR_2_CD, SRGCL_PRCDR_VRSN_CD_2, SRGCL_PRCDR_PRFRM_2_DT) as 'SRGCL_PRCDR_2_CD'
, (SRGCL_PRCDR_3_CD, SRGCL_PRCDR_VRSN_CD_3, SRGCL_PRCDR_PRFRM_3_DT) as 'SRGCL_PRCDR_3_CD'
, (SRGCL_PRCDR_4_CD, SRGCL_PRCDR_VRSN_CD_4, SRGCL_PRCDR_PRFRM_4_DT) as 'SRGCL_PRCDR_4_CD'
, (SRGCL_PRCDR_5_CD, SRGCL_PRCDR_VRSN_CD_5, SRGCL_PRCDR_PRFRM_5_DT) as 'SRGCL_PRCDR_5_CD'
, (SRGCL_PRCDR_6_CD, SRGCL_PRCDR_VRSN_CD_6, SRGCL_PRCDR_PRFRM_6_DT) as 'SRGCL_PRCDR_6_CD'
, (SRGCL_PRCDR_7_CD, SRGCL_PRCDR_VRSN_CD_7, SRGCL_PRCDR_PRFRM_7_DT) as 'SRGCL_PRCDR_7_CD'
, (SRGCL_PRCDR_8_CD, SRGCL_PRCDR_VRSN_CD_8, SRGCL_PRCDR_PRFRM_8_DT) as 'SRGCL_PRCDR_8_CD'
, (SRGCL_PRCDR_9_CD, SRGCL_PRCDR_VRSN_CD_9, SRGCL_PRCDR_PRFRM_9_DT) as 'SRGCL_PRCDR_9_CD'
, (SRGCL_PRCDR_10_CD, SRGCL_PRCDR_VRSN_CD_10, SRGCL_PRCDR_PRFRM_10_DT) as 'SRGCL_PRCDR_10_CD'
, (SRGCL_PRCDR_11_CD, SRGCL_PRCDR_VRSN_CD_11, SRGCL_PRCDR_PRFRM_11_DT) as 'SRGCL_PRCDR_11_CD'
, (SRGCL_PRCDR_12_CD, SRGCL_PRCDR_VRSN_CD_12, SRGCL_PRCDR_PRFRM_12_DT) as 'SRGCL_PRCDR_12_CD'
, (SRGCL_PRCDR_13_CD, SRGCL_PRCDR_VRSN_CD_13, SRGCL_PRCDR_PRFRM_13_DT) as 'SRGCL_PRCDR_13_CD'
, (SRGCL_PRCDR_14_CD, SRGCL_PRCDR_VRSN_CD_14, SRGCL_PRCDR_PRFRM_14_DT) as 'SRGCL_PRCDR_14_CD'
, (SRGCL_PRCDR_15_CD, SRGCL_PRCDR_VRSN_CD_15, SRGCL_PRCDR_PRFRM_15_DT) as 'SRGCL_PRCDR_15_CD'
, (SRGCL_PRCDR_16_CD, SRGCL_PRCDR_VRSN_CD_16, SRGCL_PRCDR_PRFRM_16_DT) as 'SRGCL_PRCDR_16_CD'
, (SRGCL_PRCDR_17_CD, SRGCL_PRCDR_VRSN_CD_17, SRGCL_PRCDR_PRFRM_17_DT) as 'SRGCL_PRCDR_17_CD'
, (SRGCL_PRCDR_18_CD, SRGCL_PRCDR_VRSN_CD_18, SRGCL_PRCDR_PRFRM_18_DT) as 'SRGCL_PRCDR_18_CD'
, (SRGCL_PRCDR_19_CD, SRGCL_PRCDR_VRSN_CD_19, SRGCL_PRCDR_PRFRM_19_DT) as 'SRGCL_PRCDR_19_CD'
, (SRGCL_PRCDR_20_CD, SRGCL_PRCDR_VRSN_CD_20, SRGCL_PRCDR_PRFRM_20_DT) as 'SRGCL_PRCDR_20_CD'
, (SRGCL_PRCDR_21_CD, SRGCL_PRCDR_VRSN_CD_21, SRGCL_PRCDR_PRFRM_21_DT) as 'SRGCL_PRCDR_21_CD'
, (SRGCL_PRCDR_22_CD, SRGCL_PRCDR_VRSN_CD_22, SRGCL_PRCDR_PRFRM_22_DT) as 'SRGCL_PRCDR_22_CD'
, (SRGCL_PRCDR_23_CD, SRGCL_PRCDR_VRSN_CD_23, SRGCL_PRCDR_PRFRM_23_DT) as 'SRGCL_PRCDR_23_CD'
, (SRGCL_PRCDR_24_CD, SRGCL_PRCDR_VRSN_CD_24, SRGCL_PRCDR_PRFRM_24_DT) as 'SRGCL_PRCDR_24_CD'
, (SRGCL_PRCDR_25_CD, SRGCL_PRCDR_VRSN_CD_25, SRGCL_PRCDR_PRFRM_25_DT) as 'SRGCL_PRCDR_25_CD'
))
)

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
  medpar_id
, bene_id
, prcdr_label
, px_code(prcdr_cd, vrsn) concept_cd
, coalesce(ORG_NPI_NUM, no_value.not_recorded) provider_id-- ISSUE: ORG_NPI_NUM?
, coalesce(prfrm_dt, admsn_dt) start_date -- prfrm_dt is sometimes null. odd.
, rif_modifier(table_name) modifier_cd
, ora_hash(medpar_id || detail.prcdr_label) instance_num
, valtype_cd.no_value valtype_cd
, coalesce(prfrm_dt, dschrg_dt) end_date
, LTST_CLM_ACRTN_DT update_date
,
  &&cms_source_cd sourcesystem_cd
, no_info.*
from detail
cross join no_info
cross join valtype_cd
cross join no_value
  where detail.prcdr_cd is not null
;



/* cms_medpar_design generates code to un-pivot each column from the wide table into a long-skinny EAV table.

See mbsf_pivot.sql for basic design.
*/
create or replace view cms_medpar_design
as
with information_schema as
  (select owner
  , table_name
  , column_id
  , column_name
  , data_type
  from all_tab_columns
  where owner = 'CMS_DEID'
    and table_name not like 'SYS_%'
  order by owner
  , table_name
  , column_id
  )

select column_name, ', '
  ||
  case
    when data_type = 'VARCHAR2' and column_name in ('ORG_NPI_NUM', 'PRVDR_NUM') then '('
      || column_name
      || ', INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as ''' || valtype_cd.text || ' '
      || column_name
      || ''''
    -- ISSUE: MEDPAR_YR_NUM should be numeric? or date?
    when data_type = 'VARCHAR2' then '('
      || column_name
      || ', INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as ''' || valtype_cd.no_value || ' '
      || column_name
      || ''''
    when data_type = 'NUMBER' then '(BENE_SEX_CD, '
      || column_name
      || ', EXTRACT_DT) as ''' || valtype_cd.numeric || ' '
      || column_name
      || ''''
    when data_type = 'DATE' then '(BENE_SEX_CD, INTRNL_USE_SSI_DAY_CNT, '
      || column_name
      || ') as ''' || valtype_cd.date_val || ' '
      || column_name
      || ''''
  end sql_snippet
from information_schema
cross join valtype_cd
where table_name       = 'MEDPAR_ALL'
  -- exclude the Entity column and the dummy columns
  and column_name not in('BENE_ID', 'MEDPAR_ID', 'LTST_CLM_ACRTN_DT', 'INTRNL_USE_SSI_DAY_CNT', 'EXTRACT_DT')
  and not regexp_like(column_name, '(POA_)?DGNS_(E_)?(VRSN_)?(\d+_)?(IND_)?CD(_\d+)?')
  and column_name not like 'SRGCL_PRCDR_%'
order by table_name
, column_id ;


create or replace view cms_medpar_facts as
with detail as
  (select bene_id
  , medpar_id
  , admsn_dt
  , dschrg_dt
  , ltst_clm_acrtn_dt
  , 'MEDPAR_ALL' table_name
  , ty_col -- valtype_cd and column_name combined, since the for X part can only take one column expression
  , val_cd_text -- coded or text value
  , val_num -- numeric value
  , val_dt -- date value
  from "&&CMS_RIF".medpar_all unpivot((val_cd_text, val_num, val_dt) for ty_col in(
  
  (MEDPAR_YR_NUM, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ MEDPAR_YR_NUM'
, (NCH_CLM_TYPE_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ NCH_CLM_TYPE_CD'
, (BENE_IDENT_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ BENE_IDENT_CD'
, (EQTBL_BIC_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ EQTBL_BIC_CD'
, (BENE_SEX_CD, BENE_AGE_CNT, EXTRACT_DT) as 'N BENE_AGE_CNT'
, (BENE_SEX_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ BENE_SEX_CD'
, (BENE_RACE_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ BENE_RACE_CD'
, (BENE_MDCR_STUS_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ BENE_MDCR_STUS_CD'
, (BENE_RSDNC_SSA_STATE_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ BENE_RSDNC_SSA_STATE_CD'
, (BENE_RSDNC_SSA_CNTY_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ BENE_RSDNC_SSA_CNTY_CD'
, (BENE_MLG_CNTCT_ZIP_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ BENE_MLG_CNTCT_ZIP_CD'
, (BENE_DSCHRG_STUS_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ BENE_DSCHRG_STUS_CD'
, (FICARR_IDENT_NUM, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ FICARR_IDENT_NUM'
, (WRNG_IND_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ WRNG_IND_CD'
, (GHO_PD_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ GHO_PD_CD'
, (PPS_IND_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ PPS_IND_CD'
, (ORG_NPI_NUM, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as 'T ORG_NPI_NUM'
, (PRVDR_NUM, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as 'T PRVDR_NUM'
, (PRVDR_NUM_SPCL_UNIT_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ PRVDR_NUM_SPCL_UNIT_CD'
, (SS_LS_SNF_IND_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ SS_LS_SNF_IND_CD'
, (ACTV_XREF_IND, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ ACTV_XREF_IND'
, (SLCT_RSN_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ SLCT_RSN_CD'
, (BENE_SEX_CD, STAY_FINL_ACTN_CLM_CNT, EXTRACT_DT) as 'N STAY_FINL_ACTN_CLM_CNT'
, (BENE_SEX_CD, INTRNL_USE_SSI_DAY_CNT, BENE_MDCR_BNFT_EXHST_DT) as 'D BENE_MDCR_BNFT_EXHST_DT'
, (BENE_SEX_CD, INTRNL_USE_SSI_DAY_CNT, SNF_QUALN_FROM_DT) as 'D SNF_QUALN_FROM_DT'
, (BENE_SEX_CD, INTRNL_USE_SSI_DAY_CNT, SNF_QUALN_THRU_DT) as 'D SNF_QUALN_THRU_DT'
, (SRC_IP_ADMSN_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ SRC_IP_ADMSN_CD'
, (IP_ADMSN_TYPE_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ IP_ADMSN_TYPE_CD'
-- ISSUE: should DAY_CD be shifted?
, (ADMSN_DAY_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ ADMSN_DAY_CD'
-- ISSUE: facts for ADMSN_DT, DSGRG, DT? or just rely on LOS?
-- , (BENE_SEX_CD, INTRNL_USE_SSI_DAY_CNT, ADMSN_DT) as 'D ADMSN_DT'
-- , (BENE_SEX_CD, INTRNL_USE_SSI_DAY_CNT, DSCHRG_DT) as 'D DSCHRG_DT'
, (DSCHRG_DSTNTN_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ DSCHRG_DSTNTN_CD'
, (BENE_SEX_CD, INTRNL_USE_SSI_DAY_CNT, CVRD_LVL_CARE_THRU_DT) as 'D CVRD_LVL_CARE_THRU_DT'
, (BENE_SEX_CD, INTRNL_USE_SSI_DAY_CNT, BENE_DEATH_DT) as 'D BENE_DEATH_DT'
, (BENE_DEATH_DT_VRFY_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ BENE_DEATH_DT_VRFY_CD'
, (BENE_SEX_CD, ADMSN_DEATH_DAY_CNT, EXTRACT_DT) as 'N ADMSN_DEATH_DAY_CNT'
, (INTRNL_USE_SSI_IND_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ INTRNL_USE_SSI_IND_CD'
, (INTRNL_USE_SSI_DATA, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ INTRNL_USE_SSI_DATA'
, (INTRNL_USE_IPSB_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ INTRNL_USE_IPSB_CD'
, (INTRNL_USE_FIL_DT_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ INTRNL_USE_FIL_DT_CD'
, (INTRNL_USE_SMPL_SIZE_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ INTRNL_USE_SMPL_SIZE_CD'
, (BENE_SEX_CD, LOS_DAY_CNT, EXTRACT_DT) as 'N LOS_DAY_CNT'
, (BENE_SEX_CD, OUTLIER_DAY_CNT, EXTRACT_DT) as 'N OUTLIER_DAY_CNT'
, (BENE_SEX_CD, UTLZTN_DAY_CNT, EXTRACT_DT) as 'N UTLZTN_DAY_CNT'
, (BENE_SEX_CD, TOT_COINSRNC_DAY_CNT, EXTRACT_DT) as 'N TOT_COINSRNC_DAY_CNT'
, (BENE_SEX_CD, BENE_LRD_USE_CNT, EXTRACT_DT) as 'N BENE_LRD_USE_CNT'
, (BENE_SEX_CD, BENE_PTA_COINSRNC_AMT, EXTRACT_DT) as 'N BENE_PTA_COINSRNC_AMT'
, (BENE_SEX_CD, BENE_IP_DDCTBL_AMT, EXTRACT_DT) as 'N BENE_IP_DDCTBL_AMT'
, (BENE_SEX_CD, BENE_BLOOD_DDCTBL_AMT, EXTRACT_DT) as 'N BENE_BLOOD_DDCTBL_AMT'
, (BENE_PRMRY_PYR_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ BENE_PRMRY_PYR_CD'
, (BENE_SEX_CD, BENE_PRMRY_PYR_AMT, EXTRACT_DT) as 'N BENE_PRMRY_PYR_AMT'
, (DRG_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ DRG_CD'
, (DRG_OUTLIER_STAY_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ DRG_OUTLIER_STAY_CD'
, (BENE_SEX_CD, DRG_OUTLIER_PMT_AMT, EXTRACT_DT) as 'N DRG_OUTLIER_PMT_AMT'
, (BENE_SEX_CD, DRG_PRICE_AMT, EXTRACT_DT) as 'N DRG_PRICE_AMT'
, (BENE_SEX_CD, IP_DSPRPRTNT_SHR_AMT, EXTRACT_DT) as 'N IP_DSPRPRTNT_SHR_AMT'
, (BENE_SEX_CD, IME_AMT, EXTRACT_DT) as 'N IME_AMT'
, (BENE_SEX_CD, PASS_THRU_AMT, EXTRACT_DT) as 'N PASS_THRU_AMT'
, (BENE_SEX_CD, TOT_PPS_CPTL_AMT, EXTRACT_DT) as 'N TOT_PPS_CPTL_AMT'
, (BENE_SEX_CD, IP_LOW_VOL_PYMT_AMT, EXTRACT_DT) as 'N IP_LOW_VOL_PYMT_AMT'
, (BENE_SEX_CD, TOT_CHRG_AMT, EXTRACT_DT) as 'N TOT_CHRG_AMT'
, (BENE_SEX_CD, TOT_CVR_CHRG_AMT, EXTRACT_DT) as 'N TOT_CVR_CHRG_AMT'
, (BENE_SEX_CD, MDCR_PMT_AMT, EXTRACT_DT) as 'N MDCR_PMT_AMT'
, (BENE_SEX_CD, ACMDTNS_TOT_CHRG_AMT, EXTRACT_DT) as 'N ACMDTNS_TOT_CHRG_AMT'
, (BENE_SEX_CD, DPRTMNTL_TOT_CHRG_AMT, EXTRACT_DT) as 'N DPRTMNTL_TOT_CHRG_AMT'
, (BENE_SEX_CD, PRVT_ROOM_DAY_CNT, EXTRACT_DT) as 'N PRVT_ROOM_DAY_CNT'
, (BENE_SEX_CD, SEMIPRVT_ROOM_DAY_CNT, EXTRACT_DT) as 'N SEMIPRVT_ROOM_DAY_CNT'
, (BENE_SEX_CD, WARD_DAY_CNT, EXTRACT_DT) as 'N WARD_DAY_CNT'
, (BENE_SEX_CD, INTNSV_CARE_DAY_CNT, EXTRACT_DT) as 'N INTNSV_CARE_DAY_CNT'
, (BENE_SEX_CD, CRNRY_CARE_DAY_CNT, EXTRACT_DT) as 'N CRNRY_CARE_DAY_CNT'
, (BENE_SEX_CD, PRVT_ROOM_CHRG_AMT, EXTRACT_DT) as 'N PRVT_ROOM_CHRG_AMT'
, (BENE_SEX_CD, SEMIPRVT_ROOM_CHRG_AMT, EXTRACT_DT) as 'N SEMIPRVT_ROOM_CHRG_AMT'
, (BENE_SEX_CD, WARD_CHRG_AMT, EXTRACT_DT) as 'N WARD_CHRG_AMT'
, (BENE_SEX_CD, INTNSV_CARE_CHRG_AMT, EXTRACT_DT) as 'N INTNSV_CARE_CHRG_AMT'
, (BENE_SEX_CD, CRNRY_CARE_CHRG_AMT, EXTRACT_DT) as 'N CRNRY_CARE_CHRG_AMT'
, (BENE_SEX_CD, OTHR_SRVC_CHRG_AMT, EXTRACT_DT) as 'N OTHR_SRVC_CHRG_AMT'
, (BENE_SEX_CD, PHRMCY_CHRG_AMT, EXTRACT_DT) as 'N PHRMCY_CHRG_AMT'
, (BENE_SEX_CD, MDCL_SUPLY_CHRG_AMT, EXTRACT_DT) as 'N MDCL_SUPLY_CHRG_AMT'
, (BENE_SEX_CD, DME_CHRG_AMT, EXTRACT_DT) as 'N DME_CHRG_AMT'
, (BENE_SEX_CD, USED_DME_CHRG_AMT, EXTRACT_DT) as 'N USED_DME_CHRG_AMT'
, (BENE_SEX_CD, PHYS_THRPY_CHRG_AMT, EXTRACT_DT) as 'N PHYS_THRPY_CHRG_AMT'
, (BENE_SEX_CD, OCPTNL_THRPY_CHRG_AMT, EXTRACT_DT) as 'N OCPTNL_THRPY_CHRG_AMT'
, (BENE_SEX_CD, SPCH_PTHLGY_CHRG_AMT, EXTRACT_DT) as 'N SPCH_PTHLGY_CHRG_AMT'
, (BENE_SEX_CD, INHLTN_THRPY_CHRG_AMT, EXTRACT_DT) as 'N INHLTN_THRPY_CHRG_AMT'
, (BENE_SEX_CD, BLOOD_CHRG_AMT, EXTRACT_DT) as 'N BLOOD_CHRG_AMT'
, (BENE_SEX_CD, BLOOD_ADMIN_CHRG_AMT, EXTRACT_DT) as 'N BLOOD_ADMIN_CHRG_AMT'
, (BENE_SEX_CD, BLOOD_PT_FRNSH_QTY, EXTRACT_DT) as 'N BLOOD_PT_FRNSH_QTY'
, (BENE_SEX_CD, OPRTG_ROOM_CHRG_AMT, EXTRACT_DT) as 'N OPRTG_ROOM_CHRG_AMT'
, (BENE_SEX_CD, LTHTRPSY_CHRG_AMT, EXTRACT_DT) as 'N LTHTRPSY_CHRG_AMT'
, (BENE_SEX_CD, CRDLGY_CHRG_AMT, EXTRACT_DT) as 'N CRDLGY_CHRG_AMT'
, (BENE_SEX_CD, ANSTHSA_CHRG_AMT, EXTRACT_DT) as 'N ANSTHSA_CHRG_AMT'
, (BENE_SEX_CD, LAB_CHRG_AMT, EXTRACT_DT) as 'N LAB_CHRG_AMT'
, (BENE_SEX_CD, RDLGY_CHRG_AMT, EXTRACT_DT) as 'N RDLGY_CHRG_AMT'
, (BENE_SEX_CD, MRI_CHRG_AMT, EXTRACT_DT) as 'N MRI_CHRG_AMT'
, (BENE_SEX_CD, OP_SRVC_CHRG_AMT, EXTRACT_DT) as 'N OP_SRVC_CHRG_AMT'
, (BENE_SEX_CD, ER_CHRG_AMT, EXTRACT_DT) as 'N ER_CHRG_AMT'
, (BENE_SEX_CD, AMBLNC_CHRG_AMT, EXTRACT_DT) as 'N AMBLNC_CHRG_AMT'
, (BENE_SEX_CD, PROFNL_FEES_CHRG_AMT, EXTRACT_DT) as 'N PROFNL_FEES_CHRG_AMT'
, (BENE_SEX_CD, ORGN_ACQSTN_CHRG_AMT, EXTRACT_DT) as 'N ORGN_ACQSTN_CHRG_AMT'
, (BENE_SEX_CD, ESRD_REV_SETG_CHRG_AMT, EXTRACT_DT) as 'N ESRD_REV_SETG_CHRG_AMT'
, (BENE_SEX_CD, CLNC_VISIT_CHRG_AMT, EXTRACT_DT) as 'N CLNC_VISIT_CHRG_AMT'
, (ICU_IND_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ ICU_IND_CD'
, (CRNRY_CARE_IND_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ CRNRY_CARE_IND_CD'
, (PHRMCY_IND_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ PHRMCY_IND_CD'
, (TRNSPLNT_IND_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ TRNSPLNT_IND_CD'
, (RDLGY_ONCLGY_IND_SW, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ RDLGY_ONCLGY_IND_SW'
, (RDLGY_DGNSTC_IND_SW, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ RDLGY_DGNSTC_IND_SW'
, (RDLGY_THRPTC_IND_SW, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ RDLGY_THRPTC_IND_SW'
, (RDLGY_NUCLR_MDCN_IND_SW, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ RDLGY_NUCLR_MDCN_IND_SW'
, (RDLGY_CT_SCAN_IND_SW, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ RDLGY_CT_SCAN_IND_SW'
, (RDLGY_OTHR_IMGNG_IND_SW, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ RDLGY_OTHR_IMGNG_IND_SW'
, (OP_SRVC_IND_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ OP_SRVC_IND_CD'
, (ORGN_ACQSTN_IND_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ ORGN_ACQSTN_IND_CD'
, (ESRD_COND_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ ESRD_COND_CD'
, (ESRD_SETG_IND_1_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ ESRD_SETG_IND_1_CD'
, (ESRD_SETG_IND_2_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ ESRD_SETG_IND_2_CD'
, (ESRD_SETG_IND_3_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ ESRD_SETG_IND_3_CD'
, (ESRD_SETG_IND_4_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ ESRD_SETG_IND_4_CD'
, (ESRD_SETG_IND_5_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ ESRD_SETG_IND_5_CD'
, (DGNS_POA_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ DGNS_POA_CD'
, (CLM_PTNT_RLTNSHP_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ CLM_PTNT_RLTNSHP_CD'
, (CARE_IMPRVMT_MODEL_1_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ CARE_IMPRVMT_MODEL_1_CD'
, (CARE_IMPRVMT_MODEL_2_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ CARE_IMPRVMT_MODEL_2_CD'
, (CARE_IMPRVMT_MODEL_3_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ CARE_IMPRVMT_MODEL_3_CD'
, (CARE_IMPRVMT_MODEL_4_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ CARE_IMPRVMT_MODEL_4_CD'
, (VBP_PRTCPNT_IND_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ VBP_PRTCPNT_IND_CD'
, (HRR_PRTCPNT_IND_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ HRR_PRTCPNT_IND_CD'
, (BENE_SEX_CD, BNDLD_MODEL_DSCNT_PCT, EXTRACT_DT) as 'N BNDLD_MODEL_DSCNT_PCT'
, (BENE_SEX_CD, VBP_ADJSTMT_PCT, EXTRACT_DT) as 'N VBP_ADJSTMT_PCT'
, (BENE_SEX_CD, HRR_ADJSTMT_PCT, EXTRACT_DT) as 'N HRR_ADJSTMT_PCT'
, (INFRMTL_ENCTR_IND_SW, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ INFRMTL_ENCTR_IND_SW'
, (MA_TCHNG_IND_SW, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ MA_TCHNG_IND_SW'
, (PROD_RPLCMT_LIFECYC_SW, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ PROD_RPLCMT_LIFECYC_SW'
, (PROD_RPLCMT_RCLL_SW, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ PROD_RPLCMT_RCLL_SW'
, (CRED_RCVD_RPLCD_DVC_SW, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ CRED_RCVD_RPLCD_DVC_SW'
, (OBSRVTN_SW, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ OBSRVTN_SW'
, (BENE_SEX_CD, NEW_TCHNLGY_ADD_ON_AMT, EXTRACT_DT) as 'N NEW_TCHNLGY_ADD_ON_AMT'
, (BENE_SEX_CD, BASE_OPRTG_DRG_AMT, EXTRACT_DT) as 'N BASE_OPRTG_DRG_AMT'
, (BENE_SEX_CD, OPRTG_HSP_AMT, EXTRACT_DT) as 'N OPRTG_HSP_AMT'
, (BENE_SEX_CD, MDCL_SRGCL_GNRL_AMT, EXTRACT_DT) as 'N MDCL_SRGCL_GNRL_AMT'
, (BENE_SEX_CD, MDCL_SRGCL_NSTRL_AMT, EXTRACT_DT) as 'N MDCL_SRGCL_NSTRL_AMT'
, (BENE_SEX_CD, MDCL_SRGCL_STRL_AMT, EXTRACT_DT) as 'N MDCL_SRGCL_STRL_AMT'
, (BENE_SEX_CD, TAKE_HOME_AMT, EXTRACT_DT) as 'N TAKE_HOME_AMT'
, (BENE_SEX_CD, PRSTHTC_ORTHTC_AMT, EXTRACT_DT) as 'N PRSTHTC_ORTHTC_AMT'
, (BENE_SEX_CD, MDCL_SRGCL_PCMKR_AMT, EXTRACT_DT) as 'N MDCL_SRGCL_PCMKR_AMT'
, (BENE_SEX_CD, INTRAOCULAR_LENS_AMT, EXTRACT_DT) as 'N INTRAOCULAR_LENS_AMT'
, (BENE_SEX_CD, OXYGN_TAKE_HOME_AMT, EXTRACT_DT) as 'N OXYGN_TAKE_HOME_AMT'
, (BENE_SEX_CD, OTHR_IMPLANTS_AMT, EXTRACT_DT) as 'N OTHR_IMPLANTS_AMT'
, (BENE_SEX_CD, OTHR_SUPLIES_DVC_AMT, EXTRACT_DT) as 'N OTHR_SUPLIES_DVC_AMT'
, (BENE_SEX_CD, INCDNT_RDLGY_AMT, EXTRACT_DT) as 'N INCDNT_RDLGY_AMT'
, (BENE_SEX_CD, INCDNT_DGNSTC_SRVCS_AMT, EXTRACT_DT) as 'N INCDNT_DGNSTC_SRVCS_AMT'
, (BENE_SEX_CD, MDCL_SRGCL_DRSNG_AMT, EXTRACT_DT) as 'N MDCL_SRGCL_DRSNG_AMT'
, (BENE_SEX_CD, INVSTGTNL_DVC_AMT, EXTRACT_DT) as 'N INVSTGTNL_DVC_AMT'
, (BENE_SEX_CD, MDCL_SRGCL_MISC_AMT, EXTRACT_DT) as 'N MDCL_SRGCL_MISC_AMT'
, (BENE_SEX_CD, RDLGY_ONCOLOGY_AMT, EXTRACT_DT) as 'N RDLGY_ONCOLOGY_AMT'
, (BENE_SEX_CD, RDLGY_DGNSTC_AMT, EXTRACT_DT) as 'N RDLGY_DGNSTC_AMT'
, (BENE_SEX_CD, RDLGY_THRPTC_AMT, EXTRACT_DT) as 'N RDLGY_THRPTC_AMT'
, (BENE_SEX_CD, RDLGY_NUCLR_MDCN_AMT, EXTRACT_DT) as 'N RDLGY_NUCLR_MDCN_AMT'
, (BENE_SEX_CD, RDLGY_CT_SCAN_AMT, EXTRACT_DT) as 'N RDLGY_CT_SCAN_AMT'
, (BENE_SEX_CD, RDLGY_OTHR_IMGNG_AMT, EXTRACT_DT) as 'N RDLGY_OTHR_IMGNG_AMT'
, (BENE_SEX_CD, OPRTG_ROOM_AMT, EXTRACT_DT) as 'N OPRTG_ROOM_AMT'
, (BENE_SEX_CD, OR_LABOR_DLVRY_AMT, EXTRACT_DT) as 'N OR_LABOR_DLVRY_AMT'
, (BENE_SEX_CD, CRDC_CATHRZTN_AMT, EXTRACT_DT) as 'N CRDC_CATHRZTN_AMT'
, (BENE_SEX_CD, SQSTRTN_RDCTN_AMT, EXTRACT_DT) as 'N SQSTRTN_RDCTN_AMT'
, (BENE_SEX_CD, UNCOMPD_CARE_PYMT_AMT, EXTRACT_DT) as 'N UNCOMPD_CARE_PYMT_AMT'
, (BENE_SEX_CD, BNDLD_ADJSTMT_AMT, EXTRACT_DT) as 'N BNDLD_ADJSTMT_AMT'
, (BENE_SEX_CD, VBP_ADJSTMT_AMT, EXTRACT_DT) as 'N VBP_ADJSTMT_AMT'
, (BENE_SEX_CD, HRR_ADJSTMT_AMT, EXTRACT_DT) as 'N HRR_ADJSTMT_AMT'
, (BENE_SEX_CD, EHR_PYMT_ADJSTMT_AMT, EXTRACT_DT) as 'N EHR_PYMT_ADJSTMT_AMT'
, (BENE_SEX_CD, PPS_STD_VAL_PYMT_AMT, EXTRACT_DT) as 'N PPS_STD_VAL_PYMT_AMT'
, (BENE_SEX_CD, FINL_STD_AMT, EXTRACT_DT) as 'N FINL_STD_AMT'
, (BENE_SEX_CD, IPPS_FLEX_PYMT_6_AMT, EXTRACT_DT) as 'N IPPS_FLEX_PYMT_6_AMT'
, (BENE_SEX_CD, IPPS_FLEX_PYMT_7_AMT, EXTRACT_DT) as 'N IPPS_FLEX_PYMT_7_AMT'
, (BENE_SEX_CD, PTNT_ADD_ON_PYMT_AMT, EXTRACT_DT) as 'N PTNT_ADD_ON_PYMT_AMT'
, (HAC_PGM_RDCTN_IND_SW, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ HAC_PGM_RDCTN_IND_SW'
, (PGM_RDCTN_IND_SW, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ PGM_RDCTN_IND_SW'
, (PA_IND_CD, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ PA_IND_CD'
, (UNIQ_TRKNG_NUM, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ UNIQ_TRKNG_NUM'
, (STAY_2_IND_SW, INTRNL_USE_SSI_DAY_CNT, EXTRACT_DT) as '@ STAY_2_IND_SW'
  
  ))
  )

, pivot_valtype as -- parse ty_col into valtype_cd, scheme
  (select detail.*
  , substr(ty_col, 1, 1) valtype_cd
  , substr(ty_col, 3) column_name
  from detail
  )

, pivot_dates as
  (select pivot_valtype.*
  , ltst_clm_acrtn_dt update_date
  , case
      when valtype_cd = valtype_cd.date_val then val_dt
      else admsn_dt
    end start_date
  from pivot_valtype cross join valtype_cd
  ),
  no_info as
  (select no_value.not_recorded provider_id
  , null valueflag_cd
  , null quantity_num
  , null units_cd
  , null location_cd
  , null confidence_num
  from no_value
  )

select bene_id, medpar_id
, case
    when column_name = 'DRG_CD' then drg_cd(val_cd_text)
    when valtype_cd = valtype_cd.no_value
    then column_name
      || ':'
      || val_cd_text
    else column_name
      || ':'
  end concept_cd
, start_date
, rif_modifier(table_name) modifier_cd
, ora_hash(medpar_id
  || column_name) instance_num
, valtype_cd
, case
    when valtype_cd = valtype_cd.text then detail.val_cd_text
    when valtype_cd = valtype_cd.date_val then to_char(val_dt, 'YYYY-MM-DD')
    else null
  end tval_char
, case
    when valtype_cd = valtype_cd.numeric then val_num
    else null
  end nval_num
, dschrg_dt end_date
, update_date
, &&cms_source_cd sourcesystem_cd
, no_info.*
from pivot_dates detail
cross join no_info
cross join valtype_cd
cross join cms_key_sources key_sources
where
  (
    valtype_cd in (valtype_cd.no_value, valtype_cd.text)
    and val_cd_text is not null
  )
  or
  (
    valtype_cd   = valtype_cd.numeric
    and val_num is not null
  )
  or
  (
    valtype_cd  = valtype_cd.date_val
    and val_dt is not null
  )
;
-- eyeball it: select * from CMS_MEDPAR_FACTS

create or replace view medpar_pivot_design as
select &&design_digest design_digest from dual;

select 1 up_to_date
from medpar_pivot_design where design_digest = &&design_digest;
