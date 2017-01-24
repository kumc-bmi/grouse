-- events.sql: Per-bene_id date events
-- Copyright (c) 2017 University of Kansas Medical Center

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(maxdata_ps,12) */
'maxdata_ps' table_name,
  bene_id,
  msis_id,
  state_cd,
  EL_DOB,
  EL_DOD,
  MDCR_DOD,
  SSA_DOD
from maxdata_ps
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  EL_DOB as 'EL_DOB',
  EL_DOD as 'EL_DOD',
  MDCR_DOD as 'MDCR_DOD',
  SSA_DOD as 'SSA_DOD'
));
commit;

insert /*+ APPEND */ into date_events
select /*+ PARALLEL(outpatient_occurrnce_codes,12) */
'outpatient_occurrnce_codes' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  'CLM_RLT_OCRNC_DT' COL_DT,
  CLM_RLT_OCRNC_DT DT
from outpatient_occurrnce_codes;
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(outpatient_base_claims,12) */
'outpatient_base_claims' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  CLM_FROM_DT,
  CLM_THRU_DT,
  NCH_WKLY_PROC_DT,
  FI_CLM_PROC_DT,
  PRCDR_DT1,
  PRCDR_DT2,
  PRCDR_DT3,
  PRCDR_DT4,
  PRCDR_DT5,
  PRCDR_DT6,
  PRCDR_DT7,
  PRCDR_DT8,
  PRCDR_DT9,
  PRCDR_DT10,
  PRCDR_DT11,
  PRCDR_DT12,
  PRCDR_DT13,
  PRCDR_DT14,
  PRCDR_DT15,
  PRCDR_DT16,
  PRCDR_DT17,
  PRCDR_DT18,
  PRCDR_DT19,
  PRCDR_DT20,
  PRCDR_DT21,
  PRCDR_DT22,
  PRCDR_DT23,
  PRCDR_DT24,
  PRCDR_DT25,
  DOB_DT
from outpatient_base_claims
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  CLM_FROM_DT as 'CLM_FROM_DT',
  CLM_THRU_DT as 'CLM_THRU_DT',
  NCH_WKLY_PROC_DT as 'NCH_WKLY_PROC_DT',
  FI_CLM_PROC_DT as 'FI_CLM_PROC_DT',
  PRCDR_DT1 as 'PRCDR_DT1',
  PRCDR_DT2 as 'PRCDR_DT2',
  PRCDR_DT3 as 'PRCDR_DT3',
  PRCDR_DT4 as 'PRCDR_DT4',
  PRCDR_DT5 as 'PRCDR_DT5',
  PRCDR_DT6 as 'PRCDR_DT6',
  PRCDR_DT7 as 'PRCDR_DT7',
  PRCDR_DT8 as 'PRCDR_DT8',
  PRCDR_DT9 as 'PRCDR_DT9',
  PRCDR_DT10 as 'PRCDR_DT10',
  PRCDR_DT11 as 'PRCDR_DT11',
  PRCDR_DT12 as 'PRCDR_DT12',
  PRCDR_DT13 as 'PRCDR_DT13',
  PRCDR_DT14 as 'PRCDR_DT14',
  PRCDR_DT15 as 'PRCDR_DT15',
  PRCDR_DT16 as 'PRCDR_DT16',
  PRCDR_DT17 as 'PRCDR_DT17',
  PRCDR_DT18 as 'PRCDR_DT18',
  PRCDR_DT19 as 'PRCDR_DT19',
  PRCDR_DT20 as 'PRCDR_DT20',
  PRCDR_DT21 as 'PRCDR_DT21',
  PRCDR_DT22 as 'PRCDR_DT22',
  PRCDR_DT23 as 'PRCDR_DT23',
  PRCDR_DT24 as 'PRCDR_DT24',
  PRCDR_DT25 as 'PRCDR_DT25',
  DOB_DT as 'DOB_DT'
));
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(hospice_base_claims,12) */
'hospice_base_claims' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  CLM_FROM_DT,
  CLM_THRU_DT,
  NCH_WKLY_PROC_DT,
  FI_CLM_PROC_DT,
  NCH_BENE_DSCHRG_DT,
  CLM_HOSPC_START_DT_ID,
  DOB_DT
from hospice_base_claims
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  CLM_FROM_DT as 'CLM_FROM_DT',
  CLM_THRU_DT as 'CLM_THRU_DT',
  NCH_WKLY_PROC_DT as 'NCH_WKLY_PROC_DT',
  FI_CLM_PROC_DT as 'FI_CLM_PROC_DT',
  NCH_BENE_DSCHRG_DT as 'NCH_BENE_DSCHRG_DT',
  CLM_HOSPC_START_DT_ID as 'CLM_HOSPC_START_DT_ID',
  DOB_DT as 'DOB_DT'
));
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(outpatient_span_codes,12) */
'outpatient_span_codes' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  CLM_SPAN_FROM_DT,
  CLM_SPAN_THRU_DT
from outpatient_span_codes
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  CLM_SPAN_FROM_DT as 'CLM_SPAN_FROM_DT',
  CLM_SPAN_THRU_DT as 'CLM_SPAN_THRU_DT'
));
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(maxdata_ot,12) */
'maxdata_ot' table_name,
  bene_id,
  msis_id,
  state_cd,
  EL_DOB,
  PYMT_DT,
  SRVC_BGN_DT,
  SRVC_END_DT
from maxdata_ot
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  EL_DOB as 'EL_DOB',
  PYMT_DT as 'PYMT_DT',
  SRVC_BGN_DT as 'SRVC_BGN_DT',
  SRVC_END_DT as 'SRVC_END_DT'
));
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(mbsf_ab_summary,12) */
'mbsf_ab_summary' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  COVSTART,
  BENE_BIRTH_DT,
  BENE_DEATH_DT,
  NDI_DEATH_DT
from mbsf_ab_summary
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  COVSTART as 'COVSTART',
  BENE_BIRTH_DT as 'BENE_BIRTH_DT',
  BENE_DEATH_DT as 'BENE_DEATH_DT',
  NDI_DEATH_DT as 'NDI_DEATH_DT'
));
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(medpar_all,12) */
'medpar_all' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  LTST_CLM_ACRTN_DT,
  BENE_MDCR_BNFT_EXHST_DT,
  SNF_QUALN_FROM_DT,
  SNF_QUALN_THRU_DT,
  ADMSN_DT,
  DSCHRG_DT,
  CVRD_LVL_CARE_THRU_DT,
  BENE_DEATH_DT,
  SRGCL_PRCDR_PRFRM_1_DT,
  SRGCL_PRCDR_PRFRM_2_DT,
  SRGCL_PRCDR_PRFRM_3_DT,
  SRGCL_PRCDR_PRFRM_4_DT,
  SRGCL_PRCDR_PRFRM_5_DT,
  SRGCL_PRCDR_PRFRM_6_DT,
  SRGCL_PRCDR_PRFRM_7_DT,
  SRGCL_PRCDR_PRFRM_8_DT,
  SRGCL_PRCDR_PRFRM_9_DT,
  SRGCL_PRCDR_PRFRM_10_DT,
  SRGCL_PRCDR_PRFRM_11_DT,
  SRGCL_PRCDR_PRFRM_12_DT,
  SRGCL_PRCDR_PRFRM_13_DT,
  SRGCL_PRCDR_PRFRM_14_DT,
  SRGCL_PRCDR_PRFRM_15_DT,
  SRGCL_PRCDR_PRFRM_16_DT,
  SRGCL_PRCDR_PRFRM_17_DT,
  SRGCL_PRCDR_PRFRM_18_DT,
  SRGCL_PRCDR_PRFRM_19_DT,
  SRGCL_PRCDR_PRFRM_20_DT,
  SRGCL_PRCDR_PRFRM_21_DT,
  SRGCL_PRCDR_PRFRM_22_DT,
  SRGCL_PRCDR_PRFRM_23_DT,
  SRGCL_PRCDR_PRFRM_24_DT,
  SRGCL_PRCDR_PRFRM_25_DT
from medpar_all
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  LTST_CLM_ACRTN_DT as 'LTST_CLM_ACRTN_DT',
  BENE_MDCR_BNFT_EXHST_DT as 'BENE_MDCR_BNFT_EXHST_DT',
  SNF_QUALN_FROM_DT as 'SNF_QUALN_FROM_DT',
  SNF_QUALN_THRU_DT as 'SNF_QUALN_THRU_DT',
  ADMSN_DT as 'ADMSN_DT',
  DSCHRG_DT as 'DSCHRG_DT',
  CVRD_LVL_CARE_THRU_DT as 'CVRD_LVL_CARE_THRU_DT',
  BENE_DEATH_DT as 'BENE_DEATH_DT',
  SRGCL_PRCDR_PRFRM_1_DT as 'SRGCL_PRCDR_PRFRM_1_DT',
  SRGCL_PRCDR_PRFRM_2_DT as 'SRGCL_PRCDR_PRFRM_2_DT',
  SRGCL_PRCDR_PRFRM_3_DT as 'SRGCL_PRCDR_PRFRM_3_DT',
  SRGCL_PRCDR_PRFRM_4_DT as 'SRGCL_PRCDR_PRFRM_4_DT',
  SRGCL_PRCDR_PRFRM_5_DT as 'SRGCL_PRCDR_PRFRM_5_DT',
  SRGCL_PRCDR_PRFRM_6_DT as 'SRGCL_PRCDR_PRFRM_6_DT',
  SRGCL_PRCDR_PRFRM_7_DT as 'SRGCL_PRCDR_PRFRM_7_DT',
  SRGCL_PRCDR_PRFRM_8_DT as 'SRGCL_PRCDR_PRFRM_8_DT',
  SRGCL_PRCDR_PRFRM_9_DT as 'SRGCL_PRCDR_PRFRM_9_DT',
  SRGCL_PRCDR_PRFRM_10_DT as 'SRGCL_PRCDR_PRFRM_10_DT',
  SRGCL_PRCDR_PRFRM_11_DT as 'SRGCL_PRCDR_PRFRM_11_DT',
  SRGCL_PRCDR_PRFRM_12_DT as 'SRGCL_PRCDR_PRFRM_12_DT',
  SRGCL_PRCDR_PRFRM_13_DT as 'SRGCL_PRCDR_PRFRM_13_DT',
  SRGCL_PRCDR_PRFRM_14_DT as 'SRGCL_PRCDR_PRFRM_14_DT',
  SRGCL_PRCDR_PRFRM_15_DT as 'SRGCL_PRCDR_PRFRM_15_DT',
  SRGCL_PRCDR_PRFRM_16_DT as 'SRGCL_PRCDR_PRFRM_16_DT',
  SRGCL_PRCDR_PRFRM_17_DT as 'SRGCL_PRCDR_PRFRM_17_DT',
  SRGCL_PRCDR_PRFRM_18_DT as 'SRGCL_PRCDR_PRFRM_18_DT',
  SRGCL_PRCDR_PRFRM_19_DT as 'SRGCL_PRCDR_PRFRM_19_DT',
  SRGCL_PRCDR_PRFRM_20_DT as 'SRGCL_PRCDR_PRFRM_20_DT',
  SRGCL_PRCDR_PRFRM_21_DT as 'SRGCL_PRCDR_PRFRM_21_DT',
  SRGCL_PRCDR_PRFRM_22_DT as 'SRGCL_PRCDR_PRFRM_22_DT',
  SRGCL_PRCDR_PRFRM_23_DT as 'SRGCL_PRCDR_PRFRM_23_DT',
  SRGCL_PRCDR_PRFRM_24_DT as 'SRGCL_PRCDR_PRFRM_24_DT',
  SRGCL_PRCDR_PRFRM_25_DT as 'SRGCL_PRCDR_PRFRM_25_DT'
));
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(maxdata_ip,12) */
'maxdata_ip' table_name,
  bene_id,
  msis_id,
  state_cd,
  EL_DOB,
  PYMT_DT,
  ADMSN_DT,
  SRVC_BGN_DT,
  SRVC_END_DT,
  PRNCPL_PRCDR_DT
from maxdata_ip
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  EL_DOB as 'EL_DOB',
  PYMT_DT as 'PYMT_DT',
  ADMSN_DT as 'ADMSN_DT',
  SRVC_BGN_DT as 'SRVC_BGN_DT',
  SRVC_END_DT as 'SRVC_END_DT',
  PRNCPL_PRCDR_DT as 'PRNCPL_PRCDR_DT'
));
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(hha_revenue_center,12) */
'hha_revenue_center' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  CLM_THRU_DT,
  REV_CNTR_DT
from hha_revenue_center
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  CLM_THRU_DT as 'CLM_THRU_DT',
  REV_CNTR_DT as 'REV_CNTR_DT'
));
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(outpatient_revenue_center,12) */
'outpatient_revenue_center' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  CLM_THRU_DT,
  REV_CNTR_DT
from outpatient_revenue_center
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  CLM_THRU_DT as 'CLM_THRU_DT',
  REV_CNTR_DT as 'REV_CNTR_DT'
));
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(maxdata_rx,12) */
'maxdata_rx' table_name,
  bene_id,
  msis_id,
  state_cd,
  EL_DOB,
  PYMT_DT,
  PRSC_WRTE_DT,
  PRSCRPTN_FILL_DT
from maxdata_rx
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  EL_DOB as 'EL_DOB',
  PYMT_DT as 'PYMT_DT',
  PRSC_WRTE_DT as 'PRSC_WRTE_DT',
  PRSCRPTN_FILL_DT as 'PRSCRPTN_FILL_DT'
));
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(bcarrier_line,12) */
'bcarrier_line' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  CLM_THRU_DT,
  LINE_1ST_EXPNS_DT,
  LINE_LAST_EXPNS_DT
from bcarrier_line
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  CLM_THRU_DT as 'CLM_THRU_DT',
  LINE_1ST_EXPNS_DT as 'LINE_1ST_EXPNS_DT',
  LINE_LAST_EXPNS_DT as 'LINE_LAST_EXPNS_DT'
));
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(maxdata_lt,12) */
'maxdata_lt' table_name,
  bene_id,
  msis_id,
  state_cd,
  EL_DOB,
  PYMT_DT,
  ADMSN_DT,
  SRVC_BGN_DT,
  SRVC_END_DT
from maxdata_lt
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  EL_DOB as 'EL_DOB',
  PYMT_DT as 'PYMT_DT',
  ADMSN_DT as 'ADMSN_DT',
  SRVC_BGN_DT as 'SRVC_BGN_DT',
  SRVC_END_DT as 'SRVC_END_DT'
));
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(hha_base_claims,12) */
'hha_base_claims' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  CLM_FROM_DT,
  CLM_THRU_DT,
  NCH_WKLY_PROC_DT,
  FI_CLM_PROC_DT,
  CLM_ADMSN_DT,
  DOB_DT
from hha_base_claims
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  CLM_FROM_DT as 'CLM_FROM_DT',
  CLM_THRU_DT as 'CLM_THRU_DT',
  NCH_WKLY_PROC_DT as 'NCH_WKLY_PROC_DT',
  FI_CLM_PROC_DT as 'FI_CLM_PROC_DT',
  CLM_ADMSN_DT as 'CLM_ADMSN_DT',
  DOB_DT as 'DOB_DT'
));
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(hospice_revenue_center,12) */
'hospice_revenue_center' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  CLM_THRU_DT,
  REV_CNTR_DT
from hospice_revenue_center
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  CLM_THRU_DT as 'CLM_THRU_DT',
  REV_CNTR_DT as 'REV_CNTR_DT'
));
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(hha_span_codes,12) */
'hha_span_codes' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  CLM_SPAN_FROM_DT,
  CLM_SPAN_THRU_DT
from hha_span_codes
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  CLM_SPAN_FROM_DT as 'CLM_SPAN_FROM_DT',
  CLM_SPAN_THRU_DT as 'CLM_SPAN_THRU_DT'
));
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(hospice_span_codes,12) */
'hospice_span_codes' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  CLM_SPAN_FROM_DT,
  CLM_SPAN_THRU_DT
from hospice_span_codes
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  CLM_SPAN_FROM_DT as 'CLM_SPAN_FROM_DT',
  CLM_SPAN_THRU_DT as 'CLM_SPAN_THRU_DT'
));
commit;

insert /*+ APPEND */ into date_events
select /*+ PARALLEL(pde_saf,12) */
'pde_saf' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  'SRVC_DT' COL_DT,
  SRVC_DT DT
from pde_saf;
commit;

insert /*+ APPEND */ into date_events
select /*+ PARALLEL(hospice_occurrnce_codes,12) */
'hospice_occurrnce_codes' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  'CLM_RLT_OCRNC_DT' COL_DT,
  CLM_RLT_OCRNC_DT DT
from hospice_occurrnce_codes;
commit;

insert /*+ APPEND */ into date_events
with dates as (
select /*+ PARALLEL(bcarrier_claims,12) */
'bcarrier_claims' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  CLM_FROM_DT,
  CLM_THRU_DT,
  NCH_WKLY_PROC_DT,
  DOB_DT
from bcarrier_claims
  )
select * from dates
unpivot exclude nulls(
  dt for col_date in (
  CLM_FROM_DT as 'CLM_FROM_DT',
  CLM_THRU_DT as 'CLM_THRU_DT',
  NCH_WKLY_PROC_DT as 'NCH_WKLY_PROC_DT',
  DOB_DT as 'DOB_DT'
));
commit;

insert /*+ APPEND */ into date_events
select /*+ PARALLEL(pde,12) */
'pde' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  'SRVC_DT' COL_DT,
  SRVC_DT DT
from pde;
commit;

insert /*+ APPEND */ into date_events
select /*+ PARALLEL(hha_occurrnce_codes,12) */
'hha_occurrnce_codes' table_name,
  bene_id,
  null msis_id,
  null state_cd,
  'CLM_RLT_OCRNC_DT' COL_DT,
  CLM_RLT_OCRNC_DT DT
from hha_occurrnce_codes;
commit;
