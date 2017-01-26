-- cms_deid.sql: Deidentify CMS data
-- Copyright (c) 2017 University of Kansas Medical Center

insert /*+ APPEND */ into "&&deid_schema".outpatient_condition_codes
select /*+ PARALLEL(outpatient_condition_codes,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.RLT_COND_CD_SEQ, -- Claim Related Condition Code Sequence
  idt.CLM_RLT_COND_CD, -- Claim Related Condition Code
  idt.EXTRACT_DT
from outpatient_condition_codes idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".outpatient_value_codes
select /*+ PARALLEL(outpatient_value_codes,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.RLT_VAL_CD_SEQ, -- Claim Related Value Code Sequence
  idt.CLM_VAL_CD, -- Claim Value Code
  idt.CLM_VAL_AMT, -- Claim Value Amount
  idt.EXTRACT_DT
from outpatient_value_codes idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".outpatient_occurrnce_codes
select /*+ PARALLEL(outpatient_occurrnce_codes,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.RLT_OCRNC_CD_SEQ, -- Claim Related Occurrence Code Sequence
  idt.CLM_RLT_OCRNC_CD, -- Claim Related Occurrence Code
  idt.CLM_RLT_OCRNC_DT + bm.date_shift_days CLM_RLT_OCRNC_DT, -- Claim Related Occurrence Date
  idt.EXTRACT_DT
from outpatient_occurrnce_codes idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".outpatient_base_claims
select /*+ PARALLEL(outpatient_base_claims,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_NEAR_LINE_REC_IDENT_CD, -- NCH Near Line Record Identification Code
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.CLM_FROM_DT + bm.date_shift_days CLM_FROM_DT, -- Claim From Date
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date  (Determines Year of Claim)
  idt.NCH_WKLY_PROC_DT + bm.date_shift_days NCH_WKLY_PROC_DT, -- NCH Weekly Claim Processing Date
  idt.FI_CLM_PROC_DT + bm.date_shift_days FI_CLM_PROC_DT, -- FI Claim Process Date
  idt.CLAIM_QUERY_CODE, -- Claim Query Code
  idt.PRVDR_NUM, -- Provider Number
  idt.CLM_FAC_TYPE_CD, -- Claim Facility Type Code
  idt.CLM_SRVC_CLSFCTN_TYPE_CD, -- Claim Service classification Type Code
  idt.CLM_FREQ_CD, -- Claim Frequency Code
  idt.FI_NUM, -- FI Number
  idt.CLM_MDCR_NON_PMT_RSN_CD, -- Claim Medicare Non Payment Reason Code
  idt.CLM_PMT_AMT, -- Claim Payment Amount
  idt.NCH_PRMRY_PYR_CLM_PD_AMT, -- NCH Primary Payer Claim Paid Amount
  idt.NCH_PRMRY_PYR_CD, -- NCH Primary Payer Code
  idt.PRVDR_STATE_CD, -- NCH Provider State Code
  idt.ORG_NPI_NUM, -- Organization NPI Number
  idt.AT_PHYSN_UPIN, -- Claim Attending Physician UPIN Number
  idt.AT_PHYSN_NPI, -- Claim Attending Physician NPI Number
  idt.OP_PHYSN_UPIN, -- Claim Operating Physician UPIN Number
  idt.OP_PHYSN_NPI, -- Claim Operating Physician NPI Number
  idt.OT_PHYSN_UPIN, -- Claim Other Physician UPIN Number
  idt.OT_PHYSN_NPI, -- Claim Other Physician NPI Number
  idt.CLM_MCO_PD_SW, -- Claim MCO Paid Switch
  idt.PTNT_DSCHRG_STUS_CD, -- Patient Discharge Status Code
  idt.CLM_TOT_CHRG_AMT, -- Claim Total Charge Amount
  idt.NCH_BENE_BLOOD_DDCTBL_LBLTY_AM, -- NCH Beneficiary Blood Deductible Liability Amount
  idt.NCH_PROFNL_CMPNT_CHRG_AMT, -- NCH Professional Component Charge
  idt.PRNCPAL_DGNS_CD, -- Primary Claim Diagnosis Code
  idt.PRNCPAL_DGNS_VRSN_CD, -- Primary Claim Diagnosis Code Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD1, -- Claim Diagnosis Code I
  idt.ICD_DGNS_VRSN_CD1, -- Claim Diagnosis Code I Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD2, -- Claim Diagnosis Code II
  idt.ICD_DGNS_VRSN_CD2, -- Claim Diagnosis Code II Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD3, -- Claim Diagnosis Code III
  idt.ICD_DGNS_VRSN_CD3, -- Claim Diagnosis Code III Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD4, -- Claim Diagnosis Code IV
  idt.ICD_DGNS_VRSN_CD4, -- Claim Diagnosis Code IV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD5, -- Claim Diagnosis Code V
  idt.ICD_DGNS_VRSN_CD5, -- Claim Diagnosis Code V Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD6, -- Claim Diagnosis Code VI
  idt.ICD_DGNS_VRSN_CD6, -- Claim Diagnosis Code VI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD7, -- Claim Diagnosis Code VII
  idt.ICD_DGNS_VRSN_CD7, -- Claim Diagnosis Code VII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD8, -- Claim Diagnosis Code VIII
  idt.ICD_DGNS_VRSN_CD8, -- Claim Diagnosis Code VIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD9, -- Claim Diagnosis Code IX
  idt.ICD_DGNS_VRSN_CD9, -- Claim Diagnosis Code IX Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD10, -- Claim Diagnosis Code X
  idt.ICD_DGNS_VRSN_CD10, -- Claim Diagnosis Code X Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD11, -- Claim Diagnosis Code XI
  idt.ICD_DGNS_VRSN_CD11, -- Claim Diagnosis Code XI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD12, -- Claim Diagnosis Code XII
  idt.ICD_DGNS_VRSN_CD12, -- Claim Diagnosis Code XII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD13, -- Claim Diagnosis Code XIII
  idt.ICD_DGNS_VRSN_CD13, -- Claim Diagnosis Code XIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD14, -- Claim Diagnosis Code XIV
  idt.ICD_DGNS_VRSN_CD14, -- Claim Diagnosis Code XIV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD15, -- Claim Diagnosis Code XV
  idt.ICD_DGNS_VRSN_CD15, -- Claim Diagnosis Code XV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD16, -- Claim Diagnosis Code XVI
  idt.ICD_DGNS_VRSN_CD16, -- Claim Diagnosis Code XVI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD17, -- Claim Diagnosis Code XVII
  idt.ICD_DGNS_VRSN_CD17, -- Claim Diagnosis Code XVII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD18, -- Claim Diagnosis Code XVIII
  idt.ICD_DGNS_VRSN_CD18, -- Claim Diagnosis Code XVIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD19, -- Claim Diagnosis Code XIX
  idt.ICD_DGNS_VRSN_CD19, -- Claim Diagnosis Code XIX Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD20, -- Claim Diagnosis Code XX
  idt.ICD_DGNS_VRSN_CD20, -- Claim Diagnosis Code XX Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD21, -- Claim Diagnosis Code XXI
  idt.ICD_DGNS_VRSN_CD21, -- Claim Diagnosis Code XXI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD22, -- Claim Diagnosis Code XXII
  idt.ICD_DGNS_VRSN_CD22, -- Claim Diagnosis Code XXII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD23, -- Claim Diagnosis Code XXIII
  idt.ICD_DGNS_VRSN_CD23, -- Claim Diagnosis Code XXIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD24, -- Claim Diagnosis Code XXIV
  idt.ICD_DGNS_VRSN_CD24, -- Claim Diagnosis Code XXIV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD25, -- Claim Diagnosis Code XXV
  idt.ICD_DGNS_VRSN_CD25, -- Claim Diagnosis Code XXV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.FST_DGNS_E_CD, -- First Claim Diagnosis E Code
  idt.FST_DGNS_E_VRSN_CD, -- First Claim Diagnosis E Code Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD1, -- Claim Diagnosis E Code I
  idt.ICD_DGNS_E_VRSN_CD1, -- Claim Diagnosis E Code I Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD2, -- Claim Diagnosis E Code II
  idt.ICD_DGNS_E_VRSN_CD2, -- Claim Diagnosis E Code II Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD3, -- Claim Diagnosis E Code III
  idt.ICD_DGNS_E_VRSN_CD3, -- Claim Diagnosis E Code III Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD4, -- Claim Diagnosis E Code IV
  idt.ICD_DGNS_E_VRSN_CD4, -- Claim Diagnosis E Code IV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD5, -- Claim Diagnosis E Code V
  idt.ICD_DGNS_E_VRSN_CD5, -- Claim Diagnosis E Code V Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD6, -- Claim Diagnosis E Code VI
  idt.ICD_DGNS_E_VRSN_CD6, -- Claim Diagnosis E Code VI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD7, -- Claim Diagnosis E Code VII
  idt.ICD_DGNS_E_VRSN_CD7, -- Claim Diagnosis E Code VII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD8, -- Claim Diagnosis E Code VIII
  idt.ICD_DGNS_E_VRSN_CD8, -- Claim Diagnosis E Code VIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD9, -- Claim Diagnosis E Code IX
  idt.ICD_DGNS_E_VRSN_CD9, -- Claim Diagnosis E Code IX Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD10, -- Claim Diagnosis E Code X
  idt.ICD_DGNS_E_VRSN_CD10, -- Claim Diagnosis E Code X Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD11, -- Claim Diagnosis E Code XI
  idt.ICD_DGNS_E_VRSN_CD11, -- Claim Diagnosis E Code XI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD12, -- Claim Diagnosis E Code XII
  idt.ICD_DGNS_E_VRSN_CD12, -- Claim Diagnosis E Code XII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_PRCDR_CD1, -- Claim Procedure Code I
  idt.ICD_PRCDR_VRSN_CD1, -- Claim Procedure Code I Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT1 + bm.date_shift_days PRCDR_DT1, -- Claim Procedure Code I Date
  idt.ICD_PRCDR_CD2, -- Claim Procedure Code II
  idt.ICD_PRCDR_VRSN_CD2, -- Claim Procedure Code II Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT2 + bm.date_shift_days PRCDR_DT2, -- Claim Procedure Code II Date
  idt.ICD_PRCDR_CD3, -- Claim Procedure Code III
  idt.ICD_PRCDR_VRSN_CD3, -- Claim Procedure Code III Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT3 + bm.date_shift_days PRCDR_DT3, -- Claim Procedure Code III Date
  idt.ICD_PRCDR_CD4, -- Claim Procedure Code IV
  idt.ICD_PRCDR_VRSN_CD4, -- Claim Procedure Code IV Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT4 + bm.date_shift_days PRCDR_DT4, -- Claim Procedure Code IV Date
  idt.ICD_PRCDR_CD5, -- Claim Procedure Code V
  idt.ICD_PRCDR_VRSN_CD5, -- Claim Procedure Code V Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT5 + bm.date_shift_days PRCDR_DT5, -- Claim Procedure Code V Date
  idt.ICD_PRCDR_CD6, -- Claim Procedure Code VI
  idt.ICD_PRCDR_VRSN_CD6, -- Claim Procedure Code VI Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT6 + bm.date_shift_days PRCDR_DT6, -- Claim Procedure Code VI Date
  idt.ICD_PRCDR_CD7, -- Claim Procedure Code VII
  idt.ICD_PRCDR_VRSN_CD7, -- Claim Procedure Code VII Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT7 + bm.date_shift_days PRCDR_DT7, -- Claim Procedure Code VII Date
  idt.ICD_PRCDR_CD8, -- Claim Procedure Code VIII
  idt.ICD_PRCDR_VRSN_CD8, -- Claim Procedure Code VIII Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT8 + bm.date_shift_days PRCDR_DT8, -- Claim Procedure Code VIII Date
  idt.ICD_PRCDR_CD9, -- Claim Procedure Code IX
  idt.ICD_PRCDR_VRSN_CD9, -- Claim Procedure Code IX Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT9 + bm.date_shift_days PRCDR_DT9, -- Claim Procedure Code IX Date
  idt.ICD_PRCDR_CD10, -- Claim Procedure Code X
  idt.ICD_PRCDR_VRSN_CD10, -- Claim Procedure Code X Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT10 + bm.date_shift_days PRCDR_DT10, -- Claim Procedure Code X Date
  idt.ICD_PRCDR_CD11, -- Claim Procedure Code XI
  idt.ICD_PRCDR_VRSN_CD11, -- Claim Procedure Code XI Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT11 + bm.date_shift_days PRCDR_DT11, -- Claim Procedure Code XI Date
  idt.ICD_PRCDR_CD12, -- Claim Procedure Code XII
  idt.ICD_PRCDR_VRSN_CD12, -- Claim Procedure Code XII Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT12 + bm.date_shift_days PRCDR_DT12, -- Claim Procedure Code XII Date
  idt.ICD_PRCDR_CD13, -- Claim Procedure Code XIII
  idt.ICD_PRCDR_VRSN_CD13, -- Claim Procedure Code XIII Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT13 + bm.date_shift_days PRCDR_DT13, -- Claim Procedure Code XIII Date
  idt.ICD_PRCDR_CD14, -- Claim Procedure Code XIV
  idt.ICD_PRCDR_VRSN_CD14, -- Claim Procedure Code XIV Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT14 + bm.date_shift_days PRCDR_DT14, -- Claim Procedure Code XIV Date
  idt.ICD_PRCDR_CD15, -- Claim Procedure Code XV
  idt.ICD_PRCDR_VRSN_CD15, -- Claim Procedure Code XV Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT15 + bm.date_shift_days PRCDR_DT15, -- Claim Procedure Code XV Date
  idt.ICD_PRCDR_CD16, -- Claim Procedure Code XVI
  idt.ICD_PRCDR_VRSN_CD16, -- Claim Procedure Code XVI Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT16 + bm.date_shift_days PRCDR_DT16, -- Claim Procedure Code XVI Date
  idt.ICD_PRCDR_CD17, -- Claim Procedure Code XVII
  idt.ICD_PRCDR_VRSN_CD17, -- Claim Procedure Code XVII Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT17 + bm.date_shift_days PRCDR_DT17, -- Claim Procedure Code XVII Date
  idt.ICD_PRCDR_CD18, -- Claim Procedure Code XVIII
  idt.ICD_PRCDR_VRSN_CD18, -- Claim Procedure Code XVIII Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT18 + bm.date_shift_days PRCDR_DT18, -- Claim Procedure Code XVIII Date
  idt.ICD_PRCDR_CD19, -- Claim Procedure Code XIX
  idt.ICD_PRCDR_VRSN_CD19, -- Claim Procedure Code XIX Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT19 + bm.date_shift_days PRCDR_DT19, -- Claim Procedure Code XIX Date
  idt.ICD_PRCDR_CD20, -- Claim Procedure Code XX
  idt.ICD_PRCDR_VRSN_CD20, -- Claim Procedure Code XX Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT20 + bm.date_shift_days PRCDR_DT20, -- Claim Procedure Code XX Date
  idt.ICD_PRCDR_CD21, -- Claim Procedure Code XXI
  idt.ICD_PRCDR_VRSN_CD21, -- Claim Procedure Code XXI Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT21 + bm.date_shift_days PRCDR_DT21, -- Claim Procedure Code XXI Date
  idt.ICD_PRCDR_CD22, -- Claim Procedure Code XXII
  idt.ICD_PRCDR_VRSN_CD22, -- Claim Procedure Code XXII Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT22 + bm.date_shift_days PRCDR_DT22, -- Claim Procedure Code XXII Date
  idt.ICD_PRCDR_CD23, -- Claim Procedure Code XXIII
  idt.ICD_PRCDR_VRSN_CD23, -- Claim Procedure Code XXIII Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT23 + bm.date_shift_days PRCDR_DT23, -- Claim Procedure Code XXIII Date
  idt.ICD_PRCDR_CD24, -- Claim Procedure Code XXIV
  idt.ICD_PRCDR_VRSN_CD24, -- Claim Procedure Code XXIV Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT24 + bm.date_shift_days PRCDR_DT24, -- Claim Procedure Code XXIV Date
  idt.ICD_PRCDR_CD25, -- Claim Procedure Code XXV
  idt.ICD_PRCDR_VRSN_CD25, -- Claim Procedure Code XXV Claim Procedure Version Code (ICD-9 or ICD-10)
  idt.PRCDR_DT25 + bm.date_shift_days PRCDR_DT25, -- Claim Procedure Code XXV Date
  idt.RSN_VISIT_CD1, -- Reason for Visit Diagnosis Code I
  idt.RSN_VISIT_VRSN_CD1, -- Reason for Visit Diagnosis Code I Diagnosis Version Code (ICD-9 or ICD-10)
  idt.RSN_VISIT_CD2, -- Reason for Visit Diagnosis Code II
  idt.RSN_VISIT_VRSN_CD2, -- Reason for Visit Diagnosis Code II Diagnosis Version Code (ICD-9 or ICD-10)
  idt.RSN_VISIT_CD3, -- Reason for Visit Diagnosis Code III
  idt.RSN_VISIT_VRSN_CD3, -- Reason for Visit Diagnosis Code III Diagnosis Version Code (ICD-9 or ICD-10)
  idt.NCH_BENE_PTB_DDCTBL_AMT, -- NCH Beneficiary Part B Deductible Amount
  idt.NCH_BENE_PTB_COINSRNC_AMT, -- NCH Beneficiary Part B Coinsurance Amount
  idt.CLM_OP_PRVDR_PMT_AMT, -- Claim Outpatient Provider Payment Amount
  idt.CLM_OP_BENE_PMT_AMT, -- Claim Outpatient Beneficiary Payment Amount
  case
    when bm.dob_shift_months is not null
    then add_months(DOB_DT, bm.dob_shift_months)
    else idt.DOB_DT + bm.date_shift_days
  end DOB_DT, -- Date of Birth from Claim (Date)
  idt.GNDR_CD, -- Gender Code from Claim
  idt.BENE_RACE_CD, -- Race Code from Claim
  NULL BENE_CNTY_CD, -- County Code from Claim (SSA)
  idt.BENE_STATE_CD, -- State Code from Claim (SSA)
  NULL BENE_MLG_CNTCT_ZIP_CD, -- Zip Code of Residence from Claim
  idt.CLM_MDCL_REC, -- Claim Medical Record Number
  idt.EXTRACT_DT
from outpatient_base_claims idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hospice_base_claims
select /*+ PARALLEL(hospice_base_claims,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_NEAR_LINE_REC_IDENT_CD, -- NCH Near Line Record Identification Code
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.CLM_FROM_DT + bm.date_shift_days CLM_FROM_DT, -- Claim From Date
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date  (Determines Year of Claim)
  idt.NCH_WKLY_PROC_DT + bm.date_shift_days NCH_WKLY_PROC_DT, -- NCH Weekly Claim Processing Date
  idt.FI_CLM_PROC_DT + bm.date_shift_days FI_CLM_PROC_DT, -- FI Claim Process Date
  idt.PRVDR_NUM, -- Provider Number
  idt.CLM_FAC_TYPE_CD, -- Claim Facility Type Code
  idt.CLM_SRVC_CLSFCTN_TYPE_CD, -- Claim Service classification Type Code
  idt.CLM_FREQ_CD, -- Claim Frequency Code
  idt.FI_NUM, -- FI Number
  idt.CLM_MDCR_NON_PMT_RSN_CD, -- Claim Medicare Non Payment Reason Code
  idt.CLM_PMT_AMT, -- Claim Payment Amount
  idt.NCH_PRMRY_PYR_CLM_PD_AMT, -- NCH Primary Payer Claim Paid Amount
  idt.NCH_PRMRY_PYR_CD, -- NCH Primary Payer Code
  idt.PRVDR_STATE_CD, -- NCH Provider State Code
  idt.ORG_NPI_NUM, -- Organization NPI Number
  idt.AT_PHYSN_UPIN, -- Claim Attending Physician UPIN Number
  idt.AT_PHYSN_NPI, -- Claim Attending Physician NPI Number
  idt.PTNT_DSCHRG_STUS_CD, -- Patient Discharge Status Code
  idt.CLM_TOT_CHRG_AMT, -- Claim Total Charge Amount
  idt.NCH_PTNT_STATUS_IND_CD, -- NCH Patient Status Indicator Code
  idt.CLM_UTLZTN_DAY_CNT, -- Claim Utilization Day Count
  idt.NCH_BENE_DSCHRG_DT + bm.date_shift_days NCH_BENE_DSCHRG_DT, -- NCH Beneficiary Discharge Date
  idt.PRNCPAL_DGNS_CD, -- Primary Claim Diagnosis Code
  idt.PRNCPAL_DGNS_VRSN_CD, -- Primary Claim Diagnosis Code Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD1, -- Claim Diagnosis Code I
  idt.ICD_DGNS_VRSN_CD1, -- Claim Diagnosis Code I Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD2, -- Claim Diagnosis Code II
  idt.ICD_DGNS_VRSN_CD2, -- Claim Diagnosis Code II Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD3, -- Claim Diagnosis Code III
  idt.ICD_DGNS_VRSN_CD3, -- Claim Diagnosis Code III Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD4, -- Claim Diagnosis Code IV
  idt.ICD_DGNS_VRSN_CD4, -- Claim Diagnosis Code IV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD5, -- Claim Diagnosis Code V
  idt.ICD_DGNS_VRSN_CD5, -- Claim Diagnosis Code V Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD6, -- Claim Diagnosis Code VI
  idt.ICD_DGNS_VRSN_CD6, -- Claim Diagnosis Code VI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD7, -- Claim Diagnosis Code VII
  idt.ICD_DGNS_VRSN_CD7, -- Claim Diagnosis Code VII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD8, -- Claim Diagnosis Code VIII
  idt.ICD_DGNS_VRSN_CD8, -- Claim Diagnosis Code VIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD9, -- Claim Diagnosis Code IX
  idt.ICD_DGNS_VRSN_CD9, -- Claim Diagnosis Code IX Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD10, -- Claim Diagnosis Code X
  idt.ICD_DGNS_VRSN_CD10, -- Claim Diagnosis Code X Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD11, -- Claim Diagnosis Code XI
  idt.ICD_DGNS_VRSN_CD11, -- Claim Diagnosis Code XI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD12, -- Claim Diagnosis Code XII
  idt.ICD_DGNS_VRSN_CD12, -- Claim Diagnosis Code XII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD13, -- Claim Diagnosis Code XIII
  idt.ICD_DGNS_VRSN_CD13, -- Claim Diagnosis Code XIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD14, -- Claim Diagnosis Code XIV
  idt.ICD_DGNS_VRSN_CD14, -- Claim Diagnosis Code XIV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD15, -- Claim Diagnosis Code XV
  idt.ICD_DGNS_VRSN_CD15, -- Claim Diagnosis Code XV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD16, -- Claim Diagnosis Code XVI
  idt.ICD_DGNS_VRSN_CD16, -- Claim Diagnosis Code XVI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD17, -- Claim Diagnosis Code XVII
  idt.ICD_DGNS_VRSN_CD17, -- Claim Diagnosis Code XVII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD18, -- Claim Diagnosis Code XVIII
  idt.ICD_DGNS_VRSN_CD18, -- Claim Diagnosis Code XVIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD19, -- Claim Diagnosis Code XIX
  idt.ICD_DGNS_VRSN_CD19, -- Claim Diagnosis Code XIX Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD20, -- Claim Diagnosis Code XX
  idt.ICD_DGNS_VRSN_CD20, -- Claim Diagnosis Code XX Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD21, -- Claim Diagnosis Code XXI
  idt.ICD_DGNS_VRSN_CD21, -- Claim Diagnosis Code XXI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD22, -- Claim Diagnosis Code XXII
  idt.ICD_DGNS_VRSN_CD22, -- Claim Diagnosis Code XXII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD23, -- Claim Diagnosis Code XXIII
  idt.ICD_DGNS_VRSN_CD23, -- Claim Diagnosis Code XXIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD24, -- Claim Diagnosis Code XXIV
  idt.ICD_DGNS_VRSN_CD24, -- Claim Diagnosis Code XXIV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD25, -- Claim Diagnosis Code XXV
  idt.ICD_DGNS_VRSN_CD25, -- Claim Diagnosis Code XXV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.FST_DGNS_E_CD, -- First Claim Diagnosis E Code
  idt.FST_DGNS_E_VRSN_CD, -- First Claim Diagnosis E Code Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD1, -- Claim Diagnosis E Code I
  idt.ICD_DGNS_E_VRSN_CD1, -- Claim Diagnosis E Code I Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD2, -- Claim Diagnosis E Code II
  idt.ICD_DGNS_E_VRSN_CD2, -- Claim Diagnosis E Code II Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD3, -- Claim Diagnosis E Code III
  idt.ICD_DGNS_E_VRSN_CD3, -- Claim Diagnosis E Code III Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD4, -- Claim Diagnosis E Code IV
  idt.ICD_DGNS_E_VRSN_CD4, -- Claim Diagnosis E Code IV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD5, -- Claim Diagnosis E Code V
  idt.ICD_DGNS_E_VRSN_CD5, -- Claim Diagnosis E Code V Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD6, -- Claim Diagnosis E Code VI
  idt.ICD_DGNS_E_VRSN_CD6, -- Claim Diagnosis E Code VI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD7, -- Claim Diagnosis E Code VII
  idt.ICD_DGNS_E_VRSN_CD7, -- Claim Diagnosis E Code VII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD8, -- Claim Diagnosis E Code VIII
  idt.ICD_DGNS_E_VRSN_CD8, -- Claim Diagnosis E Code VIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD9, -- Claim Diagnosis E Code IX
  idt.ICD_DGNS_E_VRSN_CD9, -- Claim Diagnosis E Code IX Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD10, -- Claim Diagnosis E Code X
  idt.ICD_DGNS_E_VRSN_CD10, -- Claim Diagnosis E Code X Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD11, -- Claim Diagnosis E Code XI
  idt.ICD_DGNS_E_VRSN_CD11, -- Claim Diagnosis E Code XI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD12, -- Claim Diagnosis E Code XII
  idt.ICD_DGNS_E_VRSN_CD12, -- Claim Diagnosis E Code XII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.CLM_HOSPC_START_DT_ID + bm.date_shift_days CLM_HOSPC_START_DT_ID, -- Claim Hospice Start Date
  idt.BENE_HOSPC_PRD_CNT, -- Beneficiary's Hospice Period Count
  case
    when bm.dob_shift_months is not null
    then add_months(DOB_DT, bm.dob_shift_months)
    else idt.DOB_DT + bm.date_shift_days
  end DOB_DT, -- Date of Birth from Claim (Date)
  idt.GNDR_CD, -- Gender Code from Claim
  idt.BENE_RACE_CD, -- Race Code from Claim
  NULL BENE_CNTY_CD, -- County Code from Claim (SSA)
  idt.BENE_STATE_CD, -- State Code from Claim (SSA)
  NULL BENE_MLG_CNTCT_ZIP_CD, -- Zip Code of Residence from Claim
  idt.CLM_MDCL_REC, -- Claim Medical Record Number
  idt.EXTRACT_DT
from hospice_base_claims idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".outpatient_span_codes
select /*+ PARALLEL(outpatient_span_codes,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.RLT_SPAN_CD_SEQ, -- Claim Related Span Code Sequence
  idt.CLM_SPAN_CD, -- Claim Occurrence Span Code
  idt.CLM_SPAN_FROM_DT + bm.date_shift_days CLM_SPAN_FROM_DT, -- Claim Occurrence Span From Date
  idt.CLM_SPAN_THRU_DT + bm.date_shift_days CLM_SPAN_THRU_DT, -- Claim Occurrence Span Through Date
  idt.EXTRACT_DT
from outpatient_span_codes idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".mbsf_ab_summary
select /*+ PARALLEL(mbsf_ab_summary,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID (Unique Key)
  idt.BENE_ENROLLMT_REF_YR, -- Beneficiary Enrollment Reference Year
  idt.FIVE_PERCENT_FLAG, -- Strict 5% Flag
  idt.ENHANCED_FIVE_PERCENT_FLAG, -- Enhanced 5% Flag
  idt.COVSTART + bm.date_shift_days COVSTART, -- Medicare Coverage Start Date
  idt.CRNT_BIC_CD, -- Beneficiary Identification Code
  idt.STATE_CODE, -- State Code
  NULL BENE_COUNTY_CD, -- County Code
  NULL BENE_ZIP_CD, -- Zip Code of Residence
  case
    when BENE_AGE_AT_END_REF_YR is null then null
    when bm.dob_shift_months is not null then
      case
        when BENE_AGE_AT_END_REF_YR - round(bm.dob_shift_months/12) <= 89
        then BENE_AGE_AT_END_REF_YR - round(bm.dob_shift_months/12)
        else 89
      end
    when BENE_AGE_AT_END_REF_YR > 89 then 89
    else BENE_AGE_AT_END_REF_YR
  end BENE_AGE_AT_END_REF_YR, -- Age at End of Reference Year
  case
    when bm.dob_shift_months is not null
    then add_months(BENE_BIRTH_DT, bm.dob_shift_months)
    else idt.BENE_BIRTH_DT + bm.date_shift_days
  end BENE_BIRTH_DT, -- Date of Birth
  idt.BENE_VALID_DEATH_DT_SW, -- Valid Date of Death Switch
  idt.BENE_DEATH_DT + bm.date_shift_days BENE_DEATH_DT, -- Date of Death
  idt.NDI_DEATH_DT + bm.date_shift_days NDI_DEATH_DT, -- NDI Date of Death
  idt.BENE_SEX_IDENT_CD, -- Sex
  idt.BENE_RACE_CD, -- Beneficiary Race Code
  idt.RTI_RACE_CD, -- Research Triangle Institute (RTI) Race Code
  idt.BENE_ENTLMT_RSN_ORIG, -- Original Reason for Entitlement Code
  idt.BENE_ENTLMT_RSN_CURR, -- Current Reason for Entitlement Code
  idt.BENE_ESRD_IND, -- ESRD Indicator
  idt.BENE_MDCR_STATUS_CD, -- Medicare Status Code
  idt.BENE_PTA_TRMNTN_CD, -- Part A Termination Code
  idt.BENE_PTB_TRMNTN_CD, -- Part B Termination Code
  idt.BENE_HI_CVRAGE_TOT_MONS, -- HI Coverage Count
  idt.BENE_SMI_CVRAGE_TOT_MONS, -- SMI Coverage Count
  idt.BENE_STATE_BUYIN_TOT_MONS, -- State Buy-In Coverage Count
  idt.BENE_HMO_CVRAGE_TOT_MONS, -- HMO Coverage Count
  idt.BENE_MDCR_ENTLMT_BUYIN_IND_01, -- Medicare Entitlement/Buy-In Indicator I
  idt.BENE_MDCR_ENTLMT_BUYIN_IND_02, -- Medicare Entitlement/Buy-In Indicator II
  idt.BENE_MDCR_ENTLMT_BUYIN_IND_03, -- Medicare Entitlement/Buy-In Indicator III
  idt.BENE_MDCR_ENTLMT_BUYIN_IND_04, -- Medicare Entitlement/Buy-In Indicator IV
  idt.BENE_MDCR_ENTLMT_BUYIN_IND_05, -- Medicare Entitlement/Buy-In Indicator V
  idt.BENE_MDCR_ENTLMT_BUYIN_IND_06, -- Medicare Entitlement/Buy-In Indicator VI
  idt.BENE_MDCR_ENTLMT_BUYIN_IND_07, -- Medicare Entitlement/Buy-In Indicator VII
  idt.BENE_MDCR_ENTLMT_BUYIN_IND_08, -- Medicare Entitlement/Buy-In Indicator VIII
  idt.BENE_MDCR_ENTLMT_BUYIN_IND_09, -- Medicare Entitlement/Buy-In Indicator IX
  idt.BENE_MDCR_ENTLMT_BUYIN_IND_10, -- Medicare Entitlement/Buy-In Indicator X
  idt.BENE_MDCR_ENTLMT_BUYIN_IND_11, -- Medicare Entitlement/Buy-In Indicator XI
  idt.BENE_MDCR_ENTLMT_BUYIN_IND_12, -- Medicare Entitlement/Buy-In Indicator XII
  idt.BENE_HMO_IND_01, -- HMO Indicator I
  idt.BENE_HMO_IND_02, -- HMO Indicator II
  idt.BENE_HMO_IND_03, -- HMO Indicator III
  idt.BENE_HMO_IND_04, -- HMO Indicator IV
  idt.BENE_HMO_IND_05, -- HMO Indicator V
  idt.BENE_HMO_IND_06, -- HMO Indicator VI
  idt.BENE_HMO_IND_07, -- HMO Indicator VII
  idt.BENE_HMO_IND_08, -- HMO Indicator VIII
  idt.BENE_HMO_IND_09, -- HMO Indicator IX
  idt.BENE_HMO_IND_10, -- HMO Indicator X
  idt.BENE_HMO_IND_11, -- HMO Indicator XI
  idt.BENE_HMO_IND_12, -- HMO Indicator XII
  idt.EXTRACT_DT
from mbsf_ab_summary idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".maxdata_ot
select /*+ PARALLEL(maxdata_ot,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 CCW Beneficiary ID
  mm.MSIS_ID_DEID MSIS_ID, -- Encrypted MSIS Identification Number
  idt.STATE_CD, -- State
  idt.YR_NUM, -- Year of MAX Record
  case
    when coalesce(bm.dob_shift_months, mp.dob_shift_months) is not null
    then add_months(EL_DOB, coalesce(bm.dob_shift_months, mp.dob_shift_months))
    else idt.EL_DOB + coalesce(bm.date_shift_days, mp.date_shift_days)
  end EL_DOB, -- Birth date
  idt.EL_SEX_CD, -- Sex
  idt.EL_RACE_ETHNCY_CD, -- Race/ethnicity (from MSIS)
  idt.RACE_CODE_1, -- Race - White (from MSIS)
  idt.RACE_CODE_2, -- Race - Black (from MSIS)
  idt.RACE_CODE_3, -- Race - Am Indian/Alaskan (from MSIS)
  idt.RACE_CODE_4, -- Race - Asian (from MSIS)
  idt.RACE_CODE_5, -- Race - Hawaiian/Pac) Islands (from MSIS)
  idt.ETHNICITY_CODE, -- Ethnicity - Hispanic (from MSIS)
  idt.EL_SS_ELGBLTY_CD_LTST, -- State specific eligiblity - most recent
  idt.EL_SS_ELGBLTY_CD_MO, -- State specific eligiblity - mo of svc
  idt.EL_MAX_ELGBLTY_CD_LTST, -- MAX eligibility - most recent
  idt.EL_MAX_ELGBLTY_CD_MO, -- MAX eligibility - mo of svc
  idt.EL_MDCR_ANN_XOVR_OLD, -- Crossover code (Annual) old values
  idt.MSNG_ELG_DATA, -- Missing eligibility data
  idt.EL_MDCR_XOVR_CLM_BSD_CD, -- Crossover code (from claims only)
  idt.EL_MDCR_ANN_XOVR_99, -- Crossover code (Annual)
  idt.MSIS_TOS, -- MSIS Type of Service (TOS)
  idt.MSIS_TOP, -- MSIS Type of Program (TOP)
  idt.HCBS_TXNMY_WVR_CD, -- Home and community based services (HCBS) taxonomy code for w
  idt.MAX_TOS, -- MAX Type of Service (TOS)
  idt.CLTC_FLAG, -- Community-based LT care (CLTC) flag
  idt.PRVDR_ID_NMBR, -- Billing provider identification number
  idt.NPI, -- National Provider Identifier
  idt.TAXONOMY, -- Provider Taxonomy
  idt.TYPE_CLM_CD, -- Type of claim
  idt.ADJUST_CD, -- Adjustment code
  idt.PHP_TYPE, -- Managed care type of plan code
  idt.PHP_ID, -- Managed care plan identification code
  idt.MDCD_PYMT_AMT, -- Medicaid payment amount
  idt.TP_PYMT_AMT, -- Third party payment amount
  idt.PYMT_DT + coalesce(bm.date_shift_days, mp.date_shift_days) PYMT_DT, -- Payment/adjudication date
  idt.CHRG_AMT, -- Charge amount
  idt.PHP_VAL, -- Prepaid plan value
  idt.MDCR_COINSUR_PYMT_AMT, -- Medicare coinsurance payment amount
  idt.MDCR_DED_PYMT_AMT, -- Medicare deductible payment amount
  idt.SRVC_BGN_DT + coalesce(bm.date_shift_days, mp.date_shift_days) SRVC_BGN_DT, -- Beginning date of service
  idt.SRVC_END_DT + coalesce(bm.date_shift_days, mp.date_shift_days) SRVC_END_DT, -- Ending date of service
  idt.PRCDR_CD_SYS, -- Procedure (service) coding system
  idt.PRCDR_CD, -- Procedure (service) code
  idt.PRCDR_SRVC_MDFR_CD, -- Procedure (service) code modifier
  idt.DIAG_CD_1, -- Principle Diagnosis code
  idt.DIAG_CD_2, -- Diagnosis codes (2nd diagnosis)
  idt.QTY_SRVC_UNITS, -- Quantity of service
  idt.SRVC_PRVDR_ID_NMBR, -- Servicing provider identification number
  idt.SRVC_PRVDR_SPEC_CD, -- Servicing provider specialty code
  idt.PLC_OF_SRVC_CD, -- Place of service
  idt.UB_92_REV_CD, -- UB-92 revenue code
  idt.EXTRACT_DT
from maxdata_ot idt 
left join bene_id_mapping bm on bm.bene_id = idt.bene_id
join msis_id_mapping mm on mm.msis_id = idt.msis_id
join msis_person mp on mp.msis_id = idt.msis_id and mp.state_cd = idt.state_cd;
commit;


insert /*+ APPEND */ into "&&deid_schema".medpar_all
select /*+ PARALLEL(medpar_all,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.MEDPAR_ID, -- Unique Key for CCW MedPAR Table
  idt.MEDPAR_YR_NUM, -- Year of MedPAR Record
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.BENE_IDENT_CD, -- BIC reported on first claim included in stay
  idt.EQTBL_BIC_CD, -- Equated BIC
  case
    when BENE_AGE_CNT is null then null
    when bm.dob_shift_months is not null then
      case
        when BENE_AGE_CNT - round(bm.dob_shift_months/12) <= 89
        then BENE_AGE_CNT - round(bm.dob_shift_months/12)
        else 89
      end
    when BENE_AGE_CNT > 89 then 89
    else BENE_AGE_CNT
  end BENE_AGE_CNT, -- Age as of Date of Admission.
  idt.BENE_SEX_CD, -- Sex of Beneficiary
  idt.BENE_RACE_CD, -- Race of Beneficiary
  idt.BENE_MDCR_STUS_CD, -- Reason for entitlement to Medicare benefits as of CLM_THRU_DT
  idt.BENE_RSDNC_SSA_STATE_CD, -- SSA standard state code of a beneficiarys residence.
  NULL BENE_RSDNC_SSA_CNTY_CD, -- SSA standard county code of a beneficiarys residence.
  NULL BENE_MLG_CNTCT_ZIP_CD, -- Zip code of the mailing address where the beneficiary may be contacted.
  idt.BENE_DSCHRG_STUS_CD, -- Code identifying status of patient as of CLM_THRU_DT
  idt.FICARR_IDENT_NUM, -- Intermediary processor identification
  idt.WRNG_IND_CD, -- Warn ind spcfyng dtld billing info obtnd frm clms analyzd for stay prcss
  idt.GHO_PD_CD, -- Code indicating whether or not GHO has paid provider for claim(s)
  idt.PPS_IND_CD, -- Code indicating whether or not facility is being paid under PPS
  idt.ORG_NPI_NUM, -- Organization NPI Number
  idt.PRVDR_NUM, -- MEDPAR Provider Number
  idt.PRVDR_NUM_SPCL_UNIT_CD, -- Special num system code for hosp units that are PPS/SNF SB dsgntn excl.
  idt.SS_LS_SNF_IND_CD, -- Code indicating whether stay is short stay, long stay, or SNF
  idt.ACTV_XREF_IND, -- Active Cross-Refference Indicator
  idt.SLCT_RSN_CD, -- Specifies whether this record is a case or control record.
  idt.STAY_FINL_ACTN_CLM_CNT, -- Claims (final action) included in stay
  idt.LTST_CLM_ACRTN_DT + bm.date_shift_days LTST_CLM_ACRTN_DT, -- Date latest claim incl in stay accreted to bene mstr rec at the CWF host
  idt.BENE_MDCR_BNFT_EXHST_DT + bm.date_shift_days BENE_MDCR_BNFT_EXHST_DT, -- Last date beneficiary had Medicare coverage
  idt.SNF_QUALN_FROM_DT + bm.date_shift_days SNF_QUALN_FROM_DT, -- Beginning date of beneficiarys qualifying stay
  idt.SNF_QUALN_THRU_DT + bm.date_shift_days SNF_QUALN_THRU_DT, -- Ending date of beneficiarys qualifying stay
  idt.SRC_IP_ADMSN_CD, -- Admssn to an Inp facility or, for newborn admssn, type of delivery code
  idt.IP_ADMSN_TYPE_CD, -- Type and priority of benes admission to facility for Inp hosp stay code
  idt.ADMSN_DAY_CD, -- Code indicating day of week beneficiary was admitted to facility.
  idt.ADMSN_DT + bm.date_shift_days ADMSN_DT, -- Date beneficiary admitted for Inpatient care or date care started
  idt.DSCHRG_DT + bm.date_shift_days DSCHRG_DT, -- Date beneficiary was discharged or died
  idt.DSCHRG_DSTNTN_CD, -- Destination upon discharge from facility code
  idt.CVRD_LVL_CARE_THRU_DT + bm.date_shift_days CVRD_LVL_CARE_THRU_DT, -- Date covered level of care ended in a SNF
  idt.BENE_DEATH_DT + bm.date_shift_days BENE_DEATH_DT, -- Date beneficiary died
  idt.BENE_DEATH_DT_VRFY_CD, -- Death Date Verification Code
  idt.ADMSN_DEATH_DAY_CNT, -- Days from date admitted to facility to date of death
  idt.INTRNL_USE_SSI_IND_CD, -- MEDPAR Internal Use SSI Indicator Code
  idt.INTRNL_USE_SSI_DAY_CNT, -- MEDPAR Internal Use SSI Day Count
  idt.INTRNL_USE_SSI_DATA, -- Internal Use SSI Data
  idt.INTRNL_USE_IPSB_CD, -- For internal Use Only. IPSB Code
  idt.INTRNL_USE_FIL_DT_CD, -- For internal use only.  Fiscal year/calendar year segments.
  idt.INTRNL_USE_SMPL_SIZE_CD, -- For internal use.  MEDPAR sample size.
  idt.LOS_DAY_CNT, -- Days of beneficiarys stay in a hospital/SNF
  idt.OUTLIER_DAY_CNT, -- Days paid as outliers (either day or cost) under PPS beyond DRG threshld
  idt.UTLZTN_DAY_CNT, -- Covered days of care chargeable to Medicare utilization for stay
  idt.TOT_COINSRNC_DAY_CNT, -- MEDPAR Beneficiary Total Coinsurance Day Count
  idt.BENE_LRD_USE_CNT, -- Lifetime reserve days (LRD) used by beneficiary for stay
  idt.BENE_PTA_COINSRNC_AMT, -- Beneficiarys liability for part A coinsurance for stay ($)
  idt.BENE_IP_DDCTBL_AMT, -- Beneficiarys liability for stay ($)
  idt.BENE_BLOOD_DDCTBL_AMT, -- Beneficiarys liability for blood deductible for stay ($)
  idt.BENE_PRMRY_PYR_CD, -- Primary payer responsibility code
  idt.BENE_PRMRY_PYR_AMT, -- Primry payer other than Medicare for covered Medicare chrgs for stay ($)
  idt.DRG_CD, -- DRG Code
  idt.DRG_OUTLIER_STAY_CD, -- Cost or Day Outlier code
  idt.DRG_OUTLIER_PMT_AMT, -- Addtnl approved due to outlier situation over DRG allowance for stay ($)
  idt.DRG_PRICE_AMT, -- Wld hv bn pd if no dedctbls,coinsrnc,prmry payrs,otlrs were invlvd ($)
  idt.IP_DSPRPRTNT_SHR_AMT, -- Over the DRG amount for disproportionate share hospital for stay ($)
  idt.IME_AMT, -- Additional payment made to teaching hospitals for IME for stay ($)
  idt.PASS_THRU_AMT, -- Total of all claim pass thru for stay ($)
  idt.TOT_PPS_CPTL_AMT, -- Total payable for capital PPS ($)
  idt.IP_LOW_VOL_PYMT_AMT, -- Inpatient Low Volume Payment Amount.
  idt.TOT_CHRG_AMT, -- Total all charges for all srvcs provided to beneficiary for stay ($)
  idt.TOT_CVR_CHRG_AMT, -- Portion of total charges covered by Medicare for stay ($)
  idt.MDCR_PMT_AMT, -- Amt of payment from Medicare trust fund for srvcs covered by claim ($)
  idt.ACMDTNS_TOT_CHRG_AMT, -- Total charge for all accommodations related to beneficiarys stay ($)
  idt.DPRTMNTL_TOT_CHRG_AMT, -- Total charge for all ancillary depts related to beneficiarys stay ($)
  idt.PRVT_ROOM_DAY_CNT, -- Private room days used by beneficiary for stay
  idt.SEMIPRVT_ROOM_DAY_CNT, -- Semi-private room days used by beneficiary for stay
  idt.WARD_DAY_CNT, -- Ward days used by beneficiary for stay
  idt.INTNSV_CARE_DAY_CNT, -- Intensive care days used by beneficiary for stay
  idt.CRNRY_CARE_DAY_CNT, -- Coronary care days used by beneficiary for stay
  idt.PRVT_ROOM_CHRG_AMT, -- Private room accommodations related to beneficiarys stay ($)
  idt.SEMIPRVT_ROOM_CHRG_AMT, -- Semi-private room accommodations related to beneficiarys stay ($)
  idt.WARD_CHRG_AMT, -- Ward accommodations related to beneficiarys stay ($)
  idt.INTNSV_CARE_CHRG_AMT, -- Intensive care accommodations related to beneficiarys stay ($)
  idt.CRNRY_CARE_CHRG_AMT, -- Coronary care accommodations related to beneficiarys stay ($)
  idt.OTHR_SRVC_CHRG_AMT, -- Other services related to beneficiarys stay ($)
  idt.PHRMCY_CHRG_AMT, -- Pharmaceutical costs related to beneficiarys stay ($)
  idt.MDCL_SUPLY_CHRG_AMT, -- Medical/surgical supplies related to beneficiarys stay ($)
  idt.DME_CHRG_AMT, -- DME related to beneficiarys stay ($)
  idt.USED_DME_CHRG_AMT, -- Used DME related to beneficiarys stay ($)
  idt.PHYS_THRPY_CHRG_AMT, -- Physical therapy services provided during beneficiarys stay ($)
  idt.OCPTNL_THRPY_CHRG_AMT, -- Occupational therapy services provided during beneficiarys stay ($)
  idt.SPCH_PTHLGY_CHRG_AMT, -- Speech pathology services provided during beneficiarys stay ($)
  idt.INHLTN_THRPY_CHRG_AMT, -- Inhalation therapy services provided during beneficiarys stay ($)
  idt.BLOOD_CHRG_AMT, -- Blood provided during beneficiarys stay ($)
  idt.BLOOD_ADMIN_CHRG_AMT, -- Blood storage and processing related to beneficiarys stay ($)
  idt.BLOOD_PT_FRNSH_QTY, -- Quantity of blood (whole pints) furnished to beneficiary during stay
  idt.OPRTG_ROOM_CHRG_AMT, -- OR, recovery rm, and labor rm delivery used by bene during stay ($)
  idt.LTHTRPSY_CHRG_AMT, -- Lithotripsy services provided during beneficiarys stay ($)
  idt.CRDLGY_CHRG_AMT, -- Cardiology services and ECG(s) provided during beneficiarys stay ($)
  idt.ANSTHSA_CHRG_AMT, -- Anesthesia services provided during beneficiarys stay ($)
  idt.LAB_CHRG_AMT, -- Laboratory costs related to beneficiarys stay ($)
  idt.RDLGY_CHRG_AMT, -- Radiology costs (excluding MRI) related to a beneficiarys stay ($)
  idt.MRI_CHRG_AMT, -- MRI services provided during beneficiarys stay ($)
  idt.OP_SRVC_CHRG_AMT, -- Outpatient services provided during beneficiarys stay ($)
  idt.ER_CHRG_AMT, -- Emergency room services provided during beneficiarys stay ($)
  idt.AMBLNC_CHRG_AMT, -- Ambulance services related to beneficiarys stay ($)
  idt.PROFNL_FEES_CHRG_AMT, -- Professional fees related to beneficiarys stay ($)
  idt.ORGN_ACQSTN_CHRG_AMT, -- Organ acquisition or oth donor bank srvcs related to benes stay ($)
  idt.ESRD_REV_SETG_CHRG_AMT, -- ESRD services related to beneficiarys stay ($)
  idt.CLNC_VISIT_CHRG_AMT, -- Clinic visits related to beneficiarys stay ($)
  idt.ICU_IND_CD, -- ICU type code
  idt.CRNRY_CARE_IND_CD, -- Coronary care unit type code
  idt.PHRMCY_IND_CD, -- Drugs type code
  idt.TRNSPLNT_IND_CD, -- Organ transplant code
  idt.RDLGY_ONCLGY_IND_SW, -- Radiology oncology services indicator
  idt.RDLGY_DGNSTC_IND_SW, -- Radiology diagnostic services indicator
  idt.RDLGY_THRPTC_IND_SW, -- Radiology therapeutic services indicator
  idt.RDLGY_NUCLR_MDCN_IND_SW, -- Radiology nuclear medicine services indicator
  idt.RDLGY_CT_SCAN_IND_SW, -- Radiology computed tomographic (CT) scan services indicator
  idt.RDLGY_OTHR_IMGNG_IND_SW, -- Radiology other imaging services indicator
  idt.OP_SRVC_IND_CD, -- Outpatient services/ambulatory surgical care code
  idt.ORGN_ACQSTN_IND_CD, -- Organ acquisition type code
  idt.ESRD_COND_CD, -- ESRD condition code
  idt.ESRD_SETG_IND_1_CD, -- Dialysis type code I
  idt.ESRD_SETG_IND_2_CD, -- Dialysis type code II
  idt.ESRD_SETG_IND_3_CD, -- Dialysis type code III
  idt.ESRD_SETG_IND_4_CD, -- Dialysis type code IV
  idt.ESRD_SETG_IND_5_CD, -- Dialysis type code V
  idt.ADMTG_DGNS_CD, -- Initial diagnosis at time of admission
  idt.ADMTG_DGNS_VRSN_CD, -- MEDPAR Admitting Diagnosis Version Code
  idt.DGNS_CD_CNT, -- Diagnosis codes included in stay
  idt.DGNS_VRSN_CD, -- Version Code - Indicate if diagnosis code is ICD-9 or ICD-10 (Earlier Version)
  idt.DGNS_VRSN_CD_1, -- Version Code 01 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_2, -- Version Code 02 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_3, -- Version Code 03 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_4, -- Version Code 04 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_5, -- Version Code 05 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_6, -- Version Code 06 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_7, -- Version Code 07 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_8, -- Version Code 08 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_9, -- Version Code 09 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_10, -- Version Code 10 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_11, -- Version Code 11 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_12, -- Version Code 12 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_13, -- Version Code 13 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_14, -- Version Code 14 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_15, -- Version Code 15 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_16, -- Version Code 16 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_17, -- Version Code 17 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_18, -- Version Code 18 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_19, -- Version Code 19 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_20, -- Version Code 20 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_21, -- Version Code 21 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_22, -- Version Code 22 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_23, -- Version Code 23 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_24, -- Version Code 24 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_VRSN_CD_25, -- Version Code 25 - Indicate if diagnosis code is ICD-9 or ICD-10.
  idt.DGNS_1_CD, -- Primary ICD-9-CM code
  idt.DGNS_2_CD, -- ICD-9-CM Diagnosis code II
  idt.DGNS_3_CD, -- ICD-9-CM Diagnosis code III
  idt.DGNS_4_CD, -- ICD-9-CM Diagnosis code IV
  idt.DGNS_5_CD, -- ICD-9-CM Diagnosis code V
  idt.DGNS_6_CD, -- ICD-9-CM Diagnosis code VI
  idt.DGNS_7_CD, -- ICD-9-CM Diagnosis code VII
  idt.DGNS_8_CD, -- ICD-9-CM Diagnosis code VIII
  idt.DGNS_9_CD, -- ICD-9-CM Diagnosis code IX
  idt.DGNS_10_CD, -- ICD-9-CM Diagnosis code X
  idt.DGNS_11_CD, -- ICD-9-CM Diagnosis code XI
  idt.DGNS_12_CD, -- ICD-9-CM Diagnosis code XII
  idt.DGNS_13_CD, -- ICD-9-CM Diagnosis code XIII
  idt.DGNS_14_CD, -- ICD-9-CM Diagnosis code XIV
  idt.DGNS_15_CD, -- ICD-9-CM Diagnosis code XV
  idt.DGNS_16_CD, -- ICD-9-CM Diagnosis code XVI
  idt.DGNS_17_CD, -- ICD-9-CM Diagnosis code XVII
  idt.DGNS_18_CD, -- ICD-9-CM Diagnosis code XVIII
  idt.DGNS_19_CD, -- ICD-9-CM Diagnosis code XIX
  idt.DGNS_20_CD, -- ICD-9-CM Diagnosis code XX
  idt.DGNS_21_CD, -- ICD-9-CM Diagnosis code XXI
  idt.DGNS_22_CD, -- ICD-9-CM Diagnosis code XXII
  idt.DGNS_23_CD, -- ICD-9-CM Diagnosis code XXIII
  idt.DGNS_24_CD, -- ICD-9-CM Diagnosis code XXIV
  idt.DGNS_25_CD, -- ICD-9-CM Diagnosis code XXV
  idt.DGNS_POA_CD, -- Diagnosis Code POA Array
  idt.POA_DGNS_CD_CNT, -- MEDPAR Claim Present on Admission Diagnosis Code Count
  idt.POA_DGNS_1_IND_CD, -- Diagnosis Present on Admission Indicator 1
  idt.POA_DGNS_2_IND_CD, -- Diagnosis Present on Admission Indicator 2
  idt.POA_DGNS_3_IND_CD, -- Diagnosis Present on Admission Indicator 3
  idt.POA_DGNS_4_IND_CD, -- Diagnosis Present on Admission Indicator 4
  idt.POA_DGNS_5_IND_CD, -- Diagnosis Present on Admission Indicator 5
  idt.POA_DGNS_6_IND_CD, -- Diagnosis Present on Admission Indicator 6
  idt.POA_DGNS_7_IND_CD, -- Diagnosis Present on Admission Indicator 7
  idt.POA_DGNS_8_IND_CD, -- Diagnosis Present on Admission Indicator 8
  idt.POA_DGNS_9_IND_CD, -- Diagnosis Present on Admission Indicator 9
  idt.POA_DGNS_10_IND_CD, -- Diagnosis Present on Admission Indicator 10
  idt.POA_DGNS_11_IND_CD, -- Diagnosis Present on Admission Indicator 11
  idt.POA_DGNS_12_IND_CD, -- Diagnosis Present on Admission Indicator 12
  idt.POA_DGNS_13_IND_CD, -- Diagnosis Present on Admission Indicator 13
  idt.POA_DGNS_14_IND_CD, -- Diagnosis Present on Admission Indicator 14
  idt.POA_DGNS_15_IND_CD, -- Diagnosis Present on Admission Indicator 15
  idt.POA_DGNS_16_IND_CD, -- Diagnosis Present on Admission Indicator 16
  idt.POA_DGNS_17_IND_CD, -- Diagnosis Present on Admission Indicator 17
  idt.POA_DGNS_18_IND_CD, -- Diagnosis Present on Admission Indicator 18
  idt.POA_DGNS_19_IND_CD, -- Diagnosis Present on Admission Indicator 19
  idt.POA_DGNS_20_IND_CD, -- Diagnosis Present on Admission Indicator 20
  idt.POA_DGNS_21_IND_CD, -- Diagnosis Present on Admission Indicator 21
  idt.POA_DGNS_22_IND_CD, -- Diagnosis Present on Admission Indicator 22
  idt.POA_DGNS_23_IND_CD, -- Diagnosis Present on Admission Indicator 23
  idt.POA_DGNS_24_IND_CD, -- Diagnosis Present on Admission Indicator 24
  idt.POA_DGNS_25_IND_CD, -- Diagnosis Present on Admission Indicator 25
  idt.DGNS_E_CD_CNT, -- MEDPAR Diagnosis E Code Count
  idt.DGNS_E_VRSN_CD, -- MEDPAR Diagnosis E Version Code (Earlier Version)
  idt.DGNS_E_VRSN_CD_1, -- MEDPAR Diagnosis E Version Code 01
  idt.DGNS_E_VRSN_CD_2, -- MEDPAR Diagnosis E Version Code 02
  idt.DGNS_E_VRSN_CD_3, -- MEDPAR Diagnosis E Version Code 03
  idt.DGNS_E_VRSN_CD_4, -- MEDPAR Diagnosis E Version Code 04
  idt.DGNS_E_VRSN_CD_5, -- MEDPAR Diagnosis E Version Code 05
  idt.DGNS_E_VRSN_CD_6, -- MEDPAR Diagnosis E Version Code 06
  idt.DGNS_E_VRSN_CD_7, -- MEDPAR Diagnosis E Version Code 07
  idt.DGNS_E_VRSN_CD_8, -- MEDPAR Diagnosis E Version Code 08
  idt.DGNS_E_VRSN_CD_9, -- MEDPAR Diagnosis E Version Code 09
  idt.DGNS_E_VRSN_CD_10, -- MEDPAR Diagnosis E Version Code 10
  idt.DGNS_E_VRSN_CD_11, -- MEDPAR Diagnosis E Version Code 11
  idt.DGNS_E_VRSN_CD_12, -- MEDPAR Diagnosis E Version Code 12
  idt.DGNS_E_1_CD, -- E Diagnosis Code  1 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_2_CD, -- E Diagnosis Code  2 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_3_CD, -- E Diagnosis Code  3 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_4_CD, -- E Diagnosis Code  4 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_5_CD, -- E Diagnosis Code  5 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_6_CD, -- E Diagnosis Code  6 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_7_CD, -- E Diagnosis Code  7 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_8_CD, -- E Diagnosis Code  8 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_9_CD, -- E Diagnosis Code  9 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_10_CD, -- E Diagnosis Code  10 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_11_CD, -- E Diagnosis Code  11 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_12_CD, -- E Diagnosis Code  12 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.POA_DGNS_E_CD_CNT, -- MEDPAR Claim Present on Admission Diagnosis E Code Count
  idt.POA_DGNS_E_1_IND_CD, -- Diagnosis E Code Present on Admission Indicator 1
  idt.POA_DGNS_E_2_IND_CD, -- Diagnosis E Code Present on Admission Indicator 2
  idt.POA_DGNS_E_3_IND_CD, -- Diagnosis E Code Present on Admission Indicator 3
  idt.POA_DGNS_E_4_IND_CD, -- Diagnosis E Code Present on Admission Indicator 4
  idt.POA_DGNS_E_5_IND_CD, -- Diagnosis E Code Present on Admission Indicator 5
  idt.POA_DGNS_E_6_IND_CD, -- Diagnosis E Code Present on Admission Indicator 6
  idt.POA_DGNS_E_7_IND_CD, -- Diagnosis E Code Present on Admission Indicator 7
  idt.POA_DGNS_E_8_IND_CD, -- Diagnosis E Code Present on Admission Indicator 8
  idt.POA_DGNS_E_9_IND_CD, -- Diagnosis E Code Present on Admission Indicator 9
  idt.POA_DGNS_E_10_IND_CD, -- Diagnosis E Code Present on Admission Indicator 10
  idt.POA_DGNS_E_11_IND_CD, -- Diagnosis E Code Present on Admission Indicator 11
  idt.POA_DGNS_E_12_IND_CD, -- Diagnosis E Code Present on Admission Indicator 12
  idt.SRGCL_PRCDR_IND_SW, -- Surgical procedures indicator
  idt.SRGCL_PRCDR_CD_CNT, -- Surgical procedure codes included in stay
  idt.SRGCL_PRCDR_VRSN_CD, -- MEDPAR Surgical Procedure Version Code  (Earlier Version)
  idt.SRGCL_PRCDR_VRSN_CD_1, -- MEDPAR Surgical Procedure Version Code 01
  idt.SRGCL_PRCDR_VRSN_CD_2, -- MEDPAR Surgical Procedure Version Code 02
  idt.SRGCL_PRCDR_VRSN_CD_3, -- MEDPAR Surgical Procedure Version Code 03
  idt.SRGCL_PRCDR_VRSN_CD_4, -- MEDPAR Surgical Procedure Version Code 04
  idt.SRGCL_PRCDR_VRSN_CD_5, -- MEDPAR Surgical Procedure Version Code 05
  idt.SRGCL_PRCDR_VRSN_CD_6, -- MEDPAR Surgical Procedure Version Code 06
  idt.SRGCL_PRCDR_VRSN_CD_7, -- MEDPAR Surgical Procedure Version Code 07
  idt.SRGCL_PRCDR_VRSN_CD_8, -- MEDPAR Surgical Procedure Version Code 08
  idt.SRGCL_PRCDR_VRSN_CD_9, -- MEDPAR Surgical Procedure Version Code 09
  idt.SRGCL_PRCDR_VRSN_CD_10, -- MEDPAR Surgical Procedure Version Code 10
  idt.SRGCL_PRCDR_VRSN_CD_11, -- MEDPAR Surgical Procedure Version Code 11
  idt.SRGCL_PRCDR_VRSN_CD_12, -- MEDPAR Surgical Procedure Version Code 12
  idt.SRGCL_PRCDR_VRSN_CD_13, -- MEDPAR Surgical Procedure Version Code 13
  idt.SRGCL_PRCDR_VRSN_CD_14, -- MEDPAR Surgical Procedure Version Code 14
  idt.SRGCL_PRCDR_VRSN_CD_15, -- MEDPAR Surgical Procedure Version Code 15
  idt.SRGCL_PRCDR_VRSN_CD_16, -- MEDPAR Surgical Procedure Version Code 16
  idt.SRGCL_PRCDR_VRSN_CD_17, -- MEDPAR Surgical Procedure Version Code 17
  idt.SRGCL_PRCDR_VRSN_CD_18, -- MEDPAR Surgical Procedure Version Code 18
  idt.SRGCL_PRCDR_VRSN_CD_19, -- MEDPAR Surgical Procedure Version Code 19
  idt.SRGCL_PRCDR_VRSN_CD_20, -- MEDPAR Surgical Procedure Version Code 20
  idt.SRGCL_PRCDR_VRSN_CD_21, -- MEDPAR Surgical Procedure Version Code 21
  idt.SRGCL_PRCDR_VRSN_CD_22, -- MEDPAR Surgical Procedure Version Code 22
  idt.SRGCL_PRCDR_VRSN_CD_23, -- MEDPAR Surgical Procedure Version Code 23
  idt.SRGCL_PRCDR_VRSN_CD_24, -- MEDPAR Surgical Procedure Version Code 24
  idt.SRGCL_PRCDR_VRSN_CD_25, -- MEDPAR Surgical Procedure Version Code 25
  idt.SRGCL_PRCDR_1_CD, -- Principal Procedure code
  idt.SRGCL_PRCDR_2_CD, -- Procedure Code II
  idt.SRGCL_PRCDR_3_CD, -- Procedure Code III
  idt.SRGCL_PRCDR_4_CD, -- Procedure Code IV
  idt.SRGCL_PRCDR_5_CD, -- Procedure Code V
  idt.SRGCL_PRCDR_6_CD, -- Procedure Code VI
  idt.SRGCL_PRCDR_7_CD, -- Procedure Code VII
  idt.SRGCL_PRCDR_8_CD, -- Procedure Code VIII
  idt.SRGCL_PRCDR_9_CD, -- Procedure Code IX
  idt.SRGCL_PRCDR_10_CD, -- Procedure Code X
  idt.SRGCL_PRCDR_11_CD, -- Procedure Code XI
  idt.SRGCL_PRCDR_12_CD, -- Procedure Code XII
  idt.SRGCL_PRCDR_13_CD, -- Procedure Code XIII
  idt.SRGCL_PRCDR_14_CD, -- Procedure Code XIV
  idt.SRGCL_PRCDR_15_CD, -- Procedure Code XV
  idt.SRGCL_PRCDR_16_CD, -- Procedure Code XVI
  idt.SRGCL_PRCDR_17_CD, -- Procedure Code XVII
  idt.SRGCL_PRCDR_18_CD, -- Procedure Code XVIII
  idt.SRGCL_PRCDR_19_CD, -- Procedure Code XIX
  idt.SRGCL_PRCDR_20_CD, -- Procedure Code XX
  idt.SRGCL_PRCDR_21_CD, -- Procedure Code XXI
  idt.SRGCL_PRCDR_22_CD, -- Procedure Code XXII
  idt.SRGCL_PRCDR_23_CD, -- Procedure Code XXIII
  idt.SRGCL_PRCDR_24_CD, -- Procedure Code XXIV
  idt.SRGCL_PRCDR_25_CD, -- Procedure Code XXV
  idt.SRGCL_PRCDR_DT_CNT, -- Dates associated with surgical procedures included in stay
  idt.SRGCL_PRCDR_PRFRM_1_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_1_DT, -- Principal Procedure Date
  idt.SRGCL_PRCDR_PRFRM_2_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_2_DT, -- Procedure Date II
  idt.SRGCL_PRCDR_PRFRM_3_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_3_DT, -- Procedure Date III
  idt.SRGCL_PRCDR_PRFRM_4_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_4_DT, -- Procedure Date IV
  idt.SRGCL_PRCDR_PRFRM_5_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_5_DT, -- Procedure Date V
  idt.SRGCL_PRCDR_PRFRM_6_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_6_DT, -- Procedure Date VI
  idt.SRGCL_PRCDR_PRFRM_7_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_7_DT, -- Procedure Date VII
  idt.SRGCL_PRCDR_PRFRM_8_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_8_DT, -- Procedure Date VIII
  idt.SRGCL_PRCDR_PRFRM_9_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_9_DT, -- Procedure Date IX
  idt.SRGCL_PRCDR_PRFRM_10_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_10_DT, -- Procedure Date X
  idt.SRGCL_PRCDR_PRFRM_11_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_11_DT, -- Procedure Date XI
  idt.SRGCL_PRCDR_PRFRM_12_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_12_DT, -- Procedure Date XII
  idt.SRGCL_PRCDR_PRFRM_13_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_13_DT, -- Procedure Date XIII
  idt.SRGCL_PRCDR_PRFRM_14_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_14_DT, -- Procedure Date XIV
  idt.SRGCL_PRCDR_PRFRM_15_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_15_DT, -- Procedure Date XV
  idt.SRGCL_PRCDR_PRFRM_16_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_16_DT, -- Procedure Date XVI
  idt.SRGCL_PRCDR_PRFRM_17_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_17_DT, -- Procedure Date XVII
  idt.SRGCL_PRCDR_PRFRM_18_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_18_DT, -- Procedure Date XVIII
  idt.SRGCL_PRCDR_PRFRM_19_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_19_DT, -- Procedure Date XIX
  idt.SRGCL_PRCDR_PRFRM_20_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_20_DT, -- Procedure Date XX
  idt.SRGCL_PRCDR_PRFRM_21_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_21_DT, -- Procedure Date XXI
  idt.SRGCL_PRCDR_PRFRM_22_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_22_DT, -- Procedure Date XXII
  idt.SRGCL_PRCDR_PRFRM_23_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_23_DT, -- Procedure Date XXIII
  idt.SRGCL_PRCDR_PRFRM_24_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_24_DT, -- Procedure Date XXIV
  idt.SRGCL_PRCDR_PRFRM_25_DT + bm.date_shift_days SRGCL_PRCDR_PRFRM_25_DT, -- Procedure Date XXV
  idt.CLM_PTNT_RLTNSHP_CD, -- Claim Patient Relationship Code
  idt.CARE_IMPRVMT_MODEL_1_CD, -- Care Improvement Model 1 Code
  idt.CARE_IMPRVMT_MODEL_2_CD, -- Care Improvement Model 2 Code
  idt.CARE_IMPRVMT_MODEL_3_CD, -- Care Improvement Model 3 Code
  idt.CARE_IMPRVMT_MODEL_4_CD, -- Care Improvement Model 4 Code
  idt.VBP_PRTCPNT_IND_CD, -- VBP Participant Indicator Code
  idt.HRR_PRTCPNT_IND_CD, -- HRR Participant Indicator Code
  idt.BNDLD_MODEL_DSCNT_PCT, -- Bundled Model Discount Percent
  idt.VBP_ADJSTMT_PCT, -- VBP Adjustment Percent
  idt.HRR_ADJSTMT_PCT, -- HRR Adjustment Percent
  idt.INFRMTL_ENCTR_IND_SW, -- Informational Encounter Indicator Switch
  idt.MA_TCHNG_IND_SW, -- MA Teaching Indicator Switch
  idt.PROD_RPLCMT_LIFECYC_SW, -- Prod Replacement Lifecycle Switch
  idt.PROD_RPLCMT_RCLL_SW, -- Prod Replacement Recall Switch
  idt.CRED_RCVD_RPLCD_DVC_SW, -- Credit Received Replaced Device Switch
  idt.OBSRVTN_SW, -- Observation Switch
  idt.NEW_TCHNLGY_ADD_ON_AMT, -- New Technology Add-On Amount
  idt.BASE_OPRTG_DRG_AMT, -- Base Operating DRG Amount
  idt.OPRTG_HSP_AMT, -- Operating Hospital Amount
  idt.MDCL_SRGCL_GNRL_AMT, -- Medical/Surgical General Amount
  idt.MDCL_SRGCL_NSTRL_AMT, -- Medical/Surgical Non-Sterile Amount
  idt.MDCL_SRGCL_STRL_AMT, -- Medical/Surgical Sterile Amount
  idt.TAKE_HOME_AMT, -- Take Home Amount
  idt.PRSTHTC_ORTHTC_AMT, -- Prosthetic Orthotic Amount
  idt.MDCL_SRGCL_PCMKR_AMT, -- Medical/Surgical Pacemaker Amount
  idt.INTRAOCULAR_LENS_AMT, -- Intraocular Lens Amount
  idt.OXYGN_TAKE_HOME_AMT, -- Oxygen Take Home Amount
  idt.OTHR_IMPLANTS_AMT, -- Other Implants Amount
  idt.OTHR_SUPLIES_DVC_AMT, -- Other Supplies Device Amount
  idt.INCDNT_RDLGY_AMT, -- Incident Radiology Amount
  idt.INCDNT_DGNSTC_SRVCS_AMT, -- Incident Diagnostic Services Amount
  idt.MDCL_SRGCL_DRSNG_AMT, -- Medical/Surgical Dressing Amount
  idt.INVSTGTNL_DVC_AMT, -- Investigational Device Amount
  idt.MDCL_SRGCL_MISC_AMT, -- Medical/Surgical Miscellaneous Amount
  idt.RDLGY_ONCOLOGY_AMT, -- Radiology/Oncology Amount
  idt.RDLGY_DGNSTC_AMT, -- Radiology Diagnostic Amount
  idt.RDLGY_THRPTC_AMT, -- Radiology Therapeutic Amount
  idt.RDLGY_NUCLR_MDCN_AMT, -- Radiology Nuclear Medicine Amount
  idt.RDLGY_CT_SCAN_AMT, -- Radiology CT Scan Amount
  idt.RDLGY_OTHR_IMGNG_AMT, -- Radiology Other Imaging Amount
  idt.OPRTG_ROOM_AMT, -- Operating Room Amount
  idt.OR_LABOR_DLVRY_AMT, -- O/R Labor Delivery Amount
  idt.CRDC_CATHRZTN_AMT, -- Cardiac Catheterization Amount
  idt.SQSTRTN_RDCTN_AMT, -- Sequestration Reduction Amount
  idt.UNCOMPD_CARE_PYMT_AMT, -- Uncompensated Care Payment Amount
  idt.BNDLD_ADJSTMT_AMT, -- Bundled Adjustment Amount
  idt.VBP_ADJSTMT_AMT, -- Hospital Value Based Purchasing (VBP) Amount
  idt.HRR_ADJSTMT_AMT, -- Hospital Readmission Reduction (HRR) Adjustment Amount
  idt.EHR_PYMT_ADJSTMT_AMT, -- Electronic Health Record (EHR) Payment Adjustment Amount
  idt.PPS_STD_VAL_PYMT_AMT, -- Claim PPS Standard Value Payment Amount
  idt.FINL_STD_AMT, -- Claim Final Standard Amount
  idt.IPPS_FLEX_PYMT_6_AMT, -- IPPS Flexible Payment Amount I
  idt.IPPS_FLEX_PYMT_7_AMT, -- IPPS Flexible Payment Amount II
  idt.PTNT_ADD_ON_PYMT_AMT, -- Revenue Center Patient/Initial Visit Add-On Amount
  idt.HAC_PGM_RDCTN_IND_SW, -- Hospital Acquired Conditions (HAC) Program Reduction Indicator Switch
  idt.PGM_RDCTN_IND_SW, -- Electronic Health Records (EHR) Program Reduction Indicator Switch
  idt.PA_IND_CD, -- Claim Prior Authorization Indicator Code
  idt.UNIQ_TRKNG_NUM, -- Claim Unique Tracking Number
  idt.STAY_2_IND_SW, -- Stay 2 Indicator Switch
  idt.EXTRACT_DT
from medpar_all idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".maxdata_ip
select /*+ PARALLEL(maxdata_ip,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 CCW Beneficiary ID
  mm.MSIS_ID_DEID MSIS_ID, -- Encrypted MSIS Identification Number
  idt.STATE_CD, -- State
  idt.YR_NUM, -- Year of MAX Record
  case
    when coalesce(bm.dob_shift_months, mp.dob_shift_months) is not null
    then add_months(EL_DOB, coalesce(bm.dob_shift_months, mp.dob_shift_months))
    else idt.EL_DOB + coalesce(bm.date_shift_days, mp.date_shift_days)
  end EL_DOB, -- Birth date
  idt.EL_SEX_CD, -- Sex
  idt.EL_RACE_ETHNCY_CD, -- Race/ethnicity (from MSIS)
  idt.RACE_CODE_1, -- Race - White (from MSIS)
  idt.RACE_CODE_2, -- Race - Black (from MSIS)
  idt.RACE_CODE_3, -- Race - Am Indian/Alaskan (from MSIS)
  idt.RACE_CODE_4, -- Race - Asian (from MSIS)
  idt.RACE_CODE_5, -- Race - Hawaiian/Pac) Islands (from MSIS)
  idt.ETHNICITY_CODE, -- Ethnicity - Hispanic (from MSIS)
  idt.EL_SS_ELGBLTY_CD_LTST, -- State specific eligiblity - most recent
  idt.EL_SS_ELGBLTY_CD_MO, -- State specific eligiblity - mo of svc
  idt.EL_MAX_ELGBLTY_CD_LTST, -- MAX eligibility - most recent
  idt.EL_MAX_ELGBLTY_CD_MO, -- MAX eligibility - mo of svc
  idt.EL_MDCR_ANN_XOVR_OLD, -- Crossover code (Annual) old values
  idt.MSNG_ELG_DATA, -- Missing eligibility data
  idt.EL_MDCR_XOVR_CLM_BSD_CD, -- Crossover code (from claims only)
  idt.EL_MDCR_ANN_XOVR_99, -- Crossover code (Annual)
  idt.MSIS_TOS, -- MSIS Type of Service (TOS)
  idt.MSIS_TOP, -- MSIS Type of Program (TOP)
  idt.MAX_TOS, -- MAX Type of Service (TOS)
  idt.PRVDR_ID_NMBR, -- Billing provider identification number
  idt.NPI, -- National Provider Identifier
  idt.TAXONOMY, -- Provider Taxonomy
  idt.TYPE_CLM_CD, -- Type of claim
  idt.ADJUST_CD, -- Adjustment code
  idt.PHP_TYPE, -- Managed care type of plan code
  idt.PHP_ID, -- Managed care plan identification code
  idt.MDCD_PYMT_AMT, -- Medicaid payment amount
  idt.TP_PYMT_AMT, -- Third party payment amount
  idt.PYMT_DT + coalesce(bm.date_shift_days, mp.date_shift_days) PYMT_DT, -- Payment/adjudication date
  idt.CHRG_AMT, -- Charge amount
  idt.PHP_VAL, -- Prepaid plan value
  idt.MDCR_COINSUR_PYMT_AMT, -- Medicare coinsurance payment amount
  idt.MDCR_DED_PYMT_AMT, -- Medicare deductible payment amount
  idt.ADMSN_DT + coalesce(bm.date_shift_days, mp.date_shift_days) ADMSN_DT, -- Admission date
  idt.SRVC_BGN_DT + coalesce(bm.date_shift_days, mp.date_shift_days) SRVC_BGN_DT, -- Beginning date of service
  idt.SRVC_END_DT + coalesce(bm.date_shift_days, mp.date_shift_days) SRVC_END_DT, -- Ending date of service
  idt.DIAG_CD_1, -- Principle Diagnosis code
  idt.DIAG_CD_2, -- Diagnosis codes (2nd diagnosis)
  idt.DIAG_CD_3, -- Diagnosis codes (3rd diagnosis)
  idt.DIAG_CD_4, -- Diagnosis codes (4th diagnosis)
  idt.DIAG_CD_5, -- Diagnosis codes (5th diagnosis)
  idt.DIAG_CD_6, -- Diagnosis codes (6th diagnosis)
  idt.DIAG_CD_7, -- Diagnosis codes (7th diagnosis)
  idt.DIAG_CD_8, -- Diagnosis codes (8th diagnosis)
  idt.DIAG_CD_9, -- Diagnosis codes (9th diagnosis)
  idt.PRNCPL_PRCDR_DT + coalesce(bm.date_shift_days, mp.date_shift_days) PRNCPL_PRCDR_DT, -- Principle procedure date
  idt.PRCDR_CD_SYS_1, -- Procedure code system- principal
  idt.PRCDR_CD_1, -- Principle procedure code
  idt.PRCDR_CD_SYS_2, -- Procedure code system (2nd procedure)
  idt.PRCDR_CD_2, -- Procedure code (2nd procedure)
  idt.PRCDR_CD_SYS_3, -- Procedure code system (3rd procedure)
  idt.PRCDR_CD_3, -- Procedure code (3rd procedure)
  idt.PRCDR_CD_SYS_4, -- Procedure code system (4th procedure)
  idt.PRCDR_CD_4, -- Procedure code (4th procedure)
  idt.PRCDR_CD_SYS_5, -- Procedure code system (5th procedure)
  idt.PRCDR_CD_5, -- Procedure code (5th procedure)
  idt.PRCDR_CD_SYS_6, -- Procedure code system (6th procedure)
  idt.PRCDR_CD_6, -- Procedure code (6th procedure)
  idt.RCPNT_DLVRY_CD, -- Delivery code
  idt.MDCD_CVRD_IP_DAYS, -- Medicaid covered inpatient days
  idt.PATIENT_STATUS_CD, -- Patient status
  idt.DRG_REL_GROUP_IND, -- Diagnosis Related Group (DRG) indicator
  idt.DRG_REL_GROUP, -- Diagnosis Related Group (DRG)
  idt.UB_92_REV_CD_GP_1, -- UB-92 revenue code (1st)
  idt.UB_92_REV_CD_CHGS_1, -- UB-92 revenue code charge (1st)
  idt.UB_92_REV_CD_UNITS_1, -- UB-92 revenue code units (1st)
  idt.UB_92_REV_CD_GP_2, -- UB-92 revenue code (2nd)
  idt.UB_92_REV_CD_CHGS_2, -- UB-92 revenue code charge (2nd)
  idt.UB_92_REV_CD_UNITS_2, -- UB-92 revenue code units (2nd)
  idt.UB_92_REV_CD_GP_3, -- UB-92 revenue code (3rd)
  idt.UB_92_REV_CD_CHGS_3, -- UB-92 revenue code charge (3rd)
  idt.UB_92_REV_CD_UNITS_3, -- UB-92 revenue code units (3rd)
  idt.UB_92_REV_CD_GP_4, -- UB-92 revenue code (4th)
  idt.UB_92_REV_CD_CHGS_4, -- UB-92 revenue code charge (4th)
  idt.UB_92_REV_CD_UNITS_4, -- UB-92 revenue code units (4th)
  idt.UB_92_REV_CD_GP_5, -- UB-92 revenue code (5th)
  idt.UB_92_REV_CD_CHGS_5, -- UB-92 revenue code charge (5th)
  idt.UB_92_REV_CD_UNITS_5, -- UB-92 revenue code units (5th)
  idt.UB_92_REV_CD_GP_6, -- UB-92 revenue code (6th)
  idt.UB_92_REV_CD_CHGS_6, -- UB-92 revenue code charge (6th)
  idt.UB_92_REV_CD_UNITS_6, -- UB-92 revenue code units (6th)
  idt.UB_92_REV_CD_GP_7, -- UB-92 revenue code (7th)
  idt.UB_92_REV_CD_CHGS_7, -- UB-92 revenue code charge (7th)
  idt.UB_92_REV_CD_UNITS_7, -- UB-92 revenue code units (7th)
  idt.UB_92_REV_CD_GP_8, -- UB-92 revenue code (8th)
  idt.UB_92_REV_CD_CHGS_8, -- UB-92 revenue code charge (8th)
  idt.UB_92_REV_CD_UNITS_8, -- UB-92 revenue code units (8th)
  idt.UB_92_REV_CD_GP_9, -- UB-92 revenue code (9th)
  idt.UB_92_REV_CD_CHGS_9, -- UB-92 revenue code charge (9th)
  idt.UB_92_REV_CD_UNITS_9, -- UB-92 revenue code units (9th)
  idt.UB_92_REV_CD_GP_10, -- UB-92 revenue code (10th)
  idt.UB_92_REV_CD_CHGS_10, -- UB-92 revenue code charge (10th)
  idt.UB_92_REV_CD_UNITS_10, -- UB-92 revenue code units (10th)
  idt.UB_92_REV_CD_GP_11, -- UB-92 revenue code (11th)
  idt.UB_92_REV_CD_CHGS_11, -- UB-92 revenue code charge (11th)
  idt.UB_92_REV_CD_UNITS_11, -- UB-92 revenue code units (11th)
  idt.UB_92_REV_CD_GP_12, -- UB-92 revenue code (12th)
  idt.UB_92_REV_CD_CHGS_12, -- UB-92 revenue code charge (12th)
  idt.UB_92_REV_CD_UNITS_12, -- UB-92 revenue code units (12th)
  idt.UB_92_REV_CD_GP_13, -- UB-92 revenue code (13th)
  idt.UB_92_REV_CD_CHGS_13, -- UB-92 revenue code charge (13th)
  idt.UB_92_REV_CD_UNITS_13, -- UB-92 revenue code units (13th)
  idt.UB_92_REV_CD_GP_14, -- UB-92 revenue code (14th)
  idt.UB_92_REV_CD_CHGS_14, -- UB-92 revenue code charge (14th)
  idt.UB_92_REV_CD_UNITS_14, -- UB-92 revenue code units (14th)
  idt.UB_92_REV_CD_GP_15, -- UB-92 revenue code (15th)
  idt.UB_92_REV_CD_CHGS_15, -- UB-92 revenue code charge (15th)
  idt.UB_92_REV_CD_UNITS_15, -- UB-92 revenue code units (15th)
  idt.UB_92_REV_CD_GP_16, -- UB-92 revenue code (16th)
  idt.UB_92_REV_CD_CHGS_16, -- UB-92 revenue code charge (16th)
  idt.UB_92_REV_CD_UNITS_16, -- UB-92 revenue code units (16th)
  idt.UB_92_REV_CD_GP_17, -- UB-92 revenue code (17th)
  idt.UB_92_REV_CD_CHGS_17, -- UB-92 revenue code charge (17th)
  idt.UB_92_REV_CD_UNITS_17, -- UB-92 revenue code units (17th)
  idt.UB_92_REV_CD_GP_18, -- UB-92 revenue code (18th)
  idt.UB_92_REV_CD_CHGS_18, -- UB-92 revenue code charge (18th)
  idt.UB_92_REV_CD_UNITS_18, -- UB-92 revenue code units (18th)
  idt.UB_92_REV_CD_GP_19, -- UB-92 revenue code (19th)
  idt.UB_92_REV_CD_CHGS_19, -- UB-92 revenue code charge (19th)
  idt.UB_92_REV_CD_UNITS_19, -- UB-92 revenue code units (19th)
  idt.UB_92_REV_CD_GP_20, -- UB-92 revenue code (20th)
  idt.UB_92_REV_CD_CHGS_20, -- UB-92 revenue code charge (20th)
  idt.UB_92_REV_CD_UNITS_20, -- UB-92 revenue code units (20th)
  idt.UB_92_REV_CD_GP_21, -- UB-92 revenue code (21st)
  idt.UB_92_REV_CD_CHGS_21, -- UB-92 revenue code charge (21st)
  idt.UB_92_REV_CD_UNITS_21, -- UB-92 revenue code units (21st)
  idt.UB_92_REV_CD_GP_22, -- UB-92 revenue code (22nd)
  idt.UB_92_REV_CD_CHGS_22, -- UB-92 revenue code charge (22nd)
  idt.UB_92_REV_CD_UNITS_22, -- UB-92 revenue code units (22nd)
  idt.UB_92_REV_CD_GP_23, -- UB-92 revenue code (23rd)
  idt.UB_92_REV_CD_CHGS_23, -- UB-92 revenue code charge (23rd)
  idt.UB_92_REV_CD_UNITS_23, -- UB-92 revenue code units (23rd)
  idt.EXTRACT_DT
from maxdata_ip idt 
left join bene_id_mapping bm on bm.bene_id = idt.bene_id
join msis_id_mapping mm on mm.msis_id = idt.msis_id
join msis_person mp on mp.msis_id = idt.msis_id and mp.state_cd = idt.state_cd;
commit;


insert /*+ APPEND */ into "&&deid_schema".hha_revenue_center
select /*+ PARALLEL(hha_revenue_center,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date  (Determines Year of Claim)
  idt.CLM_LINE_NUM, -- Claim Line Number
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.REV_CNTR, -- Revenue Center Code
  idt.REV_CNTR_DT + bm.date_shift_days REV_CNTR_DT, -- Revenue Center Date
  idt.REV_CNTR_1ST_ANSI_CD, -- Revenue Center 1st ANSI Code
  idt.REV_CNTR_APC_HIPPS_CD, -- Revenue Center APC/HIPPS
  idt.HCPCS_CD, -- Revenue Center Healthcare Common Procedure Coding System
  idt.HCPCS_1ST_MDFR_CD, -- Revenue Center HCPCS Initial Modifier Code
  idt.HCPCS_2ND_MDFR_CD, -- Revenue Center HCPCS Second Modifier Code
  idt.REV_CNTR_PMT_MTHD_IND_CD, -- Revenue Center Payment Method Indicator Code
  idt.REV_CNTR_UNIT_CNT, -- Revenue Center Unit Count
  idt.REV_CNTR_RATE_AMT, -- Revenue Center Rate Amount
  idt.REV_CNTR_PMT_AMT_AMT, -- Revenue Center Payment Amount Amount
  idt.REV_CNTR_TOT_CHRG_AMT, -- Revenue Center Total Charge Amount
  idt.REV_CNTR_NCVRD_CHRG_AMT, -- Revenue Center Non-Covered Charge Amount
  idt.REV_CNTR_DDCTBL_COINSRNC_CD, -- Revenue Center Deductible Coinsurance Code
  idt.REV_CNTR_STUS_IND_CD, -- Revenue Center Status Indicator Code
  idt.REV_CNTR_NDC_QTY, -- Revenue Center NDC Quantity
  idt.REV_CNTR_NDC_QTY_QLFR_CD, -- Revenue Center NDC Quantity Qualifier Code
  idt.RNDRNG_PHYSN_UPIN, -- Revenue Center Rendering Physician UPIN
  idt.RNDRNG_PHYSN_NPI, -- Revenue Center Rendering Physician NPI
  idt.EXTRACT_DT
from hha_revenue_center idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hospice_value_codes
select /*+ PARALLEL(hospice_value_codes,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.RLT_VAL_CD_SEQ, -- Claim Related Value Code Sequence
  idt.CLM_VAL_CD, -- Claim Value Code
  idt.CLM_VAL_AMT, -- Claim Value Amount
  idt.EXTRACT_DT
from hospice_value_codes idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hospice_condition_codes
select /*+ PARALLEL(hospice_condition_codes,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.RLT_COND_CD_SEQ, -- Claim Related Condition Code Sequence
  idt.CLM_RLT_COND_CD, -- Claim Related Condition Code
  idt.EXTRACT_DT
from hospice_condition_codes idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hospice_span_codes
select /*+ PARALLEL(hospice_span_codes,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.RLT_SPAN_CD_SEQ, -- Claim Related Span Code Sequence
  idt.CLM_SPAN_CD, -- Claim Occurrence Span Code
  idt.CLM_SPAN_FROM_DT + bm.date_shift_days CLM_SPAN_FROM_DT, -- Claim Occurrence Span From Date
  idt.CLM_SPAN_THRU_DT + bm.date_shift_days CLM_SPAN_THRU_DT, -- Claim Occurrence Span Through Date
  idt.EXTRACT_DT
from hospice_span_codes idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".maxdata_ps
select /*+ PARALLEL(maxdata_ps,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 CCW Beneficiary ID
  mm.MSIS_ID_DEID MSIS_ID, -- Encrypted MSIS Identification Number
  idt.STATE_CD, -- State
  idt.EL_STATE_CASE_NUM, -- Encrypted Case Number
  idt.MAX_YR_DT, -- Year
  idt.HGT_FLAG, -- SSN (from MSIS) High Group Test
  idt.EXT_SSN_SRCE, -- External SSN source
  case
    when coalesce(bm.dob_shift_months, mp.dob_shift_months) is not null
    then add_months(EL_DOB, coalesce(bm.dob_shift_months, mp.dob_shift_months))
    else idt.EL_DOB + coalesce(bm.date_shift_days, mp.date_shift_days)
  end EL_DOB, -- Date of birth
  idt.EL_AGE_GRP_CD, -- Age group
  idt.EL_SEX_CD, -- Sex
  idt.EL_RACE_ETHNCY_CD, -- Race/ethnicity (from MSIS)
  idt.RACE_CODE_1, -- Race - White (from MSIS)
  idt.RACE_CODE_2, -- Race - Black (from MSIS)
  idt.RACE_CODE_3, -- Race - Am Indian/Alaskan (from MSIS)
  idt.RACE_CODE_4, -- Race - Asian (from MSIS)
  idt.RACE_CODE_5, -- Race - Hawaiian/Pac) Islands (from MSIS)
  idt.ETHNICITY_CODE, -- Ethnicity - Hispanic (from MSIS)
  idt.MDCR_RACE_ETHNCY_CD, -- Race/ethnicity (from Medicare EDB)
  idt.MDCR_LANG_CD, -- Language code (from Medicare EDB)
  idt.EL_SEX_RACE_CD, -- Sex/race
  idt.EL_DOD + coalesce(bm.date_shift_days, mp.date_shift_days) EL_DOD, -- Date of death (from MSIS)
  idt.MDCR_DOD + coalesce(bm.date_shift_days, mp.date_shift_days) MDCR_DOD, -- Date of death (from Medicare EDB)
  idt.MDCR_DEATH_DAY_SW, -- Day of death verified (from Mcare EDB)
  NULL EL_RSDNC_CNTY_CD_LTST, -- County of residence
  NULL EL_RSDNC_ZIP_CD_LTST, -- Zip code of residence
  idt.EL_SS_ELGBLTY_CD_LTST, -- State specific eligiblity - most recent
  idt.EL_MAX_ELGBLTY_CD_LTST, -- MAX eligibility - most recent
  idt.MSNG_ELG_DATA, -- Missing eligibility data
  idt.EL_ELGBLTY_MO_CNT, -- Eligible months
  idt.EL_PRVT_INSRNC_MO_CNT, -- Private insurance months
  idt.EL_MDCR_ANN_XOVR_OLD, -- Crossover code (Old Annual)
  idt.EL_MDCR_QTR_XOVR_OLD_1, -- Quarterly crossover code (Old Quarter 1)
  idt.EL_MDCR_QTR_XOVR_OLD_2, -- Quarterly crossover code (Old Quarter 2)
  idt.EL_MDCR_QTR_XOVR_OLD_3, -- Quarterly crossover code (Old Quarter 3)
  idt.EL_MDCR_QTR_XOVR_OLD_4, -- Quarterly crossover code (Old Quarter 4)
  idt.EL_MDCR_DUAL_ANN, -- Medicare dual code (Annual)
  idt.EL_MDCR_QTR_XOVR_99_1, -- Quarterly crossover code (Quarter 1)
  idt.EL_MDCR_QTR_XOVR_99_2, -- Quarterly crossover code (Quarter 2)
  idt.EL_MDCR_QTR_XOVR_99_3, -- Quarterly crossover code (Quarter 3)
  idt.EL_MDCR_QTR_XOVR_99_4, -- Quarterly crossover code (Quarter 4)
  idt.EL_MDCR_BEN_MO_CNT, -- Medicare benefic mos (from Medicare EDB)
  idt.MDCR_ORIG_REAS_CD, -- Mcare orig entitlemt reason (from EDB)
  idt.EL_MDCR_DUAL_MO_1, -- Medicare dual code (Jan)
  idt.EL_MDCR_DUAL_MO_2, -- Medicare dual code (Feb)
  idt.EL_MDCR_DUAL_MO_3, -- Medicare dual code (Mar)
  idt.EL_MDCR_DUAL_MO_4, -- Medicare dual code (Apr)
  idt.EL_MDCR_DUAL_MO_5, -- Medicare dual code (May)
  idt.EL_MDCR_DUAL_MO_6, -- Medicare dual code (Jun)
  idt.EL_MDCR_DUAL_MO_7, -- Medicare dual code (Jul)
  idt.EL_MDCR_DUAL_MO_8, -- Medicare dual code (Aug)
  idt.EL_MDCR_DUAL_MO_9, -- Medicare dual code (Sep)
  idt.EL_MDCR_DUAL_MO_10, -- Medicare dual code (Oct)
  idt.EL_MDCR_DUAL_MO_11, -- Medicare dual code (Nov)
  idt.EL_MDCR_DUAL_MO_12, -- Medicare dual code (Dec)
  idt.SS_ELG_CD_MO_1, -- State specific eligibility group (Jan)
  idt.SS_ELG_CD_MO_2, -- State specific eligibility group (Feb)
  idt.SS_ELG_CD_MO_3, -- State specific eligibility group (Mar)
  idt.SS_ELG_CD_MO_4, -- State specific eligibility group (Apr)
  idt.SS_ELG_CD_MO_5, -- State specific eligibility group (May)
  idt.SS_ELG_CD_MO_6, -- State specific eligibility group (Jun)
  idt.SS_ELG_CD_MO_7, -- State specific eligibility group (Jul)
  idt.SS_ELG_CD_MO_8, -- State specific eligibility group (Aug)
  idt.SS_ELG_CD_MO_9, -- State specific eligibility group (Sep)
  idt.SS_ELG_CD_MO_10, -- State specific eligibility group (Oct)
  idt.SS_ELG_CD_MO_11, -- State specific eligibility group (Nov)
  idt.SS_ELG_CD_MO_12, -- State specific eligibility group (Dec)
  idt.MAX_ELG_CD_MO_1, -- MAX eligibility group (Jan)
  idt.MAX_ELG_CD_MO_2, -- MAX eligibility group (Feb)
  idt.MAX_ELG_CD_MO_3, -- MAX eligibility group (Mar)
  idt.MAX_ELG_CD_MO_4, -- MAX eligibility group (Apr)
  idt.MAX_ELG_CD_MO_5, -- MAX eligibility group (May)
  idt.MAX_ELG_CD_MO_6, -- MAX eligibility group (Jun)
  idt.MAX_ELG_CD_MO_7, -- MAX eligibility group (Jul)
  idt.MAX_ELG_CD_MO_8, -- MAX eligibility group (Aug)
  idt.MAX_ELG_CD_MO_9, -- MAX eligibility group (Sep)
  idt.MAX_ELG_CD_MO_10, -- MAX eligibility group (Oct)
  idt.MAX_ELG_CD_MO_11, -- MAX eligibility group (Nov)
  idt.MAX_ELG_CD_MO_12, -- MAX eligibility group (Dec)
  idt.EL_PVT_INS_CD_1, -- Private health insurance group (Jan)
  idt.EL_PVT_INS_CD_2, -- Private health insurance group (Feb)
  idt.EL_PVT_INS_CD_3, -- Private health insurance group (Mar)
  idt.EL_PVT_INS_CD_4, -- Private health insurance group (Apr)
  idt.EL_PVT_INS_CD_5, -- Private health insurance group (May)
  idt.EL_PVT_INS_CD_6, -- Private health insurance group (Jun)
  idt.EL_PVT_INS_CD_7, -- Private health insurance group (Jul)
  idt.EL_PVT_INS_CD_8, -- Private health insurance group (Aug)
  idt.EL_PVT_INS_CD_9, -- Private health insurance group (Sep)
  idt.EL_PVT_INS_CD_10, -- Private health insurance group (Oct)
  idt.EL_PVT_INS_CD_11, -- Private health insurance group (Nov)
  idt.EL_PVT_INS_CD_12, -- Private health insurance group (Dec)
  idt.EL_MDCR_BEN_MO_1, -- Medicare beneficiary (Jan)
  idt.EL_MDCR_BEN_MO_2, -- Medicare beneficiary (Feb)
  idt.EL_MDCR_BEN_MO_3, -- Medicare beneficiary (Mar)
  idt.EL_MDCR_BEN_MO_4, -- Medicare beneficiary (Apr)
  idt.EL_MDCR_BEN_MO_5, -- Medicare beneficiary (May)
  idt.EL_MDCR_BEN_MO_6, -- Medicare beneficiary (Jun)
  idt.EL_MDCR_BEN_MO_7, -- Medicare beneficiary (Jul)
  idt.EL_MDCR_BEN_MO_8, -- Medicare beneficiary (Aug)
  idt.EL_MDCR_BEN_MO_9, -- Medicare beneficiary (Sep)
  idt.EL_MDCR_BEN_MO_10, -- Medicare beneficiary (Oct)
  idt.EL_MDCR_BEN_MO_11, -- Medicare beneficiary (Nov)
  idt.EL_MDCR_BEN_MO_12, -- Medicare beneficiary (Dec)
  idt.EL_PPH_PLN_MO_CNT_CMCP, -- Prepaid plan months (comprehen plans)
  idt.EL_PPH_PLN_MO_CNT_DMCP, -- Prepaid plan months (DMCP)
  idt.EL_PPH_PLN_MO_CNT_BMCP, -- Prepaid plan months (BMCP)
  idt.EL_PPH_PLN_MO_CNT_PDMC, -- Prepaid plan months (PDMC)
  idt.EL_PPH_PLN_MO_CNT_LTCM, -- Prepaid plan months (LTCM)
  idt.EL_PPH_PLN_MO_CNT_AICE, -- Prepaid plan months (AICE)
  idt.EL_PPH_PLN_MO_CNT_PCCM, -- Prepaid plan months (PCCM)
  idt.EL_PHP_TYPE_1_1, -- Prepaid plan type-1 (Jan)
  idt.EL_PHP_ID_1_1, -- Prepaid plan identifier-1 (Jan)
  idt.EL_PHP_TYPE_2_1, -- Prepaid plan type-2 (Jan)
  idt.EL_PHP_ID_2_1, -- Prepaid plan identifier-2 (Jan)
  idt.EL_PHP_TYPE_3_1, -- Prepaid plan type-3 (Jan)
  idt.EL_PHP_ID_3_1, -- Prepaid plan identifier-3 (Jan)
  idt.EL_PHP_TYPE_4_1, -- Prepaid plan type-4 (Jan)
  idt.EL_PHP_ID_4_1, -- Prepaid plan identifier-4 (Jan)
  idt.EL_PHP_TYPE_1_2, -- Prepaid plan type-1 (Feb)
  idt.EL_PHP_ID_1_2, -- Prepaid plan identifier-1 (Feb)
  idt.EL_PHP_TYPE_2_2, -- Prepaid plan type-2 (Feb)
  idt.EL_PHP_ID_2_2, -- Prepaid plan identifier-2 (Feb)
  idt.EL_PHP_TYPE_3_2, -- Prepaid plan type-3 (Feb)
  idt.EL_PHP_ID_3_2, -- Prepaid plan identifier-3 (Feb)
  idt.EL_PHP_TYPE_4_2, -- Prepaid plan type-4 (Feb)
  idt.EL_PHP_ID_4_2, -- Prepaid plan identifier-4 (Feb)
  idt.EL_PHP_TYPE_1_3, -- Prepaid plan type-1 (Mar)
  idt.EL_PHP_ID_1_3, -- Prepaid plan identifier-1 (Mar)
  idt.EL_PHP_TYPE_2_3, -- Prepaid plan type-2 (Mar)
  idt.EL_PHP_ID_2_3, -- Prepaid plan identifier-2 (Mar)
  idt.EL_PHP_TYPE_3_3, -- Prepaid plan type-3 (Mar)
  idt.EL_PHP_ID_3_3, -- Prepaid plan identifier-3 (Mar)
  idt.EL_PHP_TYPE_4_3, -- Prepaid plan type-4 (Mar)
  idt.EL_PHP_ID_4_3, -- Prepaid plan identifier-4 (Mar)
  idt.EL_PHP_TYPE_1_4, -- Prepaid plan type-1 (Apr)
  idt.EL_PHP_ID_1_4, -- Prepaid plan identifier-1 (Apr)
  idt.EL_PHP_TYPE_2_4, -- Prepaid plan type-2 (Apr)
  idt.EL_PHP_ID_2_4, -- Prepaid plan identifier-2 (Apr)
  idt.EL_PHP_TYPE_3_4, -- Prepaid plan type-3 (Apr)
  idt.EL_PHP_ID_3_4, -- Prepaid plan identifier-3 (Apr)
  idt.EL_PHP_TYPE_4_4, -- Prepaid plan type-4 (Apr)
  idt.EL_PHP_ID_4_4, -- Prepaid plan identifier-4 (Apr)
  idt.EL_PHP_TYPE_1_5, -- Prepaid plan type-1 (May)
  idt.EL_PHP_ID_1_5, -- Prepaid plan identifier-1 (May)
  idt.EL_PHP_TYPE_2_5, -- Prepaid plan type-2 (May)
  idt.EL_PHP_ID_2_5, -- Prepaid plan identifier-2 (May)
  idt.EL_PHP_TYPE_3_5, -- Prepaid plan type-3 (May)
  idt.EL_PHP_ID_3_5, -- Prepaid plan identifier-3 (May)
  idt.EL_PHP_TYPE_4_5, -- Prepaid plan type-4 (May)
  idt.EL_PHP_ID_4_5, -- Prepaid plan identifier-4 (May)
  idt.EL_PHP_TYPE_1_6, -- Prepaid plan type-1 (Jun)
  idt.EL_PHP_ID_1_6, -- Prepaid plan identifier-1 (Jun)
  idt.EL_PHP_TYPE_2_6, -- Prepaid plan type-2 (Jun)
  idt.EL_PHP_ID_2_6, -- Prepaid plan identifier-2 (Jun)
  idt.EL_PHP_TYPE_3_6, -- Prepaid plan type-3 (Jun)
  idt.EL_PHP_ID_3_6, -- Prepaid plan identifier-3 (Jun)
  idt.EL_PHP_TYPE_4_6, -- Prepaid plan type-4 (Jun)
  idt.EL_PHP_ID_4_6, -- Prepaid plan identifier-4 (Jun)
  idt.EL_PHP_TYPE_1_7, -- Prepaid plan type-1 (Jul)
  idt.EL_PHP_ID_1_7, -- Prepaid plan identifier-1 (Jul)
  idt.EL_PHP_TYPE_2_7, -- Prepaid plan type-2 (Jul)
  idt.EL_PHP_ID_2_7, -- Prepaid plan identifier-2 (Jul)
  idt.EL_PHP_TYPE_3_7, -- Prepaid plan type-3 (Jul)
  idt.EL_PHP_ID_3_7, -- Prepaid plan identifier-3 (Jul)
  idt.EL_PHP_TYPE_4_7, -- Prepaid plan type-4 (Jul)
  idt.EL_PHP_ID_4_7, -- Prepaid plan identifier-4 (Jul)
  idt.EL_PHP_TYPE_1_8, -- Prepaid plan type-1 (Aug)
  idt.EL_PHP_ID_1_8, -- Prepaid plan identifier-1 (Aug)
  idt.EL_PHP_TYPE_2_8, -- Prepaid plan type-2 (Aug)
  idt.EL_PHP_ID_2_8, -- Prepaid plan identifier-2 (Aug)
  idt.EL_PHP_TYPE_3_8, -- Prepaid plan type-3 (Aug)
  idt.EL_PHP_ID_3_8, -- Prepaid plan identifier-3 (Aug)
  idt.EL_PHP_TYPE_4_8, -- Prepaid plan type-4 (Aug)
  idt.EL_PHP_ID_4_8, -- Prepaid plan identifier-4 (Aug)
  idt.EL_PHP_TYPE_1_9, -- Prepaid plan type-1 (Sep)
  idt.EL_PHP_ID_1_9, -- Prepaid plan identifier-1 (Sep)
  idt.EL_PHP_TYPE_2_9, -- Prepaid plan type-2 (Sep)
  idt.EL_PHP_ID_2_9, -- Prepaid plan identifier-2 (Sep)
  idt.EL_PHP_TYPE_3_9, -- Prepaid plan type-3 (Sep)
  idt.EL_PHP_ID_3_9, -- Prepaid plan identifier-3 (Sep)
  idt.EL_PHP_TYPE_4_9, -- Prepaid plan type-4 (Sep)
  idt.EL_PHP_ID_4_9, -- Prepaid plan identifier-4 (Sep)
  idt.EL_PHP_TYPE_1_10, -- Prepaid plan type-1 (Oct)
  idt.EL_PHP_ID_1_10, -- Prepaid plan identifier-1 (Oct)
  idt.EL_PHP_TYPE_2_10, -- Prepaid plan type-2 (Oct)
  idt.EL_PHP_ID_2_10, -- Prepaid plan identifier-2 (Oct)
  idt.EL_PHP_TYPE_3_10, -- Prepaid plan type-3 (Oct)
  idt.EL_PHP_ID_3_10, -- Prepaid plan identifier-3 (Oct)
  idt.EL_PHP_TYPE_4_10, -- Prepaid plan type-4 (Oct)
  idt.EL_PHP_ID_4_10, -- Prepaid plan identifier-4 (Oct)
  idt.EL_PHP_TYPE_1_11, -- Prepaid plan type-1 (Nov)
  idt.EL_PHP_ID_1_11, -- Prepaid plan identifier-1 (Nov)
  idt.EL_PHP_TYPE_2_11, -- Prepaid plan type-2 (Nov)
  idt.EL_PHP_ID_2_11, -- Prepaid plan identifier-2 (Nov)
  idt.EL_PHP_TYPE_3_11, -- Prepaid plan type-3 (Nov)
  idt.EL_PHP_ID_3_11, -- Prepaid plan identifier-3 (Nov)
  idt.EL_PHP_TYPE_4_11, -- Prepaid plan type-4 (Nov)
  idt.EL_PHP_ID_4_11, -- Prepaid plan identifier-4 (Nov)
  idt.EL_PHP_TYPE_1_12, -- Prepaid plan type-1 (Dec)
  idt.EL_PHP_ID_1_12, -- Prepaid plan identifier-1 (Dec)
  idt.EL_PHP_TYPE_2_12, -- Prepaid plan type-2 (Dec)
  idt.EL_PHP_ID_2_12, -- Prepaid plan identifier-2 (Dec)
  idt.EL_PHP_TYPE_3_12, -- Prepaid plan type-3 (Dec)
  idt.EL_PHP_ID_3_12, -- Prepaid plan identifier-3 (Dec)
  idt.EL_PHP_TYPE_4_12, -- Prepaid plan type-4 (Dec)
  idt.EL_PHP_ID_4_12, -- Prepaid plan identifier-4 (Dec)
  idt.MC_COMBO_MO_1, -- Managed care combinations (Jan)
  idt.MC_COMBO_MO_2, -- Managed care combinations (Feb)
  idt.MC_COMBO_MO_3, -- Managed care combinations (Mar)
  idt.MC_COMBO_MO_4, -- Managed care combinations (Apr)
  idt.MC_COMBO_MO_5, -- Managed care combinations (May)
  idt.MC_COMBO_MO_6, -- Managed care combinations (Jun)
  idt.MC_COMBO_MO_7, -- Managed care combinations (Jul)
  idt.MC_COMBO_MO_8, -- Managed care combinations (Aug)
  idt.MC_COMBO_MO_9, -- Managed care combinations (Sep)
  idt.MC_COMBO_MO_10, -- Managed care combinations (Oct)
  idt.MC_COMBO_MO_11, -- Managed care combinations (Nov)
  idt.MC_COMBO_MO_12, -- Managed care combinations (Dec)
  idt.EL_DAYS_EL_CNT_1, -- Days of eligibility (Jan)
  idt.EL_DAYS_EL_CNT_2, -- Days of eligibility (Feb)
  idt.EL_DAYS_EL_CNT_3, -- Days of eligibility (Mar)
  idt.EL_DAYS_EL_CNT_4, -- Days of eligibility (Apr)
  idt.EL_DAYS_EL_CNT_5, -- Days of eligibility (May)
  idt.EL_DAYS_EL_CNT_6, -- Days of eligibility (Jun)
  idt.EL_DAYS_EL_CNT_7, -- Days of eligibility (Jul)
  idt.EL_DAYS_EL_CNT_8, -- Days of eligibility (Aug)
  idt.EL_DAYS_EL_CNT_9, -- Days of eligibility (Sep)
  idt.EL_DAYS_EL_CNT_10, -- Days of eligibility (Oct)
  idt.EL_DAYS_EL_CNT_11, -- Days of eligibility (Nov)
  idt.EL_DAYS_EL_CNT_12, -- Days of eligibility (Dec)
  idt.EL_TANF_CASH_FLG_1, -- TANF cash eligibility (Jan)
  idt.EL_TANF_CASH_FLG_2, -- TANF cash eligibility (Feb)
  idt.EL_TANF_CASH_FLG_3, -- TANF cash eligibility (Mar)
  idt.EL_TANF_CASH_FLG_4, -- TANF cash eligibility (Apr)
  idt.EL_TANF_CASH_FLG_5, -- TANF cash eligibility (May)
  idt.EL_TANF_CASH_FLG_6, -- TANF cash eligibility (Jun)
  idt.EL_TANF_CASH_FLG_7, -- TANF cash eligibility (Jul)
  idt.EL_TANF_CASH_FLG_8, -- TANF cash eligibility (Aug)
  idt.EL_TANF_CASH_FLG_9, -- TANF cash eligibility (Sep)
  idt.EL_TANF_CASH_FLG_10, -- TANF cash eligibility (Oct)
  idt.EL_TANF_CASH_FLG_11, -- TANF cash eligibility (Nov)
  idt.EL_TANF_CASH_FLG_12, -- TANF cash eligibility (Dec)
  idt.EL_RSTRCT_BNFT_FLG_1, -- Restricted benefits (Jan)
  idt.EL_RSTRCT_BNFT_FLG_2, -- Restricted benefits (Feb)
  idt.EL_RSTRCT_BNFT_FLG_3, -- Restricted benefits (Mar)
  idt.EL_RSTRCT_BNFT_FLG_4, -- Restricted benefits (Apr)
  idt.EL_RSTRCT_BNFT_FLG_5, -- Restricted benefits (May)
  idt.EL_RSTRCT_BNFT_FLG_6, -- Restricted benefits (Jun)
  idt.EL_RSTRCT_BNFT_FLG_7, -- Restricted benefits (Jul)
  idt.EL_RSTRCT_BNFT_FLG_8, -- Restricted benefits (Aug)
  idt.EL_RSTRCT_BNFT_FLG_9, -- Restricted benefits (Sep)
  idt.EL_RSTRCT_BNFT_FLG_10, -- Restricted benefits (Oct)
  idt.EL_RSTRCT_BNFT_FLG_11, -- Restricted benefits (Nov)
  idt.EL_RSTRCT_BNFT_FLG_12, -- Restricted benefits (Dec)
  idt.EL_CHIP_FLAG_1, -- SCHIP eligibility (Jan)
  idt.EL_CHIP_FLAG_2, -- SCHIP eligibility (Feb)
  idt.EL_CHIP_FLAG_3, -- SCHIP eligibility (Mar)
  idt.EL_CHIP_FLAG_4, -- SCHIP eligibility (Apr)
  idt.EL_CHIP_FLAG_5, -- SCHIP eligibility (May)
  idt.EL_CHIP_FLAG_6, -- SCHIP eligibility (Jun)
  idt.EL_CHIP_FLAG_7, -- SCHIP eligibility (Jul)
  idt.EL_CHIP_FLAG_8, -- SCHIP eligibility (Aug)
  idt.EL_CHIP_FLAG_9, -- SCHIP eligibility (Sep)
  idt.EL_CHIP_FLAG_10, -- SCHIP eligibility (Oct)
  idt.EL_CHIP_FLAG_11, -- SCHIP eligibility (Nov)
  idt.EL_CHIP_FLAG_12, -- SCHIP eligibility (Dec)
  idt.MAX_WAIVER_TYPE_1_MO_1, -- MAX Waiver Type Code -1 (Jan)
  idt.MAX_WAIVER_ID_1_MO_1, -- Waiver ID-1 (Jan)
  idt.MAX_WAIVER_TYPE_2_MO_1, -- MAX Waiver Type Code -2 (Jan)
  idt.MAX_WAIVER_ID_2_MO_1, -- Waiver ID-2 (Jan)
  idt.MAX_WAIVER_TYPE_3_MO_1, -- MAX Waiver Type Code -3 (Jan)
  idt.MAX_WAIVER_ID_3_MO_1, -- Waiver ID-3 (Jan)
  idt.MAX_WAIVER_TYPE_1_MO_2, -- MAX Waiver Type Code -1 (Feb)
  idt.MAX_WAIVER_ID_1_MO_2, -- Waiver ID-1 (Feb)
  idt.MAX_WAIVER_TYPE_2_MO_2, -- MAX Waiver Type Code -2 (Feb)
  idt.MAX_WAIVER_ID_2_MO_2, -- Waiver ID-2 (Feb)
  idt.MAX_WAIVER_TYPE_3_MO_2, -- MAX Waiver Type Code -3 (Feb)
  idt.MAX_WAIVER_ID_3_MO_2, -- Waiver ID-3 (Feb)
  idt.MAX_WAIVER_TYPE_1_MO_3, -- MAX Waiver Type Code -1 (Mar)
  idt.MAX_WAIVER_ID_1_MO_3, -- Waiver ID-1 (Mar)
  idt.MAX_WAIVER_TYPE_2_MO_3, -- MAX Waiver Type Code -2 (Mar)
  idt.MAX_WAIVER_ID_2_MO_3, -- Waiver ID-2 (Mar)
  idt.MAX_WAIVER_TYPE_3_MO_3, -- MAX Waiver Type Code -3 (Mar)
  idt.MAX_WAIVER_ID_3_MO_3, -- Waiver ID-3 (Mar)
  idt.MAX_WAIVER_TYPE_1_MO_4, -- MAX Waiver Type Code -1 (Apr)
  idt.MAX_WAIVER_ID_1_MO_4, -- Waiver ID-1 (Apr)
  idt.MAX_WAIVER_TYPE_2_MO_4, -- MAX Waiver Type Code -2 (Apr)
  idt.MAX_WAIVER_ID_2_MO_4, -- Waiver ID-2 (Apr)
  idt.MAX_WAIVER_TYPE_3_MO_4, -- MAX Waiver Type Code -3 (Apr)
  idt.MAX_WAIVER_ID_3_MO_4, -- Waiver ID-3 (Apr)
  idt.MAX_WAIVER_TYPE_1_MO_5, -- MAX Waiver Type Code -1 (May)
  idt.MAX_WAIVER_ID_1_MO_5, -- Waiver ID-1 (May)
  idt.MAX_WAIVER_TYPE_2_MO_5, -- MAX Waiver Type Code -2 (May)
  idt.MAX_WAIVER_ID_2_MO_5, -- Waiver ID-2 (May)
  idt.MAX_WAIVER_TYPE_3_MO_5, -- MAX Waiver Type Code -3 (May)
  idt.MAX_WAIVER_ID_3_MO_5, -- Waiver ID-3 (May)
  idt.MAX_WAIVER_TYPE_1_MO_6, -- MAX Waiver Type Code -1 (Jun)
  idt.MAX_WAIVER_ID_1_MO_6, -- Waiver ID-1 (Jun)
  idt.MAX_WAIVER_TYPE_2_MO_6, -- MAX Waiver Type Code -2 (Jun)
  idt.MAX_WAIVER_ID_2_MO_6, -- Waiver ID-2 (Jun)
  idt.MAX_WAIVER_TYPE_3_MO_6, -- MAX Waiver Type Code -3 (Jun)
  idt.MAX_WAIVER_ID_3_MO_6, -- Waiver ID-3 (Jun)
  idt.MAX_WAIVER_TYPE_1_MO_7, -- MAX Waiver Type Code -1 (Jul)
  idt.MAX_WAIVER_ID_1_MO_7, -- Waiver ID-1 (Jul)
  idt.MAX_WAIVER_TYPE_2_MO_7, -- MAX Waiver Type Code -2 (Jul)
  idt.MAX_WAIVER_ID_2_MO_7, -- Waiver ID-2 (Jul)
  idt.MAX_WAIVER_TYPE_3_MO_7, -- MAX Waiver Type Code -3 (Jul)
  idt.MAX_WAIVER_ID_3_MO_7, -- Waiver ID-3 (Jul)
  idt.MAX_WAIVER_TYPE_1_MO_8, -- MAX Waiver Type Code -1 (Aug)
  idt.MAX_WAIVER_ID_1_MO_8, -- Waiver ID-1 (Aug)
  idt.MAX_WAIVER_TYPE_2_MO_8, -- MAX Waiver Type Code -2 (Aug)
  idt.MAX_WAIVER_ID_2_MO_8, -- Waiver ID-2 (Aug)
  idt.MAX_WAIVER_TYPE_3_MO_8, -- MAX Waiver Type Code -3 (Aug)
  idt.MAX_WAIVER_ID_3_MO_8, -- Waiver ID-3 (Aug)
  idt.MAX_WAIVER_TYPE_1_MO_9, -- MAX Waiver Type Code -1 (Sep)
  idt.MAX_WAIVER_ID_1_MO_9, -- Waiver ID-1 (Sep)
  idt.MAX_WAIVER_TYPE_2_MO_9, -- MAX Waiver Type Code -2 (Sep)
  idt.MAX_WAIVER_ID_2_MO_9, -- Waiver ID-2 (Sep)
  idt.MAX_WAIVER_TYPE_3_MO_9, -- MAX Waiver Type Code -3 (Sep)
  idt.MAX_WAIVER_ID_3_MO_9, -- Waiver ID-3 (Sep)
  idt.MAX_WAIVER_TYPE_1_MO_10, -- MAX Waiver Type Code -1 (Oct)
  idt.MAX_WAIVER_ID_1_MO_10, -- Waiver ID-1 (Oct)
  idt.MAX_WAIVER_TYPE_2_MO_10, -- MAX Waiver Type Code -2 (Oct)
  idt.MAX_WAIVER_ID_2_MO_10, -- Waiver ID-2 (Oct)
  idt.MAX_WAIVER_TYPE_3_MO_10, -- MAX Waiver Type Code -3 (Oct)
  idt.MAX_WAIVER_ID_3_MO_10, -- Waiver ID-3 (Oct)
  idt.MAX_WAIVER_TYPE_1_MO_11, -- MAX Waiver Type Code -1 (Nov)
  idt.MAX_WAIVER_ID_1_MO_11, -- Waiver ID-1 (Nov)
  idt.MAX_WAIVER_TYPE_2_MO_11, -- MAX Waiver Type Code -2 (Nov)
  idt.MAX_WAIVER_ID_2_MO_11, -- Waiver ID-2 (Nov)
  idt.MAX_WAIVER_TYPE_3_MO_11, -- MAX Waiver Type Code -3 (Nov)
  idt.MAX_WAIVER_ID_3_MO_11, -- Waiver ID-3 (Nov)
  idt.MAX_WAIVER_TYPE_1_MO_12, -- MAX Waiver Type Code -1 (Dec)
  idt.MAX_WAIVER_ID_1_MO_12, -- Waiver ID-1 (Dec)
  idt.MAX_WAIVER_TYPE_2_MO_12, -- MAX Waiver Type Code -2 (Dec)
  idt.MAX_WAIVER_ID_2_MO_12, -- Waiver ID-2 (Dec)
  idt.MAX_WAIVER_TYPE_3_MO_12, -- MAX Waiver Type Code -3 (Dec)
  idt.MAX_WAIVER_ID_3_MO_12, -- Waiver ID-3 (Dec)
  idt.MAX_1915C_WAIVER_TYPE_LTST, -- Annual 1915(c) MAX Waiver Type
  idt.RCPNT_IND, -- Recipient indicator
  idt.TOT_IP_DSCHRG_CNT, -- IP discharges
  idt.TOT_IP_STAY_CNT, -- IP stays
  idt.TOT_IP_DAY_CNT_DSCHRG, -- Length of Stay (LOS) - for discharges
  idt.TOT_IP_DAY_CNT_STAYS, -- Length of Stay (LOS) - for stays
  idt.TOT_IP_CVR_DAY_CNT_DSCHRG, -- Covered days - for discharges
  idt.TOT_IP_CVR_DAY_CNT_STAYS, -- Covered days - for stays
  idt.TOT_LTC_CVR_DAY_CNT_AGED, -- Mental hospital covered days
  idt.TOT_LTC_CVR_DAY_CNT_PSYCH, -- Inpatient psych (age < 21) covered days
  idt.TOT_LTC_CVR_DAY_CNT_ICFMR, -- ICF/MR covered days
  idt.TOT_LTC_CVR_DAY_CNT_NF, -- Nursing facility covered days
  idt.TOT_LTC_CVR_DAY_CNT, -- Total LT covered days
  idt.TOT_MDCD_CLM_CNT, -- Total record count
  idt.TOT_MDCD_FFS_CLM_CNT, -- Fee-for-service claim count
  idt.TOT_MDCD_PREM_CLM_CNT, -- Premium payment claim count
  idt.TOT_MDCD_ENCT_CLM_CNT, -- Encounter record count
  idt.TOT_MDCD_PYMT_AMT, -- Total Medicaid payment amount
  idt.TOT_MDCD_FFS_PYMT_AMT, -- Fee-for-service Medicaid payment amount
  idt.TOT_MDCD_PREM_PYMT_AMT, -- Premium payment Medicaid payment amount
  idt.TOT_MDCD_CHRG_AMT, -- Charge amount
  idt.TOT_MDCD_TP_PYMT_AMT, -- Third party payment amount
  idt.IP_HOSP_REC_FP, -- Inpatient hospital records (FP)
  idt.IP_HOSP_PYMT_FP, -- Inpatient hospital payments (FP)
  idt.LT_REC_CNT_FP, -- Institutional LT care records (FP)
  idt.LT_PYMT_AMT_FP, -- Institutional LT care payments (FP)
  idt.OT_REC_CNT_FP, -- Other service records (FP)
  idt.OT_PYMT_AMT_FP, -- Other service payments (FP)
  idt.RX_REC_CNT_FP, -- Prescription drug records (FP)
  idt.RX_PYMT_AMT_FP, -- Prescription drug payments (FP)
  idt.TOT_REC_CNT_FP, -- Total records (FP)
  idt.TOT_PYMT_AMT_FP, -- Total payments (FP)
  idt.IP_HOSP_REC_RHC, -- Inpatient hospital records (RHC)
  idt.IP_HOSP_PYMT_RHC, -- Inpatient hospital payments (RHC)
  idt.LT_REC_CNT_RHC, -- Institutional LT care records (RHC)
  idt.LT_PYMT_AMT_RHC, -- Institutional LT care payments (RHC)
  idt.OT_REC_CNT_RHC, -- Other service records (RHC)
  idt.OT_PYMT_AMT_RHC, -- Other service payments (RHC)
  idt.RX_REC_CNT_RHC, -- Prescription drug records (RHC)
  idt.RX_PYMT_AMT_RHC, -- Prescription drug payments (RHC)
  idt.TOT_REC_CNT_RHC, -- Total records (RHC)
  idt.TOT_PYMT_AMT_RHC, -- Total payments (RHC)
  idt.IP_HOSP_REC_FQHC, -- Inpatient hospital records (FQHC)
  idt.IP_HOSP_PYMT_FQHC, -- Inpatient hospital payments (FQHC)
  idt.LT_REC_CNT_FQHC, -- Institutional LT care records (FQHC)
  idt.LT_PYMT_AMT_FQHC, -- Institutional LT care payments (FQHC)
  idt.OT_REC_CNT_FQHC, -- Other service records (FQHC)
  idt.OT_PYMT_AMT_FQHC, -- Other service payments (FQHC)
  idt.RX_REC_CNT_FQHC, -- Prescription drug records (FQHC)
  idt.RX_PYMT_AMT_FQHC, -- Prescription drug payments (FQHC)
  idt.TOT_REC_CNT_FQHC, -- Total records (FQHC)
  idt.TOT_PYMT_AMT_FQHC, -- Total payments (FQHC)
  idt.IP_HOSP_REC_IHS, -- Inpatient hospital records (IHS)
  idt.IP_HOSP_PYMT_IHS, -- Inpatient hospital payments (IHS)
  idt.LT_REC_CNT_IHS, -- Institutional LT care records (IHS)
  idt.LT_PYMT_AMT_IHS, -- Institutional LT care payments (IHS)
  idt.OT_REC_CNT_IHS, -- Other service records (IHS)
  idt.OT_PYMT_AMT_IHS, -- Other service payments (IHS)
  idt.RX_REC_CNT_IHS, -- Prescription drug records (IHS)
  idt.RX_PYMT_AMT_IHS, -- Prescription drug payments (IHS)
  idt.TOT_REC_CNT_IHS, -- Total records (IHS)
  idt.TOT_PYMT_AMT_IHS, -- Total payments (IHS)
  idt.IP_HOSP_REC_HCBCA, -- Inpatient hospital records (HCBCA)
  idt.IP_HOSP_PYMT_HCBCA, -- Inpatient hospital payments (HCBCA)
  idt.LT_REC_CNT_HCBCA, -- Institutional LT care records (HCBCA)
  idt.LT_PYMT_AMT_HCBCA, -- Institutional LT care payments (HCBCA)
  idt.OT_REC_CNT_HCBCA, -- Other service records (HCBCA)
  idt.OT_PYMT_AMT_HCBCA, -- Other service payments (HCBCA)
  idt.RX_REC_CNT_HCBCA, -- Prescription drug records (HCBCA)
  idt.RX_PYMT_AMT_HCBCA, -- Prescription drug payments (HCBCA)
  idt.TOT_REC_CNT_HCBCA, -- Total records (HCBCA)
  idt.TOT_PYMT_AMT_HCBCA, -- Total payments (HCBCA)
  idt.IP_HOSP_REC_HCBCS, -- Inpatient hospital records (HCBCS)
  idt.IP_HOSP_PYMT_HCBCS, -- Inpatient hospital payments (HCBCS)
  idt.LT_REC_CNT_HCBCS, -- Institutional LT care records (HCBCS)
  idt.LT_PYMT_AMT_HCBCS, -- Institutional LT care payments (HCBCS)
  idt.OT_REC_CNT_HCBCS, -- Other service records (HCBCS)
  idt.OT_PYMT_AMT_HCBCS, -- Other service payments (HCBCS)
  idt.RX_REC_CNT_HCBCS, -- Prescription drug records (HCBCS)
  idt.RX_PYMT_AMT_HCBCS, -- Prescription drug payments (HCBCS)
  idt.TOT_REC_CNT_HCBCS, -- Total records (HCBCS)
  idt.TOT_PYMT_AMT_HCBCS, -- Total payments (HCBCS)
  idt.RCPNT_DLVRY_CD, -- Delivery code
  idt.FEE_FOR_SRVC_IND_01, -- Recipient indicator (MAX TOS 01)
  idt.FFS_CLM_CNT_01, -- Claim count (MAX TOS 01)
  idt.FFS_PYMT_AMT_01, -- Medicaid payment amount (MAX TOS 01)
  idt.FFS_CHRG_AMT_01, -- Charge amount (MAX TOS 01)
  idt.FFS_TP_AMT_01, -- Third party payment amount (MAX TOS 01)
  idt.ENCTR_REC_CNT_01, -- Encounter record count (MAX TOS 01)
  idt.FEE_FOR_SRVC_IND_02, -- Recipient indicator (MAX TOS 02)
  idt.FFS_CLM_CNT_02, -- Claim count (MAX TOS 02)
  idt.FFS_PYMT_AMT_02, -- Medicaid payment amount (MAX TOS 02)
  idt.FFS_CHRG_AMT_02, -- Charge amount (MAX TOS 02)
  idt.FFS_TP_AMT_02, -- Third party payment amount (MAX TOS 02)
  idt.ENCTR_REC_CNT_02, -- Encounter record count (MAX TOS 02)
  idt.FEE_FOR_SRVC_IND_04, -- Recipient indicator (MAX TOS 04)
  idt.FFS_CLM_CNT_04, -- Claim count (MAX TOS 04)
  idt.FFS_PYMT_AMT_04, -- Medicaid payment amount (MAX TOS 04)
  idt.FFS_CHRG_AMT_04, -- Charge amount (MAX TOS 04)
  idt.FFS_TP_AMT_04, -- Third party payment amount (MAX TOS 04)
  idt.ENCTR_REC_CNT_04, -- Encounter record count (MAX TOS 04)
  idt.FEE_FOR_SRVC_IND_05, -- Recipient indicator (MAX TOS 05)
  idt.FFS_CLM_CNT_05, -- Claim count (MAX TOS 05)
  idt.FFS_PYMT_AMT_05, -- Medicaid payment amount (MAX TOS 05)
  idt.FFS_CHRG_AMT_05, -- Charge amount (MAX TOS 05)
  idt.FFS_TP_AMT_05, -- Third party payment amount (MAX TOS 05)
  idt.ENCTR_REC_CNT_05, -- Encounter record count (MAX TOS 05)
  idt.FEE_FOR_SRVC_IND_07, -- Recipient indicator (MAX TOS 07)
  idt.FFS_CLM_CNT_07, -- Claim count (MAX TOS 07)
  idt.FFS_PYMT_AMT_07, -- Medicaid payment amount (MAX TOS 07)
  idt.FFS_CHRG_AMT_07, -- Charge amount (MAX TOS 07)
  idt.FFS_TP_AMT_07, -- Third party payment amount (MAX TOS 07)
  idt.ENCTR_REC_CNT_07, -- Encounter record count (MAX TOS 07)
  idt.FEE_FOR_SRVC_IND_08, -- Recipient indicator (MAX TOS 08)
  idt.FFS_CLM_CNT_08, -- Claim count (MAX TOS 08)
  idt.FFS_PYMT_AMT_08, -- Medicaid payment amount (MAX TOS 08)
  idt.FFS_CHRG_AMT_08, -- Charge amount (MAX TOS 08)
  idt.FFS_TP_AMT_08, -- Third party payment amount (MAX TOS 08)
  idt.ENCTR_REC_CNT_08, -- Encounter record count (MAX TOS 08)
  idt.FEE_FOR_SRVC_IND_09, -- Recipient indicator (MAX TOS 09)
  idt.FFS_CLM_CNT_09, -- Claim count (MAX TOS 09)
  idt.FFS_PYMT_AMT_09, -- Medicaid payment amount (MAX TOS 09)
  idt.FFS_CHRG_AMT_09, -- Charge amount (MAX TOS 09)
  idt.FFS_TP_AMT_09, -- Third party payment amount (MAX TOS 09)
  idt.ENCTR_REC_CNT_09, -- Encounter record count (MAX TOS 09)
  idt.FEE_FOR_SRVC_IND_10, -- Recipient indicator (MAX TOS 10)
  idt.FFS_CLM_CNT_10, -- Claim count (MAX TOS 10)
  idt.FFS_PYMT_AMT_10, -- Medicaid payment amount (MAX TOS 10)
  idt.FFS_CHRG_AMT_10, -- Charge amount (MAX TOS 10)
  idt.FFS_TP_AMT_10, -- Third party payment amount (MAX TOS 10)
  idt.ENCTR_REC_CNT_10, -- Encounter record count (MAX TOS 10)
  idt.FEE_FOR_SRVC_IND_11, -- Recipient indicator (MAX TOS 11)
  idt.FFS_CLM_CNT_11, -- Claim count (MAX TOS 11)
  idt.FFS_PYMT_AMT_11, -- Medicaid payment amount (MAX TOS 11)
  idt.FFS_CHRG_AMT_11, -- Charge amount (MAX TOS 11)
  idt.FFS_TP_AMT_11, -- Third party payment amount (MAX TOS 11)
  idt.ENCTR_REC_CNT_11, -- Encounter record count (MAX TOS 11)
  idt.FEE_FOR_SRVC_IND_12, -- Recipient indicator (MAX TOS 12)
  idt.FFS_CLM_CNT_12, -- Claim count (MAX TOS 12)
  idt.FFS_PYMT_AMT_12, -- Medicaid payment amount (MAX TOS 12)
  idt.FFS_CHRG_AMT_12, -- Charge amount (MAX TOS 12)
  idt.FFS_TP_AMT_12, -- Third party payment amount (MAX TOS 12)
  idt.ENCTR_REC_CNT_12, -- Encounter record count (MAX TOS 12)
  idt.FEE_FOR_SRVC_IND_13, -- Recipient indicator (MAX TOS 13)
  idt.FFS_CLM_CNT_13, -- Claim count (MAX TOS 13)
  idt.FFS_PYMT_AMT_13, -- Medicaid payment amount (MAX TOS 13)
  idt.FFS_CHRG_AMT_13, -- Charge amount (MAX TOS 13)
  idt.FFS_TP_AMT_13, -- Third party payment amount (MAX TOS 13)
  idt.ENCTR_REC_CNT_13, -- Encounter record count (MAX TOS 13)
  idt.FEE_FOR_SRVC_IND_15, -- Recipient indicator (MAX TOS 15)
  idt.FFS_CLM_CNT_15, -- Claim count (MAX TOS 15)
  idt.FFS_PYMT_AMT_15, -- Medicaid payment amount (MAX TOS 15)
  idt.FFS_CHRG_AMT_15, -- Charge amount (MAX TOS 15)
  idt.FFS_TP_AMT_15, -- Third party payment amount (MAX TOS 15)
  idt.ENCTR_REC_CNT_15, -- Encounter record count (MAX TOS 15)
  idt.FEE_FOR_SRVC_IND_16, -- Recipient indicator (MAX TOS 16)
  idt.FFS_CLM_CNT_16, -- Claim count (MAX TOS 16)
  idt.FFS_PYMT_AMT_16, -- Medicaid payment amount (MAX TOS 16)
  idt.FFS_CHRG_AMT_16, -- Charge amount (MAX TOS 16)
  idt.FFS_TP_AMT_16, -- Third party payment amount (MAX TOS 16)
  idt.ENCTR_REC_CNT_16, -- Encounter record count (MAX TOS 16)
  idt.FEE_FOR_SRVC_IND_19, -- Recipient indicator (MAX TOS 19)
  idt.FFS_CLM_CNT_19, -- Claim count (MAX TOS 19)
  idt.FFS_PYMT_AMT_19, -- Medicaid payment amount (MAX TOS 19)
  idt.FFS_CHRG_AMT_19, -- Charge amount (MAX TOS 19)
  idt.FFS_TP_AMT_19, -- Third party payment amount (MAX TOS 19)
  idt.ENCTR_REC_CNT_19, -- Encounter record count (MAX TOS 19)
  idt.FEE_FOR_SRVC_IND_24, -- Recipient indicator (MAX TOS 24)
  idt.FFS_CLM_CNT_24, -- Claim count (MAX TOS 24)
  idt.FFS_PYMT_AMT_24, -- Medicaid payment amount (MAX TOS 24)
  idt.FFS_CHRG_AMT_24, -- Charge amount (MAX TOS 24)
  idt.FFS_TP_AMT_24, -- Third party payment amount (MAX TOS 24)
  idt.ENCTR_REC_CNT_24, -- Encounter record count (MAX TOS 24)
  idt.FEE_FOR_SRVC_IND_25, -- Recipient indicator (MAX TOS 25)
  idt.FFS_CLM_CNT_25, -- Claim count (MAX TOS 25)
  idt.FFS_PYMT_AMT_25, -- Medicaid payment amount (MAX TOS 25)
  idt.FFS_CHRG_AMT_25, -- Charge amount (MAX TOS 25)
  idt.FFS_TP_AMT_25, -- Third party payment amount (MAX TOS 25)
  idt.ENCTR_REC_CNT_25, -- Encounter record count (MAX TOS 25)
  idt.FEE_FOR_SRVC_IND_26, -- Recipient indicator (MAX TOS 26)
  idt.FFS_CLM_CNT_26, -- Claim count (MAX TOS 26)
  idt.FFS_PYMT_AMT_26, -- Medicaid payment amount (MAX TOS 26)
  idt.FFS_CHRG_AMT_26, -- Charge amount (MAX TOS 26)
  idt.FFS_TP_AMT_26, -- Third party payment amount (MAX TOS 26)
  idt.ENCTR_REC_CNT_26, -- Encounter record count (MAX TOS 26)
  idt.FEE_FOR_SRVC_IND_30, -- Recipient indicator (MAX TOS 30)
  idt.FFS_CLM_CNT_30, -- Claim count (MAX TOS 30)
  idt.FFS_PYMT_AMT_30, -- Medicaid payment amount (MAX TOS 30)
  idt.FFS_CHRG_AMT_30, -- Charge amount (MAX TOS 30)
  idt.FFS_TP_AMT_30, -- Third party payment amount (MAX TOS 30)
  idt.ENCTR_REC_CNT_30, -- Encounter record count (MAX TOS 30)
  idt.FEE_FOR_SRVC_IND_31, -- Recipient indicator (MAX TOS 31)
  idt.FFS_CLM_CNT_31, -- Claim count (MAX TOS 31)
  idt.FFS_PYMT_AMT_31, -- Medicaid payment amount (MAX TOS 31)
  idt.FFS_CHRG_AMT_31, -- Charge amount (MAX TOS 31)
  idt.FFS_TP_AMT_31, -- Third party payment amount (MAX TOS 31)
  idt.ENCTR_REC_CNT_31, -- Encounter record count (MAX TOS 31)
  idt.FEE_FOR_SRVC_IND_33, -- Recipient indicator (MAX TOS 33)
  idt.FFS_CLM_CNT_33, -- Claim count (MAX TOS 33)
  idt.FFS_PYMT_AMT_33, -- Medicaid payment amount (MAX TOS 33)
  idt.FFS_CHRG_AMT_33, -- Charge amount (MAX TOS 33)
  idt.FFS_TP_AMT_33, -- Third party payment amount (MAX TOS 33)
  idt.ENCTR_REC_CNT_33, -- Encounter record count (MAX TOS 33)
  idt.FEE_FOR_SRVC_IND_34, -- Recipient indicator (MAX TOS 34)
  idt.FFS_CLM_CNT_34, -- Claim count (MAX TOS 34)
  idt.FFS_PYMT_AMT_34, -- Medicaid payment amount (MAX TOS 34)
  idt.FFS_CHRG_AMT_34, -- Charge amount (MAX TOS 34)
  idt.FFS_TP_AMT_34, -- Third party payment amount (MAX TOS 34)
  idt.ENCTR_REC_CNT_34, -- Encounter record count (MAX TOS 34)
  idt.FEE_FOR_SRVC_IND_35, -- Recipient indicator (MAX TOS 35)
  idt.FFS_CLM_CNT_35, -- Claim count (MAX TOS 35)
  idt.FFS_PYMT_AMT_35, -- Medicaid payment amount (MAX TOS 35)
  idt.FFS_CHRG_AMT_35, -- Charge amount (MAX TOS 35)
  idt.FFS_TP_AMT_35, -- Third party payment amount (MAX TOS 35)
  idt.ENCTR_REC_CNT_35, -- Encounter record count (MAX TOS 35)
  idt.FEE_FOR_SRVC_IND_36, -- Recipient indicator (MAX TOS 36)
  idt.FFS_CLM_CNT_36, -- Claim count (MAX TOS 36)
  idt.FFS_PYMT_AMT_36, -- Medicaid payment amount (MAX TOS 36)
  idt.FFS_CHRG_AMT_36, -- Charge amount (MAX TOS 36)
  idt.FFS_TP_AMT_36, -- Third party payment amount (MAX TOS 36)
  idt.ENCTR_REC_CNT_36, -- Encounter record count (MAX TOS 36)
  idt.FEE_FOR_SRVC_IND_37, -- Recipient indicator (MAX TOS 37)
  idt.FFS_CLM_CNT_37, -- Claim count (MAX TOS 37)
  idt.FFS_PYMT_AMT_37, -- Medicaid payment amount (MAX TOS 37)
  idt.FFS_CHRG_AMT_37, -- Charge amount (MAX TOS 37)
  idt.FFS_TP_AMT_37, -- Third party payment amount (MAX TOS 37)
  idt.ENCTR_REC_CNT_37, -- Encounter record count (MAX TOS 37)
  idt.FEE_FOR_SRVC_IND_38, -- Recipient indicator (MAX TOS 38)
  idt.FFS_CLM_CNT_38, -- Claim count (MAX TOS 38)
  idt.FFS_PYMT_AMT_38, -- Medicaid payment amount (MAX TOS 38)
  idt.FFS_CHRG_AMT_38, -- Charge amount (MAX TOS 38)
  idt.FFS_TP_AMT_38, -- Third party payment amount (MAX TOS 38)
  idt.ENCTR_REC_CNT_38, -- Encounter record count (MAX TOS 38)
  idt.FEE_FOR_SRVC_IND_39, -- Recipient indicator (MAX TOS 39)
  idt.FFS_CLM_CNT_39, -- Claim count (MAX TOS 39)
  idt.FFS_PYMT_AMT_39, -- Medicaid payment amount (MAX TOS 39)
  idt.FFS_CHRG_AMT_39, -- Charge amount (MAX TOS 39)
  idt.FFS_TP_AMT_39, -- Third party payment amount (MAX TOS 39)
  idt.ENCTR_REC_CNT_39, -- Encounter record count (MAX TOS 39)
  idt.FEE_FOR_SRVC_IND_51, -- Recipient indicator (MAX TOS 51)
  idt.FFS_CLM_CNT_51, -- Claim count (MAX TOS 51)
  idt.FFS_PYMT_AMT_51, -- Medicaid payment amount (MAX TOS 51)
  idt.FFS_CHRG_AMT_51, -- Charge amount (MAX TOS 51)
  idt.FFS_TP_AMT_51, -- Third party payment amount (MAX TOS 51)
  idt.ENCTR_REC_CNT_51, -- Encounter record count (MAX TOS 51)
  idt.FEE_FOR_SRVC_IND_52, -- Recipient indicator (MAX TOS 52)
  idt.FFS_CLM_CNT_52, -- Claim count (MAX TOS 52)
  idt.FFS_PYMT_AMT_52, -- Medicaid payment amount (MAX TOS 52)
  idt.FFS_CHRG_AMT_52, -- Charge amount (MAX TOS 52)
  idt.FFS_TP_AMT_52, -- Third party payment amount (MAX TOS 52)
  idt.ENCTR_REC_CNT_52, -- Encounter record count (MAX TOS 52)
  idt.FEE_FOR_SRVC_IND_53, -- Recipient indicator (MAX TOS 53)
  idt.FFS_CLM_CNT_53, -- Claim count (MAX TOS 53)
  idt.FFS_PYMT_AMT_53, -- Medicaid payment amount (MAX TOS 53)
  idt.FFS_CHRG_AMT_53, -- Charge amount (MAX TOS 53)
  idt.FFS_TP_AMT_53, -- Third party payment amount (MAX TOS 53)
  idt.ENCTR_REC_CNT_53, -- Encounter record count (MAX TOS 53)
  idt.FEE_FOR_SRVC_IND_54, -- Recipient indicator (MAX TOS 54)
  idt.FFS_CLM_CNT_54, -- Claim count (MAX TOS 54)
  idt.FFS_PYMT_AMT_54, -- Medicaid payment amount (MAX TOS 54)
  idt.FFS_CHRG_AMT_54, -- Charge amount (MAX TOS 54)
  idt.FFS_TP_AMT_54, -- Third party payment amount (MAX TOS 54)
  idt.ENCTR_REC_CNT_54, -- Encounter record count (MAX TOS 54)
  idt.FEE_FOR_SRVC_IND_99, -- Recipient indicator (Unknown)
  idt.FFS_CLM_CNT_99, -- Claim count (Unknown)
  idt.FFS_PYMT_AMT_99, -- Medicaid payment amount (Unknown)
  idt.FFS_CHRG_AMT_99, -- Charge amount (Unknown)
  idt.FFS_TP_AMT_99, -- Third party payment amount (Unknown)
  idt.ENCTR_REC_CNT_99, -- Encounter record count (Unknown)
  idt.CLTC_FFS_PYMT_AMT_11, -- Medicaid payment amount (CLTC 11)
  idt.CLTC_FFS_PYMT_AMT_12, -- Medicaid payment amount (CLTC 12)
  idt.CLTC_FFS_PYMT_AMT_13, -- Medicaid payment amount (CLTC 13)
  idt.CLTC_FFS_PYMT_AMT_14, -- Medicaid payment amount (CLTC 14)
  idt.CLTC_FFS_PYMT_AMT_15, -- Medicaid payment amount (CLTC 15)
  idt.CLTC_FFS_PYMT_AMT_16, -- Medicaid payment amount (CLTC 16)
  idt.CLTC_FFS_PYMT_AMT_17, -- Medicaid payment amount (CLTC 17)
  idt.CLTC_FFS_PYMT_AMT_18, -- Medicaid payment amount (CLTC 18)
  idt.CLTC_FFS_PYMT_AMT_19, -- Medicaid payment amount (CLTC 19)
  idt.CLTC_FFS_PYMT_AMT_20, -- Medicaid payment amount (CLTC 20)
  idt.CLTC_FFS_PYMT_AMT_30, -- Medicaid payment amount (CLTC 30)
  idt.CLTC_FFS_PYMT_AMT_31, -- Medicaid payment amount (CLTC 31)
  idt.CLTC_FFS_PYMT_AMT_32, -- Medicaid payment amount (CLTC 32)
  idt.CLTC_FFS_PYMT_AMT_33, -- Medicaid payment amount (CLTC 33)
  idt.CLTC_FFS_PYMT_AMT_34, -- Medicaid payment amount (CLTC 34)
  idt.CLTC_FFS_PYMT_AMT_35, -- Medicaid payment amount (CLTC 35)
  idt.CLTC_FFS_PYMT_AMT_36, -- Medicaid payment amount (CLTC 36)
  idt.CLTC_FFS_PYMT_AMT_37, -- Medicaid payment amount (CLTC 37)
  idt.CLTC_FFS_PYMT_AMT_38, -- Medicaid payment amount (CLTC 38)
  idt.CLTC_FFS_PYMT_AMT_39, -- Medicaid payment amount (CLTC 39)
  idt.CLTC_FFS_PYMT_AMT_40, -- Medicaid payment amount (CLTC 40)
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_01, -- Medicaid payment amount (HCBS) - Case Management
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_02, -- Medicaid payment amount (HCBS) - Round the Clock Services
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_03, -- Medicaid payment amount (HCBS) - Supported Employment
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_04, -- Medicaid payment amount (HCBS) - Day Services
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_05, -- Medicaid payment amount (HCBS) - Nursing Services
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_06, -- Medicaid payment amount (HCBS) - Home Delivered Meals
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_07, -- Medicaid payment amount (HCBS) - Rent and Food Expenses for
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_08, -- Medicaid payment amount (HCBS) - Home Based Services
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_09, -- Medicaid payment amount (HCBS) - Caregiver Support
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_10, -- Medicaid payment amount (HCBS) - Other Mental Health and BHS
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_11, -- Medicaid payment amount (HCBS) - Other Health and Therapeuti
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_12, -- Medicaid payment amount (HCBS) - Services Supporting Partici
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_13, -- Medicaid payment amount (HCBS) - Participant Training
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_14, -- Medicaid payment amount (HCBS) - Equipment, Technology, and
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_15, -- Medicaid payment amount (HCBS) - Non-Medical Transportation
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_16, -- Medicaid payment amount (HCBS) - Community Transition Servic
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_17, -- Medicaid payment amount (HCBS) - Other Services
  idt.HCBS_TXNMY_MDCD_PYMT_AMT_18, -- Medicaid payment amount (HCBS) - Unknown
  idt.PREM_PYMT_IND_HMO, -- Premium payment indicator (HMO/HIO)
  idt.PREM_PYMT_REC_CNT_HMO, -- Premium payment records (HMO/HIO)
  idt.PREM_MDCD_PYMT_AMT_HMO, -- Medicaid premium payments (HMO/HIO)
  idt.PREM_PYMT_IND_PHP, -- Premium payment indicator (PHP)
  idt.PREM_PYMT_REC_CNT_PHP, -- Premium payment records (PHP)
  idt.PREM_MDCD_PYMT_AMT_PHP, -- Medicaid premium payments (PHP)
  idt.PREM_PYMT_IND_PCCM, -- Premium payment indicator (PCCM)
  idt.PREM_PYMT_REC_CNT_PCCM, -- Premium payment records (PCCM)
  idt.PREM_MDCD_PYMT_AMT_PCCM, -- Medicaid premium payments (PCCM)
  idt.SSA_DOD + coalesce(bm.date_shift_days, mp.date_shift_days) SSA_DOD, -- Date of death (from SSA Death Master File) - No Longer Popul
  idt.EL_MDCR_ANN_XOVR_99, -- Crossover code (Annual) - Same As Dual Code - In Place for H
  idt.EL_MDCR_XOVR_MO_1, -- Medicare crossover code (Jan) - Same As Dual Code - In Place
  idt.EL_MDCR_XOVR_MO_2, -- Medicare crossover code (Feb) - Same As Dual Code - In Place
  idt.EL_MDCR_XOVR_MO_3, -- Medicare crossover code (Mar) - Same As Dual Code - In Place
  idt.EL_MDCR_XOVR_MO_4, -- Medicare crossover code (Apr) - Same As Dual Code - In Place
  idt.EL_MDCR_XOVR_MO_5, -- Medicare crossover code (May) - Same As Dual Code - In Place
  idt.EL_MDCR_XOVR_MO_6, -- Medicare crossover code (Jun) - Same As Dual Code - In Place
  idt.EL_MDCR_XOVR_MO_7, -- Medicare crossover code (Jul) - Same As Dual Code - In Place
  idt.EL_MDCR_XOVR_MO_8, -- Medicare crossover code (Aug) - Same As Dual Code - In Place
  idt.EL_MDCR_XOVR_MO_9, -- Medicare crossover code (Sep) - Same As Dual Code - In Place
  idt.EL_MDCR_XOVR_MO_10, -- Medicare crossover code (Oct) - Same As Dual Code - In Place
  idt.EL_MDCR_XOVR_MO_11, -- Medicare crossover code (Nov) - Same As Dual Code - In Place
  idt.EL_MDCR_XOVR_MO_12, -- Medicare crossover code (Dec) - Same As Dual Code - In Place
  idt.EXTRACT_DT
from maxdata_ps idt 
left join bene_id_mapping bm on bm.bene_id = idt.bene_id
join msis_id_mapping mm on mm.msis_id = idt.msis_id
join msis_person mp on mp.msis_id = idt.msis_id and mp.state_cd = idt.state_cd;
commit;


insert /*+ APPEND */ into "&&deid_schema".maxdata_rx
select /*+ PARALLEL(maxdata_rx,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 CCW Beneficiary ID
  mm.MSIS_ID_DEID MSIS_ID, -- Encrypted MSIS Identification Number
  idt.STATE_CD, -- State
  idt.YR_NUM, -- Year of MAX Record
  case
    when coalesce(bm.dob_shift_months, mp.dob_shift_months) is not null
    then add_months(EL_DOB, coalesce(bm.dob_shift_months, mp.dob_shift_months))
    else idt.EL_DOB + coalesce(bm.date_shift_days, mp.date_shift_days)
  end EL_DOB, -- Birth date
  idt.EL_SEX_CD, -- Sex
  idt.EL_RACE_ETHNCY_CD, -- Race/ethnicity (from MSIS)
  idt.RACE_CODE_1, -- Race - White (from MSIS)
  idt.RACE_CODE_2, -- Race - Black (from MSIS)
  idt.RACE_CODE_3, -- Race - Am Indian/Alaskan (from MSIS)
  idt.RACE_CODE_4, -- Race - Asian (from MSIS)
  idt.RACE_CODE_5, -- Race - Hawaiian/Pac) Islands (from MSIS)
  idt.ETHNICITY_CODE, -- Ethnicity - Hispanic (from MSIS)
  idt.EL_SS_ELGBLTY_CD_LTST, -- State specific eligiblity - most recent
  idt.EL_SS_ELGBLTY_CD_MO, -- State specific eligiblity - mo of svc
  idt.EL_MAX_ELGBLTY_CD_LTST, -- MAX eligibility - most recent
  idt.EL_MAX_ELGBLTY_CD_MO, -- MAX eligibility - mo of svc
  idt.EL_MDCR_ANN_XOVR_OLD, -- Crossover code (Annual) old values
  idt.EL_MDCR_XOVR_CLM_BSD_CD, -- Crossover code (from claims only)
  idt.MSNG_ELG_DATA, -- Missing eligibility data
  idt.EL_MDCR_ANN_XOVR_99, -- Crossover code (Annual)
  idt.MSIS_TOS, -- MSIS Type of Service (TOS)
  idt.MSIS_TOP, -- MSIS Type of Program (TOP)
  idt.MAX_TOS, -- MAX Type of Service (TOS)
  idt.PRVDR_ID_NMBR, -- Billing provider identification number
  idt.NPI, -- National Provider Identifier
  idt.TAXONOMY, -- Provider Taxonomy
  idt.TYPE_CLM_CD, -- Type of claim
  idt.ADJUST_CD, -- Adjustment code
  idt.PHP_TYPE, -- Managed care type of plan code
  idt.PHP_ID, -- Managed care plan identification code
  idt.MDCD_PYMT_AMT, -- Medicaid payment amount
  idt.TP_PYMT_AMT, -- Third party payment amount
  idt.PYMT_DT + coalesce(bm.date_shift_days, mp.date_shift_days) PYMT_DT, -- Payment/adjudication date
  idt.CHRG_AMT, -- Charge amount
  idt.PHP_VAL, -- Prepaid plan value
  idt.MDCR_COINSUR_PYMT_AMT, -- Medicare coinsurance payment amount
  idt.MDCR_DED_PYMT_AMT, -- Medicare deductible payment amount
  idt.PRES_PHYSICIAN_ID_NUM, -- Prescribing physician id number
  idt.PRSC_WRTE_DT + coalesce(bm.date_shift_days, mp.date_shift_days) PRSC_WRTE_DT, -- Prescribed date
  idt.PRSCRPTN_FILL_DT + coalesce(bm.date_shift_days, mp.date_shift_days) PRSCRPTN_FILL_DT, -- Prescription fill date
  idt.NEW_REFILL_IND, -- New or refill indicator
  idt.NDC, -- National Drug Code (NDC)
  idt.QTY_SRVC_UNITS, -- Quantity of service
  idt.DAYS_SUPPLY, -- Days supply
  idt.EXTRACT_DT
from maxdata_rx idt 
left join bene_id_mapping bm on bm.bene_id = idt.bene_id
join msis_id_mapping mm on mm.msis_id = idt.msis_id
join msis_person mp on mp.msis_id = idt.msis_id and mp.state_cd = idt.state_cd;
commit;


insert /*+ APPEND */ into "&&deid_schema".bcarrier_line
select /*+ PARALLEL(bcarrier_line,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.LINE_NUM, -- Claim Line Number
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date  (Determines Year of Claim)
  idt.CARR_PRFRNG_PIN_NUM, -- Carrier Line Claim Performing PIN Number
  idt.PRF_PHYSN_UPIN, -- Carrier Line Performing UPIN Number
  idt.PRF_PHYSN_NPI, -- Carrier Line Performing NPI Number
  idt.ORG_NPI_NUM, -- Carrier Line Performing Group NPI Number
  idt.CARR_LINE_PRVDR_TYPE_CD, -- Carrier Line Provider Type Code
  idt.TAX_NUM, -- Line Provider Tax Number
  idt.PRVDR_STATE_CD, -- Line NCH Provider State Code
  idt.PRVDR_ZIP, -- Carrier Line Performing Provider ZIP Code
  idt.PRVDR_SPCLTY, -- Line HCFA Provider Specialty Code
  idt.PRTCPTNG_IND_CD, -- Line Provider Participating Indicator Code
  idt.CARR_LINE_RDCD_PMT_PHYS_ASTN_C, -- Carrier Line Reduced Payment Physician Assistant Code
  idt.LINE_SRVC_CNT, -- Line Service Count
  idt.LINE_CMS_TYPE_SRVC_CD, -- Line HCFA Type Service Code
  idt.LINE_PLACE_OF_SRVC_CD, -- Line Place Of Service Code
  idt.CARR_LINE_PRCNG_LCLTY_CD, -- Carrier Line Pricing Locality Code
  idt.LINE_1ST_EXPNS_DT + bm.date_shift_days LINE_1ST_EXPNS_DT, -- Line First Expense Date
  idt.LINE_LAST_EXPNS_DT + bm.date_shift_days LINE_LAST_EXPNS_DT, -- Line Last Expense Date
  idt.HCPCS_CD, -- Line Healthcare Common Procedure Coding System
  idt.HCPCS_1ST_MDFR_CD, -- Line HCPCS Initial Modifier Code
  idt.HCPCS_2ND_MDFR_CD, -- Line HCPCS Second Modifier Code
  idt.BETOS_CD, -- Line NCH BETOS Code
  idt.LINE_NCH_PMT_AMT, -- Line NCH Payment Amount
  idt.LINE_BENE_PMT_AMT, -- Line Beneficiary Payment Amount
  idt.LINE_PRVDR_PMT_AMT, -- Line Provider Payment Amount
  idt.LINE_BENE_PTB_DDCTBL_AMT, -- Line Beneficiary Part B Deductible Amount
  idt.LINE_BENE_PRMRY_PYR_CD, -- Line Beneficiary Primary Payer Code
  idt.LINE_BENE_PRMRY_PYR_PD_AMT, -- Line Beneficiary Primary Payer Paid Amount
  idt.LINE_COINSRNC_AMT, -- Line Coinsurance Amount
  idt.LINE_SBMTD_CHRG_AMT, -- Line Submitted Charge Amount
  idt.LINE_ALOWD_CHRG_AMT, -- Line Allowed Charge Amount
  idt.LINE_PRCSG_IND_CD, -- Line Processing Indicator Code
  idt.LINE_PMT_80_100_CD, -- Line Payment 80%/100% Code
  idt.LINE_SERVICE_DEDUCTIBLE, -- Line Service Deductible Indicator Switch
  idt.CARR_LINE_MTUS_CNT, -- Carrier Line Miles/Time/Units/Services Count
  idt.CARR_LINE_MTUS_CD, -- Carrier Line Miles/Time/Units/Services Indicator Code
  idt.LINE_ICD_DGNS_CD, -- Line Diagnosis Code Code
  idt.LINE_ICD_DGNS_VRSN_CD, -- Line Diagnosis Code Diagnosis Version Code (ICD-9 or ICD-10)
  idt.HPSA_SCRCTY_IND_CD, -- Carrier Line HPSA/Scarcity Indicator Code
  idt.CARR_LINE_RX_NUM, -- Carrier Line RX Number
  idt.LINE_HCT_HGB_RSLT_NUM, -- Hematocrit/Hemoglobin Test Results
  idt.LINE_HCT_HGB_TYPE_CD, -- Hematocrit/Hemoglobin Test Type code
  idt.LINE_NDC_CD, -- Line National Drug Code
  idt.CARR_LINE_CLIA_LAB_NUM, -- Clinical Laboratory Improvement Amendments monitored laboratory number
  idt.CARR_LINE_ANSTHSA_UNIT_CNT, -- Carrier Line Anesthesia Unit Count
  idt.EXTRACT_DT
from bcarrier_line idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".maxdata_lt
select /*+ PARALLEL(maxdata_lt,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 CCW Beneficiary ID
  mm.MSIS_ID_DEID MSIS_ID, -- Encrypted MSIS Identification Number
  idt.STATE_CD, -- State
  idt.YR_NUM, -- Year of MAX Record
  case
    when coalesce(bm.dob_shift_months, mp.dob_shift_months) is not null
    then add_months(EL_DOB, coalesce(bm.dob_shift_months, mp.dob_shift_months))
    else idt.EL_DOB + coalesce(bm.date_shift_days, mp.date_shift_days)
  end EL_DOB, -- Birth date
  idt.EL_SEX_CD, -- Sex
  idt.EL_RACE_ETHNCY_CD, -- Race/ethnicity (from MSIS)
  idt.RACE_CODE_1, -- Race - White (from MSIS)
  idt.RACE_CODE_2, -- Race - Black (from MSIS)
  idt.RACE_CODE_3, -- Race - Am Indian/Alaskan (from MSIS)
  idt.RACE_CODE_4, -- Race - Asian (from MSIS)
  idt.RACE_CODE_5, -- Race - Hawaiian/Pac) Islands (from MSIS)
  idt.ETHNICITY_CODE, -- Ethnicity - Hispanic (from MSIS)
  idt.EL_SS_ELGBLTY_CD_LTST, -- State specific eligiblity - most recent
  idt.EL_SS_ELGBLTY_CD_MO, -- State specific eligiblity - mo of svc
  idt.EL_MAX_ELGBLTY_CD_LTST, -- MAX eligibility - most recent
  idt.EL_MAX_ELGBLTY_CD_MO, -- MAX eligibility - mo of svc
  idt.EL_MDCR_ANN_XOVR_OLD, -- Crossover code (Annual) old values
  idt.MSNG_ELG_DATA, -- Missing eligibility data
  idt.EL_MDCR_XOVR_CLM_BSD_CD, -- Crossover code (from claims only)
  idt.EL_MDCR_ANN_XOVR_99, -- Crossover code (Annual)
  idt.MSIS_TOS, -- MSIS Type of Service (TOS)
  idt.MSIS_TOP, -- MSIS Type of Program (TOP)
  idt.MAX_TOS, -- MAX Type of Service (TOS)
  idt.PRVDR_ID_NMBR, -- Billing provider identification number
  idt.NPI, -- National Provider Identifier
  idt.TAXONOMY, -- Provider Taxonomy
  idt.TYPE_CLM_CD, -- Type of claim
  idt.ADJUST_CD, -- Adjustment code
  idt.PHP_TYPE, -- Managed care type of plan code
  idt.PHP_ID, -- Managed care plan identification code
  idt.MDCD_PYMT_AMT, -- Medicaid payment amount
  idt.TP_PYMT_AMT, -- Third party payment amount
  idt.PYMT_DT + coalesce(bm.date_shift_days, mp.date_shift_days) PYMT_DT, -- Payment/adjudication date
  idt.CHRG_AMT, -- Charge amount
  idt.PHP_VAL, -- Prepaid plan value
  idt.MDCR_COINSUR_PYMT_AMT, -- Medicare coinsurance payment amount
  idt.MDCR_DED_PYMT_AMT, -- Medicare deductible payment amount
  idt.ADMSN_DT + coalesce(bm.date_shift_days, mp.date_shift_days) ADMSN_DT, -- Admission date
  idt.SRVC_BGN_DT + coalesce(bm.date_shift_days, mp.date_shift_days) SRVC_BGN_DT, -- Beginning date of service
  idt.SRVC_END_DT + coalesce(bm.date_shift_days, mp.date_shift_days) SRVC_END_DT, -- Ending date of service
  idt.DIAG_CD_1, -- Principle Diagnosis code
  idt.DIAG_CD_2, -- Diagnosis codes (2nd diagnosis)
  idt.DIAG_CD_3, -- Diagnosis codes (3rd diagnosis)
  idt.DIAG_CD_4, -- Diagnosis codes (4th diagnosis)
  idt.DIAG_CD_5, -- Diagnosis codes (5th diagnosis)
  idt.MDCD_CVRD_MENTL_DAY_CNT, -- Mental hospital for the aged days
  idt.MDCD_CVRD_PSYCH_DAY_CNT, -- Inpatient Psychiatric (age < 21) days
  idt.INTRMDT_FAC_MR_DAY_CNT, -- ICF-MR days
  idt.NRSNG_FAC_DAY_CNT, -- Nursing facility days
  idt.LT_CARE_LVE_DAY_CNT, -- Leave days
  idt.PATIENT_STATUS_CD, -- Patient status
  idt.PATIENT_LIB_AMT, -- Patient liability amount
  idt.EXTRACT_DT
from maxdata_lt idt 
left join bene_id_mapping bm on bm.bene_id = idt.bene_id
join msis_id_mapping mm on mm.msis_id = idt.msis_id
join msis_person mp on mp.msis_id = idt.msis_id and mp.state_cd = idt.state_cd;
commit;


insert /*+ APPEND */ into "&&deid_schema".hha_base_claims
select /*+ PARALLEL(hha_base_claims,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_NEAR_LINE_REC_IDENT_CD, -- NCH Near Line Record Identification Code
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.CLM_FROM_DT + bm.date_shift_days CLM_FROM_DT, -- Claim From Date
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date  (Determines Year of Claim)
  idt.NCH_WKLY_PROC_DT + bm.date_shift_days NCH_WKLY_PROC_DT, -- NCH Weekly Claim Processing Date
  idt.FI_CLM_PROC_DT + bm.date_shift_days FI_CLM_PROC_DT, -- FI Claim Process Date
  idt.PRVDR_NUM, -- Provider Number
  idt.CLM_FAC_TYPE_CD, -- Claim Facility Type Code
  idt.CLM_SRVC_CLSFCTN_TYPE_CD, -- Claim Service classification Type Code
  idt.CLM_FREQ_CD, -- Claim Frequency Code
  idt.FI_NUM, -- FI Number
  idt.CLM_MDCR_NON_PMT_RSN_CD, -- Claim Medicare Non Payment Reason Code
  idt.CLM_PMT_AMT, -- Claim Payment Amount
  idt.NCH_PRMRY_PYR_CLM_PD_AMT, -- NCH Primary Payer Claim Paid Amount
  idt.NCH_PRMRY_PYR_CD, -- NCH Primary Payer Code
  idt.PRVDR_STATE_CD, -- NCH Provider State Code
  idt.ORG_NPI_NUM, -- Organization NPI Number
  idt.AT_PHYSN_UPIN, -- Claim Attending Physician UPIN Number
  idt.AT_PHYSN_NPI, -- Claim Attending Physician NPI Number
  idt.PTNT_DSCHRG_STUS_CD, -- Patient Discharge Status Code
  idt.CLM_PPS_IND_CD, -- Claim PPS Indicator Code
  idt.CLM_TOT_CHRG_AMT, -- Claim Total Charge Amount
  idt.PRNCPAL_DGNS_CD, -- Primary Claim Diagnosis Code
  idt.PRNCPAL_DGNS_VRSN_CD, -- Primary Claim Diagnosis Code Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD1, -- Claim Diagnosis Code I
  idt.ICD_DGNS_VRSN_CD1, -- Claim Diagnosis Code I Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD2, -- Claim Diagnosis Code II
  idt.ICD_DGNS_VRSN_CD2, -- Claim Diagnosis Code II Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD3, -- Claim Diagnosis Code III
  idt.ICD_DGNS_VRSN_CD3, -- Claim Diagnosis Code III Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD4, -- Claim Diagnosis Code IV
  idt.ICD_DGNS_VRSN_CD4, -- Claim Diagnosis Code IV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD5, -- Claim Diagnosis Code V
  idt.ICD_DGNS_VRSN_CD5, -- Claim Diagnosis Code V Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD6, -- Claim Diagnosis Code VI
  idt.ICD_DGNS_VRSN_CD6, -- Claim Diagnosis Code VI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD7, -- Claim Diagnosis Code VII
  idt.ICD_DGNS_VRSN_CD7, -- Claim Diagnosis Code VII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD8, -- Claim Diagnosis Code VIII
  idt.ICD_DGNS_VRSN_CD8, -- Claim Diagnosis Code VIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD9, -- Claim Diagnosis Code IX
  idt.ICD_DGNS_VRSN_CD9, -- Claim Diagnosis Code IX Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD10, -- Claim Diagnosis Code X
  idt.ICD_DGNS_VRSN_CD10, -- Claim Diagnosis Code X Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD11, -- Claim Diagnosis Code XI
  idt.ICD_DGNS_VRSN_CD11, -- Claim Diagnosis Code XI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD12, -- Claim Diagnosis Code XII
  idt.ICD_DGNS_VRSN_CD12, -- Claim Diagnosis Code XII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD13, -- Claim Diagnosis Code XIII
  idt.ICD_DGNS_VRSN_CD13, -- Claim Diagnosis Code XIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD14, -- Claim Diagnosis Code XIV
  idt.ICD_DGNS_VRSN_CD14, -- Claim Diagnosis Code XIV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD15, -- Claim Diagnosis Code XV
  idt.ICD_DGNS_VRSN_CD15, -- Claim Diagnosis Code XV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD16, -- Claim Diagnosis Code XVI
  idt.ICD_DGNS_VRSN_CD16, -- Claim Diagnosis Code XVI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD17, -- Claim Diagnosis Code XVII
  idt.ICD_DGNS_VRSN_CD17, -- Claim Diagnosis Code XVII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD18, -- Claim Diagnosis Code XVIII
  idt.ICD_DGNS_VRSN_CD18, -- Claim Diagnosis Code XVIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD19, -- Claim Diagnosis Code XIX
  idt.ICD_DGNS_VRSN_CD19, -- Claim Diagnosis Code XIX Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD20, -- Claim Diagnosis Code XX
  idt.ICD_DGNS_VRSN_CD20, -- Claim Diagnosis Code XX Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD21, -- Claim Diagnosis Code XXI
  idt.ICD_DGNS_VRSN_CD21, -- Claim Diagnosis Code XXI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD22, -- Claim Diagnosis Code XXII
  idt.ICD_DGNS_VRSN_CD22, -- Claim Diagnosis Code XXII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD23, -- Claim Diagnosis Code XXIII
  idt.ICD_DGNS_VRSN_CD23, -- Claim Diagnosis Code XXIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD24, -- Claim Diagnosis Code XXIV
  idt.ICD_DGNS_VRSN_CD24, -- Claim Diagnosis Code XXIV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD25, -- Claim Diagnosis Code XXV
  idt.ICD_DGNS_VRSN_CD25, -- Claim Diagnosis Code XXV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.FST_DGNS_E_CD, -- First Claim Diagnosis E Code
  idt.FST_DGNS_E_VRSN_CD, -- First Claim Diagnosis E Code Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD1, -- Claim Diagnosis E Code I
  idt.ICD_DGNS_E_VRSN_CD1, -- Claim Diagnosis E Code I Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD2, -- Claim Diagnosis E Code II
  idt.ICD_DGNS_E_VRSN_CD2, -- Claim Diagnosis E Code II Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD3, -- Claim Diagnosis E Code III
  idt.ICD_DGNS_E_VRSN_CD3, -- Claim Diagnosis E Code III Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD4, -- Claim Diagnosis E Code IV
  idt.ICD_DGNS_E_VRSN_CD4, -- Claim Diagnosis E Code IV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD5, -- Claim Diagnosis E Code V
  idt.ICD_DGNS_E_VRSN_CD5, -- Claim Diagnosis E Code V Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD6, -- Claim Diagnosis E Code VI
  idt.ICD_DGNS_E_VRSN_CD6, -- Claim Diagnosis E Code VI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD7, -- Claim Diagnosis E Code VII
  idt.ICD_DGNS_E_VRSN_CD7, -- Claim Diagnosis E Code VII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD8, -- Claim Diagnosis E Code VIII
  idt.ICD_DGNS_E_VRSN_CD8, -- Claim Diagnosis E Code VIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD9, -- Claim Diagnosis E Code IX
  idt.ICD_DGNS_E_VRSN_CD9, -- Claim Diagnosis E Code IX Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD10, -- Claim Diagnosis E Code X
  idt.ICD_DGNS_E_VRSN_CD10, -- Claim Diagnosis E Code X Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD11, -- Claim Diagnosis E Code XI
  idt.ICD_DGNS_E_VRSN_CD11, -- Claim Diagnosis E Code XI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_E_CD12, -- Claim Diagnosis E Code XII
  idt.ICD_DGNS_E_VRSN_CD12, -- Claim Diagnosis E Code XII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.CLM_HHA_LUPA_IND_CD, -- Claim HHA Low Utilization Payment Adjustment (LUPA) Indicator Code
  idt.CLM_HHA_RFRL_CD, -- Claim HHA Referral Code
  idt.CLM_HHA_TOT_VISIT_CNT, -- Claim HHA Total Visit Count
  idt.CLM_ADMSN_DT + bm.date_shift_days CLM_ADMSN_DT, -- Claim HHA Care Start Date
  case
    when bm.dob_shift_months is not null
    then add_months(DOB_DT, bm.dob_shift_months)
    else idt.DOB_DT + bm.date_shift_days
  end DOB_DT, -- Date of Birth from Claim (Date)
  idt.GNDR_CD, -- Gender Code from Claim
  idt.BENE_RACE_CD, -- Race Code from Claim
  NULL BENE_CNTY_CD, -- County Code from Claim (SSA)
  idt.BENE_STATE_CD, -- State Code from Claim (SSA)
  NULL BENE_MLG_CNTCT_ZIP_CD, -- Zip Code of Residence from Claim
  idt.CLM_MDCL_REC, -- Claim Medical Record Number
  idt.EXTRACT_DT
from hha_base_claims idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hospice_revenue_center
select /*+ PARALLEL(hospice_revenue_center,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date  (Determines Year of Claim)
  idt.CLM_LINE_NUM, -- Claim Line Number
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.REV_CNTR, -- Revenue Center Code
  idt.REV_CNTR_DT + bm.date_shift_days REV_CNTR_DT, -- Revenue Center Date
  idt.HCPCS_CD, -- Revenue Center Healthcare Common Procedure Coding System
  idt.HCPCS_1ST_MDFR_CD, -- Revenue Center HCPCS Initial Modifier Code
  idt.HCPCS_2ND_MDFR_CD, -- Revenue Center HCPCS Second Modifier Code
  idt.REV_CNTR_UNIT_CNT, -- Revenue Center Unit Count
  idt.REV_CNTR_RATE_AMT, -- Revenue Center Rate Amount
  idt.REV_CNTR_PRVDR_PMT_AMT, -- Revenue Center Provider Payment Amount
  idt.REV_CNTR_BENE_PMT_AMT, -- Revenue Center Beneficiary Payment Amount
  idt.REV_CNTR_PMT_AMT_AMT, -- Revenue Center Payment Amount Amount
  idt.REV_CNTR_TOT_CHRG_AMT, -- Revenue Center Total Charge Amount
  idt.REV_CNTR_NCVRD_CHRG_AMT, -- Revenue Center Non-Covered Charge Amount
  idt.REV_CNTR_DDCTBL_COINSRNC_CD, -- Revenue Center Deductible Coinsurance Code
  idt.REV_CNTR_NDC_QTY, -- Revenue Center NDC Quantity
  idt.REV_CNTR_NDC_QTY_QLFR_CD, -- Revenue Center NDC Quantity Qualifier Code
  idt.RNDRNG_PHYSN_UPIN, -- Revenue Center Rendering Physician UPIN
  idt.RNDRNG_PHYSN_NPI, -- Revenue Center Rendering Physician NPI
  idt.EXTRACT_DT
from hospice_revenue_center idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hha_span_codes
select /*+ PARALLEL(hha_span_codes,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.RLT_SPAN_CD_SEQ, -- Claim Related Span Code Sequence
  idt.CLM_SPAN_CD, -- Claim Occurrence Span Code
  idt.CLM_SPAN_FROM_DT + bm.date_shift_days CLM_SPAN_FROM_DT, -- Claim Occurrence Span From Date
  idt.CLM_SPAN_THRU_DT + bm.date_shift_days CLM_SPAN_THRU_DT, -- Claim Occurrence Span Through Date
  idt.EXTRACT_DT
from hha_span_codes idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".outpatient_revenue_center
select /*+ PARALLEL(outpatient_revenue_center,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date  (Determines Year of Claim)
  idt.CLM_LINE_NUM, -- Claim Line Number
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.REV_CNTR, -- Revenue Center Code
  idt.REV_CNTR_DT + bm.date_shift_days REV_CNTR_DT, -- Revenue Center Date
  idt.REV_CNTR_1ST_ANSI_CD, -- Revenue Center 1st ANSI Code
  idt.REV_CNTR_2ND_ANSI_CD, -- Revenue Center 2nd ANSI Code
  idt.REV_CNTR_3RD_ANSI_CD, -- Revenue Center 3rd ANSI Code
  idt.REV_CNTR_4TH_ANSI_CD, -- Revenue Center 4th ANSI Code
  idt.REV_CNTR_APC_HIPPS_CD, -- Revenue Center APC/HIPPS
  idt.HCPCS_CD, -- Revenue Center Healthcare Common Procedure Coding System
  idt.HCPCS_1ST_MDFR_CD, -- Revenue Center HCPCS Initial Modifier Code
  idt.HCPCS_2ND_MDFR_CD, -- Revenue Center HCPCS Second Modifier Code
  idt.REV_CNTR_PMT_MTHD_IND_CD, -- Revenue Center Payment Method Indicator Code
  idt.REV_CNTR_DSCNT_IND_CD, -- Revenue Center Discount Indicator Code
  idt.REV_CNTR_PACKG_IND_CD, -- Revenue Center Packaging Indicator Code
  idt.REV_CNTR_OTAF_PMT_CD, -- Revenue Center Obligation to Accept As Full (OTAF) Payment Code
  idt.REV_CNTR_IDE_NDC_UPC_NUM, -- Revenue Center IDE, NDC, UPC Number
  idt.REV_CNTR_UNIT_CNT, -- Revenue Center Unit Count
  idt.REV_CNTR_RATE_AMT, -- Revenue Center Rate Amount
  idt.REV_CNTR_BLOOD_DDCTBL_AMT, -- Revenue Center Blood Deductible Amount
  idt.REV_CNTR_CASH_DDCTBL_AMT, -- Revenue Center Cash Deductible Amount
  idt.REV_CNTR_COINSRNC_WGE_ADJSTD_C, -- Revenue Center Coinsurance/Wage Adjusted Coinsurance Amount
  idt.REV_CNTR_RDCD_COINSRNC_AMT, -- Revenue Center Reduced Coinsurance Amount
  idt.REV_CNTR_1ST_MSP_PD_AMT, -- Revenue Center 1st Medicare Secondary Payer Paid Amount
  idt.REV_CNTR_2ND_MSP_PD_AMT, -- Revenue Center 2nd Medicare Secondary Payer Paid Amount
  idt.REV_CNTR_PRVDR_PMT_AMT, -- Revenue Center Provider Payment Amount
  idt.REV_CNTR_BENE_PMT_AMT, -- Revenue Center Beneficiary Payment Amount
  idt.REV_CNTR_PTNT_RSPNSBLTY_PMT, -- Revenue Center Patient Responsibility Payment
  idt.REV_CNTR_PMT_AMT_AMT, -- Revenue Center Payment Amount Amount
  idt.REV_CNTR_TOT_CHRG_AMT, -- Revenue Center Total Charge Amount
  idt.REV_CNTR_NCVRD_CHRG_AMT, -- Revenue Center Non-Covered Charge Amount
  idt.REV_CNTR_STUS_IND_CD, -- Revenue Center Status Indicator Code
  idt.REV_CNTR_NDC_QTY, -- Revenue Center NDC Quantity
  idt.REV_CNTR_NDC_QTY_QLFR_CD, -- Revenue Center NDC Quantity Qualifier Code
  idt.RNDRNG_PHYSN_UPIN, -- Revenue Center Rendering Physician UPIN
  idt.RNDRNG_PHYSN_NPI, -- Revenue Center Rendering Physician NPI
  idt.EXTRACT_DT
from outpatient_revenue_center idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".pde_saf
select /*+ PARALLEL(pde_saf,12) */ 
  idt.PDE_ID, -- Encrypted 723 PDE ID
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.SRVC_DT + bm.date_shift_days SRVC_DT, -- RX Service Date (DOS)
  idt.SRVC_PRVDR_ID_QLFYR_CD, -- Service Provider ID Qualifier Code
  idt.SRVC_PRVDR_ID, -- Service Provider ID
  idt.PRSCRBR_ID_QLFYR_CD, -- Prescriber ID Qualifier Code
  idt.PRSCRBR_ID, -- Prescriber ID
  idt.PROD_SRVC_ID, -- Product Service ID
  idt.DAW_PROD_SLCTN_CD, -- Dispense as Written (DAW) Product Selection Code
  idt.QTY_DSPNSD_NUM, -- Quantity Dispensed
  idt.DAYS_SUPLY_NUM, -- Days Supply
  idt.DSPNSNG_STUS_CD, -- Dispensing Status Code
  idt.DRUG_CVRG_STUS_CD, -- Drug Coverage Status Code
  idt.GDC_BLW_OOPT_AMT, -- Gross Drug Cost Below Out-of-Pocket Threshold (GDCB)
  idt.GDC_ABV_OOPT_AMT, -- Gross Drug Cost Above Out-of-Pocket Threshold (GDCA)
  idt.PTNT_PAY_AMT, -- Patient Pay Amount
  idt.OTHR_TROOP_AMT, -- Other TrOOP Amount
  idt.LICS_AMT, -- Low Income Cost Sharing Subsidy Amount (LICS)
  idt.PLRO_AMT, -- Patient Liability Reduction Due to Other Payer Amount (PLRO)
  idt.CVRD_D_PLAN_PD_AMT, -- Covered D Plan Paid Amount (CPP)
  idt.NCVRD_PLAN_PD_AMT, -- Non-Covered Plan Paid Amount (NPP)
  idt.TOT_RX_CST_AMT, -- Gross Drug Cost
  idt.BN, -- Brand Name
  idt.GCDF, -- Dosage Form Code
  idt.GCDF_DESC, -- Dosage Form Code Description
  idt.STR, -- Drug Strength Description
  idt.GNN, -- Generic Name - Short Version
  idt.BENEFIT_PHASE, -- The benefit phase of the Part D Event
  idt.EXTRACT_DT
from pde_saf idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hha_condition_codes
select /*+ PARALLEL(hha_condition_codes,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.RLT_COND_CD_SEQ, -- Claim Related Condition Code Sequence
  idt.CLM_RLT_COND_CD, -- Claim Related Condition Code
  idt.EXTRACT_DT
from hha_condition_codes idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".mbsf_d_cmpnts
select /*+ PARALLEL(mbsf_d_cmpnts,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID (Unique Key)
  idt.BENE_ENROLLMT_REF_YR, -- Beneficiary Enrollment Reference Year
  idt.CRDTBL_CVRG_SW, -- Creditable Coverage Switch
  idt.PLAN_CVRG_MOS_NUM, -- Plan Coverage Months Number
  idt.RDS_CVRG_MOS_NUM, -- Retiree Drug Subsidy Coverage Months Number
  idt.DUAL_ELGBL_MOS_NUM, -- Dual Eligible Months Number
  idt.PTD_CNTRCT_ID_01, -- Jan. Contract ID
  idt.PTD_CNTRCT_ID_02, -- Feb. Contract ID
  idt.PTD_CNTRCT_ID_03, -- Mar. Contract ID
  idt.PTD_CNTRCT_ID_04, -- Apr. Contract ID
  idt.PTD_CNTRCT_ID_05, -- May Contract ID
  idt.PTD_CNTRCT_ID_06, -- Jun. Contract ID
  idt.PTD_CNTRCT_ID_07, -- Jul. Contract ID
  idt.PTD_CNTRCT_ID_08, -- Aug. Contract ID
  idt.PTD_CNTRCT_ID_09, -- Sep. Contract ID
  idt.PTD_CNTRCT_ID_10, -- Oct. Contract ID
  idt.PTD_CNTRCT_ID_11, -- Nov. Contract ID
  idt.PTD_CNTRCT_ID_12, -- Dec. Contract ID
  idt.PTD_PBP_ID_01, -- Jan. Plan Benefit Package ID
  idt.PTD_PBP_ID_02, -- Feb. Plan Benefit Package ID
  idt.PTD_PBP_ID_03, -- Mar. Plan Benefit Package ID
  idt.PTD_PBP_ID_04, -- Apr. Plan Benefit Package ID
  idt.PTD_PBP_ID_05, -- May Plan Benefit Package ID
  idt.PTD_PBP_ID_06, -- Jun. Plan Benefit Package ID
  idt.PTD_PBP_ID_07, -- Jul. Plan Benefit Package ID
  idt.PTD_PBP_ID_08, -- Aug. Plan Benefit Package ID
  idt.PTD_PBP_ID_09, -- Sep. Plan Benefit Package ID
  idt.PTD_PBP_ID_10, -- Oct. Plan Benefit Package ID
  idt.PTD_PBP_ID_11, -- Nov. Plan Benefit Package ID
  idt.PTD_PBP_ID_12, -- Dec. Plan Benefit Package ID
  idt.PTD_SGMT_ID_01, -- Jan. Segment ID
  idt.PTD_SGMT_ID_02, -- Feb. Segment ID
  idt.PTD_SGMT_ID_03, -- Mar. Segment ID
  idt.PTD_SGMT_ID_04, -- Apr. Segment ID
  idt.PTD_SGMT_ID_05, -- May Segment ID
  idt.PTD_SGMT_ID_06, -- Jun. Segment ID
  idt.PTD_SGMT_ID_07, -- Jul. Segment ID
  idt.PTD_SGMT_ID_08, -- Aug. Segment ID
  idt.PTD_SGMT_ID_09, -- Sep. Segment ID
  idt.PTD_SGMT_ID_10, -- Oct. Segment ID
  idt.PTD_SGMT_ID_11, -- Nov. Segment ID
  idt.PTD_SGMT_ID_12, -- Dec. Segment ID
  idt.CST_SHR_GRP_CD_01, -- Jan. Cost Share Group Code
  idt.CST_SHR_GRP_CD_02, -- Feb. Cost Share Group Code
  idt.CST_SHR_GRP_CD_03, -- Mar. Cost Share Group Code
  idt.CST_SHR_GRP_CD_04, -- Apr. Cost Share Group Code
  idt.CST_SHR_GRP_CD_05, -- May Cost Share Group Code
  idt.CST_SHR_GRP_CD_06, -- Jun. Cost Share Group Code
  idt.CST_SHR_GRP_CD_07, -- Jul. Cost Share Group Code
  idt.CST_SHR_GRP_CD_08, -- Aug. Cost Share Group Code
  idt.CST_SHR_GRP_CD_09, -- Sep. Cost Share Group Code
  idt.CST_SHR_GRP_CD_10, -- Oct. Cost Share Group Code
  idt.CST_SHR_GRP_CD_11, -- Nov. Cost Share Group Code
  idt.CST_SHR_GRP_CD_12, -- Dec. Cost Share Group Code
  idt.RDS_IND_01, -- Jan. RDS Code - Retiree Drug Subsidy Code
  idt.RDS_IND_02, -- Feb. RDS Code - Retiree Drug Subsidy Code
  idt.RDS_IND_03, -- Mar. RDS Code - Retiree Drug Subsidy Code
  idt.RDS_IND_04, -- Apr. RDS Code - Retiree Drug Subsidy Code
  idt.RDS_IND_05, -- May RDS Code - Retiree Drug Subsidy Code
  idt.RDS_IND_06, -- Jun. RDS Code - Retiree Drug Subsidy Code
  idt.RDS_IND_07, -- Jul. RDS Code - Retiree Drug Subsidy Code
  idt.RDS_IND_08, -- Aug. RDS Code - Retiree Drug Subsidy Code
  idt.RDS_IND_09, -- Sep. RDS Code - Retiree Drug Subsidy Code
  idt.RDS_IND_10, -- Oct. RDS Code - Retiree Drug Subsidy Code
  idt.RDS_IND_11, -- Nov. RDS Code - Retiree Drug Subsidy Code
  idt.RDS_IND_12, -- Dec. RDS Code - Retiree Drug Subsidy Code
  idt.DUAL_STUS_CD_01, -- Jan. Dual Status Code
  idt.DUAL_STUS_CD_02, -- Feb. Dual Status Code
  idt.DUAL_STUS_CD_03, -- Mar. Dual Status Code
  idt.DUAL_STUS_CD_04, -- Apr. Dual Status Code
  idt.DUAL_STUS_CD_05, -- May Dual Status Code
  idt.DUAL_STUS_CD_06, -- Jun. Dual Status Code
  idt.DUAL_STUS_CD_07, -- Jul. Dual Status Code
  idt.DUAL_STUS_CD_08, -- Aug. Dual Status Code
  idt.DUAL_STUS_CD_09, -- Sep. Dual Status Code
  idt.DUAL_STUS_CD_10, -- Oct. Dual Status Code
  idt.DUAL_STUS_CD_11, -- Nov. Dual Status Code
  idt.DUAL_STUS_CD_12, -- Dec. Dual Status Code
  idt.EXTRACT_DT
from mbsf_d_cmpnts idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hospice_occurrnce_codes
select /*+ PARALLEL(hospice_occurrnce_codes,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.RLT_OCRNC_CD_SEQ, -- Claim Related Occurrence Code Sequence
  idt.CLM_RLT_OCRNC_CD, -- Claim Related Occurrence Code
  idt.CLM_RLT_OCRNC_DT + bm.date_shift_days CLM_RLT_OCRNC_DT, -- Claim Related Occurrence Date
  idt.EXTRACT_DT
from hospice_occurrnce_codes idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hha_value_codes
select /*+ PARALLEL(hha_value_codes,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.RLT_VAL_CD_SEQ, -- Claim Related Value Code Sequence
  idt.CLM_VAL_CD, -- Claim Value Code
  idt.CLM_VAL_AMT, -- Claim Value Amount
  idt.EXTRACT_DT
from hha_value_codes idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".bcarrier_claims
select /*+ PARALLEL(bcarrier_claims,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_NEAR_LINE_REC_IDENT_CD, -- NCH Near Line Record Identification Code
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.CLM_FROM_DT + bm.date_shift_days CLM_FROM_DT, -- Claim From Date
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date  (Determines Year of Claim)
  idt.NCH_WKLY_PROC_DT + bm.date_shift_days NCH_WKLY_PROC_DT, -- NCH Weekly Claim Processing Date
  idt.CARR_CLM_ENTRY_CD, -- Carrier Claim Entry Code
  idt.CLM_DISP_CD, -- Claim Disposition Code
  idt.CARR_NUM, -- Carrier Number
  idt.CARR_CLM_PMT_DNL_CD, -- Carrier Claim Payment Denial Code
  idt.CLM_PMT_AMT, -- Claim Payment Amount
  idt.CARR_CLM_PRMRY_PYR_PD_AMT, -- Carrier Claim Primary Payer Paid Amount
  idt.RFR_PHYSN_UPIN, -- Carrier Claim Refering Physician UPIN Number
  idt.RFR_PHYSN_NPI, -- Carrier Claim Refering Physician NPI Number
  idt.CARR_CLM_PRVDR_ASGNMT_IND_SW, -- Carrier Claim Provider Assignment Indicator Switch
  idt.NCH_CLM_PRVDR_PMT_AMT, -- NCH Claim Provider Payment Amount
  idt.NCH_CLM_BENE_PMT_AMT, -- NCH Claim Beneficiary Payment Amount
  idt.NCH_CARR_CLM_SBMTD_CHRG_AMT, -- NCH Carrier Claim Submitted Charge Amount
  idt.NCH_CARR_CLM_ALOWD_AMT, -- NCH Carrier Claim Allowed Charge Amount
  idt.CARR_CLM_CASH_DDCTBL_APLD_AMT, -- Carrier Claim Cash Deductible Applied Amount
  idt.CARR_CLM_HCPCS_YR_CD, -- Carrier Claim HCPCS Year Code
  idt.CARR_CLM_RFRNG_PIN_NUM, -- Carrier Claim Referring PIN Number
  idt.PRNCPAL_DGNS_CD, -- Primary Claim Diagnosis Code
  idt.PRNCPAL_DGNS_VRSN_CD, -- Primary Claim Diagnosis Code Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD1, -- Claim Diagnosis Code I
  idt.ICD_DGNS_VRSN_CD1, -- Claim Diagnosis Code I Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD2, -- Claim Diagnosis Code II
  idt.ICD_DGNS_VRSN_CD2, -- Claim Diagnosis Code II Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD3, -- Claim Diagnosis Code III
  idt.ICD_DGNS_VRSN_CD3, -- Claim Diagnosis Code III Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD4, -- Claim Diagnosis Code IV
  idt.ICD_DGNS_VRSN_CD4, -- Claim Diagnosis Code IV Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD5, -- Claim Diagnosis Code V
  idt.ICD_DGNS_VRSN_CD5, -- Claim Diagnosis Code V Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD6, -- Claim Diagnosis Code VI
  idt.ICD_DGNS_VRSN_CD6, -- Claim Diagnosis Code VI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD7, -- Claim Diagnosis Code VII
  idt.ICD_DGNS_VRSN_CD7, -- Claim Diagnosis Code VII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD8, -- Claim Diagnosis Code VIII
  idt.ICD_DGNS_VRSN_CD8, -- Claim Diagnosis Code VIII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD9, -- Claim Diagnosis Code IX
  idt.ICD_DGNS_VRSN_CD9, -- Claim Diagnosis Code IX Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD10, -- Claim Diagnosis Code X
  idt.ICD_DGNS_VRSN_CD10, -- Claim Diagnosis Code X Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD11, -- Claim Diagnosis Code XI
  idt.ICD_DGNS_VRSN_CD11, -- Claim Diagnosis Code XI Diagnosis Version Code (ICD-9 or ICD-10)
  idt.ICD_DGNS_CD12, -- Claim Diagnosis Code XII
  idt.ICD_DGNS_VRSN_CD12, -- Claim Diagnosis Code XII Diagnosis Version Code (ICD-9 or ICD-10)
  idt.CLM_CLNCL_TRIL_NUM, -- Clinical Trial Number
  case
    when bm.dob_shift_months is not null
    then add_months(DOB_DT, bm.dob_shift_months)
    else idt.DOB_DT + bm.date_shift_days
  end DOB_DT, -- Date of Birth from Claim (Date)
  idt.GNDR_CD, -- Gender Code from Claim
  idt.BENE_RACE_CD, -- Race Code from Claim
  NULL BENE_CNTY_CD, -- County Code from Claim (SSA)
  idt.BENE_STATE_CD, -- State Code from Claim (SSA)
  NULL BENE_MLG_CNTCT_ZIP_CD, -- Zip Code of Residence from Claim
  idt.EXTRACT_DT
from bcarrier_claims idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".pde
select /*+ PARALLEL(pde,12) */ 
  idt.PDE_ID, -- Encrypted 723 PDE ID
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.SRVC_DT + bm.date_shift_days SRVC_DT, -- RX Service Date (DOS)
  idt.SRVC_PRVDR_ID_QLFYR_CD, -- Service Provider ID Qualifier Code
  idt.SRVC_PRVDR_ID, -- Service Provider ID
  idt.PRSCRBR_ID_QLFYR_CD, -- Prescriber ID Qualifier Code
  idt.PRSCRBR_ID, -- Prescriber ID
  idt.PROD_SRVC_ID, -- Product Service ID
  idt.DAW_PROD_SLCTN_CD, -- Dispense as Written (DAW) Product Selection Code
  idt.QTY_DSPNSD_NUM, -- Quantity Dispensed
  idt.DAYS_SUPLY_NUM, -- Days Supply
  idt.DSPNSNG_STUS_CD, -- Dispensing Status Code
  idt.DRUG_CVRG_STUS_CD, -- Drug Coverage Status Code
  idt.GDC_BLW_OOPT_AMT, -- Gross Drug Cost Below Out-of-Pocket Threshold (GDCB)
  idt.GDC_ABV_OOPT_AMT, -- Gross Drug Cost Above Out-of-Pocket Threshold (GDCA)
  idt.PTNT_PAY_AMT, -- Patient Pay Amount
  idt.OTHR_TROOP_AMT, -- Other TrOOP Amount
  idt.LICS_AMT, -- Low Income Cost Sharing Subsidy Amount (LICS)
  idt.PLRO_AMT, -- Patient Liability Reduction Due to Other Payer Amount (PLRO)
  idt.CVRD_D_PLAN_PD_AMT, -- Covered D Plan Paid Amount (CPP)
  idt.NCVRD_PLAN_PD_AMT, -- Non-Covered Plan Paid Amount (NPP)
  idt.TOT_RX_CST_AMT, -- Gross Drug Cost
  idt.BN, -- Brand Name
  idt.GCDF, -- Dosage Form Code
  idt.GCDF_DESC, -- Dosage Form Code Description
  idt.STR, -- Drug Strength Description
  idt.GNN, -- Generic Name - Short Version
  idt.BENEFIT_PHASE, -- The benefit phase of the Part D Event
  idt.EXTRACT_DT
from pde idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hha_occurrnce_codes
select /*+ PARALLEL(hha_occurrnce_codes,12) */ 
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.RLT_OCRNC_CD_SEQ, -- Claim Related Occurrence Code Sequence
  idt.CLM_RLT_OCRNC_CD, -- Claim Related Occurrence Code
  idt.CLM_RLT_OCRNC_DT + bm.date_shift_days CLM_RLT_OCRNC_DT, -- Claim Related Occurrence Date
  idt.EXTRACT_DT
from hha_occurrnce_codes idt 
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;
