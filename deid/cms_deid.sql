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


insert /*+ APPEND */ into "&&deid_schema".outpatient_base_claims_k
select /*+ PARALLEL(outpatient_base_claims_k,12) */
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_NEAR_LINE_REC_IDENT_CD, -- NCH Near Line Record Identification Code
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.CLM_FROM_DT + bm.date_shift_days CLM_FROM_DT, -- Claim From Date
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date (Determines Year of Claim)
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
  idt.SRVC_LOC_NPI_NUM, -- Claim Service Location NPI Number
  idt.AT_PHYSN_UPIN, -- Claim Attending Physician UPIN Number
  idt.AT_PHYSN_NPI, -- Claim Attending Physician NPI Number
  idt.AT_PHYSN_SPCLTY_CD, -- Claim Attending Physician Specialty Code
  idt.OP_PHYSN_UPIN, -- Claim Operating Physician UPIN Number
  idt.OP_PHYSN_NPI, -- Claim Operating Physician NPI Number
  idt.OP_PHYSN_SPCLTY_CD, -- Claim Operating Physician Specialty Code
  idt.OT_PHYSN_UPIN, -- Claim Other Physician UPIN Number
  idt.OT_PHYSN_NPI, -- Claim Other Physician NPI Number
  idt.OT_PHYSN_SPCLTY_CD, -- Claim Other Physician Specialty Code
  idt.RNDRNG_PHYSN_NPI, -- Claim Rendering Physician NPI
  idt.RNDRNG_PHYSN_SPCLTY_CD, -- Claim Rendering Physician Specialty Code
  idt.RFR_PHYSN_NPI, -- Claim Referring Physician NPI
  idt.RFR_PHYSN_SPCLTY_CD, -- Claim Referring Physician Specialty Code
  idt.CLM_MCO_PD_SW, -- Claim MCO Paid Switch
  idt.PTNT_DSCHRG_STUS_CD, -- Patient Discharge Status Code
  idt.CLM_TOT_CHRG_AMT, -- Claim Total Charge Amount
  idt.NCH_BENE_BLOOD_DDCTBL_LBLTY_AM, -- NCH Beneficiary Blood Deductible Liability Amount
  idt.NCH_PROFNL_CMPNT_CHRG_AMT, -- NCH Professional Component Charge
  idt.PRNCPAL_DGNS_CD, -- Primary Claim Diagnosis Code
  idt.ICD_DGNS_CD1, -- Claim Diagnosis Code I
  idt.ICD_DGNS_CD2, -- Claim Diagnosis Code II
  idt.ICD_DGNS_CD3, -- Claim Diagnosis Code III
  idt.ICD_DGNS_CD4, -- Claim Diagnosis Code IV
  idt.ICD_DGNS_CD5, -- Claim Diagnosis Code V
  idt.ICD_DGNS_CD6, -- Claim Diagnosis Code VI
  idt.ICD_DGNS_CD7, -- Claim Diagnosis Code VII
  idt.ICD_DGNS_CD8, -- Claim Diagnosis Code VIII
  idt.ICD_DGNS_CD9, -- Claim Diagnosis Code IX
  idt.ICD_DGNS_CD10, -- Claim Diagnosis Code X
  idt.ICD_DGNS_CD11, -- Claim Diagnosis Code XI
  idt.ICD_DGNS_CD12, -- Claim Diagnosis Code XII
  idt.ICD_DGNS_CD13, -- Claim Diagnosis Code XIII
  idt.ICD_DGNS_CD14, -- Claim Diagnosis Code XIV
  idt.ICD_DGNS_CD15, -- Claim Diagnosis Code XV
  idt.ICD_DGNS_CD16, -- Claim Diagnosis Code XVI
  idt.ICD_DGNS_CD17, -- Claim Diagnosis Code XVII
  idt.ICD_DGNS_CD18, -- Claim Diagnosis Code XVIII
  idt.ICD_DGNS_CD19, -- Claim Diagnosis Code XIX
  idt.ICD_DGNS_CD20, -- Claim Diagnosis Code XX
  idt.ICD_DGNS_CD21, -- Claim Diagnosis Code XXI
  idt.ICD_DGNS_CD22, -- Claim Diagnosis Code XXII
  idt.ICD_DGNS_CD23, -- Claim Diagnosis Code XXIII
  idt.ICD_DGNS_CD24, -- Claim Diagnosis Code XXIV
  idt.ICD_DGNS_CD25, -- Claim Diagnosis Code XXV
  idt.FST_DGNS_E_CD, -- First Claim Diagnosis E Code
  idt.ICD_DGNS_E_CD1, -- Claim Diagnosis E Code I
  idt.ICD_DGNS_E_CD2, -- Claim Diagnosis E Code II
  idt.ICD_DGNS_E_CD3, -- Claim Diagnosis E Code III
  idt.ICD_DGNS_E_CD4, -- Claim Diagnosis E Code IV
  idt.ICD_DGNS_E_CD5, -- Claim Diagnosis E Code V
  idt.ICD_DGNS_E_CD6, -- Claim Diagnosis E Code VI
  idt.ICD_DGNS_E_CD7, -- Claim Diagnosis E Code VII
  idt.ICD_DGNS_E_CD8, -- Claim Diagnosis E Code VIII
  idt.ICD_DGNS_E_CD9, -- Claim Diagnosis E Code IX
  idt.ICD_DGNS_E_CD10, -- Claim Diagnosis E Code X
  idt.ICD_DGNS_E_CD11, -- Claim Diagnosis E Code XI
  idt.ICD_DGNS_E_CD12, -- Claim Diagnosis E Code XII
  idt.ICD_PRCDR_CD1, -- Claim Procedure Code I
  idt.PRCDR_DT1 + bm.date_shift_days PRCDR_DT1, -- Claim Procedure Code I Date
  idt.ICD_PRCDR_CD2, -- Claim Procedure Code II
  idt.PRCDR_DT2 + bm.date_shift_days PRCDR_DT2, -- Claim Procedure Code II Date
  idt.ICD_PRCDR_CD3, -- Claim Procedure Code III
  idt.PRCDR_DT3 + bm.date_shift_days PRCDR_DT3, -- Claim Procedure Code III Date
  idt.ICD_PRCDR_CD4, -- Claim Procedure Code IV
  idt.PRCDR_DT4 + bm.date_shift_days PRCDR_DT4, -- Claim Procedure Code IV Date
  idt.ICD_PRCDR_CD5, -- Claim Procedure Code V
  idt.PRCDR_DT5 + bm.date_shift_days PRCDR_DT5, -- Claim Procedure Code V Date
  idt.ICD_PRCDR_CD6, -- Claim Procedure Code VI
  idt.PRCDR_DT6 + bm.date_shift_days PRCDR_DT6, -- Claim Procedure Code VI Date
  idt.ICD_PRCDR_CD7, -- Claim Procedure Code VII
  idt.PRCDR_DT7 + bm.date_shift_days PRCDR_DT7, -- Claim Procedure Code VII Date
  idt.ICD_PRCDR_CD8, -- Claim Procedure Code VIII
  idt.PRCDR_DT8 + bm.date_shift_days PRCDR_DT8, -- Claim Procedure Code VIII Date
  idt.ICD_PRCDR_CD9, -- Claim Procedure Code IX
  idt.PRCDR_DT9 + bm.date_shift_days PRCDR_DT9, -- Claim Procedure Code IX Date
  idt.ICD_PRCDR_CD10, -- Claim Procedure Code X
  idt.PRCDR_DT10 + bm.date_shift_days PRCDR_DT10, -- Claim Procedure Code X Date
  idt.ICD_PRCDR_CD11, -- Claim Procedure Code XI
  idt.PRCDR_DT11 + bm.date_shift_days PRCDR_DT11, -- Claim Procedure Code XI Date
  idt.ICD_PRCDR_CD12, -- Claim Procedure Code XII
  idt.PRCDR_DT12 + bm.date_shift_days PRCDR_DT12, -- Claim Procedure Code XII Date
  idt.ICD_PRCDR_CD13, -- Claim Procedure Code XIII
  idt.PRCDR_DT13 + bm.date_shift_days PRCDR_DT13, -- Claim Procedure Code XIII Date
  idt.ICD_PRCDR_CD14, -- Claim Procedure Code XIV
  idt.PRCDR_DT14 + bm.date_shift_days PRCDR_DT14, -- Claim Procedure Code XIV Date
  idt.ICD_PRCDR_CD15, -- Claim Procedure Code XV
  idt.PRCDR_DT15 + bm.date_shift_days PRCDR_DT15, -- Claim Procedure Code XV Date
  idt.ICD_PRCDR_CD16, -- Claim Procedure Code XVI
  idt.PRCDR_DT16 + bm.date_shift_days PRCDR_DT16, -- Claim Procedure Code XVI Date
  idt.ICD_PRCDR_CD17, -- Claim Procedure Code XVII
  idt.PRCDR_DT17 + bm.date_shift_days PRCDR_DT17, -- Claim Procedure Code XVII Date
  idt.ICD_PRCDR_CD18, -- Claim Procedure Code XVIII
  idt.PRCDR_DT18 + bm.date_shift_days PRCDR_DT18, -- Claim Procedure Code XVIII Date
  idt.ICD_PRCDR_CD19, -- Claim Procedure Code XIX
  idt.PRCDR_DT19 + bm.date_shift_days PRCDR_DT19, -- Claim Procedure Code XIX Date
  idt.ICD_PRCDR_CD20, -- Claim Procedure Code XX
  idt.PRCDR_DT20 + bm.date_shift_days PRCDR_DT20, -- Claim Procedure Code XX Date
  idt.ICD_PRCDR_CD21, -- Claim Procedure Code XXI
  idt.PRCDR_DT21 + bm.date_shift_days PRCDR_DT21, -- Claim Procedure Code XXI Date
  idt.ICD_PRCDR_CD22, -- Claim Procedure Code XXII
  idt.PRCDR_DT22 + bm.date_shift_days PRCDR_DT22, -- Claim Procedure Code XXII Date
  idt.ICD_PRCDR_CD23, -- Claim Procedure Code XXIII
  idt.PRCDR_DT23 + bm.date_shift_days PRCDR_DT23, -- Claim Procedure Code XXIII Date
  idt.ICD_PRCDR_CD24, -- Claim Procedure Code XXIV
  idt.PRCDR_DT24 + bm.date_shift_days PRCDR_DT24, -- Claim Procedure Code XXIV Date
  idt.ICD_PRCDR_CD25, -- Claim Procedure Code XXV
  idt.PRCDR_DT25 + bm.date_shift_days PRCDR_DT25, -- Claim Procedure Code XXV Date
  idt.RSN_VISIT_CD1, -- Reason for Visit Diagnosis Code I
  idt.RSN_VISIT_CD2, -- Reason for Visit Diagnosis Code II
  idt.RSN_VISIT_CD3, -- Reason for Visit Diagnosis Code III
  idt.NCH_BENE_PTB_DDCTBL_AMT, -- NCH Beneficiary Part B Deductible Amount
  idt.NCH_BENE_PTB_COINSRNC_AMT, -- NCH Beneficiary Part B Coinsurance Amount
  idt.CLM_OP_PRVDR_PMT_AMT, -- Claim Outpatient Provider Payment Amount
  idt.CLM_OP_BENE_PMT_AMT, -- Claim Outpatient Beneficiary Payment Amount
  case
    when bm.dob_shift_months is not null
    then add_months(idt.DOB_DT, bm.dob_shift_months)
    else idt.DOB_DT + bm.date_shift_days
  end DOB_DT, -- Date of Birth from Claim (Date)
  idt.GNDR_CD, -- Gender Code from Claim
  idt.BENE_RACE_CD, -- Race Code from Claim
  NULL BENE_CNTY_CD, -- County Code from Claim (SSA)
  idt.BENE_STATE_CD, -- State Code from Claim (SSA)
  NULL BENE_MLG_CNTCT_ZIP_CD, -- Zip Code of Residence from Claim
  idt.CLM_MDCL_REC, -- Claim Medical Record Number
  idt.FI_CLM_ACTN_CD, -- FI Claim Action Code
  idt.NCH_BLOOD_PNTS_FRNSHD_QTY, -- NCH Blood Pints Furnished Quantity
  idt.CLM_TRTMT_AUTHRZTN_NUM, -- Claim Treatment Authorization Number
  idt.CLM_PRCR_RTRN_CD, -- Claim Pricer Return Code
  NULL CLM_SRVC_FAC_ZIP_CD, -- Claim Service Facility ZIP Code
  idt.CLM_OP_TRANS_TYPE_CD, -- Claim Outpatient Transaction Type Code
  idt.CLM_OP_ESRD_MTHD_CD, -- Claim Outpatient ESRD Method Of Reimbursement Code
  idt.CLM_NEXT_GNRTN_ACO_IND_CD1, -- Claim Next Generation Accountable Care Organization Indicator Code 1
  idt.CLM_NEXT_GNRTN_ACO_IND_CD2, -- Claim Next Generation Accountable Care Organization Indicator Code 2
  idt.CLM_NEXT_GNRTN_ACO_IND_CD3, -- Claim Next Generation Accountable Care Organization Indicator Code 3
  idt.CLM_NEXT_GNRTN_ACO_IND_CD4, -- Claim Next Generation Accountable Care Organization Indicator Code 4
  idt.CLM_NEXT_GNRTN_ACO_IND_CD5, -- Claim Next Generation Accountable Care Organization Indicator Code 5
  idt.ACO_ID_NUM, -- Claim Accountable Care Organization (ACO) Identification Number
  idt.EXTRACT_DT
from outpatient_base_claims_k idt
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hospice_base_claims_k
select /*+ PARALLEL(hospice_base_claims_k,12) */
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_NEAR_LINE_REC_IDENT_CD, -- NCH Near Line Record Identification Code
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.CLM_FROM_DT + bm.date_shift_days CLM_FROM_DT, -- Claim From Date
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date (Determines Year of Claim)
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
  idt.SRVC_LOC_NPI_NUM, -- Claim Service Location NPI Number
  idt.AT_PHYSN_UPIN, -- Claim Attending Physician UPIN Number
  idt.AT_PHYSN_NPI, -- Claim Attending Physician NPI Number
  idt.AT_PHYSN_SPCLTY_CD, -- Claim Attending Physician Specialty Code
  idt.OP_PHYSN_NPI, -- Claim Operating Physician NPI Number
  idt.OP_PHYSN_SPCLTY_CD, -- Claim Operating Physician Specialty Code
  idt.OT_PHYSN_NPI, -- Claim Other Physician NPI Number
  idt.OT_PHYSN_SPCLTY_CD, -- Claim Other Physician Specialty Code
  idt.RNDRNG_PHYSN_NPI, -- Claim Rendering Physician NPI
  idt.RNDRNG_PHYSN_SPCLTY_CD, -- Claim Rendering Physician Specialty Code
  idt.RFR_PHYSN_NPI, -- Claim Referring Physician NPI
  idt.RFR_PHYSN_SPCLTY_CD, -- Claim Referring Physician Specialty Code
  idt.PTNT_DSCHRG_STUS_CD, -- Patient Discharge Status Code
  idt.CLM_TOT_CHRG_AMT, -- Claim Total Charge Amount
  idt.NCH_PTNT_STATUS_IND_CD, -- NCH Patient Status Indicator Code
  idt.CLM_UTLZTN_DAY_CNT, -- Claim Utilization Day Count
  idt.NCH_BENE_DSCHRG_DT + bm.date_shift_days NCH_BENE_DSCHRG_DT, -- NCH Beneficiary Discharge Date
  idt.PRNCPAL_DGNS_CD, -- Primary Claim Diagnosis Code
  idt.ICD_DGNS_CD1, -- Claim Diagnosis Code I
  idt.ICD_DGNS_CD2, -- Claim Diagnosis Code II
  idt.ICD_DGNS_CD3, -- Claim Diagnosis Code III
  idt.ICD_DGNS_CD4, -- Claim Diagnosis Code IV
  idt.ICD_DGNS_CD5, -- Claim Diagnosis Code V
  idt.ICD_DGNS_CD6, -- Claim Diagnosis Code VI
  idt.ICD_DGNS_CD7, -- Claim Diagnosis Code VII
  idt.ICD_DGNS_CD8, -- Claim Diagnosis Code VIII
  idt.ICD_DGNS_CD9, -- Claim Diagnosis Code IX
  idt.ICD_DGNS_CD10, -- Claim Diagnosis Code X
  idt.ICD_DGNS_CD11, -- Claim Diagnosis Code XI
  idt.ICD_DGNS_CD12, -- Claim Diagnosis Code XII
  idt.ICD_DGNS_CD13, -- Claim Diagnosis Code XIII
  idt.ICD_DGNS_CD14, -- Claim Diagnosis Code XIV
  idt.ICD_DGNS_CD15, -- Claim Diagnosis Code XV
  idt.ICD_DGNS_CD16, -- Claim Diagnosis Code XVI
  idt.ICD_DGNS_CD17, -- Claim Diagnosis Code XVII
  idt.ICD_DGNS_CD18, -- Claim Diagnosis Code XVIII
  idt.ICD_DGNS_CD19, -- Claim Diagnosis Code XIX
  idt.ICD_DGNS_CD20, -- Claim Diagnosis Code XX
  idt.ICD_DGNS_CD21, -- Claim Diagnosis Code XXI
  idt.ICD_DGNS_CD22, -- Claim Diagnosis Code XXII
  idt.ICD_DGNS_CD23, -- Claim Diagnosis Code XXIII
  idt.ICD_DGNS_CD24, -- Claim Diagnosis Code XXIV
  idt.ICD_DGNS_CD25, -- Claim Diagnosis Code XXV
  idt.FST_DGNS_E_CD, -- First Claim Diagnosis E Code
  idt.ICD_DGNS_E_CD1, -- Claim Diagnosis E Code I
  idt.ICD_DGNS_E_CD2, -- Claim Diagnosis E Code II
  idt.ICD_DGNS_E_CD3, -- Claim Diagnosis E Code III
  idt.ICD_DGNS_E_CD4, -- Claim Diagnosis E Code IV
  idt.ICD_DGNS_E_CD5, -- Claim Diagnosis E Code V
  idt.ICD_DGNS_E_CD6, -- Claim Diagnosis E Code VI
  idt.ICD_DGNS_E_CD7, -- Claim Diagnosis E Code VII
  idt.ICD_DGNS_E_CD8, -- Claim Diagnosis E Code VIII
  idt.ICD_DGNS_E_CD9, -- Claim Diagnosis E Code IX
  idt.ICD_DGNS_E_CD10, -- Claim Diagnosis E Code X
  idt.ICD_DGNS_E_CD11, -- Claim Diagnosis E Code XI
  idt.ICD_DGNS_E_CD12, -- Claim Diagnosis E Code XII
  idt.CLM_HOSPC_START_DT_ID + bm.date_shift_days CLM_HOSPC_START_DT_ID, -- Claim Hospice Start Date
  idt.BENE_HOSPC_PRD_CNT, -- Beneficiary's Hospice Period Count
  case
    when bm.dob_shift_months is not null
    then add_months(idt.DOB_DT, bm.dob_shift_months)
    else idt.DOB_DT + bm.date_shift_days
  end DOB_DT, -- Date of Birth from Claim (Date)
  idt.GNDR_CD, -- Gender Code from Claim
  idt.BENE_RACE_CD, -- Race Code from Claim
  NULL BENE_CNTY_CD, -- County Code from Claim (SSA)
  idt.BENE_STATE_CD, -- State Code from Claim (SSA)
  NULL BENE_MLG_CNTCT_ZIP_CD, -- Zip Code of Residence from Claim
  idt.CLM_MDCL_REC, -- Claim Medical Record Number
  idt.CLAIM_QUERY_CODE, -- Claim Query Code
  idt.FI_CLM_ACTN_CD, -- FI Claim Action Code
  idt.CLM_TRTMT_AUTHRZTN_NUM, -- Claim Treatment Authorization Number
  idt.CLM_PRCR_RTRN_CD, -- Claim Pricer Return Code
  NULL CLM_SRVC_FAC_ZIP_CD, -- Claim Service Facility ZIP Code
  idt.CLM_NEXT_GNRTN_ACO_IND_CD1, -- Claim Next Generation Accountable Care Organization Indicator Code 1
  idt.CLM_NEXT_GNRTN_ACO_IND_CD2, -- Claim Next Generation Accountable Care Organization Indicator Code 2
  idt.CLM_NEXT_GNRTN_ACO_IND_CD3, -- Claim Next Generation Accountable Care Organization Indicator Code 3
  idt.CLM_NEXT_GNRTN_ACO_IND_CD4, -- Claim Next Generation Accountable Care Organization Indicator Code 4
  idt.CLM_NEXT_GNRTN_ACO_IND_CD5, -- Claim Next Generation Accountable Care Organization Indicator Code 5
  idt.ACO_ID_NUM, -- Claim Accountable Care Organization (ACO) Identification Number
  idt.EXTRACT_DT
from hospice_base_claims_k idt
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


insert /*+ APPEND */ into "&&deid_schema".mbsf_abcd_summary
select /*+ PARALLEL(mbsf_abcd_summary,12) */
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.BENE_ENROLLMT_REF_YR, -- Beneficiary Enrollment Reference Year
  idt.ENRL_SRC, -- Enrollment Source
  idt.SAMPLE_GROUP, -- Medicare 1, 5, or 20% Strict Sample Group Indicator
  idt.ENHANCED_FIVE_PERCENT_FLAG, -- Medicare Enhanced 5% Sample Indicator
  idt.CRNT_BIC_CD, -- Current Beneficiary Identification Code
  idt.STATE_CODE, -- SSA State Code
  NULL COUNTY_CD, -- SSA County Code
  NULL ZIP_CD, -- 5-digit ZIP Code
  NULL STATE_CNTY_FIPS_CD_01, -- FIPS State-County Code: January
  NULL STATE_CNTY_FIPS_CD_02, -- FIPS State-County Code: February
  NULL STATE_CNTY_FIPS_CD_03, -- FIPS State-County Code: March
  NULL STATE_CNTY_FIPS_CD_04, -- FIPS State-County Code: April
  NULL STATE_CNTY_FIPS_CD_05, -- FIPS State-County Code: May
  NULL STATE_CNTY_FIPS_CD_06, -- FIPS State-County Code: June
  NULL STATE_CNTY_FIPS_CD_07, -- FIPS State-County Code: July
  NULL STATE_CNTY_FIPS_CD_08, -- FIPS State-County Code: August
  NULL STATE_CNTY_FIPS_CD_09, -- FIPS State-County Code: September
  NULL STATE_CNTY_FIPS_CD_10, -- FIPS State-County Code: October
  NULL STATE_CNTY_FIPS_CD_11, -- FIPS State-County Code: November
  NULL STATE_CNTY_FIPS_CD_12, -- FIPS State-County Code: December
  idt.AGE_AT_END_REF_YR, -- Age at the End of the Reference Year
  case
    when bm.dob_shift_months is not null
    then add_months(idt.BENE_BIRTH_DT, bm.dob_shift_months)
    else idt.BENE_BIRTH_DT + bm.date_shift_days
  end BENE_BIRTH_DT, -- Beneficiary Date of Birth
  idt.VALID_DEATH_DT_SW, -- Valid Date of Death Switch
  idt.BENE_DEATH_DT + bm.date_shift_days BENE_DEATH_DT, -- Beneficiary Date of Death
  idt.SEX_IDENT_CD, -- Sex
  idt.BENE_RACE_CD, -- Beneficiary Race Code
  idt.RTI_RACE_CD, -- Research Triangle Institute (RTI) Race Code
  idt.COVSTART + bm.date_shift_days COVSTART, -- Medicare Coverage Start Date
  idt.ENTLMT_RSN_ORIG, -- Original Reason for Entitlement Code
  idt.ENTLMT_RSN_CURR, -- Current Reason for Entitlement Code
  idt.ESRD_IND, -- End-Stage Renal Disease (ESRD) Indicator
  idt.MDCR_STATUS_CODE_01, -- Medicare Status Code: January
  idt.MDCR_STATUS_CODE_02, -- Medicare Status Code: February
  idt.MDCR_STATUS_CODE_03, -- Medicare Status Code: March
  idt.MDCR_STATUS_CODE_04, -- Medicare Status Code: April
  idt.MDCR_STATUS_CODE_05, -- Medicare Status Code: May
  idt.MDCR_STATUS_CODE_06, -- Medicare Status Code: June
  idt.MDCR_STATUS_CODE_07, -- Medicare Status Code: July
  idt.MDCR_STATUS_CODE_08, -- Medicare Status Code: August
  idt.MDCR_STATUS_CODE_09, -- Medicare Status Code: September
  idt.MDCR_STATUS_CODE_10, -- Medicare Status Code: October
  idt.MDCR_STATUS_CODE_11, -- Medicare Status Code: November
  idt.MDCR_STATUS_CODE_12, -- Medicare Status Code: December
  idt.BENE_PTA_TRMNTN_CD, -- Part A Termination Code
  idt.BENE_PTB_TRMNTN_CD, -- Part B Termination Code
  idt.BENE_HI_CVRAGE_TOT_MONS, -- Hospital Insurance (HI) Coverage Months Count
  idt.BENE_SMI_CVRAGE_TOT_MONS, -- Supplemental Medical Insurance (SMI) Coverage Months Count
  idt.BENE_STATE_BUYIN_TOT_MONS, -- State Buy-In (SBI) Coverage Months
  idt.BENE_HMO_CVRAGE_TOT_MONS, -- Health Maintenance Organization (HMO) Coverage Months
  idt.PTD_PLAN_CVRG_MONS, -- Part D Contract Plan Coverage Months
  idt.RDS_CVRG_MONS, -- Retiree Drug Subsidy (RDS) Coverage Months
  idt.DUAL_ELGBL_MONS, -- Medicaid Dual Eligible Months
  idt.MDCR_ENTLMT_BUYIN_IND_01, -- Medicare Entitlement/ Buy-In Indicator: January
  idt.MDCR_ENTLMT_BUYIN_IND_02, -- Medicare Entitlement/ Buy-In Indicator: February
  idt.MDCR_ENTLMT_BUYIN_IND_03, -- Medicare Entitlement/ Buy-In Indicator: March
  idt.MDCR_ENTLMT_BUYIN_IND_04, -- Medicare Entitlement/ Buy-In Indicator: April
  idt.MDCR_ENTLMT_BUYIN_IND_05, -- Medicare Entitlement/ Buy-In Indicator: May
  idt.MDCR_ENTLMT_BUYIN_IND_06, -- Medicare Entitlement/ Buy-In Indicator: June
  idt.MDCR_ENTLMT_BUYIN_IND_07, -- Medicare Entitlement/ Buy-In Indicator: July
  idt.MDCR_ENTLMT_BUYIN_IND_08, -- Medicare Entitlement/ Buy-In Indicator: August
  idt.MDCR_ENTLMT_BUYIN_IND_09, -- Medicare Entitlement/ Buy-In Indicator: September
  idt.MDCR_ENTLMT_BUYIN_IND_10, -- Medicare Entitlement/ Buy-In Indicator: October
  idt.MDCR_ENTLMT_BUYIN_IND_11, -- Medicare Entitlement/ Buy-In Indicator: November
  idt.MDCR_ENTLMT_BUYIN_IND_12, -- Medicare Entitlement/ Buy-In Indicator: December
  idt.HMO_IND_01, -- HMO Indicator: January
  idt.HMO_IND_02, -- HMO Indicator: February
  idt.HMO_IND_03, -- HMO Indicator: March
  idt.HMO_IND_04, -- HMO Indicator: April
  idt.HMO_IND_05, -- HMO Indicator: May
  idt.HMO_IND_06, -- HMO Indicator: June
  idt.HMO_IND_07, -- HMO Indicator: July
  idt.HMO_IND_08, -- HMO Indicator: August
  idt.HMO_IND_09, -- HMO Indicator: September
  idt.HMO_IND_10, -- HMO Indicator: October
  idt.HMO_IND_11, -- HMO Indicator: November
  idt.HMO_IND_12, -- HMO Indicator: December
  idt.PTC_CNTRCT_ID_01, -- Part C Contract ID: January
  idt.PTC_CNTRCT_ID_02, -- Part C Contract ID: February
  idt.PTC_CNTRCT_ID_03, -- Part C Contract ID: March
  idt.PTC_CNTRCT_ID_04, -- Part C Contract ID: April
  idt.PTC_CNTRCT_ID_05, -- Part C Contract ID: May
  idt.PTC_CNTRCT_ID_06, -- Part C Contract ID: June
  idt.PTC_CNTRCT_ID_07, -- Part C Contract ID: July
  idt.PTC_CNTRCT_ID_08, -- Part C Contract ID: August
  idt.PTC_CNTRCT_ID_09, -- Part C Contract ID: September
  idt.PTC_CNTRCT_ID_10, -- Part C Contract ID: October
  idt.PTC_CNTRCT_ID_11, -- Part C Contract ID: November
  idt.PTC_CNTRCT_ID_12, -- Part C Contract ID: December
  idt.PTC_PBP_ID_01, -- Part C Plan Benefit Package ID: January
  idt.PTC_PBP_ID_02, -- Part C Plan Benefit Package ID: February
  idt.PTC_PBP_ID_03, -- Part C Plan Benefit Package ID: March
  idt.PTC_PBP_ID_04, -- Part C Plan Benefit Package ID: April
  idt.PTC_PBP_ID_05, -- Part C Plan Benefit Package ID: May
  idt.PTC_PBP_ID_06, -- Part C Plan Benefit Package ID: June
  idt.PTC_PBP_ID_07, -- Part C Plan Benefit Package ID: July
  idt.PTC_PBP_ID_08, -- Part C Plan Benefit Package ID: August
  idt.PTC_PBP_ID_09, -- Part C Plan Benefit Package ID: September
  idt.PTC_PBP_ID_10, -- Part C Plan Benefit Package ID: October
  idt.PTC_PBP_ID_11, -- Part C Plan Benefit Package ID: November
  idt.PTC_PBP_ID_12, -- Part C Plan Benefit Package ID: December
  idt.PTC_PLAN_TYPE_CD_01, -- Part C Plan Type Code: January
  idt.PTC_PLAN_TYPE_CD_02, -- Part C Plan Type Code: February
  idt.PTC_PLAN_TYPE_CD_03, -- Part C Plan Type Code: March
  idt.PTC_PLAN_TYPE_CD_04, -- Part C Plan Type Code: April
  idt.PTC_PLAN_TYPE_CD_05, -- Part C Plan Type Code: May
  idt.PTC_PLAN_TYPE_CD_06, -- Part C Plan Type Code: June
  idt.PTC_PLAN_TYPE_CD_07, -- Part C Plan Type Code: July
  idt.PTC_PLAN_TYPE_CD_08, -- Part C Plan Type Code: August
  idt.PTC_PLAN_TYPE_CD_09, -- Part C Plan Type Code: September
  idt.PTC_PLAN_TYPE_CD_10, -- Part C Plan Type Code: October
  idt.PTC_PLAN_TYPE_CD_11, -- Part C Plan Type Code: November
  idt.PTC_PLAN_TYPE_CD_12, -- Part C Plan Type Code: December
  idt.PTD_CNTRCT_ID_01, -- Part D Contract ID: January
  idt.PTD_CNTRCT_ID_02, -- Part D Contract ID: February
  idt.PTD_CNTRCT_ID_03, -- Part D Contract ID: March
  idt.PTD_CNTRCT_ID_04, -- Part D Contract ID: April
  idt.PTD_CNTRCT_ID_05, -- Part D Contract ID: May
  idt.PTD_CNTRCT_ID_06, -- Part D Contract ID: June
  idt.PTD_CNTRCT_ID_07, -- Part D Contract ID: July
  idt.PTD_CNTRCT_ID_08, -- Part D Contract ID: August
  idt.PTD_CNTRCT_ID_09, -- Part D Contract ID: September
  idt.PTD_CNTRCT_ID_10, -- Part D Contract ID: October
  idt.PTD_CNTRCT_ID_11, -- Part D Contract ID: November
  idt.PTD_CNTRCT_ID_12, -- Part D Contract ID: December
  idt.PTD_PBP_ID_01, -- Part D Plan Benefit Package ID: January
  idt.PTD_PBP_ID_02, -- Part D Plan Benefit Package ID: February
  idt.PTD_PBP_ID_03, -- Part D Plan Benefit Package ID: March
  idt.PTD_PBP_ID_04, -- Part D Plan Benefit Package ID: April
  idt.PTD_PBP_ID_05, -- Part D Plan Benefit Package ID: May
  idt.PTD_PBP_ID_06, -- Part D Plan Benefit Package ID: June
  idt.PTD_PBP_ID_07, -- Part D Plan Benefit Package ID: July
  idt.PTD_PBP_ID_08, -- Part D Plan Benefit Package ID: August
  idt.PTD_PBP_ID_09, -- Part D Plan Benefit Package ID: September
  idt.PTD_PBP_ID_10, -- Part D Plan Benefit Package ID: October
  idt.PTD_PBP_ID_11, -- Part D Plan Benefit Package ID: November
  idt.PTD_PBP_ID_12, -- Part D Plan Benefit Package ID: December
  idt.PTD_SGMT_ID_01, -- Part D Segment ID: January
  idt.PTD_SGMT_ID_02, -- Part D Segment ID: February
  idt.PTD_SGMT_ID_03, -- Part D Segment ID: March
  idt.PTD_SGMT_ID_04, -- Part D Segment ID: April
  idt.PTD_SGMT_ID_05, -- Part D Segment ID: May
  idt.PTD_SGMT_ID_06, -- Part D Segment ID: June
  idt.PTD_SGMT_ID_07, -- Part D Segment ID: July
  idt.PTD_SGMT_ID_08, -- Part D Segment ID: August
  idt.PTD_SGMT_ID_09, -- Part D Segment ID: September
  idt.PTD_SGMT_ID_10, -- Part D Segment ID: October
  idt.PTD_SGMT_ID_11, -- Part D Segment ID: November
  idt.PTD_SGMT_ID_12, -- Part D Segment ID: December
  idt.RDS_IND_01, -- Retiree Drug Subsidy Indicators: January
  idt.RDS_IND_02, -- Retiree Drug Subsidy Indicators: February
  idt.RDS_IND_03, -- Retiree Drug Subsidy Indicators: March
  idt.RDS_IND_04, -- Retiree Drug Subsidy Indicators: April
  idt.RDS_IND_05, -- Retiree Drug Subsidy Indicators: May
  idt.RDS_IND_06, -- Retiree Drug Subsidy Indicators: June
  idt.RDS_IND_07, -- Retiree Drug Subsidy Indicators: July
  idt.RDS_IND_08, -- Retiree Drug Subsidy Indicators: August
  idt.RDS_IND_09, -- Retiree Drug Subsidy Indicators: September
  idt.RDS_IND_10, -- Retiree Drug Subsidy Indicators: October
  idt.RDS_IND_11, -- Retiree Drug Subsidy Indicators: November
  idt.RDS_IND_12, -- Retiree Drug Subsidy Indicators: December
  idt.DUAL_STUS_CD_01, -- State Reported Dual Eligible Status Code: January
  idt.DUAL_STUS_CD_02, -- State Reported Dual Eligible Status Code: February
  idt.DUAL_STUS_CD_03, -- State Reported Dual Eligible Status Code: March
  idt.DUAL_STUS_CD_04, -- State Reported Dual Eligible Status Code: April
  idt.DUAL_STUS_CD_05, -- State Reported Dual Eligible Status Code: May
  idt.DUAL_STUS_CD_06, -- State Reported Dual Eligible Status Code: June
  idt.DUAL_STUS_CD_07, -- State Reported Dual Eligible Status Code: July
  idt.DUAL_STUS_CD_08, -- State Reported Dual Eligible Status Code: August
  idt.DUAL_STUS_CD_09, -- State Reported Dual Eligible Status Code: September
  idt.DUAL_STUS_CD_10, -- State Reported Dual Eligible Status Code: October
  idt.DUAL_STUS_CD_11, -- State Reported Dual Eligible Status Code: November
  idt.DUAL_STUS_CD_12, -- State Reported Dual Eligible Status Code: December
  idt.CST_SHR_GRP_CD_01, -- Cost Share Group Code: January
  idt.CST_SHR_GRP_CD_02, -- Cost Share Group Code: February
  idt.CST_SHR_GRP_CD_03, -- Cost Share Group Code: March
  idt.CST_SHR_GRP_CD_04, -- Cost Share Group Code: April
  idt.CST_SHR_GRP_CD_05, -- Cost Share Group Code: May
  idt.CST_SHR_GRP_CD_06, -- Cost Share Group Code: June
  idt.CST_SHR_GRP_CD_07, -- Cost Share Group Code: July
  idt.CST_SHR_GRP_CD_08, -- Cost Share Group Code: August
  idt.CST_SHR_GRP_CD_09, -- Cost Share Group Code: September
  idt.CST_SHR_GRP_CD_10, -- Cost Share Group Code: October
  idt.CST_SHR_GRP_CD_11, -- Cost Share Group Code: November
  idt.CST_SHR_GRP_CD_12, -- Cost Share Group Code: December
  idt.EXTRACT_DT
from mbsf_abcd_summary idt
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
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
    when idt.BENE_AGE_CNT is null then null
    when idt.BENE_AGE_CNT + round(months_between(idt.EXTRACT_DT, idt.ADMSN_DT)/12) > 89 then 89
    else idt.BENE_AGE_CNT
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
  idt.INTRNL_USE_FIL_DT_CD, -- For internal use only. Fiscal year/calendar year segments.
  idt.INTRNL_USE_SMPL_SIZE_CD, -- For internal use. MEDPAR sample size.
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
  idt.DGNS_E_1_CD, -- E Diagnosis Code 1 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_2_CD, -- E Diagnosis Code 2 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_3_CD, -- E Diagnosis Code 3 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_4_CD, -- E Diagnosis Code 4 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_5_CD, -- E Diagnosis Code 5 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_6_CD, -- E Diagnosis Code 6 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_7_CD, -- E Diagnosis Code 7 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_8_CD, -- E Diagnosis Code 8 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_9_CD, -- E Diagnosis Code 9 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_10_CD, -- E Diagnosis Code 10 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_11_CD, -- E Diagnosis Code 11 - Extrnl cause of injury, poisoning, or oth adverse effect
  idt.DGNS_E_12_CD, -- E Diagnosis Code 12 - Extrnl cause of injury, poisoning, or oth adverse effect
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
  idt.SRGCL_PRCDR_VRSN_CD, -- MEDPAR Surgical Procedure Version Code (Earlier Version)
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
  idt.HAC_RDCTN_PMT_AMT, -- Hospital Acquired Conditions Reduction Payment Amount (IPPS_FLEX_PYMT_6_AMT)
  idt.IPPS_FLEX_PYMT_7_AMT, -- IPPS Flexible Payment Amount II
  idt.PTNT_ADD_ON_PYMT_AMT, -- Revenue Center Patient/Initial Visit Add-On Amount
  idt.HAC_PGM_RDCTN_IND_SW, -- Hospital Acquired Conditions (HAC) Program Reduction Indicator Switch
  idt.PGM_RDCTN_IND_SW, -- Electronic Health Records (EHR) Program Reduction Indicator Switch
  idt.PA_IND_CD, -- Claim Prior Authorization Indicator Code
  idt.UNIQ_TRKNG_NUM, -- Claim Unique Tracking Number
  idt.STAY_2_IND_SW, -- Stay 2 Indicator Switch
  idt.CLM_SITE_NTRL_PYMT_CST_AMT, -- Claim Site Neutral Payment Based on Cost Amount
  idt.CLM_SITE_NTRL_PYMT_IPPS_AMT, -- Claim Site Neutral Payment Based on IPPS Amount
  idt.CLM_FULL_STD_PYMT_AMT, -- Claim Full Standard Payment Amount
  idt.CLM_SS_OUTLIER_STD_PYMT_AMT, -- Claim Short Stay Outlier (SSO) Standard Payment Amount
  idt.CLM_NGACO_IND_1_CD, -- Claim Next Generation (NG) Accountable Care Organization (ACO) Indicator Code 1
  idt.CLM_NGACO_IND_2_CD, -- Claim Next Generation (NG) Accountable Care Organization (ACO) Indicator Code 2
  idt.CLM_NGACO_IND_3_CD, -- Claim Next Generation (NG) Accountable Care Organization (ACO) Indicator Code 3
  idt.CLM_NGACO_IND_4_CD, -- Claim Next Generation (NG) Accountable Care Organization (ACO) Indicator Code 4
  idt.CLM_NGACO_IND_5_CD, -- Claim Next Generation (NG) Accountable Care Organization (ACO) Indicator Code 5
  idt.CLM_RSDL_PYMT_IND_CD, -- Claim Residual Payment Indicator Code
  idt.CLM_RP_IND_CD, -- Claim Representative Payee (RP) Indicator Code
  idt.RC_RP_IND_CD, -- Revenue Center Representative Payee (RP) Indicator Code
  idt.EXTRACT_DT
from medpar_all idt
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hha_revenue_center_k
select /*+ PARALLEL(hha_revenue_center_k,12) */
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date (Determines Year of Claim)
  idt.CLM_LINE_NUM, -- Claim Line Number
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.REV_CNTR, -- Revenue Center Code
  idt.REV_CNTR_DT + bm.date_shift_days REV_CNTR_DT, -- Revenue Center Date
  idt.REV_CNTR_1ST_ANSI_CD, -- Revenue Center 1st ANSI Code
  idt.REV_CNTR_APC_HIPPS_CD, -- Revenue Center APC/HIPPS
  idt.HCPCS_CD, -- Revenue Center Healthcare Common Procedure Coding System
  idt.HCPCS_1ST_MDFR_CD, -- Revenue Center HCPCS Initial Modifier Code
  idt.HCPCS_2ND_MDFR_CD, -- Revenue Center HCPCS Second Modifier Code
  idt.HCPCS_3RD_MDFR_CD, -- Revenue Center HCPCS Third Modifier Code
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
  idt.RNDRNG_PHYSN_SPCLTY_CD, -- Revenue Center Rendering Physician Specialty Code
  idt.REV_CNTR_DSCNT_IND_CD, -- Revenue Center Discount Indicator Code
  idt.REV_CNTR_IDE_NDC_UPC_NUM, -- Revenue Center IDE, NDC, UPC Number
  idt.REV_CNTR_PRVDR_PMT_AMT, -- Revenue Center Provider Payment Amount
  idt.REV_CNTR_PTNT_RSPNSBLTY_PMT, -- Revenue Center Patient Responsibility Payment
  idt.REV_CNTR_PRCNG_IND_CD, -- Revenue Center Pricing Indicator Code
  idt.THRPY_CAP_IND_CD1, -- Revenue Center Therapy Cap Indicator Code 1
  idt.THRPY_CAP_IND_CD2, -- Revenue Center Therapy Cap Indicator Code 2
  idt.EXTRACT_DT
from hha_revenue_center_k idt
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


insert /*+ APPEND */ into "&&deid_schema".bcarrier_line_k
select /*+ PARALLEL(bcarrier_line_k,12) */
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.LINE_NUM, -- Claim Line Number
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date (Determines Year of Claim)
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
  idt.CARR_LINE_CL_CHRG_AMT, -- Carrier Line Clinical Lab Charge Amount
  NULL PHYSN_ZIP_CD, -- Line Place Of Service (POS) Physician Zip Code
  idt.LINE_OTHR_APLD_IND_CD1, -- Line Other Applied Indicator Code 1
  idt.LINE_OTHR_APLD_IND_CD2, -- Line Other Applied Indicator Code 2
  idt.LINE_OTHR_APLD_IND_CD3, -- Line Other Applied Indicator Code 3
  idt.LINE_OTHR_APLD_IND_CD4, -- Line Other Applied Indicator Code 4
  idt.LINE_OTHR_APLD_IND_CD5, -- Line Other Applied Indicator Code 5
  idt.LINE_OTHR_APLD_IND_CD6, -- Line Other Applied Indicator Code 6
  idt.LINE_OTHR_APLD_IND_CD7, -- Line Other Applied Indicator Code 7
  idt.LINE_OTHR_APLD_AMT1, -- Line Other Applied Amount 1
  idt.LINE_OTHR_APLD_AMT2, -- Line Other Applied Amount 2
  idt.LINE_OTHR_APLD_AMT3, -- Line Other Applied Amount 3
  idt.LINE_OTHR_APLD_AMT4, -- Line Other Applied Amount 4
  idt.LINE_OTHR_APLD_AMT5, -- Line Other Applied Amount 5
  idt.LINE_OTHR_APLD_AMT6, -- Line Other Applied Amount 6
  idt.LINE_OTHR_APLD_AMT7, -- Line Other Applied Amount 7
  idt.THRPY_CAP_IND_CD1, -- Line Therapy Cap Indicator Code 1
  idt.THRPY_CAP_IND_CD2, -- Line Therapy Cap Indicator Code 2
  idt.THRPY_CAP_IND_CD3, -- Line Therapy Cap Indicator Code 3
  idt.THRPY_CAP_IND_CD4, -- Line Therapy Cap Indicator Code 4
  idt.THRPY_CAP_IND_CD5, -- Line Therapy Cap Indicator Code 5
  idt.CLM_NEXT_GNRTN_ACO_IND_CD1, -- Claim Next Generation Accountable Care Organization Indicator Code 1
  idt.CLM_NEXT_GNRTN_ACO_IND_CD2, -- Claim Next Generation Accountable Care Organization Indicator Code 2
  idt.CLM_NEXT_GNRTN_ACO_IND_CD3, -- Claim Next Generation Accountable Care Organization Indicator Code 3
  idt.CLM_NEXT_GNRTN_ACO_IND_CD4, -- Claim Next Generation Accountable Care Organization Indicator Code 4
  idt.CLM_NEXT_GNRTN_ACO_IND_CD5, -- Claim Next Generation Accountable Care Organization Indicator Code 5
  idt.EXTRACT_DT
from bcarrier_line_k idt
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hha_base_claims_k
select /*+ PARALLEL(hha_base_claims_k,12) */
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_NEAR_LINE_REC_IDENT_CD, -- NCH Near Line Record Identification Code
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.CLM_FROM_DT + bm.date_shift_days CLM_FROM_DT, -- Claim From Date
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date (Determines Year of Claim)
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
  idt.SRVC_LOC_NPI_NUM, -- Claim Service Location NPI Number
  idt.AT_PHYSN_UPIN, -- Claim Attending Physician UPIN Number
  idt.AT_PHYSN_NPI, -- Claim Attending Physician NPI Number
  idt.AT_PHYSN_SPCLTY_CD, -- Claim Attending Physician Specialty Code
  idt.OP_PHYSN_NPI, -- Claim Operating Physician NPI Number
  idt.OP_PHYSN_SPCLTY_CD, -- Claim Operating Physician Specialty Code
  idt.OT_PHYSN_NPI, -- Claim Other Physician NPI Number
  idt.OT_PHYSN_SPCLTY_CD, -- Claim Other Physician Specialty Code
  idt.RNDRNG_PHYSN_NPI, -- Claim Rendering Physician NPI
  idt.RNDRNG_PHYSN_SPCLTY_CD, -- Claim Rendering Physician Specialty Code
  idt.RFR_PHYSN_NPI, -- Claim Referring Physician NPI
  idt.RFR_PHYSN_SPCLTY_CD, -- Claim Referring Physician Specialty Code
  idt.PTNT_DSCHRG_STUS_CD, -- Patient Discharge Status Code
  idt.CLM_PPS_IND_CD, -- Claim PPS Indicator Code
  idt.CLM_TOT_CHRG_AMT, -- Claim Total Charge Amount
  idt.PRNCPAL_DGNS_CD, -- Primary Claim Diagnosis Code
  idt.ICD_DGNS_CD1, -- Claim Diagnosis Code I
  idt.ICD_DGNS_CD2, -- Claim Diagnosis Code II
  idt.ICD_DGNS_CD3, -- Claim Diagnosis Code III
  idt.ICD_DGNS_CD4, -- Claim Diagnosis Code IV
  idt.ICD_DGNS_CD5, -- Claim Diagnosis Code V
  idt.ICD_DGNS_CD6, -- Claim Diagnosis Code VI
  idt.ICD_DGNS_CD7, -- Claim Diagnosis Code VII
  idt.ICD_DGNS_CD8, -- Claim Diagnosis Code VIII
  idt.ICD_DGNS_CD9, -- Claim Diagnosis Code IX
  idt.ICD_DGNS_CD10, -- Claim Diagnosis Code X
  idt.ICD_DGNS_CD11, -- Claim Diagnosis Code XI
  idt.ICD_DGNS_CD12, -- Claim Diagnosis Code XII
  idt.ICD_DGNS_CD13, -- Claim Diagnosis Code XIII
  idt.ICD_DGNS_CD14, -- Claim Diagnosis Code XIV
  idt.ICD_DGNS_CD15, -- Claim Diagnosis Code XV
  idt.ICD_DGNS_CD16, -- Claim Diagnosis Code XVI
  idt.ICD_DGNS_CD17, -- Claim Diagnosis Code XVII
  idt.ICD_DGNS_CD18, -- Claim Diagnosis Code XVIII
  idt.ICD_DGNS_CD19, -- Claim Diagnosis Code XIX
  idt.ICD_DGNS_CD20, -- Claim Diagnosis Code XX
  idt.ICD_DGNS_CD21, -- Claim Diagnosis Code XXI
  idt.ICD_DGNS_CD22, -- Claim Diagnosis Code XXII
  idt.ICD_DGNS_CD23, -- Claim Diagnosis Code XXIII
  idt.ICD_DGNS_CD24, -- Claim Diagnosis Code XXIV
  idt.ICD_DGNS_CD25, -- Claim Diagnosis Code XXV
  idt.FST_DGNS_E_CD, -- First Claim Diagnosis E Code
  idt.ICD_DGNS_E_CD1, -- Claim Diagnosis E Code I
  idt.ICD_DGNS_E_CD2, -- Claim Diagnosis E Code II
  idt.ICD_DGNS_E_CD3, -- Claim Diagnosis E Code III
  idt.ICD_DGNS_E_CD4, -- Claim Diagnosis E Code IV
  idt.ICD_DGNS_E_CD5, -- Claim Diagnosis E Code V
  idt.ICD_DGNS_E_CD6, -- Claim Diagnosis E Code VI
  idt.ICD_DGNS_E_CD7, -- Claim Diagnosis E Code VII
  idt.ICD_DGNS_E_CD8, -- Claim Diagnosis E Code VIII
  idt.ICD_DGNS_E_CD9, -- Claim Diagnosis E Code IX
  idt.ICD_DGNS_E_CD10, -- Claim Diagnosis E Code X
  idt.ICD_DGNS_E_CD11, -- Claim Diagnosis E Code XI
  idt.ICD_DGNS_E_CD12, -- Claim Diagnosis E Code XII
  idt.CLM_HHA_LUPA_IND_CD, -- Claim HHA Low Utilization Payment Adjustment (LUPA) Indicator Code
  idt.CLM_HHA_RFRL_CD, -- Claim HHA Referral Code
  idt.CLM_HHA_TOT_VISIT_CNT, -- Claim HHA Total Visit Count
  idt.CLM_ADMSN_DT + bm.date_shift_days CLM_ADMSN_DT, -- Claim HHA Care Start Date
  case
    when bm.dob_shift_months is not null
    then add_months(idt.DOB_DT, bm.dob_shift_months)
    else idt.DOB_DT + bm.date_shift_days
  end DOB_DT, -- Date of Birth from Claim (Date)
  idt.GNDR_CD, -- Gender Code from Claim
  idt.BENE_RACE_CD, -- Race Code from Claim
  NULL BENE_CNTY_CD, -- County Code from Claim (SSA)
  idt.BENE_STATE_CD, -- State Code from Claim (SSA)
  NULL BENE_MLG_CNTCT_ZIP_CD, -- Zip Code of Residence from Claim
  idt.CLM_MDCL_REC, -- Claim Medical Record Number
  idt.CLAIM_QUERY_CODE, -- Claim Query Code
  idt.FI_CLM_ACTN_CD, -- FI Claim Action Code
  idt.CLM_MCO_PD_SW, -- Claim MCO Paid Switch
  idt.NCH_BENE_DSCHRG_DT + bm.date_shift_days NCH_BENE_DSCHRG_DT, -- NCH Beneficiary Discharge Date
  idt.CLM_TRTMT_AUTHRZTN_NUM, -- Claim Treatment Authorization Number
  idt.CLM_PRCR_RTRN_CD, -- Claim Pricer Return Code
  NULL CLM_SRVC_FAC_ZIP_CD, -- Claim Service Facility ZIP Code
  idt.CLM_NEXT_GNRTN_ACO_IND_CD1, -- Claim Next Generation Accountable Care Organization Indicator Code 1
  idt.CLM_NEXT_GNRTN_ACO_IND_CD2, -- Claim Next Generation Accountable Care Organization Indicator Code 2
  idt.CLM_NEXT_GNRTN_ACO_IND_CD3, -- Claim Next Generation Accountable Care Organization Indicator Code 3
  idt.CLM_NEXT_GNRTN_ACO_IND_CD4, -- Claim Next Generation Accountable Care Organization Indicator Code 4
  idt.CLM_NEXT_GNRTN_ACO_IND_CD5, -- Claim Next Generation Accountable Care Organization Indicator Code 5
  idt.ACO_ID_NUM, -- Claim Accountable Care Organization (ACO) Identification Number
  idt.EXTRACT_DT
from hha_base_claims_k idt
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hospice_revenue_center_k
select /*+ PARALLEL(hospice_revenue_center_k,12) */
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date (Determines Year of Claim)
  idt.CLM_LINE_NUM, -- Claim Line Number
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.REV_CNTR, -- Revenue Center Code
  idt.REV_CNTR_DT + bm.date_shift_days REV_CNTR_DT, -- Revenue Center Date
  idt.HCPCS_CD, -- Revenue Center Healthcare Common Procedure Coding System
  idt.HCPCS_1ST_MDFR_CD, -- Revenue Center HCPCS Initial Modifier Code
  idt.HCPCS_2ND_MDFR_CD, -- Revenue Center HCPCS Second Modifier Code
  idt.HCPCS_3RD_MDFR_CD, -- Revenue Center HCPCS Third Modifier Code
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
  idt.RNDRNG_PHYSN_SPCLTY_CD, -- Revenue Center Rendering Physician Specialty Code
  idt.REV_CNTR_IDE_NDC_UPC_NUM, -- Revenue Center IDE, NDC, UPC Number
  idt.REV_CNTR_STUS_IND_CD, -- Revenue Center Status Indicator Code
  idt.REV_CNTR_PRCNG_IND_CD, -- Revenue Center Pricing Indicator Code
  idt.THRPY_CAP_IND_CD1, -- Revenue Center Therapy Cap Indicator Code 1
  idt.THRPY_CAP_IND_CD2, -- Revenue Center Therapy Cap Indicator Code 2
  idt.EXTRACT_DT
from hospice_revenue_center_k idt
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


insert /*+ APPEND */ into "&&deid_schema".outpatient_revenue_center_k
select /*+ PARALLEL(outpatient_revenue_center_k,12) */
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date (Determines Year of Claim)
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
  idt.HCPCS_3RD_MDFR_CD, -- Revenue Center HCPCS Third Modifier Code
  idt.HCPCS_4TH_MDFR_CD, -- Revenue Center HCPCS Fourth Modifier Code
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
  idt.RNDRNG_PHYSN_SPCLTY_CD, -- Revenue Center Rendering Physician Specialty Code
  idt.REV_CNTR_DDCTBL_COINSRNC_CD, -- Revenue Center Deductible Coinsurance Code
  idt.REV_CNTR_PRCNG_IND_CD, -- Revenue Center Pricing Indicator Code
  idt.THRPY_CAP_IND_CD1, -- Revenue Center Therapy Cap Indicator Code 1
  idt.THRPY_CAP_IND_CD2, -- Revenue Center Therapy Cap Indicator Code 2
  idt.RC_PTNT_ADD_ON_PYMT_AMT, -- Revenue Center Patient/Initial Visit Add-On Payment Amount
  idt.EXTRACT_DT
from outpatient_revenue_center_k idt
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


insert /*+ APPEND */ into "&&deid_schema".bcarrier_claims_k
select /*+ PARALLEL(bcarrier_claims_k,12) */
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_NEAR_LINE_REC_IDENT_CD, -- NCH Near Line Record Identification Code
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.CLM_FROM_DT + bm.date_shift_days CLM_FROM_DT, -- Claim From Date
  idt.CLM_THRU_DT + bm.date_shift_days CLM_THRU_DT, -- Claim Through Date (Determines Year of Claim)
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
    then add_months(idt.DOB_DT, bm.dob_shift_months)
    else idt.DOB_DT + bm.date_shift_days
  end DOB_DT, -- Date of Birth from Claim (Date)
  idt.GNDR_CD, -- Gender Code from Claim
  idt.BENE_RACE_CD, -- Race Code from Claim
  NULL BENE_CNTY_CD, -- County Code from Claim (SSA)
  idt.BENE_STATE_CD, -- State Code from Claim (SSA)
  NULL BENE_MLG_CNTCT_ZIP_CD, -- Zip Code of Residence from Claim
  idt.CLM_BENE_PD_AMT, -- Carrier Claim Beneficiary Paid Amount
  idt.CPO_PRVDR_NUM, -- Care Plan Oversight (CPO) Provider Number
  idt.CPO_ORG_NPI_NUM, -- CPO Organization NPI Number
  idt.CARR_CLM_BLG_NPI_NUM, -- Carrier Claim Billing NPI Number
  idt.ACO_ID_NUM, -- Claim Accountable Care Organization (ACO) Identification Number
  idt.EXTRACT_DT
from bcarrier_claims_k idt
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



insert /*+ APPEND */ into "&&deid_schema".bcarrier_demo_codes
select /*+ PARALLEL(bcarrier_demo_codes,12) */
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.DEMO_ID_SQNC_NUM, -- Claim Demonstration Sequence
  idt.DEMO_ID_NUM, -- Claim Demonstration Identification Number
  idt.DEMO_INFO_TXT, -- Claim Demonstration Information Text
  idt.EXTRACT_DT
from bcarrier_demo_codes idt
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hha_demo_codes
select /*+ PARALLEL(hha_demo_codes,12) */
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.DEMO_ID_SQNC_NUM, -- Claim Demonstration Sequence
  idt.DEMO_ID_NUM, -- Claim Demonstration Identification Number
  idt.DEMO_INFO_TXT, -- Claim Demonstration Information Text
  idt.EXTRACT_DT
from hha_demo_codes idt
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".hospice_demo_codes
select /*+ PARALLEL(hospice_demo_codes,12) */
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.DEMO_ID_SQNC_NUM, -- Claim Demonstration Sequence
  idt.DEMO_ID_NUM, -- Claim Demonstration Identification Number
  idt.DEMO_INFO_TXT, -- Claim Demonstration Information Text
  idt.EXTRACT_DT
from hospice_demo_codes idt
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;


insert /*+ APPEND */ into "&&deid_schema".outpatient_demo_codes
select /*+ PARALLEL(outpatient_demo_codes,12) */
  bm.BENE_ID_DEID BENE_ID, -- Encrypted 723 Beneficiary ID
  idt.CLM_ID, -- Encrypted Claim ID
  idt.NCH_CLM_TYPE_CD, -- NCH Claim Type Code
  idt.DEMO_ID_SQNC_NUM, -- Claim Demonstration Sequence
  idt.DEMO_ID_NUM, -- Claim Demonstration Identification Number
  idt.DEMO_INFO_TXT, -- Claim Demonstration Information Text
  idt.EXTRACT_DT
from outpatient_demo_codes idt
join bene_id_mapping bm on bm.bene_id = idt.bene_id;
commit;
