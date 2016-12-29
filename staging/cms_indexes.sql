/* Indexes below generated with the following SQL:
with 
  user_schema as (
  select 'CMS_ID' s from dual
  ),
parts as (
  select * from (
    select 
      'CMS_ID_INDEX_' || to_char(ora_hash(table_name || column_name)) idx_name,
      table_name, column_name
    from all_tab_cols
    cross join user_schema
    where owner = s and column_name like '%_ID'
    )
  )
select 
  'create index ' || idx_name || ' on ' || table_name || '(' || column_name || ') tablespace ' || s || 
  ' parallel (degree 24) nologging;' idx_sql
  --, length(idx_name)
from parts cross join user_schema
order by table_name, column_name;
*/
create index CMS_ID_INDEX_2833021899 on BCARRIER_CLAIMS(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_643850222 on BCARRIER_CLAIMS(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_2558741578 on BCARRIER_LINE(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1715148373 on BCARRIER_LINE(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_670868645 on HHA_BASE_CLAIMS(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_381742070 on HHA_BASE_CLAIMS(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_610451425 on HHA_CONDITION_CODES(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_502227026 on HHA_CONDITION_CODES(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1660047909 on HHA_OCCURRNCE_CODES(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_897413642 on HHA_OCCURRNCE_CODES(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_2911753874 on HHA_REVENUE_CENTER(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_871361390 on HHA_REVENUE_CENTER(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_4230122522 on HHA_SPAN_CODES(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_316818514 on HHA_SPAN_CODES(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_3043286956 on HHA_VALUE_CODES(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_2703891228 on HHA_VALUE_CODES(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_3224903465 on HOSPICE_BASE_CLAIMS(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_974843338 on HOSPICE_BASE_CLAIMS(CLM_HOSPC_START_DT_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_3568490022 on HOSPICE_BASE_CLAIMS(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_2956233842 on HOSPICE_CONDITION_CODES(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_2549928598 on HOSPICE_CONDITION_CODES(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1442300683 on HOSPICE_OCCURRNCE_CODES(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1069230177 on HOSPICE_OCCURRNCE_CODES(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1350784193 on HOSPICE_REVENUE_CENTER(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_2742199571 on HOSPICE_REVENUE_CENTER(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_2974230073 on HOSPICE_SPAN_CODES(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_744823196 on HOSPICE_SPAN_CODES(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_505916541 on HOSPICE_VALUE_CODES(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1915989662 on HOSPICE_VALUE_CODES(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_2751855087 on MAXDATA_IP(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_3869193368 on MAXDATA_IP(MSIS_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_183217185 on MAXDATA_IP(PHP_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_3500432670 on MAXDATA_LT(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1066952388 on MAXDATA_LT(MSIS_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_2247300306 on MAXDATA_LT(PHP_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_965975250 on MAXDATA_OT(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1764576864 on MAXDATA_OT(MSIS_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_4025901265 on MAXDATA_OT(PHP_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1482819696 on MAXDATA_PS(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_514483213 on MAXDATA_PS(MSIS_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1208226005 on MAXDATA_RX(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_53645694 on MAXDATA_RX(MSIS_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_19021131 on MAXDATA_RX(PHP_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_343700305 on MBSF_AB_SUMMARY(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_2296744901 on MBSF_D_CMPNTS(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1329757078 on MEDPAR_ALL(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1083602265 on MEDPAR_ALL(MEDPAR_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_3309932752 on OUTPATIENT_BASE_CLAIMS(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_4037417918 on OUTPATIENT_BASE_CLAIMS(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_2889783893 on OUTPATIENT_CONDITION_CODES(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1104111205 on OUTPATIENT_CONDITION_CODES(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_3142250526 on OUTPATIENT_OCCURRNCE_CODES(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1877306875 on OUTPATIENT_OCCURRNCE_CODES(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_873202168 on OUTPATIENT_REVENUE_CENTER(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_967624971 on OUTPATIENT_REVENUE_CENTER(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1221688439 on OUTPATIENT_SPAN_CODES(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_2390127758 on OUTPATIENT_SPAN_CODES(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_3852131160 on OUTPATIENT_VALUE_CODES(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_3508982497 on OUTPATIENT_VALUE_CODES(CLM_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_2080469128 on PDE(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_4044377052 on PDE(PDE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_3174887975 on PDE(PROD_SRVC_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_4001382458 on PDE(PRSCRBR_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1900484016 on PDE(SRVC_PRVDR_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_2146258142 on PDE_SAF(BENE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_3583284622 on PDE_SAF(PDE_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_2535082669 on PDE_SAF(PROD_SRVC_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_156252097 on PDE_SAF(PRSCRBR_ID) tablespace CMS_ID parallel (degree 24) nologging;
create index CMS_ID_INDEX_1008038595 on PDE_SAF(SRVC_PRVDR_ID) tablespace CMS_ID parallel (degree 24) nologging;
