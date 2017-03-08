/* Index SQL generated with hackish SQL and Python snippits at the end of the file.

It's assumed that the default tablespace of the currently logged in user is the
right place to put these indexes
*/
whenever sqlerror continue;

drop index CMS_IX_BCACLA_BENID;
drop index CMS_IX_BCACLA_CLMID;
drop index CMS_IX_BCALIN_BENID;
drop index CMS_IX_BCALIN_CLMID;
drop index CMS_IX_HHABASCLA_BENID;
drop index CMS_IX_HHABASCLA_CLMID;
drop index CMS_IX_HHACONCOD_BENID;
drop index CMS_IX_HHACONCOD_CLMID;
drop index CMS_IX_HHAOCCCOD_BENID;
drop index CMS_IX_HHAOCCCOD_CLMID;
drop index CMS_IX_HHAREVCEN_BENID;
drop index CMS_IX_HHAREVCEN_CLMID;
drop index CMS_IX_HHASPACOD_BENID;
drop index CMS_IX_HHASPACOD_CLMID;
drop index CMS_IX_HHAVALCOD_BENID;
drop index CMS_IX_HHAVALCOD_CLMID;
drop index CMS_IX_HOSBASCLA_BENID;
drop index CMS_IX_HOSBASCLA_CLMHOSSTADTID;
drop index CMS_IX_HOSBASCLA_CLMID;
drop index CMS_IX_HOSCONCOD_BENID;
drop index CMS_IX_HOSCONCOD_CLMID;
drop index CMS_IX_HOSOCCCOD_BENID;
drop index CMS_IX_HOSOCCCOD_CLMID;
drop index CMS_IX_HOSREVCEN_BENID;
drop index CMS_IX_HOSREVCEN_CLMID;
drop index CMS_IX_HOSSPACOD_BENID;
drop index CMS_IX_HOSSPACOD_CLMID;
drop index CMS_IX_HOSVALCOD_BENID;
drop index CMS_IX_HOSVALCOD_CLMID;
drop index CMS_IX_MAXIP_BENID;
drop index CMS_IX_MAXIP_MSIID;
drop index CMS_IX_MAXIP_PHPID;
drop index CMS_IX_MAXLT_BENID;
drop index CMS_IX_MAXLT_MSIID;
drop index CMS_IX_MAXLT_PHPID;
drop index CMS_IX_MAXOT_BENID;
drop index CMS_IX_MAXOT_MSIID;
drop index CMS_IX_MAXOT_PHPID;
drop index CMS_IX_MAXPS_BENID;
drop index CMS_IX_MAXPS_MSIID;
drop index CMS_IX_MAXRX_BENID;
drop index CMS_IX_MAXRX_MSIID;
drop index CMS_IX_MAXRX_PHPID;
drop index CMS_IX_MBSABSUM_BENID;
drop index CMS_IX_MBSDCMP_BENID;
drop index CMS_IX_MEDALL_BENID;
drop index CMS_IX_MEDALL_MEDID;
drop index CMS_IX_OUTBASCLA_BENID;
drop index CMS_IX_OUTBASCLA_CLMID;
drop index CMS_IX_OUTCONCOD_BENID;
drop index CMS_IX_OUTCONCOD_CLMID;
drop index CMS_IX_OUTOCCCOD_BENID;
drop index CMS_IX_OUTOCCCOD_CLMID;
drop index CMS_IX_OUTREVCEN_BENID;
drop index CMS_IX_OUTREVCEN_CLMID;
drop index CMS_IX_OUTSPACOD_BENID;
drop index CMS_IX_OUTSPACOD_CLMID;
drop index CMS_IX_OUTVALCOD_BENID;
drop index CMS_IX_OUTVALCOD_CLMID;
drop index CMS_IX_PDE_BENID;
drop index CMS_IX_PDE_PDEID;
drop index CMS_IX_PDE_PROSRVID;
drop index CMS_IX_PDE_PRSID;
drop index CMS_IX_PDE_SRVPRVID;
drop index CMS_IX_PDESAF_BENID;
drop index CMS_IX_PDESAF_PDEID;
drop index CMS_IX_PDESAF_PROSRVID;
drop index CMS_IX_PDESAF_PRSID;
drop index CMS_IX_PDESAF_SRVPRVID;

whenever sqlerror exit;

create index CMS_IX_BCACLA_BENID on BCARRIER_CLAIMS(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_BCACLA_CLMID on BCARRIER_CLAIMS(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_BCALIN_BENID on BCARRIER_LINE(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_BCALIN_CLMID on BCARRIER_LINE(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_HHABASCLA_BENID on HHA_BASE_CLAIMS(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_HHABASCLA_CLMID on HHA_BASE_CLAIMS(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_HHACONCOD_BENID on HHA_CONDITION_CODES(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_HHACONCOD_CLMID on HHA_CONDITION_CODES(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_HHAOCCCOD_BENID on HHA_OCCURRNCE_CODES(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_HHAOCCCOD_CLMID on HHA_OCCURRNCE_CODES(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_HHAREVCEN_BENID on HHA_REVENUE_CENTER(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_HHAREVCEN_CLMID on HHA_REVENUE_CENTER(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_HHASPACOD_BENID on HHA_SPAN_CODES(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_HHASPACOD_CLMID on HHA_SPAN_CODES(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_HHAVALCOD_BENID on HHA_VALUE_CODES(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_HHAVALCOD_CLMID on HHA_VALUE_CODES(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_HOSBASCLA_BENID on HOSPICE_BASE_CLAIMS(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_HOSBASCLA_CLMHOSSTADTID on HOSPICE_BASE_CLAIMS(CLM_HOSPC_START_DT_ID) parallel (degree 24) nologging;
create index CMS_IX_HOSBASCLA_CLMID on HOSPICE_BASE_CLAIMS(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_HOSCONCOD_BENID on HOSPICE_CONDITION_CODES(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_HOSCONCOD_CLMID on HOSPICE_CONDITION_CODES(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_HOSOCCCOD_BENID on HOSPICE_OCCURRNCE_CODES(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_HOSOCCCOD_CLMID on HOSPICE_OCCURRNCE_CODES(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_HOSREVCEN_BENID on HOSPICE_REVENUE_CENTER(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_HOSREVCEN_CLMID on HOSPICE_REVENUE_CENTER(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_HOSSPACOD_BENID on HOSPICE_SPAN_CODES(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_HOSSPACOD_CLMID on HOSPICE_SPAN_CODES(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_HOSVALCOD_BENID on HOSPICE_VALUE_CODES(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_HOSVALCOD_CLMID on HOSPICE_VALUE_CODES(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_MAXIP_BENID on MAXDATA_IP(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_MAXIP_MSIID on MAXDATA_IP(MSIS_ID) parallel (degree 24) nologging;
create index CMS_IX_MAXIP_PHPID on MAXDATA_IP(PHP_ID) parallel (degree 24) nologging;
create index CMS_IX_MAXLT_BENID on MAXDATA_LT(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_MAXLT_MSIID on MAXDATA_LT(MSIS_ID) parallel (degree 24) nologging;
create index CMS_IX_MAXLT_PHPID on MAXDATA_LT(PHP_ID) parallel (degree 24) nologging;
create index CMS_IX_MAXOT_BENID on MAXDATA_OT(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_MAXOT_MSIID on MAXDATA_OT(MSIS_ID) parallel (degree 24) nologging;
create index CMS_IX_MAXOT_PHPID on MAXDATA_OT(PHP_ID) parallel (degree 24) nologging;
create index CMS_IX_MAXPS_BENID on MAXDATA_PS(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_MAXPS_MSIID on MAXDATA_PS(MSIS_ID) parallel (degree 24) nologging;
create index CMS_IX_MAXRX_BENID on MAXDATA_RX(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_MAXRX_MSIID on MAXDATA_RX(MSIS_ID) parallel (degree 24) nologging;
create index CMS_IX_MAXRX_PHPID on MAXDATA_RX(PHP_ID) parallel (degree 24) nologging;
create index CMS_IX_MBSABSUM_BENID on MBSF_AB_SUMMARY(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_MBSDCMP_BENID on MBSF_D_CMPNTS(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_MEDALL_BENID on MEDPAR_ALL(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_MEDALL_MEDID on MEDPAR_ALL(MEDPAR_ID) parallel (degree 24) nologging;
create index CMS_IX_OUTBASCLA_BENID on OUTPATIENT_BASE_CLAIMS(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_OUTBASCLA_CLMID on OUTPATIENT_BASE_CLAIMS(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_OUTCONCOD_BENID on OUTPATIENT_CONDITION_CODES(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_OUTCONCOD_CLMID on OUTPATIENT_CONDITION_CODES(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_OUTOCCCOD_BENID on OUTPATIENT_OCCURRNCE_CODES(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_OUTOCCCOD_CLMID on OUTPATIENT_OCCURRNCE_CODES(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_OUTREVCEN_BENID on OUTPATIENT_REVENUE_CENTER(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_OUTREVCEN_CLMID on OUTPATIENT_REVENUE_CENTER(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_OUTSPACOD_BENID on OUTPATIENT_SPAN_CODES(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_OUTSPACOD_CLMID on OUTPATIENT_SPAN_CODES(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_OUTVALCOD_BENID on OUTPATIENT_VALUE_CODES(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_OUTVALCOD_CLMID on OUTPATIENT_VALUE_CODES(CLM_ID) parallel (degree 24) nologging;
create index CMS_IX_PDE_BENID on PDE(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_PDE_PDEID on PDE(PDE_ID) parallel (degree 24) nologging;
create index CMS_IX_PDE_PROSRVID on PDE(PROD_SRVC_ID) parallel (degree 24) nologging;
create index CMS_IX_PDE_PRSID on PDE(PRSCRBR_ID) parallel (degree 24) nologging;
create index CMS_IX_PDE_SRVPRVID on PDE(SRVC_PRVDR_ID) parallel (degree 24) nologging;
create index CMS_IX_PDESAF_BENID on PDE_SAF(BENE_ID) parallel (degree 24) nologging;
create index CMS_IX_PDESAF_PDEID on PDE_SAF(PDE_ID) parallel (degree 24) nologging;
create index CMS_IX_PDESAF_PROSRVID on PDE_SAF(PROD_SRVC_ID) parallel (degree 24) nologging;
create index CMS_IX_PDESAF_PRSID on PDE_SAF(PRSCRBR_ID) parallel (degree 24) nologging;
create index CMS_IX_PDESAF_SRVPRVID on PDE_SAF(SRVC_PRVDR_ID) parallel (degree 24) nologging;

/*
1) In SQLDeveloper, copy the output of the following...
with 
  cms_tables as (
  -- row_counts generated as part of staging at KU - one row per CMS table
  select table_name from cms_id.row_counts
  ),
parts as (
  select 
    atc.table_name, atc.column_name
  from all_tab_cols atc
  join cms_tables ct on ct.table_name = atc.table_name
  where atc.column_name like '%_ID'
  )
select distinct * from parts  
order by table_name, column_name
;

2) Paste into variable sql_output and run:
def abbrv(s):
    return ''.join([p[:3] for p in s.split('_')])

all_ix_names = list()
creates = list()
drops = list()
for table, column in sorted([(t, c) for (t, c) in
                             [r.split('\t')
                              for r in
                              sql_output.split('\n')]]):
    ix_name = 'CMS_IX_' + '_'.join([abbrv(table), abbrv(column)])
    if ix_name in all_ix_names:
        print '**** DUPLICATE: %s' % ix_name
    all_ix_names.append(ix_name)
    if len(ix_name) > 30:
        print '**** TOO LONG: %s' % ix_name
    creates.append(('create index %(ix_name)s '
                    'on %(table)s(%(column)s) '
                    'parallel (degree 24) nologging;') % dict(
                   table=table, ix_name=ix_name, column=column))
    drops.append('drop index %(ix_name)s;' % dict(ix_name=ix_name))

print '\n'.join(['whenever sqlerror continue;'] +
                drops +
                ['whenever sqlerror exit;'] +
                creates)

3) Profit
*/