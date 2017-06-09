/** medpar_pivot - (un)pivot CMS claims files into i2b2 fact table shape

Refs:

Chronic Conditions Data Warehouse Data Dictionaries 
https://www.ccwdata.org/web/guest/data-dictionaries

Carrier RIF
https://www.resdac.org/cms-data/files/carrier-rif/data-documentation


Outpatient RIF
https://www.resdac.org/cms-data/files/op-rif

@@i2b2 CRC design

*/

create or replace view cms_dx_design
as
with information_schema as
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
  || (case
      when dgns_poa.column_name is not null
      then ', ' || dgns_poa.column_name
      else '' end)
  || ') as '''
  || dgns.column_name
  || '''' sql_snippet
from information_schema dgns_vrsn
join information_schema dgns
on dgns.table_name                         = dgns_vrsn.table_name
  and replace(dgns.column_name, '_CD', '') = replace(replace(dgns_vrsn.column_name, '_CD', ''), '_VRSN', '')
left join information_schema dgns_poa
on dgns.table_name                         =dgns_poa.table_name
  --  DGNS_11_CD -> DGNS_11                 POA_DGNS_11_IND_CD
  and replace(dgns.column_name, '_CD', '') = replace(replace(dgns_poa.column_name, '_IND_CD', ''), 'POA_', '')
where dgns_vrsn.column_name like '%DGNS%'
  and dgns_vrsn.column_name like '%VRSN%'
  and (dgns_poa.column_name is null or dgns_poa.column_name like 'POA_DGNS_%')
  and dgns.column_name like '%DGNS%'
  and dgns.column_name not like '%VRSN%'
  and dgns.column_name not like 'POA_%'
order by table_name
, column_id ;


create or replace view bcarrier_dx_detail as
  (select /*+ index (dx) */ 'BCARRIER_CLAIMS' table_name
  , bene_id
  , clm_id
  , clm_from_dt
  , clm_thru_dt
  , no_value.not_recorded provider_id
  , nch_wkly_proc_dt
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
  ) dx cross join no_value);
  
create or replace view bcarrier_line_dx_detail as
  (select 'BCARRIER_LINE' table_name
  , bene_id
  , clm_id
  , clm_thru_dt start_date
  , clm_thru_dt end_date
  , coalesce(PRF_PHYSN_NPI, no_value.not_recorded) provider_id
  , line_last_expns_dt
  , line_icd_dgns_cd dgns_cd
  , line_icd_dgns_vrsn_cd dgns_vrsn
  , to_char(line_num) dgns_label
  from "&&CMS_RIF".bcarrier_line
  cross join no_value
  );

create or replace view outpatient_dx_detail as
  (select 'OUTPATIENT_BASE_CLAIMS' table_name
  , bene_id
  , clm_id
  , clm_from_dt
  , clm_thru_dt
  , coalesce(ORG_NPI_NUM, (select not_recorded from no_value)) provider_id
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
)));

create or replace view hba_dx_detail as (
  select 'HHA_BASE_CLAIMS' table_name
  , bene_id
  , clm_id
  , clm_from_dt
  , clm_thru_dt
  , coalesce(ORG_NPI_NUM, (select not_recorded from no_value)) provider_id
  , nch_wkly_proc_dt update_date
  , dgns_cd
  , dgns_vrsn
  , dgns_label
  from "&&CMS_RIF".HHA_BASE_CLAIMS unpivot((dgns_cd, dgns_vrsn) for dgns_label in(
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
)));


create or replace view hospice_dx_detail as (
  select 'HOSPICE_BASE_CLAIMS' table_name
  , bene_id
  , clm_id
  , clm_from_dt
  , clm_thru_dt
  , coalesce(ORG_NPI_NUM, (select not_recorded from no_value)) provider_id
  , nch_wkly_proc_dt update_date
  , dgns_cd
  , dgns_vrsn
  , dgns_label
  from "&&CMS_RIF".HOSPICE_BASE_CLAIMS unpivot((dgns_cd, dgns_vrsn) for dgns_label in(
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
;

create or replace view cms_claim_dx as
with 
detail as (
  select * from bcarrier_dx_detail
  union all
  select * from bcarrier_line_dx_detail
  union all
  select * from outpatient_dx_detail
  union all
  select * from hba_dx_detail
  union all
  select * from hospice_dx_detail
)
, no_info as (
  select null tval_char
  , to_number(null) nval_num
  , null valueflag_cd
  , null quantity_num
  , null units_cd
  , null location_cd
  , to_number(null) confidence_num
  from dual)

select
  null medpar_id, bene_id, clm_from_dt start_date
, dx_code(dgns_cd, dgns_vrsn) concept_cd
, detail.provider_id
, case
  when dgns_label = 'PRNCPAL_DGNS_CD' then pdx_flags.primary
  else rif_modifier(table_name)
  end modifier_cd
, ora_hash(detail.clm_id || detail.dgns_label) instance_num -- ISSUE: collision could violate primary key
, valtype_cd.no_value valtype_cd
, clm_thru_dt end_date
, nch_wkly_proc_dt update_date
,
  &&cms_source_cd sourcesystem_cd
, no_info.*
from detail
cross join no_info
cross join valtype_cd
cross join pdx_flags
cross join no_value
cross join cms_key_sources key_sources
  where detail.dgns_cd is not null ;
-- select * from cms_bcarrier_dx



create or replace view cms_prcdr_design as
;;;;;
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
from information_schema prcdr
where prcdr.column_name like '%PRCDR%'
or prcdr.column_name = 'HCPCS_CD'
;
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
on prcdr.table_name     = prcdr_vrsn.table_name
  -- ICD_PRCDR_CD1        ICD_PRCDR_VRSN_CD1 -> ICD_PRCDR_CD1
  and prcdr.column_name = replace(prcdr_vrsn.column_name, '_VRSN', '')
join information_schema prcdr_dt
on prcdr.table_name                         = prcdr_dt.table_name
  -- ICD_PRCDR_CD1        PRCDR_DT1 -> ICD_PRCDR_CD1
  and prcdr.column_name = 'ICD_' || replace(prcdr_dt.column_name, '_DT', '_CD')
where prcdr.column_name like '%PRCDR%'
  and prcdr.column_name not like '%VRSN%'
  and prcdr_vrsn.column_name like '%VRSN%'

order by table_name
, column_id ;

from information_schema info
where column_name like 'HCPCS_CD'
or column_name like '%PRCDR%'
;


create or replace view bcarrier_line_px_detail as
select 'BCARRIER_LINE' table_name
  , bene_id, clm_id
  , clm_thru_dt
  , coalesce(ORG_NPI_NUM, (select not_recorded from no_value)) provider_id
  , line_last_expns_dt update_date
  , hcpcs_cd prcdr_cd
  , 'HCPCS' prcdr_vrsn
  , 'HCPCS_CD' prcdr_label
from "&&CMS_RIF".bcarrier_line
;


create or replace view outpatient_px_detail as
select 'OUTPATIENT_BASE_CLAIMS' table_name
  , bene_id, clm_id
  , prcdr_dt
  , coalesce(ORG_NPI_NUM, (select not_recorded from no_value)) provider_id
  , NCH_WKLY_PROC_DT update_date
  , prcdr_cd
  , prcdr_vrsn
  , prcdr_label
from "&&CMS_RIF".OUTPATIENT_BASE_CLAIMS
unpivot((prcdr_cd, prcdr_vrsn, prcdr_dt) for prcdr_label in(
  (ICD_PRCDR_CD1, ICD_PRCDR_VRSN_CD1, PRCDR_DT1) as 'ICD_PRCDR_CD1'
, (ICD_PRCDR_CD2, ICD_PRCDR_VRSN_CD2, PRCDR_DT2) as 'ICD_PRCDR_CD2'
, (ICD_PRCDR_CD3, ICD_PRCDR_VRSN_CD3, PRCDR_DT3) as 'ICD_PRCDR_CD3'
, (ICD_PRCDR_CD4, ICD_PRCDR_VRSN_CD4, PRCDR_DT4) as 'ICD_PRCDR_CD4'
, (ICD_PRCDR_CD5, ICD_PRCDR_VRSN_CD5, PRCDR_DT5) as 'ICD_PRCDR_CD5'
, (ICD_PRCDR_CD6, ICD_PRCDR_VRSN_CD6, PRCDR_DT6) as 'ICD_PRCDR_CD6'
, (ICD_PRCDR_CD7, ICD_PRCDR_VRSN_CD7, PRCDR_DT7) as 'ICD_PRCDR_CD7'
, (ICD_PRCDR_CD8, ICD_PRCDR_VRSN_CD8, PRCDR_DT8) as 'ICD_PRCDR_CD8'
, (ICD_PRCDR_CD9, ICD_PRCDR_VRSN_CD9, PRCDR_DT9) as 'ICD_PRCDR_CD9'
, (ICD_PRCDR_CD10, ICD_PRCDR_VRSN_CD10, PRCDR_DT10) as 'ICD_PRCDR_CD10'
, (ICD_PRCDR_CD11, ICD_PRCDR_VRSN_CD11, PRCDR_DT11) as 'ICD_PRCDR_CD11'
, (ICD_PRCDR_CD12, ICD_PRCDR_VRSN_CD12, PRCDR_DT12) as 'ICD_PRCDR_CD12'
, (ICD_PRCDR_CD13, ICD_PRCDR_VRSN_CD13, PRCDR_DT13) as 'ICD_PRCDR_CD13'
, (ICD_PRCDR_CD14, ICD_PRCDR_VRSN_CD14, PRCDR_DT14) as 'ICD_PRCDR_CD14'
, (ICD_PRCDR_CD15, ICD_PRCDR_VRSN_CD15, PRCDR_DT15) as 'ICD_PRCDR_CD15'
, (ICD_PRCDR_CD16, ICD_PRCDR_VRSN_CD16, PRCDR_DT16) as 'ICD_PRCDR_CD16'
, (ICD_PRCDR_CD17, ICD_PRCDR_VRSN_CD17, PRCDR_DT17) as 'ICD_PRCDR_CD17'
, (ICD_PRCDR_CD18, ICD_PRCDR_VRSN_CD18, PRCDR_DT18) as 'ICD_PRCDR_CD18'
, (ICD_PRCDR_CD19, ICD_PRCDR_VRSN_CD19, PRCDR_DT19) as 'ICD_PRCDR_CD19'
, (ICD_PRCDR_CD20, ICD_PRCDR_VRSN_CD20, PRCDR_DT20) as 'ICD_PRCDR_CD20'
, (ICD_PRCDR_CD21, ICD_PRCDR_VRSN_CD21, PRCDR_DT21) as 'ICD_PRCDR_CD21'
, (ICD_PRCDR_CD22, ICD_PRCDR_VRSN_CD22, PRCDR_DT22) as 'ICD_PRCDR_CD22'
, (ICD_PRCDR_CD23, ICD_PRCDR_VRSN_CD23, PRCDR_DT23) as 'ICD_PRCDR_CD23'
, (ICD_PRCDR_CD24, ICD_PRCDR_VRSN_CD24, PRCDR_DT24) as 'ICD_PRCDR_CD24'
, (ICD_PRCDR_CD25, ICD_PRCDR_VRSN_CD25, PRCDR_DT25) as 'ICD_PRCDR_CD25'
))
;

with detail as (
select * from bcarrier_line_px_detail
union all
select * from outpatient_px_detail
)
select * from detail
where bene_id = 4963638
;

create or replace view cms_claims_design as
;;;;
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
select distinct table_name
, column_id
, column_name
, data_type
/*
from information_schema
where column_name like '%DGNS%' or column_name like '%PRCDR%'
;
*/

/*****
, ', '
  ||
  case
    when data_type = 'VARCHAR2'
      and column_name like 'ICD_DGNS%' then '('
      || column_name
      || ',  line_num, extract_dt) as ''DX '
      || column_name
      || ''''
    when data_type = 'VARCHAR2' then '('
      || column_name
      || ',  line_num, extract_dt) as ''@_ '
      || column_name
      || ''''
    when data_type = 'NUMBER' then '(clm_id, '
      || column_name
      || ', extract_dt) as ''n_ '
      || column_name
      || ''''
    when data_type = 'DATE' then '(clm_id, bene_age_at_end_ref_yr, '
      || column_name
      || ') as ''d_ '
      || column_name
      || ''''
  end sql_snippet ***/
from information_schema
where table_name in('BCARRIER_CLAIMS',
	'BCARRIER_LINE',
	'OUTPATIENT_BASE_CLAIMS',
  /* TODO:
	'OUTPATIENT_CONDITION_CODES', *
	'OUTPATIENT_OCCURRNCE_CODES', **
	'OUTPATIENT_REVENUE_CENTER', *
	'OUTPATIENT_SPAN_CODES',
	'OUTPATIENT_VALUE_CODES'
  */
  'HHA_BASE_CLAIMS',
/* TODO:
	'HHA_CONDITION_CODES',
	'HHA_OCCURRNCE_CODES',
	'HHA_REVENUE_CENTER',
	'HHA_SPAN_CODES',
	'HHA_VALUE_CODES',
  */
	'HOSPICE_BASE_CLAIMS'
/* TODO:
	'HOSPICE_CONDITION_CODES',
	'HOSPICE_OCCURRNCE_CODES',
	'HOSPICE_REVENUE_CENTER',
	'HOSPICE_SPAN_CODES',
	'HOSPICE_VALUE_CODES'
  */
  )
  -- issue: clm_id as fact? if so, what code col to use as dummy?
  and column_name not in('BENE_ID', 'CLM_ID', 'CLM_FROM_DT', 'CLM_THRU_DT', 'EXTRACT_DT')
order by table_name
, column_id ;