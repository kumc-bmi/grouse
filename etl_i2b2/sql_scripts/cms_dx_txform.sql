/** cms_dx_txform - Make i2b2 facts from CMS diagnoses

for DRGs from MAXDATA_IP, ref:
  - Medicaid Analytic Extract Inpatient (IP) Record Layout and Description 2013
    https://www.cms.gov/Research-Statistics-Data-and-Systems/Computer-Data-and-Systems/MedicaidDataSourcesGenInfo/Downloads/geninfomax2013.zip
    https://www.cms.gov/Research-Statistics-Data-and-Systems/Computer-Data-and-Systems/MedicaidDataSourcesGenInfo/MAXGeneralInformation.html
*/

select clm_line_cd from cms_key_sources where 'dep' = 'cms_keys.pls';

select 1 / 0 not_implemented from dual; -- TODO: pivot whole tables. See mbsf_pivot.sql







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


create or replace view cms_dx_txform_digest as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_dx_txform_digest where design_digest = &&design_digest;
