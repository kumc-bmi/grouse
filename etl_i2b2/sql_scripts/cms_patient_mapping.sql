/** cms_patient_mapping - map CMS beneficiaries to i2b2 patients
*/

select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql';
select bene_cd from cms_key_sources where 'dep' = 'cms_keys.pls';


/** cms_bene_map_design - find bene_id tables at design-time
*/
create or replace view cms_bene_map_design
as
with cms_schema as
  (select *
  from all_tab_columns
  where owner = '&&CMS_RIF'
    and table_name not like 'SYS_%'
  )
, ea as
  (select cs.owner
  , cs.table_name
  , cs.column_id
  , cs.column_name
  , 'select distinct /*+ index(who) */ bene_id from "&&CMS_RIF".'
    || table_name
    || ' who' sql_snippet
  from cms_schema cs
  where column_name = 'BENE_ID'
    /* Any bene_id from one of these subordinate tables must have been used in a superior table. */
    and table_name not like '%_LINE'
    and table_name not like '%_CODES'
    and table_name not like '%_REVENUE_CENTER'
    and table_name not like '%_D_CMPNTS'
    and table_name not like '%_SAF'
  )
select listagg(sql_snippet, '
union
') within group(
order by sql_snippet) sql_q
from ea;


insert
  /*+ append */
into "&&I2B2STAR".patient_mapping
  (
    patient_ide
  , patient_ide_source
  , patient_num
  , patient_ide_status
  , project_id
  , upload_date
  , update_date
  , download_date
  , import_date
  , sourcesystem_cd
  , upload_id
  )
with bene as
  (
select distinct /*+ index(who) */ bene_id from "&&CMS_RIF".BCARRIER_CLAIMS who
union
select distinct /*+ index(who) */ bene_id from "&&CMS_RIF".HHA_BASE_CLAIMS who
union
select distinct /*+ index(who) */ bene_id from "&&CMS_RIF".HOSPICE_BASE_CLAIMS who
union
select distinct /*+ index(who) */ bene_id from "&&CMS_RIF".MAXDATA_IP who
union
select distinct /*+ index(who) */ bene_id from "&&CMS_RIF".MAXDATA_LT who
union
select distinct /*+ index(who) */ bene_id from "&&CMS_RIF".MAXDATA_OT who
union
select distinct /*+ index(who) */ bene_id from "&&CMS_RIF".MAXDATA_PS who
union
select distinct /*+ index(who) */ bene_id from "&&CMS_RIF".MAXDATA_RX who
union
select distinct /*+ index(who) */ bene_id from "&&CMS_RIF".MBSF_AB_SUMMARY who
union
select distinct /*+ index(who) */ bene_id from "&&CMS_RIF".MEDPAR_ALL who
union
select distinct /*+ index(who) */ bene_id from "&&CMS_RIF".OUTPATIENT_BASE_CLAIMS who
union
select distinct /*+ index(who) */ bene_id from "&&CMS_RIF".PDE who
  )
select bene.bene_id patient_ide
, key_sources.bene_cd patient_ide_source
, "&&I2B2STAR".sq_up_patdim_patientnum.nextval patient_num
, i2b2_status.active patient_ide_status
, :project_id project_id
, sysdate upload_date
, sysdate update_date
, :download_date
, sysdate import_date
,
  &&cms_source_cd sourcesystem_cd
, :upload_id upload_id
from bene
, i2b2_status
, cms_key_sources key_sources
where bene.bene_id is not null
  and bene.bene_id between coalesce(:bene_id_first, bene.bene_id) and coalesce(:bene_id_last, bene.bene_id) ;


create or replace view bene_id_mapping
as
  (select patient_ide bene_id
  , patient_num
  from "&&I2B2STAR".patient_mapping pat_map
  cross join cms_key_sources key_sources
  where patient_ide_source = key_sources.bene_cd
  ) ;


select 1 complete
from "&&I2B2STAR".patient_mapping
where upload_id =
  (select max(upload_id)
  from "&&I2B2STAR".upload_status up
  where up.transform_name = :task_id
  )
  and rownum = 1;
