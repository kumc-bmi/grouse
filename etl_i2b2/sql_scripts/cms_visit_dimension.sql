/** cms_visit_dimension - transform CMS visit info using PCORNET_ENC mappings

handy for dev:
truncate table "&&I2B2STAR".visit_dimension;

*/

select drg from "&&I2B2STAR".visit_dimension where 'dep' = 'vdim_add_cols.sql';
select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql';

-- Check for PCORNET ENCOUNTER metadata. ISSUE: parameterize metadata schema?
select c_fullname, c_basecode from grousemetadata.pcornet_enc where 1=0;


/***
 * enc_code_meta - organize mapping from CMS codes to PCORNet codes.
 *
 *  - Mappings from codes such as NCH_CLM_TYPE_CD 40 to ENC_TYPE AV
 *    are curated in a spreadsheet, exported to metadata/cms_pcornet_mapping.csv
 *  - The `cms_term_map.ipynb` notebook formats the mapping as i2b2 metadata
 *    and inserts it into a `cms_pcornet_terms` table
 *  - The `cms_enc_map` view picks out mappings related to PCORNET_ENC
 *  - The MigrateRows luigi task inserts from `cms_enc_map` into `grousemetadata.pcornet_enc`
 *
 * Since multiple facts may assign the same enc_type (or discharge_status, ...)
 * to the same encounter, here we assign ranks to the PCORNet codes. Below,
 * we pick the best code by rank.
 */
whenever sqlerror continue; drop table enc_code_meta; whenever sqlerror exit;
create table enc_code_meta as
  -- There are 4 PCORNet ENCOUNTER fields with coded valuesets.
with ea_field as (
  select 'ENC_TYPE' field_name from dual union all
  select 'DISCHARGE_DISPOSITION' field_name from dual union all
  select 'DISCHARGE_STATUS' field_name from dual union all
  select 'ADMITTING_SOURCE' field_name from dual
),
--  IP + ED = EI
ed_to_ip as (
  select 'ENC_TYPE' field_name, 'IP' valueset_item,  1 ei_bit from dual union all
  select 'ENC_TYPE' field_name, 'ED' valueset_item,  2 ei_bit from dual
),
ranks as (
-- per heron_encounter_style.sql: EI > IP || ED > OS > IS > AV > OA > OT > NI > UN
-- ISSUE: assign rank to NI?
-- ISSUE: move ranks to curation spreadsheet?
  select 'ENC_TYPE' field_name, 'UN' valueset_item, 99 pc_rank from dual union all
  select 'ENC_TYPE' field_name, 'OT' valueset_item, 98 pc_rank from dual union all
  select 'ENC_TYPE' field_name, 'OA' valueset_item,  7 pc_rank from dual union all
  select 'ENC_TYPE' field_name, 'AV' valueset_item,  6 pc_rank from dual union all
  select 'ENC_TYPE' field_name, 'IS' valueset_item,  5 pc_rank from dual union all
  select 'ENC_TYPE' field_name, 'IC' valueset_item,  4 pc_rank from dual union all  -- ISSUE: IC weight?
  select 'ENC_TYPE' field_name, 'OS' valueset_item,  3 pc_rank from dual union all
  select 'ENC_TYPE' field_name, 'ED' valueset_item,  2 pc_rank from dual union all
  select 'ENC_TYPE' field_name, 'IP' valueset_item,  2 pc_rank from dual union all
  select 'ENC_TYPE' field_name, 'EI' valueset_item,  1 pc_rank from dual union all

-- decode(pcori_code, 'E', 0, 'A', 1, 'OT', 2, 'UN', 3, 'NI', 4, 99) pc_rank
  select 'DISCHARGE_DISPOSITION' field_name,  'E' valueset_item, 0 pc_rank from dual union all
  select 'DISCHARGE_DISPOSITION' field_name,  'A' valueset_item, 1 pc_rank from dual union all
  select 'DISCHARGE_DISPOSITION' field_name, 'OT' valueset_item, 2 pc_rank from dual union all
  select 'DISCHARGE_DISPOSITION' field_name, 'UN' valueset_item, 3 pc_rank from dual union all
  select 'DISCHARGE_DISPOSITION' field_name, 'NI' valueset_item, 4 pc_rank from dual union all
-- Prioritize status: First EX, Middle: <arbitrary>, Last OT, UN, NI
/*     decode(pcori_code, 'EX',0,'AF',1,'AL',2,'AM',3,'AW',4,'HH',5,'HO',6,'HS',7,
                          'IP',8,'NH',9,'RH',10,'RS',11,'SH',12,'SN',12,'OT',96,
                          'UN',97,'NI',98,99) pc_rank
*/
  select 'DISCHARGE_STATUS' field_name, 'EX' valueset_item,  0 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'AF' valueset_item,  1 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'AL' valueset_item,  2 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'AM' valueset_item,  3 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'AW' valueset_item,  4 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'HH' valueset_item,  5 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'HO' valueset_item,  6 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'HS' valueset_item,  7 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'IP' valueset_item,  8 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'NH' valueset_item,  9 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'RH' valueset_item, 10 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'RS' valueset_item, 11 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'SH' valueset_item, 12 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'SN' valueset_item, 12 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'OT' valueset_item, 96 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'UN' valueset_item, 98 pc_rank from dual union all
  select 'DISCHARGE_STATUS' field_name, 'NI' valueset_item, 99 pc_rank from dual
)
  select c_basecode, ea_field.field_name, pcori_basecode valueset_item, ei_bit
         -- lacking an explicit rank, rank unknowns well below knowns
       , nvl(pc_rank, case when pcori_basecode in ('OT', 'UN', 'NI') then 99 else 1 end) pc_rank
  from grousemetadata.pcornet_enc e
  join ea_field on e.c_fullname like '\PCORI\ENCOUNTER\' || field_name || '\%'
  left join ranks    on    ranks.valueset_item = e.pcori_basecode and     ranks.field_name = ea_field.field_name
  left join ed_to_ip on ed_to_ip.valueset_item = e.pcori_basecode and  ed_to_ip.field_name = ea_field.field_name
  where e.pcori_basecode is not null
;
-- select * from enc_code_meta

/* Note: we have some dups from newborn admission sources:

select c_basecode, count(*), listagg(valueset_item, ', ') within group(order by c_basecode)
from enc_code_meta
group by c_basecode
having count(*) > 1
;

Fortunately, the ranking ensures that known codes win out in all cases:

select c_basecode, count(*)
from (
  select distinct c_basecode, valueset_item
  from enc_code_meta
  where valueset_item not in ('OT', 'UN', 'NI')
)
group by c_basecode
having count(*) > 1
;
*/


/***
 * cms_visit_dimension
 *
 * Performance note: EXPLAIN PLAN shows just two full table scans
 * over observation_fact in cms_enc_codes plus a window sort
 * and group by pivot in cms_visit_detail.
 */
-- First, pick out the facts for the 4 coded encounter fields, plus DRG:
create or replace view cms_enc_codes_v as
  select /*+ parallel(obs, 20) */
         encounter_num, patient_num, provider_id, start_date, end_date
       , meta.field_name, meta.valueset_item, pc_rank, ei_bit, obs.concept_cd
  from "&&I2B2STAR".observation_fact obs
  join enc_code_meta meta on obs.concept_cd = meta.c_basecode

union all
  select /*+ parallel(obs, 20) */
         encounter_num, patient_num, provider_id, start_date, end_date
       , 'DRG', SUBSTR(obs.concept_cd, length('MSDRG:%')) drg, 1 pc_rank, null ei_bit, obs.concept_cd
  from "&&I2B2STAR".observation_fact obs
  where concept_cd like 'MSDRG:%';
;
-- We'll populate the table later; fow now, create it so we can refer to it below.
whenever sqlerror continue; drop table cms_enc_codes_t; whenever sqlerror exit;
create table "&&I2B2STAR".cms_enc_codes_t as
select * from cms_enc_codes_v where 1=0;
/*
insert into "&&I2B2STAR".cms_enc_codes_t
select * from cms_enc_codes_v
order by patient_num, encounter_num desc, field_name, pc_rank, provider_id;
*/


-- Now pivot them down to one row per encounter.
create or replace view cms_visit_detail as
  -- For each field, pick rows with the best rank.
  with ea_enc_field as (
    select encounter_num, patient_num, provider_id, start_date, end_date, field_name, valueset_item
         , ed_bit, ip_bit
    from (
      select encounter_num, patient_num, provider_id, start_date, end_date, field_name, valueset_item, pc_rank
             -- Separate the bits into their own columns for aggregation
           , bitand(ei_bit, 1) ip_bit
           , bitand(ei_bit, 2) ed_bit
           , min(pc_rank) over (partition by encounter_num, patient_num, field_name order by encounter_num, patient_num, field_name) min_rank
      from "&&I2B2STAR".cms_enc_codes_t
    ) where pc_rank = min_rank
  )
  -- Pivot by encounter_num, patient_num
  select *
  from ea_enc_field
    pivot (
        -- max() is arbitrary; we have just one valueset_item by now
        max(valueset_item)
      , max(ed_bit) ed_bit, max(ip_bit) ip_bit
      , min(provider_id) provider_id  -- arbitrary
      , min(start_date) start_date, max(end_date) end_date
      for (field_name) in ('ENC_TYPE' as enc_type
                         , 'DRG' as DRG
                         , 'DISCHARGE_STATUS' as discharge_status
                         , 'DISCHARGE_DISPOSITION' as DISCHARGE_DISPOSITION
                         , 'ADMITTING_SOURCE' as ADMITTING_SOURCE))
;
-- select * from cms_visit_detail;

-- Finally, lay out columns and combine IP + ED = EI.
create or replace view cms_visit_dimension as
select encounter_num
, patient_num
, (select active from i2b2_status) active_status_cd
, enc_type_start_date start_date
, enc_type_end_date end_date
, case when enc_type_ed_bit > 0 and enc_type_ip_bit > 0 then 'EI' else enc_type end inout_cd
, null location_cd
, null location_path
, enc_type_end_date - enc_type_start_date + 1 length_of_stay
, null visit_blob
, enc_type_end_date update_date

  -- The 4 audit columns are the responsibility of the one doing the insert.
, null download_date
, sysdate import_date
, null sourcesystem_cd
, null upload_id

, drg
, discharge_status
, discharge_disposition
, null location_zip  -- ISSUE (#4960): use PRVDR_ZIP from BCARRIER_LINE?
, admitting_source
, null facilityid
, enc_type_provider_id providerid
from cms_visit_detail
;


-- check type compatibility
insert into "&&I2B2STAR".visit_dimension select * from cms_visit_dimension where 1=0;


-- Have we already completed this script?
create or replace view cms_visit_dim_sql as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_visit_dim_sql
where design_digest = &&design_digest
and (select count(*) from enc_code_meta) > 0
;
