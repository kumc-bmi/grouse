-- bene_id_mapping.sql: Create bene_id mapping table for CMS deidentification
-- Copyright (c) 2017 University of Kansas Medical Center

--select 
--'select /*+ PARALLEL(' || table_name ||',12) */ distinct bene_id from ' || table_name || ' union'
--from dba_tables where owner = 'CMS_ID_SAMPLE' and table_name not like 'REF_%' and table_name != 'BENE_ID_MAPPING';
-- Create bene_id_deid for users who where not in previous CMS data.
insert /*+ APPEND */ into bene_id_mapping
select 
  ubid.bene_id bene_id,
  to_char(bene_id_deid_seq.nextval) bene_id_deid,
  round(dbms_random.value(-364,0)) date_shift_days,
  dob_shift.dob_shift_months
from (
  select /*+ PARALLEL(HHA_OCCURRNCE_CODES,12) */ distinct bene_id from HHA_OCCURRNCE_CODES union
  select /*+ PARALLEL(PDE,12) */ distinct bene_id from PDE union
  select /*+ PARALLEL(BCARRIER_CLAIMS_K,12) */ distinct bene_id from BCARRIER_CLAIMS_K union
  select /*+ PARALLEL(HHA_VALUE_CODES,12) */ distinct bene_id from HHA_VALUE_CODES union
  select /*+ PARALLEL(HOSPICE_OCCURRNCE_CODES,12) */ distinct bene_id from HOSPICE_OCCURRNCE_CODES union
  select /*+ PARALLEL(HHA_CONDITION_CODES,12) */ distinct bene_id from HHA_CONDITION_CODES union
  select /*+ PARALLEL(OUTPATIENT_REVENUE_CENTER_K,12) */ distinct bene_id from OUTPATIENT_REVENUE_CENTER_K union
  select /*+ PARALLEL(HHA_SPAN_CODES,12) */ distinct bene_id from HHA_SPAN_CODES union
  select /*+ PARALLEL(HOSPICE_REVENUE_CENTER_K,12) */ distinct bene_id from HOSPICE_REVENUE_CENTER_K union
  select /*+ PARALLEL(HHA_BASE_CLAIMS_K,12) */ distinct bene_id from HHA_BASE_CLAIMS_K union
  select /*+ PARALLEL(BCARRIER_LINE_K,12) */ distinct bene_id from BCARRIER_LINE_K union
  select /*+ PARALLEL(HOSPICE_SPAN_CODES,12) */ distinct bene_id from HOSPICE_SPAN_CODES union
  select /*+ PARALLEL(HOSPICE_CONDITION_CODES,12) */ distinct bene_id from HOSPICE_CONDITION_CODES union
  select /*+ PARALLEL(HHA_REVENUE_CENTER_K,12) */ distinct bene_id from HHA_REVENUE_CENTER_K union
  select /*+ PARALLEL(MEDPAR_ALL,12) */ distinct bene_id from MEDPAR_ALL union
  select /*+ PARALLEL(HOSPICE_VALUE_CODES,12) */ distinct bene_id from HOSPICE_VALUE_CODES union
  select /*+ PARALLEL(MBSF_ABCD_SUMMARY,12) */ distinct bene_id from MBSF_ABCD_SUMMARY union
  select /*+ PARALLEL(OUTPATIENT_SPAN_CODES,12) */ distinct bene_id from OUTPATIENT_SPAN_CODES union
  select /*+ PARALLEL(HOSPICE_BASE_CLAIMS_K,12) */ distinct bene_id from HOSPICE_BASE_CLAIMS_K union
  select /*+ PARALLEL(OUTPATIENT_BASE_CLAIMS_K,12) */ distinct bene_id from OUTPATIENT_BASE_CLAIMS_K union
  select /*+ PARALLEL(OUTPATIENT_OCCURRNCE_CODES,12) */ distinct bene_id from OUTPATIENT_OCCURRNCE_CODES union
  select /*+ PARALLEL(OUTPATIENT_VALUE_CODES,12) */ distinct bene_id from OUTPATIENT_VALUE_CODES union
  select /*+ PARALLEL(OUTPATIENT_CONDITION_CODES,12) */ distinct bene_id from OUTPATIENT_CONDITION_CODES union
  select /*+ PARALLEL(BCARRIER_DEMO_CODES,12) */ distinct bene_id from BCARRIER_DEMO_CODES union
  select /*+ PARALLEL(HHA_DEMO_CODES,12) */ distinct bene_id from HHA_DEMO_CODES union
  select /*+ PARALLEL(HOSPICE_DEMO_CODES,12) */ distinct bene_id from HOSPICE_DEMO_CODES
  ) ubid
left join dob_shift on dob_shift.bene_id = ubid.bene_id
left join cms_id_sample.bene_id_mapping prev_ubid on prev_ubid.bene_id = ubid.bene_id
where prev_ubid.bene_id is null; 
commit;


-- Reuse bene_id_deid for users who where in previous CMS data.
insert /*+ APPEND */ into bene_id_mapping
select 
  ubid.bene_id bene_id,
  prev_ubid.bene_id_deid bene_id_deid,
  round(dbms_random.value(-364,0)) date_shift_days,
  dob_shift.dob_shift_months
from (
  select /*+ PARALLEL(HHA_OCCURRNCE_CODES,12) */ distinct bene_id from HHA_OCCURRNCE_CODES union
  select /*+ PARALLEL(PDE,12) */ distinct bene_id from PDE union
  select /*+ PARALLEL(BCARRIER_CLAIMS_K,12) */ distinct bene_id from BCARRIER_CLAIMS_K union
  select /*+ PARALLEL(HHA_VALUE_CODES,12) */ distinct bene_id from HHA_VALUE_CODES union
  select /*+ PARALLEL(HOSPICE_OCCURRNCE_CODES,12) */ distinct bene_id from HOSPICE_OCCURRNCE_CODES union
  select /*+ PARALLEL(HHA_CONDITION_CODES,12) */ distinct bene_id from HHA_CONDITION_CODES union
  select /*+ PARALLEL(OUTPATIENT_REVENUE_CENTER_K,12) */ distinct bene_id from OUTPATIENT_REVENUE_CENTER_K union
  select /*+ PARALLEL(HHA_SPAN_CODES,12) */ distinct bene_id from HHA_SPAN_CODES union
  select /*+ PARALLEL(HOSPICE_REVENUE_CENTER_K,12) */ distinct bene_id from HOSPICE_REVENUE_CENTER_K union
  select /*+ PARALLEL(HHA_BASE_CLAIMS_K,12) */ distinct bene_id from HHA_BASE_CLAIMS_K union
  select /*+ PARALLEL(BCARRIER_LINE_K,12) */ distinct bene_id from BCARRIER_LINE_K union
  select /*+ PARALLEL(HOSPICE_SPAN_CODES,12) */ distinct bene_id from HOSPICE_SPAN_CODES union
  select /*+ PARALLEL(HOSPICE_CONDITION_CODES,12) */ distinct bene_id from HOSPICE_CONDITION_CODES union
  select /*+ PARALLEL(HHA_REVENUE_CENTER_K,12) */ distinct bene_id from HHA_REVENUE_CENTER_K union
  select /*+ PARALLEL(MEDPAR_ALL,12) */ distinct bene_id from MEDPAR_ALL union
  select /*+ PARALLEL(HOSPICE_VALUE_CODES,12) */ distinct bene_id from HOSPICE_VALUE_CODES union
  select /*+ PARALLEL(MBSF_ABCD_SUMMARY,12) */ distinct bene_id from MBSF_ABCD_SUMMARY union
  select /*+ PARALLEL(OUTPATIENT_SPAN_CODES,12) */ distinct bene_id from OUTPATIENT_SPAN_CODES union
  select /*+ PARALLEL(HOSPICE_BASE_CLAIMS_K,12) */ distinct bene_id from HOSPICE_BASE_CLAIMS_K union
  select /*+ PARALLEL(OUTPATIENT_BASE_CLAIMS_K,12) */ distinct bene_id from OUTPATIENT_BASE_CLAIMS_K union
  select /*+ PARALLEL(OUTPATIENT_OCCURRNCE_CODES,12) */ distinct bene_id from OUTPATIENT_OCCURRNCE_CODES union
  select /*+ PARALLEL(OUTPATIENT_VALUE_CODES,12) */ distinct bene_id from OUTPATIENT_VALUE_CODES union
  select /*+ PARALLEL(OUTPATIENT_CONDITION_CODES,12) */ distinct bene_id from OUTPATIENT_CONDITION_CODES union
  select /*+ PARALLEL(BCARRIER_DEMO_CODES,12) */ distinct bene_id from BCARRIER_DEMO_CODES union
  select /*+ PARALLEL(HHA_DEMO_CODES,12) */ distinct bene_id from HHA_DEMO_CODES union
  select /*+ PARALLEL(HOSPICE_DEMO_CODES,12) */ distinct bene_id from HOSPICE_DEMO_CODES
  ) ubid
left join dob_shift on dob_shift.bene_id = ubid.bene_id
left join cms_id_sample.bene_id_mapping prev_ubid on prev_ubid.bene_id = ubid.bene_id
where prev_ubid.bene_id is not null; 
commit;
create unique index bene_id_mapping_bid_idx on bene_id_mapping (bene_id);
create unique index bene_id_mapping_deidbid_idx on bene_id_mapping (bene_id_deid);


-- Build the i2b2-shaped patient mapping in the DEID schema
whenever sqlerror continue;
drop table pmap_parts;
whenever sqlerror exit;

create table pmap_parts (
  BENE_ID_DEID VARCHAR2(15),
  MSIS_ID_DEID VARCHAR2(32),
  STATE_CD VARCHAR2(2),
  PATIENT_NUM number(38,0)
  );
alter table pmap_parts parallel (degree 12);

insert /*+ APPEND */ into pmap_parts
select /*+ PARALLEL(bene_id_mapping,12) */
  bmap.bene_id_deid, null msis_id_deid, null state_cd, 
  coalesce(to_number(bmap.bene_id_deid), bene_id_deid_seq.nextval) patient_num
from bene_id_mapping bmap
;
commit;

-- Insert bene_id_deid mappings
-- Distinct because one bene_id may be linked to multiple msis_id + state_cd and
-- therefore have multiple rows in the pmap_parts table.
insert /*+ APPEND */ into "&&deid_schema".patient_mapping
select /*+ PARALLEL(pmap_parts,12) */ distinct
  bene_id_deid patient_ide, bene_cd patient_ide_source, bene_id_deid patient_num,
  'A' patient_ide_status, '&&project_id' project_id, sysdate upload_date, sysdate update_date, 
  sysdate download_date, sysdate import_date, '&&cms_source_cd' sourcesystem_cd, &&upload_id upload_id
from pmap_parts
cross join cms_key_sources
where bene_id_deid is not null
;
commit;

-- Insert msis_id + state_cd mappings
insert /*+ APPEND */ into "&&deid_schema".patient_mapping
select /*+ PARALLEL(pmap_parts,12) */
  fmt_msis_pat_ide(to_char(msis_id_deid), state_cd) patient_ide, 
  msis_cd patient_ide_source, 
  patient_num,
  'A' patient_ide_status, '&&project_id' project_id, sysdate upload_date, sysdate update_date,
  sysdate download_date, sysdate import_date, '&&cms_source_cd' sourcesystem_cd, &&upload_id upload_id 
from pmap_parts
cross join cms_key_sources cks
where msis_id_deid is not null and state_cd is not null
;
commit;