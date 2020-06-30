-- i2b2_patient_mapping.sql: create i2b2-like patient mapping
-- Copyright (c) 2020 University of Kansas Medical Center

-- Insert bene_id_deid mappings
-- Distinct because one bene_id may be linked to multiple msis_id + state_cd and
-- therefore have multiple rows in the pmap_parts table.
insert /*+ APPEND */ into "&&deid_schema".patient_mapping
select /*+ PARALLEL(pmap_parts,12) */ 
  distinct
  bene_id_deid patient_ide, 
  bene_cd patient_ide_source, 
  bene_id_deid patient_num,
  'A' patient_ide_status, 
  '&&project_id' project_id, 
  sysdate upload_date, 
  sysdate update_date, 
  sysdate download_date, 
  sysdate import_date, 
  '&&cms_source_cd' sourcesystem_cd, 
  &&upload_id upload_id
from pmap_parts
cross join cms_key_sources
where bene_id_deid is not null
;
commit;


-- Insert msis_id + state_cd mappings
insert /*+ APPEND */ into "&&deid_schema".patient_mapping
select /*+ PARALLEL(pmap_parts,12) */
  distinct
  fmt_msis_pat_ide(to_char(msis_id_deid), state_cd) patient_ide, 
  msis_cd patient_ide_source, 
  patient_num,
  'A' patient_ide_status, 
  '&&project_id' project_id, 
  sysdate upload_date, 
  sysdate update_date,
  sysdate download_date, 
  sysdate import_date, 
  '&&cms_source_cd' sourcesystem_cd, 
  &&upload_id upload_id 
from pmap_parts
cross join cms_key_sources cks
where msis_id_deid is not null and state_cd is not null
;
commit;

