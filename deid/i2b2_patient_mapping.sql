-- i2b2_patient_mapping.sql: create i2b2-like patient mapping
-- Copyright (c) 2020 University of Kansas Medical Center

-- Insert bene_id <-> bene_id_deid mappings
insert /*+ APPEND */ into "&&id_schema".patient_mapping
select /*+ PARALLEL(bene_id_mapping,12) */ 
  distinct
  bene_id patient_ide, 
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
from bene_id_mapping
cross join cms_key_sources
where bene_id_deid is not null
;
commit;


-- Insert bene_id_deid <-> msis_id + state_cd mappings
insert /*+ APPEND */ into "&&id_schema".patient_mapping
select /*+ PARALLEL(msis_id_mapping,12) */
  distinct
  fmt_msis_pat_ide(to_char(msis_id), state_cd) patient_ide, 
  msis_cd patient_ide_source, 
  bene_id_deid patient_num,
  'A' patient_ide_status, 
  '&&project_id' project_id, 
  sysdate upload_date, 
  sysdate update_date,
  sysdate download_date, 
  sysdate import_date, 
  '&&cms_source_cd' sourcesystem_cd, 
  &&upload_id upload_id 
from msis_id_mapping
cross join cms_key_sources cks
where msis_id_deid is not null and state_cd is not null and bene_id_deid is not null
;
commit;


-- Insert msis_id_deid <-> msis_id + state_cd mappings
insert /*+ APPEND */ into "&&id_schema".patient_mapping
select /*+ PARALLEL(msis_id_mapping,12) */
  distinct
  fmt_msis_pat_ide(to_char(msis_id), state_cd) patient_ide, 
  msis_cd patient_ide_source, 
  msis_id_deid patient_num,
  'A' patient_ide_status, 
  '&&project_id' project_id, 
  sysdate upload_date, 
  sysdate update_date,
  sysdate download_date, 
  sysdate import_date, 
  '&&cms_source_cd' sourcesystem_cd, 
  &&upload_id upload_id 
from msis_id_mapping
cross join cms_key_sources cks
where msis_id_deid is not null and state_cd is not null and bene_id_deid is null
;
commit;
