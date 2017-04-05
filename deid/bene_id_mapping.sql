-- bene_id_mapping.sql: Create bene_id mapping table for CMS deidentification
-- Copyright (c) 2017 University of Kansas Medical Center

--select 
--'select /*+ PARALLEL(' || table_name ||',12) */ distinct bene_id from ' || table_name || ' union'
--from dba_tables where owner = 'CMS_ID_SAMPLE' and table_name not like 'REF_%' and table_name != 'BENE_ID_MAPPING';
insert /*+ APPEND */ into bene_id_mapping
select 
  ubid.bene_id bene_id,
  to_char(bene_id_deid_seq.nextval) bene_id_deid,
  round(dbms_random.value(-364,0)) date_shift_days,
  dob_shift.dob_shift_months
from (
  select /*+ PARALLEL(HHA_OCCURRNCE_CODES,12) */ distinct bene_id from HHA_OCCURRNCE_CODES union
  select /*+ PARALLEL(PDE,12) */ distinct bene_id from PDE union
  select /*+ PARALLEL(BCARRIER_CLAIMS,12) */ distinct bene_id from BCARRIER_CLAIMS union
  select /*+ PARALLEL(HHA_VALUE_CODES,12) */ distinct bene_id from HHA_VALUE_CODES union
  select /*+ PARALLEL(HOSPICE_OCCURRNCE_CODES,12) */ distinct bene_id from HOSPICE_OCCURRNCE_CODES union
  select /*+ PARALLEL(MBSF_D_CMPNTS,12) */ distinct bene_id from MBSF_D_CMPNTS union
  select /*+ PARALLEL(HHA_CONDITION_CODES,12) */ distinct bene_id from HHA_CONDITION_CODES union
  select /*+ PARALLEL(PDE_SAF,12) */ distinct bene_id from PDE_SAF union
  select /*+ PARALLEL(OUTPATIENT_REVENUE_CENTER,12) */ distinct bene_id from OUTPATIENT_REVENUE_CENTER union
  select /*+ PARALLEL(HHA_SPAN_CODES,12) */ distinct bene_id from HHA_SPAN_CODES union
  select /*+ PARALLEL(HOSPICE_REVENUE_CENTER,12) */ distinct bene_id from HOSPICE_REVENUE_CENTER union
  select /*+ PARALLEL(HHA_BASE_CLAIMS,12) */ distinct bene_id from HHA_BASE_CLAIMS union
  select /*+ PARALLEL(MAXDATA_LT,12) */ distinct bene_id from MAXDATA_LT union
  select /*+ PARALLEL(MAXDATA_OT,12) */ distinct bene_id from MAXDATA_OT union
  select /*+ PARALLEL(BCARRIER_LINE,12) */ distinct bene_id from BCARRIER_LINE union
  select /*+ PARALLEL(MAXDATA_RX,12) */ distinct bene_id from MAXDATA_RX union
  select /*+ PARALLEL(MAXDATA_PS,12) */ distinct bene_id from MAXDATA_PS union
  select /*+ PARALLEL(HOSPICE_SPAN_CODES,12) */ distinct bene_id from HOSPICE_SPAN_CODES union
  select /*+ PARALLEL(HOSPICE_CONDITION_CODES,12) */ distinct bene_id from HOSPICE_CONDITION_CODES union
  select /*+ PARALLEL(HHA_REVENUE_CENTER,12) */ distinct bene_id from HHA_REVENUE_CENTER union
  select /*+ PARALLEL(MAXDATA_IP,12) */ distinct bene_id from MAXDATA_IP union
  select /*+ PARALLEL(MEDPAR_ALL,12) */ distinct bene_id from MEDPAR_ALL union
  select /*+ PARALLEL(HOSPICE_VALUE_CODES,12) */ distinct bene_id from HOSPICE_VALUE_CODES union
  select /*+ PARALLEL(MBSF_AB_SUMMARY,12) */ distinct bene_id from MBSF_AB_SUMMARY union
  select /*+ PARALLEL(OUTPATIENT_SPAN_CODES,12) */ distinct bene_id from OUTPATIENT_SPAN_CODES union
  select /*+ PARALLEL(HOSPICE_BASE_CLAIMS,12) */ distinct bene_id from HOSPICE_BASE_CLAIMS union
  select /*+ PARALLEL(OUTPATIENT_BASE_CLAIMS,12) */ distinct bene_id from OUTPATIENT_BASE_CLAIMS union
  select /*+ PARALLEL(OUTPATIENT_OCCURRNCE_CODES,12) */ distinct bene_id from OUTPATIENT_OCCURRNCE_CODES union
  select /*+ PARALLEL(OUTPATIENT_VALUE_CODES,12) */ distinct bene_id from OUTPATIENT_VALUE_CODES union
  select /*+ PARALLEL(OUTPATIENT_CONDITION_CODES,12) */ distinct bene_id from OUTPATIENT_CONDITION_CODES
  ) ubid
left join dob_shift on dob_shift.bene_id = ubid.bene_id;
commit;

create unique index bene_id_mapping_bid_idx on bene_id_mapping (bene_id);
create unique index bene_id_mapping_deidbid_idx on bene_id_mapping (bene_id_deid);

-- First, insert people without bene_ids (who therefore need a date shift)
insert /*+ APPEND */ into msis_person
select
  umid.bene_id bene_id,
  umid.msis_id msis_id,
  umid.state_cd state_cd,
  round(dbms_random.value(-364,0)) date_shift_days,
  dob_shift.dob_shift_months
from (
  -- The Personal Summary File contains one record for every individual enrolled 
  -- for at least one day during the year.  So, it's not actually necessary to
  -- select from the union of all the Medicaid tables with the production data.
  -- However, in order for the process to work for the sample data, we need to
  -- look at all the tables.
  -- https://www.resdac.org/cms-data/files/max-ps
  select /*+ PARALLEL(MAXDATA_PS,12) */ distinct bene_id, msis_id, state_cd from MAXDATA_PS where bene_id is null
  union
  select /*+ PARALLEL(MAXDATA_IP,12) */ distinct bene_id, msis_id, state_cd from MAXDATA_IP where bene_id is null
  union
  select /*+ PARALLEL(MAXDATA_LT,12) */ distinct bene_id, msis_id, state_cd from MAXDATA_LT where bene_id is null
  union
  select /*+ PARALLEL(MAXDATA_OT,12) */ distinct bene_id, msis_id, state_cd from MAXDATA_OT where bene_id is null
  union
  select /*+ PARALLEL(MAXDATA_RX,12) */ distinct bene_id, msis_id, state_cd from MAXDATA_RX where bene_id is null
  ) umid
left join dob_shift on dob_shift.msis_id = umid.msis_id 
  and dob_shift.state_cd = umid.state_cd
;
commit;

-- Next, get everyone who has a bene_id (who will get a date/dob shift per bene_id)
insert /*+ APPEND */ into msis_person
select
  umid.bene_id bene_id,
  umid.msis_id msis_id,
  umid.state_cd state_cd,
  null date_shift_days,
  null dob_shift_months
from (
  select /*+ PARALLEL(MAXDATA_PS,12) */ distinct bene_id, msis_id, state_cd from MAXDATA_PS where bene_id is not null
  union
  select /*+ PARALLEL(MAXDATA_IP,12) */ distinct bene_id, msis_id, state_cd from MAXDATA_IP where bene_id is not null
  union
  select /*+ PARALLEL(MAXDATA_LT,12) */ distinct bene_id, msis_id, state_cd from MAXDATA_LT where bene_id is not null
  union
  select /*+ PARALLEL(MAXDATA_OT,12) */ distinct bene_id, msis_id, state_cd from MAXDATA_OT where bene_id is not null
  union
  select /*+ PARALLEL(MAXDATA_RX,12) */ distinct bene_id, msis_id, state_cd from MAXDATA_RX where bene_id is not null
  ) umid;
commit;

create unique index msis_person_mid_st_idx on msis_person (msis_id, state_cd);

insert /*+ APPEND */ into msis_id_mapping
select 
  umid.msis_id msis_id,
  to_char(msis_id_deid_seq.nextval) msis_id_deid
from (
  select /*+ PARALLEL(msis_person,12) */ distinct msis_id from msis_person
  ) umid;
commit;

create unique index msis_id_mapping_mid_idx on msis_id_mapping (msis_id);
create unique index msis_id_mapping_deidmid_idx on msis_id_mapping (msis_id_deid);

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
select /*+ PARALLEL(bene_id_mapping,12) PARALLEL(msis_person,12) PARALLEL(msis_id_mapping,12) */
  bmap.bene_id_deid, mmap.msis_id_deid, mper.state_cd, 
  coalesce(to_number(bmap.bene_id_deid), bene_id_deid_seq.nextval) patient_num
from bene_id_mapping bmap
left join msis_person mper on mper.bene_id = bmap.bene_id
left join msis_id_mapping mmap on mmap.msis_id = mper.msis_id
;
commit;

-- Insert bene_id_deid mappings
-- Distinct because one bene_id may be linked to multiple msis_id + state_cd and
-- therefore have multiple rows in the pmap_parts table.
insert /*+ APPEND */ into "&&deid_schema".patient_mapping
select /*+ PARALLEL(pmap_parts,12) */ distinct
  bene_id_deid patient_ide, bene_cd patient_ide_source, bene_id_deid patient_num,
  'A' patient_ide_status, :project_id project_id, sysdate upload_date, sysdate update_date, 
  sysdate download_date, sysdate import_date, :cms_source_cd sourcesystem_cd, :upload_id upload_id
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
  'A' patient_ide_status, :project_id project_id, sysdate upload_date, sysdate update_date,
  sysdate download_date, sysdate import_date, :cms_source_cd sourcesystem_cd, :upload_id upload_id 
from pmap_parts
cross join cms_key_sources cks
where msis_id_deid is not null and state_cd is not null
;
commit;
