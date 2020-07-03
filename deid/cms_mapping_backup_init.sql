-- cms_mapping_backup_init.sql: Prepare for backing up all CMS patient mappings over releases
-- Copyright (c) 2020 University of Kansas Medical Center

create table bene_id_mapping_backup (
  -- Width of 15 as per the file transfer summary documents from CMS/RESDAC
  BENE_ID VARCHAR2(15) not null,
  BENE_ID_DEID VARCHAR2(15) not null,
  DATE_SHIFT_DAYS INTEGER,
  DOB_SHIFTS_MONTHS INTEGER,
  BIRTH_DATE DATE,
  BIRTH_DATE_HIPAA DATE,
  INDEX_DATE DATE,
  BACKUP_DATE DATE,
  SOURCESYSTEM_CD VARCHAR2(15)
  );
-- Parallel degree 12 is a somewhat arbitrary value that works well at KUMC
alter table bene_id_mapping_backup parallel (degree 12);


-- From the CMS CCW documentation, the unique key for a given individual without
-- a bene_id is msis_id + state_cd.
-- Different people (bene_ids) from different states may have the same msis_id.
-- So, we want a date shift per person, but preserve the msis_id collisions.
create table msis_id_mapping_backup (
  -- Width of 32 as per the file transfer summary documents from CMS/RESDAC
  MSIS_ID VARCHAR2(32) not null,
  -- Width of 2 as per the file transfer summary documents from CMS/RESDAC
  STATE_CD VARCHAR2(2) not null,
  MSIS_ID_DEID VARCHAR2(32) not null,
  BENE_ID VARCHAR2(15),
  -- Date shift for Medicaid patients 
  DATE_SHIFT_DAYS INTEGER,
  BIRTH_DATE DATE,
  BIRTH_DATE_HIPAA DATE,
  INDEX_DATE DATE,
  BACKUP_DATE DATE,
  SOURCESYSTEM_CD VARCHAR2(15)
  );
alter table msis_id_mapping_backup parallel (degree 12);
