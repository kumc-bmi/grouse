-- cms_deid_init.sql: Prepare for de-identification of CMS data
-- Copyright (c) 2017 University of Kansas Medical Center

whenever sqlerror continue;
drop sequence bene_id_deid_seq;
drop sequence msis_id_deid_seq;
drop table bene_id_mapping;
drop table msis_id_mapping;
drop table msis_person;
drop table date_events;
drop table min_max_date_events;
drop table dob_shift;
whenever sqlerror exit;

-- De-identified bene_id (1:1 bene_id to sequence number mapping)
create sequence bene_id_deid_seq
  start with 1
  increment by 1
  cache 1024;

-- De-identified msis_id (1:1 msis_id to sequence number mapping)
create sequence msis_id_deid_seq
  start with 1
  increment by 1
  cache 1024;

create table bene_id_mapping (
  -- Width of 15 as per the file transfer summary documents from CMS/RESDAC
  BENE_ID VARCHAR2(15),
  BENE_ID_DEID VARCHAR2(15),
  DATE_SHIFT_DAYS INTEGER,
  DOB_SHIFT_MONTHS INTEGER
  );
-- Parallel degree 12 is a somewhat arbitrary value that works well at KUMC
alter table bene_id_mapping parallel (degree 12);

-- From the CMS CCW documentation, the unique key for a given individual without
-- a bene_id is msis_id + state_cd.
-- Different people (bene_ids) from different states may have the same msis_id.
-- So, we want a date shift per person, but preserve the msis_id collisions.
create table msis_person (
  -- Width of 32 as per the file transfer summary documents from CMS/RESDAC
  MSIS_ID VARCHAR2(32),
  -- Width of 2 as per the file transfer summary documents from CMS/RESDAC
  STATE_CD VARCHAR2(2),
  -- Date shift for Medicaid patients who have null bene_id
  DATE_SHIFT_DAYS INTEGER,
  DOB_SHIFT_MONTHS INTEGER
  );
alter table msis_person parallel (degree 12);
 
create table msis_id_mapping (
  MSIS_ID VARCHAR2(32),
  MSIS_ID_DEID VARCHAR2(32)
  );
alter table msis_id_mapping parallel (degree 12);

-- All date fields from all tables
create table date_events (
  TABLE_NAME VARCHAR2(32),
  BENE_ID VARCHAR2(15),
  MSIS_ID VARCHAR2(32),
  STATE_CD VARCHAR2(2),
  COL_DATE VARCHAR2(32),
  DT DATE
  );
alter table date_events parallel (degree 12);

-- min/max date events per either bene_id or msis_id + state_cd
create table min_max_date_events (
  BENE_ID VARCHAR2(15),
  MSIS_ID VARCHAR2(32),
  STATE_CD VARCHAR2(2),
  MIN_DT DATE,
  MAX_DT DATE
  );
alter table min_max_date_events parallel (degree 12);

-- Months to shift each person
create table dob_shift (
  BENE_ID VARCHAR2(15),
  MSIS_ID VARCHAR2(32),
  STATE_CD VARCHAR2(2),
  DOB_SHIFT_MONTHS INTEGER
  );
alter table dob_shift parallel (degree 12);
