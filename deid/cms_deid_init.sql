-- cms_deid_init.sql: Prepare for de-identification of CMS data
-- Copyright (c) 2017 University of Kansas Medical Center

whenever sqlerror continue;
drop table date_events;
drop table min_max_date_events;
drop table dob_shift;
whenever sqlerror exit;

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
