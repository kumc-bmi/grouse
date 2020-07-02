-- cms_dtshift_init.sql: Prepare for de-identification of CMS data
-- Copyright (c) 2020 University of Kansas Medical Center

whenever sqlerror continue;
drop table date_events;
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
