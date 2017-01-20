-- bene_id_mapping.sql: Create bene_id mapping table for CMS deidentification
-- Copyright (c) 2017 University of Kansas Medical Center

whenever sqlerror continue;
drop sequence bene_id_deid_seq;
drop sequence msis_id_deid_seq;
drop table bene_id_mapping;
drop table msis_id_mapping;
drop table msis_person;
drop table date_events;
whenever sqlerror exit;

create sequence bene_id_deid_seq
  start with 1
  increment by 1
  cache 1024;

create sequence msis_id_deid_seq
  start with 1
  increment by 1
  cache 1024;

create table bene_id_mapping (
  -- Width of 15 as per the file transfer summary documents from CMS/RESDAC
  BENE_ID VARCHAR2(15),
  BENE_ID_DEID VARCHAR2(15),
  DATE_SHIFT_DAYS INTEGER
  );
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
  DATE_SHIFT_DAYS INTEGER
  );
alter table msis_person parallel (degree 12);
 
create table msis_id_mapping (
  MSIS_ID VARCHAR2(32),
  MSIS_ID_DEID VARCHAR2(32)
  );
alter table msis_id_mapping parallel (degree 12);

--select 
--'select /*+ PARALLEL(' || table_name ||',12) */ distinct bene_id from ' || table_name || ' union'
--from dba_tables where owner = 'CMS_ID_SAMPLE' and table_name not like 'REF_%' and table_name != 'BENE_ID_MAPPING';
insert /*+ APPEND */ into bene_id_mapping
select 
  ubid.bene_id bene_id,
  to_char(bene_id_deid_seq.nextval) bene_id_deid,
  round(dbms_random.value(-364,0)) date_shift_days
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
  ) ubid;
commit;

create unique index bene_id_mapping_bid_idx on bene_id_mapping (bene_id);
create unique index bene_id_mapping_deidbid_idx on bene_id_mapping (bene_id_deid);

-- First, insert people without bene_ids (who therefore need a date shift)
insert /*+ APPEND */ into msis_person
select
  umid.msis_id msis_id,
  umid.state_cd state_cd,
  round(dbms_random.value(-364,0)) date_shift_days
from (
  -- The Personal Summary File contains one record for every individual enrolled 
  -- for at least one day during the year.
  -- https://www.resdac.org/cms-data/files/max-ps
  select /*+ PARALLEL(MAXDATA_PS,12) */ distinct msis_id, state_cd from MAXDATA_PS
  where bene_id is null
  ) umid;
commit;

-- Next, get everyone who has a bene_id (who will get a date shift per bene_id)
insert /*+ APPEND */ into msis_person
select
  umid.msis_id msis_id,
  umid.state_cd state_cd,
  null date_shift_days
from (
  select /*+ PARALLEL(MAXDATA_PS,12) */ distinct msis_id, state_cd from MAXDATA_PS
  where bene_id is not null
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

create table date_events (
  TABLE_NAME VARCHAR2(32),
  BENE_ID VARCHAR2(15),
  MSIS_ID VARCHAR2(32),
  STATE_CD VARCHAR2(2),
  COL_DATE VARCHAR2(32),
  DT DATE
  );
alter table date_events parallel (degree 12);
