-- cms_mapping_init.sql: Prepare for CMS patient mapping (bene_id,msis_id, etc.)
-- Copyright (c) 2020 University of Kansas Medical Center

whenever sqlerror continue;
drop sequence bene_id_deid_seq;
drop sequence msis_id_deid_seq;
drop table bene_id_mapping;
drop table msis_id_mapping; 
drop table patient_mapping;
whenever sqlerror exit;

-- De-identified bene_id (1:1 bene_id to sequence number mapping)
-- bene_id_deid_start = previous year's max bene_id_deid + 10
declare 
   bene_seq_stmt VARCHAR2(1000);
begin
   select 'create sequence bene_id_deid_seq'
        ||' start with '|| (max(to_number(bene_id_deid)) + 10)
        ||' increment by 1 cache 1024'
   into bene_seq_stmt
   from "&&prev_cms_id_schema"."&&bene_id_map_prev_yrs_cumu";
   execute immediate bene_seq_stmt;
end;
/

-- De-identified msis_id (1:1 msis_id to sequence number mapping)
-- msis_id_deid_seq_start = previous year's max msis_id_deid + 1
declare 
   msis_seq_stmt VARCHAR2(1000);
begin
   select 'create sequence msis_id_deid_seq'
        ||' start with '|| (max(to_number(msis_id_deid)) + 10)
        ||' increment by 1 cache 1024'
   into msis_seq_stmt
   from "&&prev_cms_id_schema"."&&msis_id_map_prev_yrs_cumu";
   execute immediate msis_seq_stmt;
end;
/

create table bene_id_mapping (
  -- Width of 15 as per the file transfer summary documents from CMS/RESDAC
  BENE_ID VARCHAR2(15),
  BENE_ID_DEID VARCHAR2(15),
  DATE_SHIFT_DAYS INTEGER,
  BIRTH_DATE DATE
  );
-- Parallel degree 12 is a somewhat arbitrary value that works well at KUMC
alter table bene_id_mapping parallel (degree 12);


-- From the CMS CCW documentation, the unique key for a given individual without
-- a bene_id is msis_id + state_cd.
-- Different people (bene_ids) from different states may have the same msis_id.
-- So, we want a date shift per person, but preserve the msis_id collisions.
create table msis_id_mapping (
  -- Width of 32 as per the file transfer summary documents from CMS/RESDAC
  MSIS_ID VARCHAR2(32),
  -- Width of 2 as per the file transfer summary documents from CMS/RESDAC
  STATE_CD VARCHAR2(2),
  MSIS_ID_DEID VARCHAR2(32),
  BENE_ID VARCHAR2(15),
  -- Date shift for Medicaid patients 
  DATE_SHIFT_DAYS INTEGER,
  BIRTH_DATE DATE
  );
alter table msis_id_mapping parallel (degree 12);


-- Build the i2b2-shaped patient mapping in the DEID schema
-- from i2b2 sources: crc_create_datamart_oracle.sql
create table patient_mapping (
    patient_ide         varchar2(200) not null,
    patient_ide_source  varchar2(50) not null,
    patient_num         number(38,0) not null,
    patient_ide_status  varchar2(50),
    project_id          varchar2(50) not null,
    upload_date         date,
    update_date         date,
    download_date       date,
    import_date         date,
    sourcesystem_cd     varchar2(50),
    upload_id           number(38,0),
    constraint patient_mapping_pk primary key(patient_ide, patient_ide_source, project_id)
 )
;
alter table patient_mapping parallel (degree 12);
