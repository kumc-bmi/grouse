-- i2b2_patient_mapping_init.sql: Prepare for i2b2-style de-id patient mapping.
-- Copyright (c) 2020 University of Kansas Medical Center

-- Build the i2b2-shaped patient mapping in the ID schema
whenever sqlerror continue;
drop table patient_mapping;
whenever sqlerror exit;

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
