-- i2b2_patient_mapping_init.sql: Prepare for i2b2-style de-id patient mapping.
-- Copyright (c) 2017 University of Kansas Medical Center

-- Indexes like i2b2 sources: crc_create_datamart_oracle.sql
create index pm_uploadid_idx on patient_mapping(upload_id);

create index pm_patnum_idx on patient_mapping(patient_num);

create index pm_encpnum_idx on
patient_mapping(patient_ide,patient_ide_source,patient_num);
