This folder contains scripts to integrate KUMC i2b2 data with GROUSE/CMS i2b2 data]

The scripts in the folder should be run in to the following order:
  - [patient_hash_map.sql](patient_hash_map.sql) builds a patient_hash_map table. This file does not have to run for sites other than KUMC. 
  - [cms_site_mapping.sql](cms_kumc_mapping.sql) builds a mapping that links blueheron patient_nums with grouse i2b2 patient_nums.
  - [cms_site_mapping_dedup.sql](cms_kumc_mapping_dedup.sql) identifies duplicates within the blueheron to grouse mapping table so they can be eliminated. 
  - [cms_site_dimensions_build.sql](cms_kumc_dimensions_build.sql) builds a new patient_dimension and visit_dimension that can be appended to grouse i2b2 patient_dimension and visit_dimension respectively.
  - [cms_site_facts_build.sql](cms_kumc_facts_build.sql) builds a new observation_fact table that can be appended to the grouse i2b2 observation_fact table.  
