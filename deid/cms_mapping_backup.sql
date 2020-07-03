-- cms_mapping_backup.sql: populate the backup table which will be important reference for de-id next time
-- Copyright (c) 2020 University of Kansas Medical Center

insert into /*+ APPEND */ bene_id_mapping_backup
select /*+ PARALLEL(bene_id_mapping,12) */ 
       bene_id,
       bene_id_deid,
       date_shift_days,
       null,
       birth_date,
       birth_date_hipaa,
       index_date,
       current_date,
       '&&cms_id_schema'
from "&&cms_id_schema".bene_id_mapping
;

insert into /*+ APPEND */ msis_id_mapping_backup
select /*+ PARALLEL(msis_id_mapping,12) */ 
       msis_id,
       state_cd,
       msis_is_deid,
       bene_id,
       bene_id_deid,
       date_shift_days,
       null,
       birth_date,
       birth_date_hipaa,
       index_date,
       current_date,
       '&&cms_id_schema'
from "&&cms_id_schema".msis_id_mapping
;

  
