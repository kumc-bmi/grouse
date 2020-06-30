-- cms_deid_mapping.sql: Create bene_id and msis_id mapping table for CMS deidentification
-- Copyright (c) 2020 University of Kansas Medical Center

drop table dob_col purge;
create table dob_col as
select table_name
      ,column_name
      ,owner
      ,case when table_name like 'MAX%' then 1       -- special identifiers in medicaid table names (may need to specify mannually)
            else 0
       end as msis_ind
from all_tab_columns
where owner = '&&CMS_ID' and
      column_name in ('BENE_BIRTH_DT', 'EL_DOB', 'DOB_DT') --need to identify manually
;

-- test
--undefine prev_cms_id_schema;
--undefine bene_id_map_prev_yrs_cumu;
--
--select 'INSERT INTO /*+ APPEND*/ bene_id_mapping'
--              || ' SELECT ubid2.bene_id,
--                          coalesce(prev_ubid.bene_id_deid,to_char(bene_id_deid_seq.nextval)) bene_id_deid,
--                          coalesce(prev_ubid.date_shift_days,round(dbms_random.value(-364,0))) date_shift_days,
--                          coalesce(prev_ubid.birth_date,ubid2.birth_date) birth_date'
--              ||' FROM ( '
--              || ' SELECT /*+ PARALLEL('|| rec.table_name || ',12) */ ' 
--              || ' ubid.bene_id, ' || 'ubid.' || rec.column_name || ' birth_date, '
--              || ' row_number() over (partition by ubid.bene_id) rn ' 
--              || ' FROM ' || rec.owner || '.' || rec.table_name || ' ubid '
--              || ' where not exists (select 1 from bene_id_mapping pre_ubid on pre_ubid.bene_id = ubid.bene_id)) ubid2' 
--              || ' LEFT JOIN ' || '&&prev_cms_id_schema.&&bene_id_map_prev_yrs_cumu' ||' prev_ubid '
--              || ' on prev_ubid.bene_id = ubid2.bene_id and ubid2.rn = 1'
--from dob_col rec
--;
--
--undefine prev_cms_id_schema;
--undefine msis_person_prev_yrs_cumu;
--select 'INSERT INTO /*+ APPEND*/ msis_id_mapping '
--              || 'SELECT umid2.msis_id msis_id,
--                         umid2.state_cd state_cd,
--                         coalesce(prev_msis.bene_id,umid2.bene_id) bene_id,
--                         coalesce(prev_msis.date_shift_days,round(dbms_random.value(-364,0)) date_shift_days,
--                         coalesce(prev_msis.birth_date,umid2.birth_date) birth_date'
--              ||' FROM ( '
--              || ' SELECT /*+ PARALLEL('|| rec.table_name || ',12) */ ' 
--              || ' umid.msis_id, umid.state_cd,' || ' umid.' || rec.column_name || ' birth_date,'
--              || ' coalesce(umid.bene_id,prev_msis.bene_id) bene_id,'
--              || ' row_number() over (partition by umid.msis_id, umid.state_cd) rn ' 
--              || ' FROM ' || rec.owner || '.' || rec.table_name || ' umid '
--              || ' where not exists (select 1 from msis_id_mapping pre_umid on pre_umid.bene_id = umid.bene_id)) umid2' 
--              || ' LEFT JOIN ' || '&&prev_cms_id_schema.&&msis_person_prev_yrs_cumu' ||' prev_msis '
--              || ' on prev_msis.msis_id = umid.msis_id and prev_msis.state_cd = umid.state_cd and umid2.rn = 1'
--from dob_col rec
--;


declare
   sql_stmt varchar(4000);

begin
  for rec in (select table_name,
                     column_name,
                     msis_ind
              from date_col)
  loop
  -- stack bene_id_mapping
  if rec.msis_ind = 0 then
    sql_stmt := 'INSERT INTO /*+ APPEND*/ bene_id_mapping'
              || ' SELECT ubid2.bene_id,
                          coalesce(prev_ubid.bene_id_deid,to_char(bene_id_deid_seq.nextval)) bene_id_deid,
                          coalesce(prev_ubid.date_shift_days,round(dbms_random.value(-364,0))) date_shift_days,
                          coalesce(prev_ubid.birth_date,ubid2.birth_date) birth_date'
              ||' FROM ( '
              || ' SELECT /*+ PARALLEL('|| rec.table_name || ',12) */ ' 
              || ' ubid.bene_id, ' || 'ubid.' || rec.column_name || ' birth_date, '
              || ' row_number() over (partition by ubid.bene_id) rn ' 
              || ' FROM ' || rec.owner || '.' || rec.table_name || ' ubid '
              || ' where not exists (select 1 from bene_id_mapping pre_ubid on pre_ubid.bene_id = ubid.bene_id)) ubid2' 
              || ' LEFT JOIN ' || '&&prev_cms_id_schema.&&bene_id_map_prev_yrs_cumu' ||' prev_ubid '
              || ' on prev_ubid.bene_id = ubid2.bene_id and ubid2.rn = 1';
  else
  -- stack msis_id_mapping
    sql_stmt := 'INSERT INTO /*+ APPEND*/ msis_id_mapping '
              || 'SELECT umid2.msis_id msis_id,
                         umid2.state_cd state_cd,
                         coalesce(prev_msis.msis_id_deid, to_char(msis_id_deid_seq.nextval)) msis_id_deid,
                         coalesce(prev_msis.bene_id,umid2.bene_id) bene_id,
                         coalesce(prev_msis.date_shift_days,round(dbms_random.value(-364,0)) date_shift_days,
                         coalesce(prev_msis.birth_date,umid2.birth_date) birth_date'
              ||' FROM ( '
              || ' SELECT /*+ PARALLEL('|| rec.table_name || ',12) */ ' 
              || ' umid.msis_id, umid.state_cd,' || ' umid.' || rec.column_name || ' birth_date,'
              || ' coalesce(umid.bene_id,prev_msis.bene_id) bene_id,'
              || ' row_number() over (partition by umid.msis_id, umid.state_cd) rn ' 
              || ' FROM ' || rec.owner || '.' || rec.table_name || ' umid '
              || ' where not exists (select 1 from msis_id_mapping pre_umid on pre_umid.bene_id = umid.bene_id)) umid2' 
              || ' LEFT JOIN ' || '&&prev_cms_id_schema.&&msis_person_prev_yrs_cumu' ||' prev_msis '
              || ' on prev_msis.msis_id = umid.msis_id and prev_msis.state_cd = umid.state_cd and umid2.rn = 1';
  end if;
  execute immediate sql_stmt; 
  commit;
  end loop;
end;

create unique index msis_id_mapping_mid_idx on msis_id_mapping (msis_id);
create unique index msis_id_mapping_deidmid_idx on msis_id_mapping (msis_id_deid);

------------------------------------------------------------------------------------------
-- Insert bene_id_deid mappings
-- Distinct because one bene_id may be linked to multiple msis_id + state_cd and
-- therefore have multiple rows in the pmap_parts table.
insert /*+ APPEND */ into "&&deid_schema".patient_mapping
select /*+ PARALLEL(pmap_parts,12) */ 
  distinct
  bene_id_deid patient_ide, 
  bene_cd patient_ide_source, 
  bene_id_deid patient_num,
  'A' patient_ide_status, 
  '&&project_id' project_id, 
  sysdate upload_date, 
  sysdate update_date, 
  sysdate download_date, 
  sysdate import_date, 
  '&&cms_source_cd' sourcesystem_cd, 
  &&upload_id upload_id
from pmap_parts
cross join cms_key_sources
where bene_id_deid is not null
;
commit;


-- Insert msis_id + state_cd mappings
insert /*+ APPEND */ into "&&deid_schema".patient_mapping
select /*+ PARALLEL(pmap_parts,12) */
  distinct
  fmt_msis_pat_ide(to_char(msis_id_deid), state_cd) patient_ide, 
  msis_cd patient_ide_source, 
  patient_num,
  'A' patient_ide_status, 
  '&&project_id' project_id, 
  sysdate upload_date, 
  sysdate update_date,
  sysdate download_date, 
  sysdate import_date, 
  '&&cms_source_cd' sourcesystem_cd, 
  &&upload_id upload_id 
from pmap_parts
cross join cms_key_sources cks
where msis_id_deid is not null and state_cd is not null
;
commit;

