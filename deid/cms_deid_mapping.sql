-- cms_deid_mapping.sql: Create bene_id and msis_id mapping table for CMS deidentification
-- Copyright (c) 2020 University of Kansas Medical Center

whenever sqlerror continue;
drop table dob_col purge;
whenever sqlerror exit;

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
/

declare
   sql_stmt varchar(4000);

begin
  for rec in (select owner,
                     table_name,
                     column_name,
                     msis_ind
              from dob_col)
  loop
  if rec.msis_ind = 0 then
    sql_stmt := 'INSERT INTO /*+ APPEND*/ bene_id_mapping'
              || ' SELECT ubid2.bene_id,
                          coalesce(prev_ubid.bene_id_deid,to_char(bene_id_deid_seq.nextval)) bene_id_deid,
                          coalesce(prev_ubid.date_shift_days,round(dbms_random.value(-364,0))) date_shift_days,
                          ubid2.birth_date'
              ||' FROM ( '
              || ' SELECT /*+ PARALLEL('|| rec.table_name || ',12) */ ' 
              || ' ubid.bene_id, ' || 'ubid.' || rec.column_name || ' birth_date, '
              || ' row_number() over (partition by ubid.bene_id order by ubid.bene_id) rn ' 
              || ' FROM ' || rec.owner || '.' || rec.table_name || ' ubid) ubid2' 
              || ' LEFT JOIN ' || '&&prev_cms_id_schema'||'.'||'&&bene_id_map_prev_yrs_cumu' ||' prev_ubid '
              || ' on prev_ubid.bene_id = ubid2.bene_id and ubid2.rn = 1';
  else
    sql_stmt := 'INSERT INTO /*+ APPEND*/ msis_id_mapping '
              || 'SELECT umid2.msis_id msis_id,
                         umid2.state_cd state_cd,
                         coalesce(prev_msis.msis_id_deid, to_char(msis_id_deid_seq.nextval)) msis_id_deid,
                         coalesce(prev_msis.bene_id,umid2.bene_id) bene_id,
                         coalesce(prev_msis.date_shift_days,round(dbms_random.value(-364,0)) date_shift_days,
                         umid2.birth_date'
              ||' FROM ( '
              || ' SELECT /*+ PARALLEL('|| rec.table_name || ',12) */ ' 
              || ' umid.msis_id, umid.state_cd,' || ' umid.' || rec.column_name || ' birth_date,'
              || ' coalesce(umid.bene_id,prev_msis.bene_id) bene_id,'
              || ' row_number() over (partition by umid.msis_id, umid.state_cd order by umid.msis_id) rn ' 
              || ' FROM ' || rec.owner || '.' || rec.table_name || ' umid) umid2' 
              || ' LEFT JOIN ' || '&&prev_cms_id_schema' || '.' || '&&msis_person_prev_yrs_cumu' ||' prev_msis '
              || ' on prev_msis.msis_id = umid.msis_id and prev_msis.state_cd = umid.state_cd and umid2.rn = 1';
  end if;
  execute immediate sql_stmt; 
  commit;
  end loop;
end;

