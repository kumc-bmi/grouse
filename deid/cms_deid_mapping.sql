-- cms_deid_mapping.sql: populate bene_id_mapping and msis_id_mapping
-- Copyright (c) 2020 University of Kansas Medical Center

--alter session set nls_date_format='YYYY-MM-DD';

-- identify all tables and columns relating to patients' birth dates
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
where owner = '&&id_schema' and 
      table_name not in ('BENE_ID_MAPPING','MSIS_ID_MAPPING') and
      (column_name like '%BIRTH%' or column_name like '%DOB%') and -- semi-auto
      data_type = 'DATE'
;

-- collect unique bene_id/msis_id+birth_date identifiers and ,
-- assign de-id identifiers and days shifted and,
-- collect all birth_dates and,
-- mask birth_dates that are already and expected to be over 89 at index_date
declare
   sql_stmt varchar(4000);
   undup_stmt varchar(4000);

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
                          ubid2.birth_date,
                          case when round((current_date + &&yr_from_now*365  - ubid2.birth_date)/365.25) > 89 
                               then Date ''1900-01-01'' else ubid2.birth_date end as birth_date_hipaa,
                          current_date + &&yr_from_now*365'
              ||' FROM ( '
              || ' SELECT /*+ PARALLEL('|| rec.table_name || ',12) */ ' 
              || ' ubid.bene_id, ' || 'ubid.' || rec.column_name || ' birth_date, '
              || ' row_number() over (partition by ubid.bene_id order by ubid.bene_id) rn ' 
              || ' FROM ' || rec.owner || '.' || rec.table_name || ' ubid) ubid2' 
              || ' LEFT JOIN ' || '&&prev_cms_id_schema'||'.'||'&&bene_id_map_prev_yrs_cumu' ||' prev_ubid '
              || ' on prev_ubid.bene_id = ubid2.bene_id and ubid2.rn = 1';
              
    undup_stmt:= 'DELETE bene_id_mapping'
              || ' where rowid not in ('
              || '  select min(rowid) from bene_id_mapping'
              || '  group by bene_id, birth_date)';
  else
    sql_stmt := 'INSERT INTO /*+ APPEND*/ msis_id_mapping '
              || 'SELECT umid2.msis_id msis_id,
                         umid2.state_cd state_cd,
                         coalesce(prev_msis.msis_id_deid, umid2.bene_id_deid, to_char(msis_id_deid_seq.nextval)) msis_id_deid,
                         coalesce(prev_msis.bene_id,umid2.bene_id) bene_id,
                         coalesce(prev_msis.bene_id_deid,umid2.bene_id_deid) bene_id_deid,
                         coalesce(prev_msis.date_shift_days,round(dbms_random.value(-364,0))) date_shift_days,
                         umid2.birth_date,
                         case when round((current_date + &&yr_from_now*365  - umid2.birth_date)/365.25) > 89 
                              then Date ''1900-01-01'' else umid2.birth_date end as birth_date_hipaa,
                         current_date + &&yr_from_now*365'
              ||' FROM ( '
              || ' SELECT /*+ PARALLEL('|| rec.table_name || ',12) */ ' 
              || ' umid.msis_id, umid.state_cd,' || ' umid.' || rec.column_name || ' birth_date,'
              || ' coalesce(umid.bene_id,prev_msis.bene_id) bene_id,'
              || ' coalesce(umid.bene_id_deid,prev_msis.bene_id) bene_id_deid,'
              || ' row_number() over (partition by umid.msis_id, umid.state_cd order by umid.msis_id) rn ' 
              || ' FROM ' || rec.owner || '.' || rec.table_name || ' umid) umid2' 
              || ' LEFT JOIN ' || '&&prev_cms_id_schema' || '.' || '&&msis_id_map_prev_yrs_cumu' ||' prev_msis '
              || ' on prev_msis.msis_id = umid.msis_id and prev_msis.state_cd = umid.state_cd and umid2.rn = 1';
              
    undup_stmt:= 'DELETE msis_id_mapping'
              || ' where rowid not in ('
              || '  select min(rowid) from msis_id_mapping'
              || '  group by msis_id, state_cd, birth_date)';
  end if;
  execute immediate sql_stmt;  
  execute immediate undup_stmt;
  commit;
  end loop;
end;

