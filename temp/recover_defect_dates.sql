-- temp/recover_defect_dates.sql: identify and recover defected dates for patients that has been shifted by dob_month_shifts
-- Copyright (c) 2020 University of Kansas Medical Center

alter session set current_schema = CMS_ID_12CAID_16CARE;

/*
previous hipaa_dob_shift.sql suggests that the table "dob_shift" contains sufficient info to 
identify patients with defected date shifts
*/

whenever sqlerror continue;
drop table dob_col;
drop table bene_id_mapping2;
drop table msis_id_mapping2;
whenever sqlerror exit;


-- add birth_date, birth_date_hipaa
create table bene_id_mapping2 (
  BENE_ID VARCHAR2(15),
  BENE_ID_DEID VARCHAR2(15),
  DATE_SHIFT_DAYS INTEGER,
  DOB_SHIFT_MONTHS INTEGER,
  BIRTH_DATE DATE,
  BIRTH_DATE_HIPAA DATE,
  INDEX_DATE DATE
  );
alter table bene_id_mapping2 parallel (degree 12);

create table msis_id_mapping2 (
  MSIS_ID VARCHAR2(32),
  STATE_CD VARCHAR2(2),
  MSIS_ID_DEID VARCHAR2(32),
  DATE_SHIFT_DAYS INTEGER,
  DOB_SHIFT_MONTHS INTEGER,
  BIRTH_DATE DATE,
  BIRTH_DATE_HIPAA DATE,
  INDEX_DATE DATE
  );
alter table msis_id_mapping parallel (degree 12);

create table dob_col as
select table_name
      ,column_name
      ,owner
      ,case when table_name like 'MAX%' then 1       -- special identifiers in medicaid table names (may need to specify mannually)
            else 0
       end as msis_ind
from all_tab_columns
where owner = 'CMS_ID_12CAID_16CARE' and 
      table_name not in ('BENE_ID_MAPPING','BENE_ID_MAPPING2','BENE_ID_MAPPING_BACKUP',
                         'MSIS_ID_MAPPING','MSIS_ID_MAPPING2','MSIS_ID_MAPPING_BACKUP') and
      table_name not like '%ORIGINAL' and
      (column_name like '%BIRTH%' or column_name like '%DOB%') and -- semi-auto
      data_type = 'DATE'
;

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
    sql_stmt := 'INSERT INTO /*+ APPEND*/ bene_id_mapping2 '
              || ' SELECT bm.bene_id,
                          bm.bene_id_deid,
                          bm.date_shift_days,
                          bm.dob_shift_months,
                          ubid2.birth_date,
                          case when round((current_date - 365 - ubid2.birth_date)/365.25) > 89 
                               then Date ''1900-01-01'' else ubid2.birth_date end as birth_date_hipaa,
                          current_date - 365'
              ||' FROM ( '
              || ' SELECT /*+ PARALLEL('|| rec.table_name || ',12) */ ' 
              || ' ubid.bene_id, ' || 'ubid.' || rec.column_name || ' birth_date, '
              || ' row_number() over (partition by ubid.bene_id order by ubid.bene_id) rn ' 
              || ' FROM ' || rec.owner || '.' || rec.table_name || ' ubid) ubid2' 
              || ' JOIN bene_id_mapping bm '
              || ' on bm.bene_id = ubid2.bene_id and ubid2.rn = 1'
              || ' where bm.dob_shift_months is not null and bm.bene_id is not null';
              
    undup_stmt:= 'DELETE bene_id_mapping2'
              || ' where rowid not in ('
              || '  select min(rowid) from bene_id_mapping2'
              || '  group by bene_id, birth_date)';
  else
    sql_stmt := 'INSERT INTO /*+ APPEND*/ msis_id_mapping2 '
              || 'SELECT mp.msis_id,
                         mp.state_cd,
                         mm.msis_id_deid,
                         mp.date_shift_days,
                         mp.dob_shift_months,
                         umid2.birth_date,
                         case when round((current_date - 365  - umid2.birth_date)/365.25) > 89 
                              then Date ''1900-01-01'' else umid2.birth_date end as birth_date_hipaa,
                         current_date - 365'
              ||' FROM ( '
              || ' SELECT /*+ PARALLEL('|| rec.table_name || ',12) */ ' 
              || ' umid.msis_id, umid.state_cd,' || ' umid.' || rec.column_name || ' birth_date,'
              || ' row_number() over (partition by umid.msis_id, umid.state_cd order by umid.msis_id) rn ' 
              || ' FROM ' || rec.owner || '.' || rec.table_name || ' umid) umid2 ' 
              || 'JOIN msis_person mp '
              || 'on mp.msis_id = umid2.msis_id and mp.state_cd = umid2.state_cd and umid2.rn = 1 '
              || 'JOIN msis_id_mapping mm '
              || 'on mm.msis_id = mp.msis_id '
              || 'where mp.dob_shift_months is not null and mp.msis_id is not null';
              
    undup_stmt:= 'DELETE msis_id_mapping2'
              || ' where rowid not in ('
              || '  select min(rowid) from msis_id_mapping2'
              || '  group by msis_id, state_cd, birth_date)';
  end if;
  execute immediate sql_stmt;  
  execute immediate undup_stmt;
  commit;
  end loop;
end;





