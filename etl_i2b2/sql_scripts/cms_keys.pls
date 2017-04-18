/** cms_keys - fit CMS multi-column keys into i2b2 mappings
*/

create or replace function fmt_patient_day(bene_id varchar2, dt date)
return varchar2 is
begin
  -- In order to split the parts apart without scanning, put the date first.
  return to_char(dt, 'YYYYMMDD') || ' ' || bene_id;
end;
/

create or replace function fmt_clm_line(clm_id varchar2, line_num number)
return varchar2 is
begin
  return 'LINE:' || lpad(line_num, 4) || ' CLM_ID:' || clm_id;
end;
/

create or replace function fmt_msis_pat_ide(msis_id_deid varchar2, state_cd varchar2)
return varchar2 is
begin
  return msis_id_deid || state_cd;
end;
/

/* patient_ide_source, encounter_ide_source codes */
create or replace view cms_key_sources
as
  select
    &&cms_source_cd
    || '(BENE_ID)' bene_cd
  ,
    &&cms_source_cd
    || '(MEDPAR_ID)' medpar_cd
  ,
    &&cms_source_cd
    || '(CLM_ID,LINE_NUM)' clm_line_cd
  ,
    &&cms_source_cd
    || '(BENE_ID,day)' patient_day_cd
  ,
    &&cms_source_cd
    || '(MSIS_ID, STATE_CD)' msis_cd
  from dual
/

create or replace view cms_keys as select &&design_digest design_digest from dual
/

select length(fmt_patient_day('pt1', date '2001-01-01')) +
       length(fmt_clm_line('c1', 1)) complete
from cms_keys
where design_digest = &&design_digest
/
