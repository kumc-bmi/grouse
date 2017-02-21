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

create or replace view cms_keys as select &&design_digest design_digest from dual
/

select length(fmt_patient_day('pt1', date '2001-01-01')) +
       length(fmt_clm_line('c1', 1)) complete
from cms_keys
where design_digest = &&design_digest
/

