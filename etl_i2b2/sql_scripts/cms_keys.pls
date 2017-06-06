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
  from dual
/

create or replace function dx_code(
    dgns_cd   varchar2,
    dgns_vrsn varchar2)
  return varchar2
is
begin
  return
  case
  when dgns_vrsn = '10' then
    'ICD10:' || dgns_cd  -- TODO: ICD10 formatting
  /* was: when dgns_vrsn = '9'
     but I found null dgns_vrsn e.g. one record with ADMTG_DGNS_CD = V5789
     so let's default to the IDC9 case
   */
  else
    'ICD9:' || substr(dgns_cd, 1, 3) ||
    case
    when length(dgns_cd) > 3 then
      '.' || substr(dgns_cd, 4)
    else
      ''
    end
  end;
end;
/


/* Find encounter_num for patient day.

If obs_date falls within a MEDPAR for the_bene_id, use the (positive) encounter_num for that MEDPAR.
Otherwise, use a (negative) hash of the bene_id and date.
ISSUE: collision risk.
*/
create or replace function pat_day_medpar_rollup(
    the_bene_id varchar2,
    obs_date    date)
  return integer
is
  the_encounter_num integer;
begin
with the_medpar as
  (select min(medpar_id) medpar_id
  from "&&CMS_RIF".medpar_all medpar
  where medpar.bene_id    = the_bene_id
    and obs_date between medpar.admsn_dt and medpar.dschrg_dt
  )
, the_emap as
  (select min(emap.encounter_num) encounter_num
  from cms_medpar_mapping emap
  join the_medpar
  on the_medpar.medpar_id = emap.medpar_id
  )
select coalesce(the_emap.encounter_num, -abs(ora_hash(fmt_patient_day(the_bene_id, obs_date))))
into the_encounter_num
from the_emap;
return the_encounter_num;
end;
/
  
create or replace view cms_keys_design as select &&design_digest design_digest from dual
/

select length(fmt_patient_day('pt1', date '2001-01-01')) +
       length(fmt_clm_line('c1', 1)) complete
from cms_keys_design
where design_digest = &&design_digest
/
