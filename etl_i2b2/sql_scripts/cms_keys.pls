/** cms_keys - fit CMS multi-column keys into i2b2 mappings


Note on Dry SQL: views of magic strings and numbers

We collect manifest constants in `select ... from dual` views; for
example: `(select active from i2b2_status)` rather than `'A'`.

While doing sub-selects or cross joins with constant views is a little
awkward, it's an idiom we have used for some time and it does seem to
work.

Alternatives considered:

  - PL/SQL inherits a lot from Ada, but not Ada's discriminated types.
  - PL/SQL has object types similar to Java, but exploration
    into scala-style with a subclass for each member showed poor support
    for singletons.
  - PL/SQL has packages of constant functions, but postgres does not
    have packages, so the `pkg.fn` client syntax is not portable.

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

create or replace view pdx_flags as
select '1' primary
     , '2' secondary
from dual
/

create or replace function rif_modifier(
    table_name varchar2)
  return varchar2
is
begin
  return 'CMS_RIF:' || table_name;
end;
/

-- Present on Admission Indicator
-- https://www.resdac.org/cms-data/variables/medpar-diagnosis-e-code-present-admission-indicator
create or replace function poa_cd(
    ind varchar2)
  return varchar2
is
begin
  return 'POA:' || ind;
end;
/

-- SCILHS / PCORNet metadata uses DRG:nnn
create or replace function drg_cd(
    code varchar2)
  return varchar2
is
begin
  return 'DRG:' || code;
end;
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

-- select px_code('9904', '9') from dual; -- ICD9:99.04
-- select px_code('064', '9') from dual; -- ICD9:06.4
-- select px_code('99321', 'HCPCS') from dual; -- CPT:99321
create or replace function px_code(
    prcdr_cd   varchar2,
    prcdr_vrsn varchar2)
  return varchar2
is
begin
  return case
  when prcdr_vrsn in ('CPT', 'HCPCS') then 'CPT:' || prcdr_cd
  when prcdr_vrsn = '9' then 'ICD9:' || substr(prcdr_cd, 1, 2) || '.' || substr(prcdr_cd, 3)
  else 'ICD9' || prcdr_vrsn || ':' || prcdr_cd
  end;
end;
/

  
create or replace view cms_keys_design as select &&design_digest design_digest from dual
/

select length(fmt_patient_day('pt1', date '2001-01-01')) +
       length(fmt_clm_line('c1', 1)) complete
from cms_keys_design
where design_digest = &&design_digest
/
