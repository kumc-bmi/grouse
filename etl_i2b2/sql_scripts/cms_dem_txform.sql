/** cms_dem_txform - view CMS demographics from an i2b2 lens

ISSUE: what was my rationale for including the visit dimension here?

Refs:

@@i2b2 CRC design

Chronic Conditions Data Warehouse Data Dictionaries 
https://www.ccwdata.org/web/guest/data-dictionaries

[mbsf] Master Beneficiary Summary - Base (A/B/D)
https://www.resdac.org/cms-data/files/mbsf/data-documentation

*/

/* ISSUE: default schema: ETL scratch space? source schema? dest?
ISSUE: parameterize schema names?
ISSUE: how to manage global names such as transformation views?
       Perhaps using the (Oracle analog of) information_schema,
       integrated with Luigi.
*/

select domain from cms_ccw where 'dep' = 'cms_ccw_spec.sql';
select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql';


/** dem_sentinel - sentinels for use in demographics*/
create or replace view dem_sentinel
as
  select date '0001-01-01' bad_date_syntax
  from dual ;


/** cms_patient_dimension -- view CMS MBSF as i2b2 patient_dimension

Note this view has bene_id where patient_dimension has patient_num.
Joining with the i2b2 patient_mapping happens in a later insert.

TODO: document the separation of transform scripts from load scripts,
      esp. w.r.t. how it works with Luigi.

ISSUE: patient_ide, encounter_ide column aliases
In HERON ETL, we renamed source-specific identifiers
to patient_ide and encounter_ide somewhat early, which caused
significant confusion. I'm inclined to keep original names until
we get to the load step. @@IOU example.

ISSUE: make better use of SQL constraints?
e.g. birth_date is nullable in the i2b2 schema,
but I think we rely on it being populated.

*/

create or replace view cms_patient_dimension
as
  select bene_id, case
      when mbsf.bene_death_dt is not null
      then 'y'
      else 'n'
    end vital_status_cd, bene_birth_dt birth_date, bene_death_dt death_date
  , mbsf.bene_sex_ident_cd
    || '-'
    || decode(mbsf.bene_sex_ident_cd, '0', 'UNKNOWN', '1', 'MALE', '2', 'FEMALE') sex_cd, round((least(sysdate, nvl(
    bene_death_dt, sysdate)) - bene_birth_dt) / 365.25) age_in_years_num
    -- , language_cd
  , mbsf.bene_race_cd
    || '-'
    || decode(mbsf.bene_race_cd, '0', 'UNKNOWN', '1', 'WHITE', '2', 'BLACK', '3', 'OTHER', '4', 'ASIAN', '5',
    'HISPANIC', '6', 'NORTH AMERICAN NATIVE') race_cd
    --, marital_status_cd
    --, religion_cd
    --, zip_cd
    --, statecityzip_path
    --, income_cd
    --, patient_blob
  , sysdate update_date -- TODO:
    --, import_date is only relevant at load time
  , cms_ccw.domain sourcesystem_cd
    -- upload_id is only relevant at load time
  ,
    &&design_digest design_digest
  from mbsf_ab_summary mbsf, cms_ccw ;


/** cms_visit_dimension -- view CMS part B carrier claims  as i2b2 patient_dimension

Note this view has bene_id where visit_dimension has patient_num.
Joining with the i2b2 patient_mapping happens in a later insert.

ref:
  - Carrier RIF
    https://www.resdac.org/cms-data/files/carrier-rif/data-documentation

ISSUE: one encounter per line? or per claim? use distinct on provider and such?
*/

create or replace view cms_visit_dimension_bc
as
  select
    bc.bene_id
  , 'CLM_ID:' || bc.clm_id || ' LINE_NUM:' || bl.line_num clm_id -- KLUDGE. at least rename clm_id
  , i2b2_status.active active_status_cd
  , bc.clm_from_dt start_date
  , bc.clm_thru_dt end_date
    -- ack: make_mapping_encounter_carr from etl_carr.sas from Duke
    -- ISSUE: a CSV mapping file would be nicer.
  , case
      when bl.line_place_of_srvc_cd in('05', '07', '11', '20', '24', '49', '50', '53', '71', '72') then 'AV'
      when bl.line_place_of_srvc_cd in('01', '03', '12', '15', '17', '57', '60', '62', '65', '81') then 'OA'
      when bl.line_place_of_srvc_cd in('04', '06', '08', '09', '13', '14', '16', '21', '22', '23', '25', '26', '31',
        '32', '33', '34', '35', '41', '42', '51', '52', '54', '55', '56', '61', '99') then 'OT'
        /*WHEN ('00', '02','10', '36', '40','84') ENC_TYPE  = 'UN';  */
      when bl.line_place_of_srvc_cd is null then 'NI'
      else 'UN'
    end inout_cd
, bl.line_place_of_srvc_cd location_cd
  -- TODO? location_path
, 1 +(clm_thru_dt - clm_from_dt) length_of_stay
  -- visit_blob
, nch_wkly_proc_dt update_date, cms_ccw.domain sourcesystem_cd,
  &&design_digest design_digest
from bcarrier_claims bc -- TODO: "&&CMS".bcarrier_claims
join bcarrier_line bl on bl.clm_id = bc.clm_id
, i2b2_status, cms_ccw ;

create or replace view cms_visit_dimension_ip
as
  select
    ma.bene_id
  , 'MEDPAR_ID:' || ma.medpar_id clm_id -- KLUDGE.
  , i2b2_status.active active_status_cd
  , ma.admsn_dt start_date
  , ma.dschrg_dt end_date
  , 'IP' inout_cd -- TODO: -> IE as in etl_ip.sas
  , to_char(null) location_cd
  , los_day_cnt length_of_stay
  , sysdate update_date -- ISSUE
  , cms_ccw.domain sourcesystem_cd
  , &&design_digest design_digest
  from
    medpar_all ma
  , i2b2_status
  , cms_ccw ;

-- ISSUE: separate cms_visit_dimension views a la fact_view?
create or replace view cms_visit_dimension
as
  select * from cms_visit_dimension_bc
  union all
  select * from cms_visit_dimension_ip
;

select 1 complete
from cms_patient_dimension pd, cms_visit_dimension_ip vd
where pd.design_digest =
  &&design_digest
  and vd.design_digest =
  &&design_digest
  and rownum <= 1 ;
