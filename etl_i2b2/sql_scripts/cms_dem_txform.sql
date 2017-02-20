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

select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql';


/** dem_sentinel - sentinels for use in demographics*/
create or replace view dem_sentinel
as
  select date '0001-01-01' bad_date_syntax
  from dual ;

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
  -- Select columns in record with most recent bene_enrollmt_ref_yr
  -- partition by ack: Mikael Eriksson Aug 19 '11 http://stackoverflow.com/a/7118233
with latest_ref_yr as
  (
  select *
  from
    (select bene_id
    , bene_birth_dt
    , bene_death_dt
    , bene_sex_ident_cd
    , bene_race_cd
    , bene_enrollmt_ref_yr
    , row_number() over(partition by bene_id order by bene_enrollmt_ref_yr desc) as rn
    from "&&CMS_RIF".mbsf_ab_summary
    ) t
  where rn = 1
  )

select bene_id
, key_sources.bene_cd patient_ide_source
, case
    when mbsf.bene_death_dt is not null then 'y'
    else 'n'
  end vital_status_cd
, bene_birth_dt birth_date
, bene_death_dt death_date
, mbsf.bene_sex_ident_cd
  || '-'
  || decode(mbsf.bene_sex_ident_cd, '0', 'UNKNOWN', '1', 'MALE', '2', 'FEMALE') sex_cd
, round((least(sysdate, nvl( bene_death_dt, sysdate)) - bene_birth_dt) / 365.25) age_in_years_num
  -- , language_cd
, mbsf.bene_race_cd
  || '-'
  || decode(mbsf.bene_race_cd, '0', 'UNKNOWN', '1', 'WHITE', '2', 'BLACK', '3', 'OTHER', '4', 'ASIAN', '5', 'HISPANIC',
  '6', 'NORTH AMERICAN NATIVE') race_cd
  --, marital_status_cd
  --, religion_cd
  --, zip_cd
  --, statecityzip_path
  --, income_cd
  --, patient_blob
, to_date(bene_enrollmt_ref_yr || '1231', 'YYYYMMDD') update_date
  --, import_date is only relevant at load time
, &&cms_source_cd sourcesystem_cd
  -- upload_id is only relevant at load time
from latest_ref_yr mbsf
cross join cms_key_sources key_sources;
-- eyeball it:
-- select * from cms_patient_dimension;

/** cms_visit_dimension -- view CMS part B carrier claims  as i2b2 patient_dimension

Note this view has bene_id where visit_dimension has patient_num.
Joining with the i2b2 patient_mapping happens in a later insert.

ref:
  - Carrier RIF
    https://www.resdac.org/cms-data/files/carrier-rif/data-documentation

ISSUE: one encounter per line? or per claim? use distinct on provider and such?
*/

create or replace view medpar_claim_type as
select * from xmltable('table/item' passing xmltype(
'<table><item code="10" value="HHA claim" />
    <item code="20" value="Non swing bed SNF claim" />
    <item code="30" value="Swing bed SNF claim" />
    <item code="40" value="Outpatient claim" />
    <item code="50" value="Hospice claim" />
    <item code="60" value="Inpatient claim" />
    <item code="61" value="Inpatient ''Full-Encounter'' claim" />
    <item code="62" value="Medicare Advantage IME/GME claims" />
    <item code="63" value="Medicare Advantage (no-pay) claims" />
    <item code="64" value="Medicare Advantage (paid as FFS) claim" />
    <item code="71" value="RIC O local carrier non-DMEPOS claim" />
    <item code="72" value="RIC O local carrier DMEPOS claim" />
    <item code="81" value="RIC M DMERC non-DMEPOS claim" />
    <item code="82" value="RIC M DMERC DMEPOS claim" />
    </table>')
    columns
    code varchar(2) path '@code',
    label varchar(120) path '@value'
)
;
comment on table medpar_claim_type
is 'ref <https://www.resdac.org/cms-data/variables/medpar-nch-claim-type-code>'
;

-- ISSUE: move this curated mapping to a .csv file?
create or replace view medpar_claim_enc_type as
select * from xmltable('table/item' passing xmltype(
'<table>
    <item code="10" enc_type="OA" value="HHA claim" />
    <item code="20" enc_type="IS" value="Non swing bed SNF claim" />
    <item code="30" enc_type="IS" value="Swing bed SNF claim" />
    <item code="40" enc_type="AV" value="Outpatient claim" />
    <item code="50" enc_type="IS" value="Hospice claim" />
    <item code="60" enc_type="IP" value="Inpatient claim" />
    <item code="61" enc_type="IP" value="Inpatient ''Full-Encounter'' claim" />
    <!-- TODO: curate these other types -->
    <item code="62" enc_type="OT" value="Medicare Advantage IME/GME claims" />
    <item code="63" enc_type="OT" value="Medicare Advantage (no-pay) claims" />
    <item code="64" enc_type="OT" value="Medicare Advantage (paid as FFS) claim" />
    <item code="71" enc_type="OT" value="RIC O local carrier non-DMEPOS claim" />
    <item code="72" enc_type="OT" value="RIC O local carrier DMEPOS claim" />
    <item code="81" enc_type="OT" value="RIC M DMERC non-DMEPOS claim" />
    <item code="82" enc_type="OT" value="RIC M DMERC DMEPOS claim" />
    </table>')
    columns
    code varchar(2) path '@code',
    enc_type varchar(2) path '@enc_type',
    label varchar(120) path '@value'
)
;


create or replace view cms_visit_dimension_bc
as
  select
    bc.bene_id
    -- ISSUE: SQL functions would be nicer
  , bc.clm_id
  , bl.line_num
  , 'LINE:' || lpad(bl.line_num, 4) || ' CLM_ID:' || bc.clm_id encounter_ide
  , key_sources.clm_line_cd encounter_ide_source
  , to_char(bc.clm_from_dt, 'YYYYMMDD') || ' ' || bc.bene_id patient_day
  , i2b2_status.active active_status_cd
  , bc.clm_from_dt start_date
  , bl.clm_thru_dt end_date
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
, 1 +(bl.clm_thru_dt - bc.clm_from_dt) length_of_stay
  -- visit_blob
, nch_wkly_proc_dt update_date
, &&cms_source_cd sourcesystem_cd
from "&&CMS_RIF".bcarrier_claims bc
join "&&CMS_RIF".bcarrier_line bl on bl.clm_id = bc.clm_id
cross join i2b2_status
cross join cms_key_sources key_sources;

create or replace view cms_visit_dimension_medpar
as
  select
    ma.bene_id
  , ma.medpar_id
  , ma.medpar_id encounter_ide
  , key_sources.medpar_cd encounter_ide_source
  , i2b2_status.active active_status_cd
  , ma.admsn_dt start_date
  , ma.dschrg_dt end_date
   -- TODO: IP -> IE as in etl_ip.sas
  , coalesce(mcet.enc_type, 'UN') || '/' || ma.nch_clm_type_cd inout_cd
  , to_char(null) location_cd
  , los_day_cnt length_of_stay
  , ltst_clm_acrtn_dt update_date
  , &&cms_source_cd sourcesystem_cd
  from
    "&&CMS_RIF".medpar_all ma
    cross join i2b2_status
    cross join cms_key_sources key_sources
    left join medpar_claim_enc_type mcet on mcet.code = ma.nch_clm_type_cd
;


create or replace view cms_dem_txform as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_dem_txform where design_digest = &&design_digest;
