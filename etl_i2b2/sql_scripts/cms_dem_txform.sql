/** cms_dem_txform - view CMS demographics from an i2b2 lens

Refs:

@@i2b2 CRC design

Chronic Conditions Data Warehouse Data Dictionaries 
https://www.ccwdata.org/web/guest/data-dictionaries

[mbsf] Master Beneficiary Summary - Base (A/B/D)
https://www.resdac.org/cms-data/files/mbsf/data-documentation

*/

select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql';
select bene_cd from cms_key_sources where 'dep' = 'cms_keys.pls';


/** cms_patient_dimension -- view CMS MBSF as i2b2 patient_dimension

Note this view has bene_id where patient_dimension has patient_num.
Joining with the i2b2 patient_mapping happens in a later insert.

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
    from "&&CMS_RIF".mbsf_abcd_summary
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


create or replace view cms_dem_txform as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_dem_txform where design_digest = &&design_digest;
