/**


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


/** dem_sentinel - sentinels for use in demographics*/
create or replace view dem_sentinel
as
  select date '0001-01-01' bad_date_syntax
  from dual ;


/** cms_patient_dimension -- view CMS MBSF as i2b2 patient_dimension

Note this view has bene_id where patient_dimension has patient_num.
Joining with the i2b2 patient_mapping happens in the insert below.

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
with decoded_dates as
  (select mbsf.*
  , to_date(mbsf.bene_birth_dt, 'YYYYMMDD') birth_date
  , to_date(mbsf.bene_death_dt, 'YYYYMMDD') death_date
  from mbsf_ab_summary mbsf
  )
select bene_id
, case
    when mbsf.death_date is not null
    then 'y'
    else 'n'
  end vital_status_cd
, birth_date
, death_date
, mbsf.bene_sex_ident_cd
  || '-'
  || decode(mbsf.bene_sex_ident_cd, '0', 'UNKNOWN', '1', 'MALE', '2', 'FEMALE') sex_cd
, round((least(sysdate, nvl(death_date, sysdate)) - birth_date) / 365.25) age_in_years_num
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
, sysdate update_date   -- TODO:
, sysdate download_date -- TODO: download date
  --, import_date is only relevant at load time
, cms_ccw.domain sourcesystem_cd
  -- upload_id is only relevant at load time
from decoded_dates mbsf
, cms_ccw ;

select 1 complete
from cms_patient_dimension
where rownum < 2 ;
