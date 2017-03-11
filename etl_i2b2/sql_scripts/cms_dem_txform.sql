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


create or replace view cms_mbsf_design
as
with information_schema as
  (select owner
  , table_name
  , column_id
  , column_name
  , data_type
  from all_tab_columns
  where owner = 'CMS_DEID'
    and table_name not like 'SYS_%'
  order by owner
  , table_name
  , column_id
  )
select ', '
  ||
  case
    when data_type = 'VARCHAR2'
      and column_name like '%_IND___' then '('
      || column_name
      || ', bene_age_at_end_ref_yr, extract_dt) as ''M '
      || column_name
      || ''''
    when data_type = 'VARCHAR2' then '('
      || column_name
      || ', bene_age_at_end_ref_yr, extract_dt) as ''@ '
      || column_name
      || ''''
    when data_type = 'NUMBER' then '(bene_sex_ident_cd, '
      || column_name
      || ', extract_dt) as ''n '
      || column_name
      || ''''
    when data_type = 'DATE' then '(bene_sex_ident_cd, bene_age_at_end_ref_yr, '
      || column_name
      || ') as ''d '
      || column_name
      || ''''
  end sql_snippet
from information_schema
where table_name       = 'MBSF_AB_SUMMARY'
  and column_name not in('BENE_ID', 'BENE_BIRTH_DT', 'BENE_ENROLLMT_REF_YR', 'EXTRACT_DT')
order by table_name
, column_id ;


/** cms_mbsf_facts -- pivot the MBSF table into observation facts

This handles coded, numeric, and date values.

Columns such as BENE_MDCR_ENTLMT_BUYIN_IND_03 = C become
BENE_MDCR_ENTLMT_BUYIN_IND:C with start_date = YYYY-03-01 where YYYY is the enrollment ref yr.

ISSUE: should columns such as BENE_MDCR_ENTLMT_BUYIN_IND_06 be date-shifted somehow? the 06 is a month

We generate the unpivot( for ty_col in(...) parts below using cms_mbsf_design.
*/
create or replace view cms_mbsf_facts
as
with bene_pivot as
  (select bene_id
  , bene_birth_dt
  , bene_enrollmt_ref_yr
  , 'MBSF_AB_SUMMARY' table_name
  , ty_col -- valtype_cd and column_name combined, since the for X part can only take one column expression
  , val_cd -- coded value
  , val_num -- numeric value
  , val_dt -- date value
  from cms_deid_sample.mbsf_ab_summary unpivot((val_cd, val_num, val_dt) for ty_col in((

  (FIVE_PERCENT_FLAG, bene_age_at_end_ref_yr, extract_dt) as '@ FIVE_PERCENT_FLAG'
, (ENHANCED_FIVE_PERCENT_FLAG, bene_age_at_end_ref_yr, extract_dt) as '@ ENHANCED_FIVE_PERCENT_FLAG'
, (bene_sex_ident_cd, bene_age_at_end_ref_yr, COVSTART) as 'd COVSTART'
, (CRNT_BIC_CD, bene_age_at_end_ref_yr, extract_dt) as '@ CRNT_BIC_CD'
, (STATE_CODE, bene_age_at_end_ref_yr, extract_dt) as '@ STATE_CODE'
, (BENE_COUNTY_CD, bene_age_at_end_ref_yr, extract_dt) as '@ BENE_COUNTY_CD'
, (BENE_ZIP_CD, bene_age_at_end_ref_yr, extract_dt) as '@ BENE_ZIP_CD'
, (bene_sex_ident_cd, BENE_AGE_AT_END_REF_YR, extract_dt) as 'n BENE_AGE_AT_END_REF_YR'
, (BENE_VALID_DEATH_DT_SW, bene_age_at_end_ref_yr, extract_dt) as '@ BENE_VALID_DEATH_DT_SW'
, (bene_sex_ident_cd, bene_age_at_end_ref_yr, BENE_DEATH_DT) as 'd BENE_DEATH_DT'
, (bene_sex_ident_cd, bene_age_at_end_ref_yr, NDI_DEATH_DT) as 'd NDI_DEATH_DT'
, (BENE_SEX_IDENT_CD, bene_age_at_end_ref_yr, extract_dt) as '@ BENE_SEX_IDENT_CD'
, (BENE_RACE_CD, bene_age_at_end_ref_yr, extract_dt) as '@ BENE_RACE_CD'
, (RTI_RACE_CD, bene_age_at_end_ref_yr, extract_dt) as '@ RTI_RACE_CD'
, (BENE_ENTLMT_RSN_ORIG, bene_age_at_end_ref_yr, extract_dt) as '@ BENE_ENTLMT_RSN_ORIG'
, (BENE_ENTLMT_RSN_CURR, bene_age_at_end_ref_yr, extract_dt) as '@ BENE_ENTLMT_RSN_CURR'
, (BENE_ESRD_IND, bene_age_at_end_ref_yr, extract_dt) as '@ BENE_ESRD_IND'
, (BENE_MDCR_STATUS_CD, bene_age_at_end_ref_yr, extract_dt) as '@ BENE_MDCR_STATUS_CD'
, (BENE_PTA_TRMNTN_CD, bene_age_at_end_ref_yr, extract_dt) as '@ BENE_PTA_TRMNTN_CD'
, (BENE_PTB_TRMNTN_CD, bene_age_at_end_ref_yr, extract_dt) as '@ BENE_PTB_TRMNTN_CD'
, (bene_sex_ident_cd, BENE_HI_CVRAGE_TOT_MONS, extract_dt) as 'n BENE_HI_CVRAGE_TOT_MONS'
, (bene_sex_ident_cd, BENE_SMI_CVRAGE_TOT_MONS, extract_dt) as 'n BENE_SMI_CVRAGE_TOT_MONS'
, (bene_sex_ident_cd, BENE_STATE_BUYIN_TOT_MONS, extract_dt) as 'n BENE_STATE_BUYIN_TOT_MONS'
, (bene_sex_ident_cd, BENE_HMO_CVRAGE_TOT_MONS, extract_dt) as 'n BENE_HMO_CVRAGE_TOT_MONS'
, (BENE_MDCR_ENTLMT_BUYIN_IND_01, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_MDCR_ENTLMT_BUYIN_IND_01'
, (BENE_MDCR_ENTLMT_BUYIN_IND_02, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_MDCR_ENTLMT_BUYIN_IND_02'
, (BENE_MDCR_ENTLMT_BUYIN_IND_03, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_MDCR_ENTLMT_BUYIN_IND_03'
, (BENE_MDCR_ENTLMT_BUYIN_IND_04, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_MDCR_ENTLMT_BUYIN_IND_04'
, (BENE_MDCR_ENTLMT_BUYIN_IND_05, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_MDCR_ENTLMT_BUYIN_IND_05'
, (BENE_MDCR_ENTLMT_BUYIN_IND_06, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_MDCR_ENTLMT_BUYIN_IND_06'
, (BENE_MDCR_ENTLMT_BUYIN_IND_07, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_MDCR_ENTLMT_BUYIN_IND_07'
, (BENE_MDCR_ENTLMT_BUYIN_IND_08, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_MDCR_ENTLMT_BUYIN_IND_08'
, (BENE_MDCR_ENTLMT_BUYIN_IND_09, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_MDCR_ENTLMT_BUYIN_IND_09'
, (BENE_MDCR_ENTLMT_BUYIN_IND_10, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_MDCR_ENTLMT_BUYIN_IND_10'
, (BENE_MDCR_ENTLMT_BUYIN_IND_11, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_MDCR_ENTLMT_BUYIN_IND_11'
, (BENE_MDCR_ENTLMT_BUYIN_IND_12, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_MDCR_ENTLMT_BUYIN_IND_12'
, (BENE_HMO_IND_01, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_HMO_IND_01'
, (BENE_HMO_IND_02, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_HMO_IND_02'
, (BENE_HMO_IND_03, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_HMO_IND_03'
, (BENE_HMO_IND_04, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_HMO_IND_04'
, (BENE_HMO_IND_05, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_HMO_IND_05'
, (BENE_HMO_IND_06, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_HMO_IND_06'
, (BENE_HMO_IND_07, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_HMO_IND_07'
, (BENE_HMO_IND_08, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_HMO_IND_08'
, (BENE_HMO_IND_09, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_HMO_IND_09'
, (BENE_HMO_IND_10, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_HMO_IND_10'
, (BENE_HMO_IND_11, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_HMO_IND_11'
, (BENE_HMO_IND_12, bene_age_at_end_ref_yr, extract_dt) as 'M BENE_HMO_IND_12'

))
  )
, bene_pivot_current -- pick the most recent record per bene_id, enrollment yr
  as(
  (select *
  from
    (select bene_pivot.*
    , row_number() over(partition by bene_id, ty_col order by bene_enrollmt_ref_yr desc) as rn
    from bene_pivot
    ) t
  where rn = 1
  ))
, bene_pivot_valtype as -- parse ty_col into valtype_cd, scheme
  (select bene_id
  , bene_birth_dt
  , bene_enrollmt_ref_yr
  , table_name
  , substr(ty_col, 1, 1) valtype_cd
  , substr(ty_col, 3) column_name
  , val_cd
  , val_num
  , val_dt
  from bene_pivot_current
  )
, bene_pivot_dates as
  (select bene_pivot_valtype.*
  , to_date(bene_enrollmt_ref_yr
    || '1231', 'YYYYMMDD') update_date
  , case
      when valtype_cd = 'd' then val_dt
      when valtype_cd = 'M' then to_date(bene_enrollmt_ref_yr
        -- BENE_MDCR_ENTLMT_BUYIN_IND_09 -> 09
        || substr(column_name, length(column_name) - 1, 2)
        || '01', 'YYYYMMDD')
      else to_date(bene_enrollmt_ref_yr
        || '1231', 'YYYYMMDD')
    end start_date
  from bene_pivot_valtype
  )
,
no_info as (
select
 '@' provider_id -- @@magic string
, null valueflag_cd
, null quantity_num
, null units_cd
, null location_cd
, null confidence_num
from dual)

select fmt_patient_day(bene_id, bene_birth_dt) encounter_ide -- ISSUE: create encounter_mapping records
, key_sources.patient_day_cd encounter_ide_source
, bene_id
, case
    when valtype_cd = '@' then column_name
      || ':'
      || val_cd
    when valtype_cd = 'M' then substr(column_name, 1, length(column_name) - 3)
      || ':'
      || val_cd
    else column_name
      || ':'
  end concept_cd
, start_date
, 'CMS_RIF:'
  || table_name modifier_cd -- @@magic string
, ora_hash(bene_id
  || bene_enrollmt_ref_yr
  || column_name) instance_num
, valtype_cd
, case
    when valtype_cd = 'd' then to_char(val_dt, 'YYYY-MM-DD')
    else null
  end tval_char
, case
    when valtype_cd = 'n' then val_num
    else null
  end nval_num
, start_date end_date
, update_date
, no_info.*
from bene_pivot_dates
cross join no_info
cross join cms_key_sources key_sources
where
  (
    valtype_cd in('@', 'M')
    and val_cd is not null
  )
  or
  (
    valtype_cd   = 'n'
    and val_num is not null
  )
  or
  (
    valtype_cd  = 'd'
    and val_dt is not null
  ) ;

-- eyeball it: select * from cms_mbsf_facts order by bene_id



create or replace view cms_dem_txform as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_dem_txform where design_digest = &&design_digest;
