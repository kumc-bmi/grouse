/** mbsf_pivot - (un)pivot CMS MBSF file into i2b2 fact table shape

Refs:

Chronic Conditions Data Warehouse Data Dictionaries 
https://www.ccwdata.org/web/guest/data-dictionaries

[mbsf] Master Beneficiary Summary - Base (A/B/D)
https://www.resdac.org/cms-data/files/mbsf/data-documentation

@@i2b2 CRC design

*/

select bene_cd from cms_key_sources where 'dep' = 'cms_keys.pls';


/* cms_mbsf_design generates code to un-pivot each column from the wide table into a long-skinny EAV table.

We use bene_id as the Entity, the column name (along with i2b2 valtyp_cd) as Attribute,
and build 3 Value columns: val_cd, val_num, val_dt.
 - For nominal data:
   - load the target column into val_cd
   - use bene_age_at_end_ref_yr as a dummy to fill val_num; the dummy
     must have the right type (numeric) and not be used as an actual target
   - fill val_dt with extract_dt as a dummy likewise
 - For numeric, fill val_num with the target and val_cd and val_dt with dummies
 - For dates, fill val_dt with the target and val_cd and val_num with dummies

TODO: concept metadata (spreadsheet, ...)
*/
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
  -- exclude the Entity column and the dummy columns
  and column_name not in('BENE_ID', 'BENE_ENROLLMT_REF_YR', 'EXTRACT_DT')
order by table_name
, column_id ;


/** cms_mbsf_facts -- pivot the MBSF table into observation facts

This handles coded, numeric, and date values.

Columns such as BENE_MDCR_ENTLMT_BUYIN_IND_03 = C become
BENE_MDCR_ENTLMT_BUYIN_IND:C with start_date = YYYY-03-01 where YYYY is the enrollment ref yr.

ISSUE: should columns such as BENE_MDCR_ENTLMT_BUYIN_IND_06 be date-shifted somehow? the 06 is a month

We generate the unpivot( for ty_col in(...) parts below using cms_mbsf_design.
select sql_snippet from cms_mbsf_design;

*/
create or replace view cms_mbsf_facts
as
with
-- pick the most recent record per bene_id, enrollment yr
-- ISSUE: turn mbsf_current into a table or materialized view?
mbsf_current as (
  select * from (
  select bsum.*
      , row_number() over(partition by bene_id order by bene_enrollmt_ref_yr desc) as rn
    from "&&CMS_RIF".mbsf_ab_summary bsum
  )
  where rn = 1
)
, bene_pivot as
  (select bene_id
  , bene_enrollmt_ref_yr
  , 'MBSF_AB_SUMMARY' table_name
  , ty_col -- valtype_cd and column_name combined, since the for X part can only take one column expression
  , val_cd -- coded value
  , val_num -- numeric value
  , val_dt -- date value
  from mbsf_current unpivot((val_cd, val_num, val_dt) for ty_col in(

  (FIVE_PERCENT_FLAG, bene_age_at_end_ref_yr, extract_dt) as '@ FIVE_PERCENT_FLAG'
, (ENHANCED_FIVE_PERCENT_FLAG, bene_age_at_end_ref_yr, extract_dt) as '@ ENHANCED_FIVE_PERCENT_FLAG'
, (bene_sex_ident_cd, bene_age_at_end_ref_yr, COVSTART) as 'd COVSTART'
, (CRNT_BIC_CD, bene_age_at_end_ref_yr, extract_dt) as '@ CRNT_BIC_CD'
, (STATE_CODE, bene_age_at_end_ref_yr, extract_dt) as '@ STATE_CODE'
, (BENE_COUNTY_CD, bene_age_at_end_ref_yr, extract_dt) as '@ BENE_COUNTY_CD' -- ISSUE: specific to state
, (BENE_ZIP_CD, bene_age_at_end_ref_yr, extract_dt) as '@ BENE_ZIP_CD'
, (bene_sex_ident_cd, BENE_AGE_AT_END_REF_YR, extract_dt) as 'n BENE_AGE_AT_END_REF_YR'
, (bene_sex_ident_cd, bene_age_at_end_ref_yr, BENE_BIRTH_DT) as 'd BENE_BIRTH_DT'
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

)
  ))
, bene_pivot_valtype as -- parse ty_col into valtype_cd, scheme
  (select bene_id
  , bene_enrollmt_ref_yr
  , table_name
  , substr(ty_col, 1, 1) valtype_cd
  , substr(ty_col, 3) column_name
  , val_cd
  , val_num
  , val_dt
  from bene_pivot
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

select bene_id
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
, case when valtype_cd = 'M' then '@' else valtype_cd end valtype_cd
, case
    when valtype_cd = 'd' then to_char(val_dt, 'YYYY-MM-DD')
    else null
  end tval_char
, case
    when valtype_cd = 'n' then val_num
    else null
  end nval_num
, case
  when valtype_cd = 'M' then add_months(start_date, 1) - 1 -- last day of month
  else start_date end
  end_date
, update_date
, &&cms_source_cd sourcesystem_cd
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


create or replace view mbsf_pivot_design as
select &&design_digest design_digest from dual;

select 1 up_to_date
from mbsf_pivot_design where design_digest = &&design_digest;
