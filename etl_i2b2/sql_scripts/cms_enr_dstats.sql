/** cms_enr_dstats - Descriptive statistics for CMS Enrollment.

We build a PCORNet CDM style ENROLLMENT table and compute descriptive statistics.

To look at enrollments for part A+B, use RAW_BASIS as follows:

select * from enrollment
where raw_basis like '_: AB%';

Note: unlike cms_enc_dstats etc. where the input is only an i2b2 star schema,
in this case we reach back to the CMS RIF tables, because we didn't include
bene_mdcr_entlmt_buyin_ind_01 when we curated columns from RIF to include in i2b2.

To look at just years and months, this is handy:
alter session set NLS_DATE_FORMAT = 'YYYY-MM';

To change it back:
alter session set NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI' ;


## I2P Workflow utilities:

-- clobber I2P output
truncate table harvest;
truncate table demographic;
truncate table encounter;
truncate table diagnosis;
truncate table procedures;
truncate table dispensing;
truncate table enrollment;

-- QA for I2P
select * from harvest;
select * from demographic_summary;
select * from encounters_per_visit_patient;  -- Table IIID
select * from id_counts_by_table;  -- just ENROLLMENT for now
select * from dx_by_enc_type;  -- Table IVA
select * from px_per_enc_by_type; -- Table IVB
select * from dispensing_trend_chart; -- Chart IF


*/

select bene_id, bene_mdcr_entlmt_buyin_ind_01 from cms_deid.mbsf_ab_summary where 1 = 0; 
select bene_id, mdcr_entlmt_buyin_ind_01 from cms_deid_2014.mbsf_abcd_summary where 1 = 0; 
select bene_id, mdcr_entlmt_buyin_ind_01 from cms_deid_2015.mbsf_abcd_summary where 1 = 0; 
select patient_num from "&&I2B2STAR".patient_dimension where 1=0;

select patid from "&&PCORNET_CDM".enrollment where 1 = 0;

/** per_bene_mo -- pivot enrollment indicator by month and filter to selected i2b2 patients

0 Not entitled
1	Part A only
2	Part B only
3	Part A and Part B
A	Part A state buy-in
B	Part B state buy-in
C	Part A and Part B state buy-in
  -- https://www.resdac.org/cms-data/variables/medicare-entitlementbuy-indicator-january
*/

select count(*) from "&&I2B2STAR".patient_dimension;  -- 7700
select count(distinct patient_num) from site_cohorts where patient_num < 22000000;  -- 8204


-- pre-flight check: do we have a date-shift for each one in the CMS range??
select case when count(*) = 0 then 1 else 1 / 0 end date_shift_ok from (
  select * from "&&I2B2STAR".patient_dimension
  where patient_num < 22000000
  and patient_num not in (
    select bene_id_deid from cms_deid.BC_BENE_ID_MAPPING_2011_13 union
    select bene_id_deid from cms_deid.BC_BENE_ID_MAPPING_2014 union
    select bene_id_deid from cms_deid.BC_BENE_ID_MAPPING_2015
  )
  and rownum = 1
);


whenever sqlerror continue; drop table per_bene_mo; whenever sqlerror exit;
create table per_bene_mo as

with per_bene_13 as (
  select * from cms_deid.mbsf_ab_summary mb
  where exists (
   select 1 from "&&I2B2STAR".patient_dimension pd
   where to_char(pd.patient_num) = bene_id)
)

, per_bene_13s as (
  select mb.*, bmap13.date_shift_days
  from per_bene_13 mb
  join cms_deid.BC_BENE_ID_MAPPING_2011_13 bmap13 on mb.bene_id = bmap13.bene_id_deid
)
, per_bene_mo_13 as (
select bene_id, bene_enrollmt_ref_yr, mo, buyin, extract_dt, date_shift_days
from per_bene_13s
unpivot(
        buyin
        for mo in (
            bene_mdcr_entlmt_buyin_ind_01 as 1
          , bene_mdcr_entlmt_buyin_ind_02 as 2
          , bene_mdcr_entlmt_buyin_ind_03 as 3
          , bene_mdcr_entlmt_buyin_ind_04 as 4
          , bene_mdcr_entlmt_buyin_ind_05 as 5
          , bene_mdcr_entlmt_buyin_ind_06 as 6
          , bene_mdcr_entlmt_buyin_ind_07 as 7
          , bene_mdcr_entlmt_buyin_ind_08 as 8
          , bene_mdcr_entlmt_buyin_ind_09 as 9
          , bene_mdcr_entlmt_buyin_ind_10 as 10
          , bene_mdcr_entlmt_buyin_ind_11 as 11
          , bene_mdcr_entlmt_buyin_ind_12 as 12
          )
      )
where buyin != '0' -- Not entitled
)

, per_bene_4 as (
  select * from cms_deid_2014.mbsf_abcd_summary mb
  where exists (
   select 1 from "&&I2B2STAR".patient_dimension pd
   where to_char(pd.patient_num) = bene_id)
)
, per_bene_4s as (
  select mb.*, bmap14.date_shift_days from per_bene_4 mb
  join cms_deid.BC_BENE_ID_MAPPING_2014 bmap14 on mb.bene_id = bmap14.bene_id_deid
)
, per_bene_5 as (
  select * from cms_deid_2015.mbsf_abcd_summary mb
  where exists (
   select 1 from "&&I2B2STAR".patient_dimension pd
   where to_char(pd.patient_num) = bene_id)
)
, per_bene_5s as (
  select mb.*, bmap15.date_shift_days from per_bene_5 mb
  join cms_deid.BC_BENE_ID_MAPPING_2015 bmap15 on mb.bene_id = bmap15.bene_id_deid
)
, per_bene_45s as (
  select * from per_bene_4s union all
  select * from per_bene_5s
)
, per_bene_mo_45 as (
select bene_id, bene_enrollmt_ref_yr, mo, buyin, extract_dt, date_shift_days
from per_bene_45s
unpivot(
        buyin
        for mo in (
            mdcr_entlmt_buyin_ind_01 as 1
          , mdcr_entlmt_buyin_ind_02 as 2
          , mdcr_entlmt_buyin_ind_03 as 3
          , mdcr_entlmt_buyin_ind_04 as 4
          , mdcr_entlmt_buyin_ind_05 as 5
          , mdcr_entlmt_buyin_ind_06 as 6
          , mdcr_entlmt_buyin_ind_07 as 7
          , mdcr_entlmt_buyin_ind_08 as 8
          , mdcr_entlmt_buyin_ind_09 as 9
          , mdcr_entlmt_buyin_ind_10 as 10
          , mdcr_entlmt_buyin_ind_11 as 11
          , mdcr_entlmt_buyin_ind_12 as 12
          )
      )
where buyin != '0' -- Not entitled
)
,
shifted as (
select bene_id, bene_enrollmt_ref_yr, mo, date_shift_days
     , to_date(to_char(bene_enrollmt_ref_yr, 'FM0000') || to_char(mo, 'FM00') || '01', 'YYYYMMDD') + date_shift_days enrollmt_mo_1st
     , buyin
     , extract_dt
from (
  select * from per_bene_mo_13
  union all
  select * from per_bene_mo_45
)
)
select shifted.*
     , extract(year from enrollmt_mo_1st) * 12 + (extract(month from enrollmt_mo_1st) - 1) enrollmt_mo
from shifted
;
-- select * from per_bene_mo;


delete from "&&PCORNET_CDM".enrollment;
commit;

insert /*+ append */ into "&&PCORNET_CDM".enrollment (
  patid, enr_start_date, enr_end_date, chart, enr_basis, raw_basis
)
with per_bene_start_mo as (
  -- ack: https://blog.jooq.org/2015/11/07/how-to-find-the-longest-consecutive-series-of-events-in-sql/
  select bene_id, buyin, enrollmt_mo_1st, enrollmt_mo,
         enrollmt_mo - (dense_rank() over (partition by bene_id order by bene_id, enrollmt_mo)) series
  from per_bene_mo
)
select bene_id patid
     , min(enrollmt_mo_1st) enr_start_date
     , max(enrollmt_mo_1st) enr_end_date
     , 'Y' chart  -- there are no contractual or other restrictions between you and
                  -- the individual (or sponsor) that would prohibit you from
                  -- requesting any chart for this patient.
     , 'I' enr_basis -- I=Medical insurance coverage
           -- TODO: part D coverage
     -- , count(*) month_dur
     , buyin || ': ' ||
       decode(buyin, '1', 'A',
                     '2', ' B',
                     '3', 'AB',
                     'A', 'A  state',
                     'B', ' B state',
                     'C', 'AB state')  raw_basis
from per_bene_start_mo
group by bene_id, buyin, series
order by 6 desc
;
commit;


create or replace view IIA_Primary_Key_Errors as
select 'ENROLLMENT' "Table"
     , count(*) "Exceptions to specifications"
from (
  select patid, enr_start_date, enr_basis, count(*)
  from "&&PCORNET_CDM".enrollment
  group by patid, enr_start_date, enr_basis
  having count(*) > 1
);
-- select * from IIA_Primary_Key_Errors;

create or replace view id_counts_by_table as
select 'ENROLLMENT' "Table"
     , count(*) "Records"
     , count(distinct patid) "Patients"
     , percentile_disc (0.05) within group (order by ENR_START_DATE) pctile_5
     , percentile_disc (0.95) within group (order by ENR_START_DATE) pctile_95
from (
 select patid
      , extract(year from ENR_START_DATE) * 100 + extract(month from ENR_START_DATE) ENR_START_DATE
   from "&&PCORNET_CDM".enrollment
   )
;
-- select * from id_counts_by_table;

create or replace view cms_enr_stats_sql as
select &&design_digest design_digest from dual;

with output as (select count(*) qty from "&&PCORNET_CDM".enrollment where rownum < 10)
select 1 up_to_date
from cms_enr_stats_sql, output, harvest
where harvest.refresh_enrollment_date is not null or (
  design_digest = &&design_digest
    -- and (select count(*) qty from per_bene_mo where rownum < 10) > 1
    and output.qty > 1
);
