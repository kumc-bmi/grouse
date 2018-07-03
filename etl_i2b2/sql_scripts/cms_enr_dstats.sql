/** cms_enr_dstats - Descriptive statistics for CMS Enrollment.

We build a PCORNet CDM style ENROLLMENT table.

Note: unlike cms_enc_dstats etc. where the input is only an i2b2 star schema,
in this case we reach back to the CMS RIF tables, because we didn't include
bene_mdcr_entlmt_buyin_ind_01 when we curated columns from RIF to include in i2b2.

To look at just years and months, this is handy:
alter session set NLS_DATE_FORMAT = 'YYYY-MM';

To change it back:
alter session set NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI' ;


*/


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

create table per_bene_mo as
with per_bene as (
  select *
  from cms_deid.mbsf_ab_summary mb  -- ISSUE: TODO: 2014, 2015; mbsf_abcd_summary
  where exists (
   select 1
   from patient_dimension pd
   where to_char(pd.patient_num) = bene_id
   )
)
select bene_id, bene_enrollmt_ref_yr
     , mo
     , to_date(to_char(bene_enrollmt_ref_yr, 'FM0000') || to_char(mo, 'FM00') || '01', 'YYYYMMDD') enrollmt_mo_1st
     , bene_enrollmt_ref_yr * 12 + (mo - 1) enrollmt_mo
     , buyin
     , extract_dt
from per_bene
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
;


delete from enrollment;
commit;

insert into enrollment (
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
     , buyin || ': ' || decode(buyin, '1', 'A', '2', ' B', '3', 'AB', 'A', 'A state', ' B', 'B state', 'C', 'AB state')  raw_basis
from per_bene_start_mo
group by bene_id, buyin, series
order by 6 desc
;
commit;


/* To look at enrollments for part A+B, use RAW_BASIS as follows:

select * from enrollment
where raw_basis like '_: AB%';
*/


create or replace view cms_enr_stats_sql as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_enr_stats_sql where design_digest = &&design_digest;
