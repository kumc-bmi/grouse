/** cms_enr_dstats - Descriptive statistics for CMS Enrollment.

We build a PCORNet CDM style ENROLLMENT table and compute descriptive statistics.

To distinguish enrollments for part AB, A, B, or D, use ENR_BASIS or RAW_BASIS as follows:

select coverage, count(*) from (
select case when enr_basis = 'I' then raw_basis else enr_basis end as coverage
from enrollment
)
group by coverage;


Note: unlike cms_enc_dstats etc. where the input is only an i2b2 star schema,
in this case we reach back to the CMS RIF tables, because we didn't include
bene_mdcr_entlmt_buyin_ind_01 when we curated columns from RIF to include in i2b2.

To look at just years and months, this is handy:
alter session set NLS_DATE_FORMAT = 'YYYY-MM';

To change it back:
alter session set NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI' ;

*/

select bene_id, bene_mdcr_entlmt_buyin_ind_01 from CMS_DEID.mbsf_ab_summary where 1 = 0;             -- '11-'13 A, B
select bene_id, mdcr_entlmt_buyin_ind_01 from CMS_DEID_2014_updated.mbsf_abcd_summary where 1 = 0;   -- '14     A, B
select bene_id, mdcr_entlmt_buyin_ind_01 from CMS_DEID_2015_updated.mbsf_abcd_summary where 1 = 0;   -- '15     A, B
select bene_id, mdcr_entlmt_buyin_ind_01 from cms_deid_12caid_16care.mbsf_abcd_summary where 1 = 0;  -- '16     A, B
select bene_id, PTD_CNTRCT_ID_01 from CMS_DEID.MBSF_D_CMPNTS where 1=0;                  -- '11-13       D
select bene_id, PTD_CNTRCT_ID_01 from CMS_DEID_2014_updated.mbsf_abcd_summary where 1 = 0;            -- '14          D
select bene_id, PTD_CNTRCT_ID_01 from CMS_DEID_2015_updated.mbsf_abcd_summary where 1 = 0;            -- '15          D
select bene_id, PTD_CNTRCT_ID_01 from cms_deid_12caid_16care.mbsf_abcd_summary where 1 = 0;            -- '15          D


select patient_num from "&&I2B2STAR".patient_dimension where 1=0;

select patid from "&&PCORNET_CDM".enrollment where 1 = 0;  -- Destination table must already exist.


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

-- optimize access to date shifts
whenever sqlerror continue; drop table per_bene_shift; whenever sqlerror exit;
create table per_bene_shift as
select distinct bene_id_deid as patid, to_number(bene_id_deid) as patient_num, date_shift_days from cms_deid.BENE_ID_MAPPING_11_15 union
select distinct bene_id_deid as patid, to_number(bene_id_deid) as patient_num, date_shift_days from cms_deid.BENE_ID_MAPPING_12_16;
create unique index per_bene_patid on per_bene_shift(patid);
create unique index per_bene_patient_num on per_bene_shift(patient_num);

/** pre-flight check: do we have a date-shift for each one in the CMS range??
select case when count(*) = 0 then 1 else 1 / 0 end date_shift_ok from (
  select * from "&&I2B2STAR".patient_dimension pd
  where patient_num < 22000000
  and not exists (
    select 1 from per_bene_shift sh where sh.patient_num = pd.patient_num
  )
  and rownum < 20
);
*/


whenever sqlerror continue; drop table per_bene_mo; whenever sqlerror exit;
create table per_bene_mo as

with per_bene_mo_13 as (
select /*+ parallel*/ bene_id, bene_enrollmt_ref_yr, mo, buyin, hmo, extract_dt
from CMS_DEID.mbsf_ab_summary
unpivot(
        (buyin,hmo) /*add hmo column*/
        for mo in (
            (bene_mdcr_entlmt_buyin_ind_01,bene_hmo_ind_01) as 1
          , (bene_mdcr_entlmt_buyin_ind_02,bene_hmo_ind_02) as 2
          , (bene_mdcr_entlmt_buyin_ind_03,bene_hmo_ind_03) as 3
          , (bene_mdcr_entlmt_buyin_ind_04,bene_hmo_ind_04) as 4
          , (bene_mdcr_entlmt_buyin_ind_05,bene_hmo_ind_05) as 5
          , (bene_mdcr_entlmt_buyin_ind_06,bene_hmo_ind_06) as 6
          , (bene_mdcr_entlmt_buyin_ind_07,bene_hmo_ind_07) as 7
          , (bene_mdcr_entlmt_buyin_ind_08,bene_hmo_ind_08) as 8
          , (bene_mdcr_entlmt_buyin_ind_09,bene_hmo_ind_09) as 9
          , (bene_mdcr_entlmt_buyin_ind_10,bene_hmo_ind_10) as 10
          , (bene_mdcr_entlmt_buyin_ind_11,bene_hmo_ind_11) as 11
          , (bene_mdcr_entlmt_buyin_ind_12,bene_hmo_ind_12) as 12
          )
      )
where buyin != '0' -- Not entitled
)
, per_bene_456s as (
  select * from CMS_DEID_2014_updated.mbsf_abcd_summary union all
  select * from CMS_DEID_2015_updated.mbsf_abcd_summary union all
  select * from cms_deid_12caid_16care.mbsf_abcd_summary
)
, per_bene_mo_456 as (
select bene_id, bene_enrollmt_ref_yr, mo, buyin, hmo, extract_dt
from per_bene_456s
unpivot(
        (buyin,hmo) /*add hmo column*/
        for mo in (
            (mdcr_entlmt_buyin_ind_01,hmo_ind_01) as 1
          , (mdcr_entlmt_buyin_ind_02,hmo_ind_02) as 2
          , (mdcr_entlmt_buyin_ind_03,hmo_ind_03) as 3
          , (mdcr_entlmt_buyin_ind_04,hmo_ind_04) as 4
          , (mdcr_entlmt_buyin_ind_05,hmo_ind_05) as 5
          , (mdcr_entlmt_buyin_ind_06,hmo_ind_06) as 6
          , (mdcr_entlmt_buyin_ind_07,hmo_ind_07) as 7
          , (mdcr_entlmt_buyin_ind_08,hmo_ind_08) as 8
          , (mdcr_entlmt_buyin_ind_09,hmo_ind_09) as 9
          , (mdcr_entlmt_buyin_ind_10,hmo_ind_10) as 10
          , (mdcr_entlmt_buyin_ind_11,hmo_ind_11) as 11
          , (mdcr_entlmt_buyin_ind_12,hmo_ind_12) as 12
          )
      )
where buyin != '0' -- Not entitled
)
,
ea as (
select bene_id, bene_enrollmt_ref_yr, mo
     , to_date(to_char(bene_enrollmt_ref_yr, 'FM0000') || to_char(mo, 'FM00') || '01', 'YYYYMMDD') enrollmt_mo_1st
     , buyin
     , hmo
     , extract_dt
from (
  select * from per_bene_mo_13
  union all
  select * from per_bene_mo_456
)
)
select ea.*
     , extract(year from enrollmt_mo_1st) * 12 + (extract(month from enrollmt_mo_1st) - 1) enrollmt_mo
from ea
;
-- select * from per_bene_mo;



/* Schroeder writes Tuesday, June 4th, 2019 6:09 PM:

The monthly state buy-in code specifies whether the state contributed to the premiums for parts A/B.  
It can tell you if the beneficiary had A/B/AB. But it is doesn???t tell you HMO status.

That monthly variable you need is: HMO_IND_XX

So you need MDCR_ENTLMT_BUYIN_IND_XX in (1,2,3,A,B,C)  and  HMO_IND_XX in (0)

Note: technically,  you can include HMO_IND_XX in (0, 4) but I???ve always excluded 
the ???Fee-for-service participant in case or disease management demonstration project??? (which is the ???4???). 
https://www.resdac.org/articles/identifying-medicare-managed-care-beneficiaries-master-beneficiary-summary-or-denominator

These variables can be helpful too
BENE_HI_CVRAGE_TOT_MONS  (number of months of A coverage for that reference year)
BENE_SMI_CVRAGE_TOT_MONS  (number of months of B coverage for that reference year)
BENE_HMO_CVRAGE_TOT_MONS  (number of months of HMO coverage for that reference year)
*/

select * from "&&PCORNET_CDM".enrollment;

-- truncate table enrollment;
delete from "&&PCORNET_CDM".enrollment;
commit;

insert /*+ append */ into "&&PCORNET_CDM".enrollment (
  patid, enr_start_date, enr_end_date, chart, enr_basis, raw_basis
)
with decode_coverage as (
select
         case when hmo = '0' and buyin in ('1','A') then 'A'
              when hmo = '0' and buyin in ('2','B') then 'B'
              when hmo = '0' and buyin in ('3','C') then 'AB'
              when hmo in ('1','2','4','A','B','C') then ('C'||hmo) 
              else 'OT'
         end as coverage  --add category 'CX' for managed care
       , per_bene_mo.*
from per_bene_mo
)
, per_bene_start_mo as (
  -- ack: https://blog.jooq.org/2015/11/07/how-to-find-the-longest-consecutive-series-of-events-in-sql/
  select bene_id, buyin, enrollmt_mo_1st, enrollmt_mo,
         coverage,
         enrollmt_mo - (dense_rank() over (partition by bene_id, coverage order by bene_id, enrollmt_mo, coverage)) series
  from decode_coverage
)
, enr_no_shift as (
select bene_id patid
     , min(enrollmt_mo_1st) enr_start_date
     , max(enrollmt_mo_1st) enr_end_date
     , 'Y' chart  -- there are no contractual or other restrictions between you and
                  -- the individual (or sponsor) that would prohibit you from
                  -- requesting any chart for this patient.
     , 'I' enr_basis -- I=Medical insurance coverage
     , coverage raw_basis
     -- , count(*) month_dur
from per_bene_start_mo
group by bene_id, coverage, series
)
select enr.patid
     , add_months(enr_start_date, date_shift_days / 30)
     , last_day(add_months(enr_end_date, date_shift_days / 30))
     , chart
     , enr_basis
     , raw_basis
from enr_no_shift enr
join per_bene_shift shifts on shifts.patid = enr.patid
--   and extract(year from enr_start_date) between yr_lo and yr_hi
; -- 16,790,748 rows inserted.
commit;


/** Part D Enrollment with date shift fix
*/

whenever sqlerror continue; drop table per_bene_mo_d; whenever sqlerror exit;
create table per_bene_mo_d as
with per_bene_mo_13d as (
  select bene_id, bene_enrollmt_ref_yr, mo, ptd_cntrct_id, PTD_PBP_ID, RDS_IND, extract_dt
  from CMS_DEID.MBSF_D_CMPNTS
  unpivot(
          (ptd_cntrct_id, PTD_PBP_ID, RDS_IND)
          for mo in (
              (PTD_CNTRCT_ID_01, PTD_PBP_ID_01, RDS_IND_01) as 1
            , (PTD_CNTRCT_ID_02, PTD_PBP_ID_02, RDS_IND_02) as 2
            , (PTD_CNTRCT_ID_03, PTD_PBP_ID_03, RDS_IND_03) as 3
            , (PTD_CNTRCT_ID_04, PTD_PBP_ID_04, RDS_IND_04) as 4
            , (PTD_CNTRCT_ID_05, PTD_PBP_ID_05, RDS_IND_05) as 5
            , (PTD_CNTRCT_ID_06, PTD_PBP_ID_06, RDS_IND_06) as 6
            , (PTD_CNTRCT_ID_07, PTD_PBP_ID_07, RDS_IND_07) as 7
            , (PTD_CNTRCT_ID_08, PTD_PBP_ID_08, RDS_IND_08) as 8
            , (PTD_CNTRCT_ID_09, PTD_PBP_ID_09, RDS_IND_09) as 9
            , (PTD_CNTRCT_ID_10, PTD_PBP_ID_10, RDS_IND_10) as 10
            , (PTD_CNTRCT_ID_11, PTD_PBP_ID_11, RDS_IND_11) as 11
            , (PTD_CNTRCT_ID_12, PTD_PBP_ID_12, RDS_IND_12) as 12
            ) ) )
, per_bene_456s as (
 select * from CMS_DEID_2014_updated.mbsf_abcd_summary union all
 select * from CMS_DEID_2015_updated.mbsf_abcd_summary union all
 select * from cms_deid_12caid_16care.mbsf_abcd_summary
)
, per_bene_mo_456d as (
  select bene_id, bene_enrollmt_ref_yr, mo, ptd_cntrct_id, PTD_PBP_ID, RDS_IND, extract_dt
  from per_bene_456s
  unpivot(
          (ptd_cntrct_id, PTD_PBP_ID, RDS_IND)
          for mo in (
              (PTD_CNTRCT_ID_01, PTD_PBP_ID_01, RDS_IND_01) as 1
            , (PTD_CNTRCT_ID_02, PTD_PBP_ID_02, RDS_IND_02) as 2
            , (PTD_CNTRCT_ID_03, PTD_PBP_ID_03, RDS_IND_03) as 3
            , (PTD_CNTRCT_ID_04, PTD_PBP_ID_04, RDS_IND_04) as 4
            , (PTD_CNTRCT_ID_05, PTD_PBP_ID_05, RDS_IND_05) as 5
            , (PTD_CNTRCT_ID_06, PTD_PBP_ID_06, RDS_IND_06) as 6
            , (PTD_CNTRCT_ID_07, PTD_PBP_ID_07, RDS_IND_07) as 7
            , (PTD_CNTRCT_ID_08, PTD_PBP_ID_08, RDS_IND_08) as 8
            , (PTD_CNTRCT_ID_09, PTD_PBP_ID_09, RDS_IND_09) as 9
            , (PTD_CNTRCT_ID_10, PTD_PBP_ID_10, RDS_IND_10) as 10
            , (PTD_CNTRCT_ID_11, PTD_PBP_ID_11, RDS_IND_11) as 11
            , (PTD_CNTRCT_ID_12, PTD_PBP_ID_12, RDS_IND_12) as 12
            )
  )
)
, ea as (
select bene_id
     , ptd_cntrct_id, PTD_PBP_ID, RDS_IND
     , extract_dt
     , bene_enrollmt_ref_yr, mo
     , to_date(to_char(bene_enrollmt_ref_yr, 'FM0000') || to_char(mo, 'FM00') || '01', 'YYYYMMDD') enrollmt_mo_1st
from (
  select * from per_bene_mo_13d
  union all
  select * from per_bene_mo_456d
)
)
select ea.*
     , extract(year from enrollmt_mo_1st) * 12 + (extract(month from enrollmt_mo_1st) - 1) enrollmt_mo
from ea
;

/* Schroeder writes Tuesday, July 31, 2018 10:37 AM:

For a given month, XX, bene_id has observable Part D events (PDE) if  (i.e. bene_id “has” Part D)

  substr(PTD_CNTRCT_ID_XX,1,1) in (‘E’, ‘H’, ‘R’, ‘S’, ‘X’)
    and PTD_PBP_ID_XX is NOT NULL
     and RDS_IND_XX = ‘N’

Interpretation:
·         PTD_CNTRCT_ID_XX are contract IDs.  Any that start with values of  (‘E’, ‘H’, ‘R’, ‘S’, ‘X’) submit all PDE to CMS (i.e. are observable)
·         PTD_PBP_ID_XX are benefit packages.  If there is a contract ID, this should also be filled in (note, technically it’s supposed to be a 3-digit alphanumeric that can include leading zeros).
·         RDS_IND_XX are employer-offered prescription drug plans.  These do not submit all PDE to CMS.  So we only include those without it.
*/
insert /*+ parallel append */ into "&&PCORNET_CDM".enrollment (
  patid, enr_start_date, enr_end_date, chart, enr_basis, raw_basis
)
with has_part_d as (
select bm.*, substr(PTD_CNTRCT_ID, 1, 1) cntrct_1 from per_bene_mo_d bm
  where  substr(PTD_CNTRCT_ID, 1, 1) in ('E', 'H', 'R', 'S', 'X')
    and PTD_PBP_ID is NOT NULL
     and RDS_IND = 'N'
)
, per_bene_start_mo as (
  select bene_id, cntrct_1, enrollmt_mo_1st, enrollmt_mo,
         enrollmt_mo - (dense_rank() over (partition by bene_id order by bene_id, enrollmt_mo)) series
  from has_part_d
)
, enr_no_shift as (
select bene_id patid
     , min(enrollmt_mo_1st) enr_start_date
     , max(enrollmt_mo_1st) enr_end_date
     , 'Y' chart  -- there are no contractual or other restrictions between you and
                  -- the individual (or sponsor) that would prohibit you from
                  -- requesting any chart for this patient.
     , 'D' enr_basis -- D=Outpatient prescription drug coverage
     , count(*) || substr(listagg(cntrct_1, '') within group (order by enrollmt_mo_1st), 1, 45) raw_basis
     -- , count(*) month_dur
from per_bene_start_mo
group by bene_id, series
order by patid, enr_start_date
)
select enr.patid
     , add_months(enr_start_date, date_shift_days / 30)
     , last_day(add_months(enr_end_date, date_shift_days / 30))
     , chart
     , enr_basis
     , raw_basis
from enr_no_shift enr
join per_bene_shift shifts on shifts.patid = enr.patid
; -- 9,754,051 rows inserted.
commit;
/* breakdown by start year:
select extract(year from enr_start_date) enr_yr, count(*) from enrollment where enr_basis = 'D'
group by extract(year from enr_start_date) order by enr_yr ;

by end year:
select extract(year from enr_end_date) enr_yr, count(*) from enrollment where enr_basis = 'D'
group by extract(year from enr_end_date) order by enr_yr ;

*/

create unique index IIA_Primary_Key on "&&PCORNET_CDM".enrollment (patid, enr_start_date, enr_basis) ;
-- select * from IIA_Primary_Key_Errors;


/**
drop table coverage_overlap;
create table coverage_overlap as
with overlap as (
  select lo.patid
       , lo.enr_start_date as start_lo, hi.enr_start_date as start_hi
       , lo.enr_end_date as end_lo, hi.enr_end_date as end_hi
       , case
         when lo.raw_basis in ('AB')      and hi.raw_basis in ('AB')      then 'AB'
         when lo.raw_basis in ('A', 'AB') and hi.raw_basis in ('A', 'AB') then 'A'
         when lo.raw_basis in ('B', 'AB') and hi.raw_basis in ('B', 'AB') then 'B'
         end as part
  from "&&PCORNET_CDM".enrollment lo
  join "&&PCORNET_CDM".enrollment hi
    on hi.patid = lo.patid
   and hi.enr_start_date >= lo.enr_start_date
   and hi.enr_start_date <= lo.enr_end_date
   and (hi.enr_start_date > lo.enr_start_date or
        hi.enr_end_date != lo.enr_end_date)
  where lo.enr_basis in ('I')
    and hi.enr_basis in ('I')
)
select *
from overlap
where part is not null
;


select 'enrollment' as info, count(*) records, count(distinct patid) as patients from enrollment
union all
select 'overlap' as info, count(*) records, count(distinct patid) as patients from coverage_overlap;
*/


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
