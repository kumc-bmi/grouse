/** cms_death - Build PCORNet CDM DEATH table, stats views from CMS I2B2 data.

Refs:

 - CMS Date of Death
   https://www.resdac.org/taxonomy/term/219
 - CMS Valid Date of Death Switch
   https://www.resdac.org/cms-data/variables/Valid-Date-Death-Switch

 - PCORNet DEATH
   PCORnet Common Data Model v4 .1 Specification (15 May 2018
   https://pcornet.org/pcornet-common-data-model/
   https://pcornet.org/wp-content/uploads/2018/05/PCORnet-Common-Data-Model-v4-1-2018_05_15.pdf
*/

-- dependencies
select patient_num from "&&I2B2STAR".observation_fact where 1=0;


create or replace view pcornet_death as
select death_dt.patient_num patid
, trunc(death_dt.start_date) death_date
, 'N' death_date_impute  -- N=Not imputed
, decode(death_validated.concept_cd,
         -- V=verified by the Social Security Administration (SSA) or the Railroad Retirement Board (RRB)
         'BENE_VALID_DEATH_DT_SW:V',
         -- D=Social Security
         'D',
         null, 'NI') death_source
, 'E' death_match_confidence  -- E=Excellent probabilistic patient matching
from (
  select * from "&&I2B2STAR".observation_fact
  where concept_cd = 'BENE_DEATH_DT:') death_dt
left join (
  
  select * from "&&I2B2STAR".observation_fact
  where concept_cd = 'BENE_VALID_DEATH_DT_SW:V') death_validated
  on death_dt.patient_num = death_validated.patient_num
  and death_dt.instance_num = death_validated.instance_num
  and death_dt.upload_id = death_validated.upload_id
;
-- eyeball it: select * from pcornet_death;

-- check types: insert into death select * from pcornet_death;


-- Table ID. Records, Patients, Encounters, and Date Ranges by Table
create or replace view ID_counts_ranges_death as
select 'DEATH' "Table"
     , count(*) "Records"
     , count(distinct patid) "Patients"
     , '---' "Encounters"
     , 'DEATH_DATE' "Field name"
     , to_char(percentile_cont(0.05) within group (order by death_date), 'YYYY_Mon') "5th Percentile"
     , to_char(percentile_cont(0.95) within group (order by death_date), 'YYYY_Mon') "95th Percentile"
     , 'MBSF_ABCD_SUMMARY' "Source Tables"
from "&&PCORNET_CDM".death
;
-- eyeball it: select * from ID_counts_ranges_death

-- Table IIIC. Illogical Dates
create or replace view iic_illogical_dates as
with death_lt_birth as (
select count(distinct death.patid) patients
    from "&&PCORNET_CDM".death death
    join "&&PCORNET_CDM".demographic dem on death.patid = dem.patid
    where death.death_date < dem.birth_date
), admin_gt_death as (
select count(distinct death.patid) patients
    from "&&PCORNET_CDM".encounter enc
    join "&&PCORNET_CDM".death on death.patid = enc.patid
    where enc.admit_date > death.death_date
), discharge_gt_death as (
select count(distinct death.patid) patients
    from "&&PCORNET_CDM".encounter enc
    join "&&PCORNET_CDM".death on death.patid = enc.patid
    where enc.discharge_date > death.death_date
), px_gt_death as (
select count(distinct death.patid) patients
    from "&&PCORNET_CDM".procedures proc
    join "&&PCORNET_CDM".death on death.patid = proc.patid
    where proc.px_date > death.death_date
), disp_gt_death as (
select count(distinct death.patid) patients
    from "&&PCORNET_CDM".dispensing disp
    join "&&PCORNET_CDM".death on death.patid = disp.patid
    where disp.dispense_date > death.death_date
)
, denom as (
select count(distinct patid) patients from encounter
)
select
   'DEATH_DATE < BIRTH_DATE' date_comparison
 , death_lt_birth.patients "Patients"
 , round(100 * death_lt_birth.patients / denom.patients, 2) "% of ENCOUNTER patients"
 , 'OBSERVATION_FACT' "Source tables"
from death_lt_birth, denom

union all

select
   'ADMIT_DATE > DEATH_DATE' date_comparison
 , admin_gt_death.patients "Patients"
 , round(100 * admin_gt_death.patients / denom.patients, 2) "% of ENCOUNTER patients"
 , 'OBSERVATION_FACT' "Source tables"
from admin_gt_death, denom

union all

select
   'DISCHARGE_DATE > DEATH_DATE' date_comparison
 , discharge_gt_death.patients "Patients"
 , round(100 * discharge_gt_death.patients / denom.patients, 2) "% of ENCOUNTER patients"
 , 'OBSERVATION_FACT' "Source tables"
from discharge_gt_death, denom

union all

select
   'PX_DATE > DEATH_DATE' date_comparison
 , px_gt_death.patients "Patients"
 , round(100 * px_gt_death.patients / denom.patients, 2) "% of ENCOUNTER patients"
 , 'OBSERVATION_FACT' "Source tables"
from px_gt_death, denom

union all

select
   'DISPENSE_DATE > DEATH_DATE' date_comparison
 , disp_gt_death.patients "Patients"
 , round(100 * disp_gt_death.patients / denom.patients, 2) "% of ENCOUNTER patients"
 , 'OBSERVATION_FACT' "Source tables"
from disp_gt_death, denom
;

-- eyeball it: select * from iic_illogical_dates

create or replace view cms_death_design as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_death_design where design_digest = &&design_digest;
