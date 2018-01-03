/* cms_drug_dstats -- CMS ETL: descriptive statistics for drugs

  - view observation_fact as pcornet_dispensing
  - quality check Table ID etc.

*/

/** pcornet_diagnosis -- view observation_fact as CDM diagnosis
 *
 * Note: One i2b2 fact corresponds to one CDM diagnosis. Other
 *       than filtering, there are no cardinality changes
 */
create or replace view pcornet_dispensing as
select obs.upload_id || ' ' || obs.patient_num || ' ' || obs.instance_num DISPENSINGID
     , obs.patient_num PATID
     , null PRESCRIBINGID -- optional relationship to the PRESCRIBING table
     , start_date DISPENSE_DATE	-- (as close as possible to date the person received the dispensing).
     , substr(concept_cd, length('NDC:_')) NDC	-- National Drug Code in the 11-digit, no-dash, HIPAA format.
     , confidence_num DISPENSE_SUP
     , quantity_num DISPENSE_AMT
     , tval_char RAW_NDC
from "&&I2B2STAR".observation_fact obs
where regexp_like(concept_cd, '^NDC:[0-9]{11}$')
;

/*Check that the view is type-compatible with the table. */
insert into "&&PCORNET_CDM".dispensing select * from pcornet_dispensing where 1=0;


-- Table ID. Records, Patients, Encounters, and Date Ranges by Table
create or replace view counts_ranges_ID_dispensing as
select 'DISPENSING' "Table"
     , count(*) "Records"
     , count(distinct patid) "Patients"
     , null "Encounters"
     , 'DISPENSE_DATE' "Field name"
     , min(dispense_date) "Min"
     , max(dispense_date) "Max"
     , '@@' "Source Tables"
from pcornet_dispensing
;

-- Chart IF. Trend in Dispensed Medications by Dispense Date, Past 5 Years
create or replace view dispensing_trend_chart as
select dispense_year, dispense_month, count(*) qty
     , substr('**********************', 1, 1 + log(2, count(*) + 1)) viz
from (
  select extract(year from dispense_date) dispense_year
       , extract(month from dispense_date) dispense_month
  from pcornet_dispensing
)
group by dispense_year, dispense_month
order by dispense_year, dispense_month
;

-- Table IVD. Missing or Unknown Values, Optional Tables (continued)
-- Table IIB. Values Outside of CDM Specifications
create or replace view ivd_missing_dispensing as
select 'DISPENSING' "Table"
     , 'DISPENSE_SUP' "Field"
     , sum(sup_missing) "Numerator"
     , count(*) "Denominator"
     , round(sum(sup_missing) / count(*) * 100, 1) "%"
     , sum(ndc_bad) NDC_BAD
     , '@@' "Source table"
from (
  select case when dispense_sup is null then 1 else null end sup_missing
       , case when length(ndc) != 11 then 1 else null end ndc_bad
  from pcornet_dispensing
)
;


create or replace view cms_drug_dstats_sql as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_drug_dstats_sql where design_digest = &&design_digest;
