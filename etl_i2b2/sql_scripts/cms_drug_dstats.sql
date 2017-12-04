/* cms_drug_dstats -- CMS ETL: descriptive statistics for drugs

  - view observation_fact as pcornet_dispensing
  - quality check (TODO)

ref
Table @@TODO:
*/

/** pcornet_diagnosis -- view observation_fact as CDM diagnosis
 *
 * Note: One i2b2 fact corresponds to one CDM diagnosis. Other
 *       than filtering, there are no cardinality changes
 */
create or replace view pcornet_dispensing as
select obs.patient_num || ' ' || obs.instance_num DISPENSINGID
     , obs.patient_num PATID
     , null PRESCRIBINGID -- optional relationship to the PRESCRIBING table
     , start_date DISPENSE_DATE	-- (as close as possible to date the person received the dispensing).
     , substr(concept_cd, length('NDC:_')) NDC	-- National Drug Code in the 11-digit, no-dash, HIPAA format.
     , confidence_num DISPENSE_SUP
     , quantity_num DISPENSE_AMT
     , tval_char RAW_NDC
from "&&I2B2STAR".observation_fact obs
where concept_cd like 'NDC:%'
;

/*Check that the view is type-compatible with the table. */
insert into "&&PCORNET_CDM".dispensing select * from pcornet_dispensing where 1=0;



create or replace view cms_drug_dstats_sql as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_drug_dstats_sql where design_digest = &&design_digest;
