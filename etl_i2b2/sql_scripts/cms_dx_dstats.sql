/* cms_dx_by_enc_type -- CMS ETL Diagnosis report based on PCORNet CDM ED Table IVA.

ref
Table IVA. Diagnosis Records Per Encounter and Per Patient, Overall and by Encounter Type

TODO: include procedures
*/

create or replace view dx_by_enc_type
as
with result_dx as
  (select start_date
  , encounter_num
  , concept_cd
  , modifier_cd
  , instance_num
  from "&&I2B2STAR".observation_fact
  where concept_cd like 'ICD9:%'
    or concept_cd like 'ICD10:%'
  )
, ea as
  (select vd.inout_cd enc_type
  , vd.encounter_num
  from result_dx dx
  left join "&&I2B2STAR".visit_dimension vd
  on vd.encounter_num = dx.encounter_num
  )
, qty as
  (select enc_type
  , count( *) count_dx
  , count(distinct encounter_num) count_enc
  from ea
  group by enc_type
  )
select enc_type
, count_dx
, count_enc
, round(count_dx /(1 + count_enc), 2) dx_per_enc
from qty
order by enc_type ;


create or replace view cms_dx_dstats as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_dx_dstats where design_digest = &&design_digest;