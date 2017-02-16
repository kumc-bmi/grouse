/** cms_dem_dstats - Descriptive statistics for CMS Demographics.

This is an initial quality check on the transformation of CMS demographics
into i2b2.

ISSUE: pass/fail testing?

*/

select start_date from cms_visit_dimension_medpar where 'dep' = 'cms_dem_txform.sql';

-- ISSUE: how to express dependency on "&&I2B2STAR".patient_dimension?
select encounter_num from "&&I2B2STAR".visit_dimension
where 'variable' = 'I2B2STAR';


/** encounters_per_visit_patient - based on PCORNet CDM EDC Table IIID
*/
create or replace view encounters_per_visit_patient
as
with
  encounter as
  (select
    nvl(substr(inout_cd, 1, 2), 'NI') enc_type, patient_num patid, start_date admit_date
  , null providerid -- TODO
  from
    "&&I2B2STAR".visit_dimension
  ), enc_tot as
  (select count( *) qty from encounter
  ), enc_ot_un as
  (select
    case
      when enc_type not in('AV', 'ED', 'EI', 'IP', 'IS', 'OA') then 'Other'
      else enc_type
    end enc_type, patid, admit_date
  , providerid
  from
    encounter
  ), enc_by_type as
  (select
    count( *) encounters, count(distinct patid) patients, round(count( *) / enc_tot.qty * 100, 1) pct
  , enc_type
  from
    enc_ot_un
  cross join enc_tot
  group by
    enc_type, enc_tot.qty
  ), known_prov as
  (select
    count( *) enc_known_provider, enc_type
  from
    enc_ot_un
  where
    providerid is not null
  group by
    enc_type
  ), visit_by_type as
  (select
    count( *) visit, enc_type
  from
    (select distinct
      patid, enc_type, admit_date
    , providerid
    from
      enc_ot_un
    )
  group by
    enc_type
  )
select
  enc_by_type.enc_type, encounters, pct
, patients, round(encounters / patients, 1) encounters_per_patient, enc_known_provider
, visit, round(enc_known_provider / visit, 2) enc_per_visit
from
  enc_by_type
join visit_by_type
on
  enc_by_type.enc_type = visit_by_type.enc_type
left join known_prov
on
  enc_by_type.enc_type = known_prov.enc_type
order by
  enc_by_type.enc_type ;


select 1 complete
from encounters_per_visit_patient
where rownum <= 1;
