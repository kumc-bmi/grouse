/** cms_enc_dstats - Descriptive statistics for CMS Encounters.

  - pcornet_encounter view based on visit_dimension
  - encounters_per_visit initial quality check

*/

select encounter_num from "&&I2B2STAR".visit_dimension
where 'variable' = 'I2B2STAR';

create or replace view drg_type_enum as
select '01' cms_drg_old
     , '02' ms_drg_current
     , 'NI' no_information
     , 'UN' unknown
     , 'OT' other
from dual
;

create or replace view pcornet_encounter as
select encounter_num encounterid
     , patient_num patid
     , trunc(start_date) admit_date
     , to_char(start_date, 'HH24:MM') admit_time
     , trunc(end_date) discharge_date
     , to_char(end_date, 'HH24:MM') discharge_time
     , providerid
     , location_zip facility_location
     , nvl(inout_cd, 'NI') enc_type
     , facilityid
     , discharge_disposition
     , discharge_status
     , drg
     , (select ms_drg_current from drg_type_enum) drg_type
     , admitting_source
     , location_zip RAW_SITEID
     , inout_cd RAW_ENC_TYPE
     , null RAW_DISCHARGE_DISPOSITION
     , null RAW_DISCHARGE_STATUS
     , null RAW_DRG_TYPE
     , null RAW_ADMITTING_SOURCE
from "&&I2B2STAR".visit_dimension
;
-- select * from pcornet_encounter;



/** encounters_per_visit_patient - based on PCORNet CDM EDC Table IIID
*/
create or replace view encounters_per_visit_patient
as
with
  enc_tot as
  (select count( *) qty from pcornet_encounter
  ), enc_ot_un as
  (select
    case
      when enc_type not in('AV', 'ED', 'EI', 'IP', 'IS', 'OA') then 'Other'
      else enc_type
    end enc_type, patid, admit_date
  , providerid
  from
    pcornet_encounter
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

-- select * from encounters_per_visit_patient;

select 1 complete
from dual
where (select null from encounters_per_visit_patient where 1 = 0) is null
  and (select null from pcornet_encounter where 1 = 0) is null
  and (select null from drg_type_enum where 1 = 0) is null
;

