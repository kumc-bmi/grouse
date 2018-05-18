/* cms_dx_by_enc_type -- CMS ETL Diagnosis report based on PCORNet CDM EDC Table IVA.

ref
Table IVA. Diagnosis Records Per Encounter and Per Patient, Overall and by Encounter Type

*/

select enc_type from pcornet_encounter where 'dep' = 'cms_enc_dstats.sql';

whenever sqlerror continue; drop table dx_meta; whenever sqlerror exit;
create table dx_meta as
select * from (
select c_basecode concept_cd, pcori_basecode dx
     , substr(c_fullname, length('\PCORI\DIAGNOSIS\_'), 2) dx_type
     -- , c_fullname
     -- , m_applied_path
from grousemetadata.pcornet_diag diag
where pcori_basecode is not null
  and c_synonym_cd = 'N'
  and c_fullname like '\PCORI\DIAGNOSIS\%'
)
-- Skip ICD-9 V codes in 10 ontology, ICD-9 E codes in 10 ontology, ICD-10 numeric codes in 10 ontology
-- adapted from SCILHS/i2p-transform 543c561 Nov 15, 2016
-- Note: makes the assumption that ICD-9 Ecodes are not ICD-10 Ecodes; same with ICD-9 V codes. On inspection seems to be true.
where not (REGEXP_LIKE (dx, '^[VE0-9].*', 'i') and dx_type = '10')
;

select case when count(*) = 0 then 1 else 1/0 end unique_dx_type from (
  select concept_cd, count(*) from dx_meta group by concept_cd having count(*) > 1
);

whenever sqlerror continue; drop table dx_source_meta; whenever sqlerror exit;
create table dx_source_meta as
with diag as (
  select c_basecode, pcori_basecode, c_fullname, c_synonym_cd from grousemetadata.pcornet_diag
)
select c_basecode modifier_cd, SUBSTR(pcori_basecode, INSTR(pcori_basecode, ':') + 1, 2) dx_source
from diag
where pcori_basecode is not null
  and c_synonym_cd = 'N'
  and c_fullname like '\PCORI_MOD\CONDITION_OR_DX\%'
;

whenever sqlerror continue; drop table pdx_meta; whenever sqlerror exit;
create table pdx_meta as
with diag as (
  select c_basecode, pcori_basecode, c_fullname, c_synonym_cd from grousemetadata.pcornet_diag
)
select c_basecode modifier_cd, SUBSTR(pcori_basecode, INSTR(pcori_basecode, ':') + 1, 2) pdx
from diag
where pcori_basecode is not null
  and c_synonym_cd = 'N'
  and c_fullname like '\PCORI_MOD\PDX\%'
;

create or replace view cdm_other_enum as
select 'NI' No_information
     , 'UN' Unknown
     , 'OT' Other
from dual;

create or replace view dx_origin_enum as
select 'OD' "Order"
     , 'BI' Billing
     , 'CL' Claim
     , other.*
from cdm_other_enum other;

/** pcornet_diagnosis -- view observation_fact as CDM diagnosis
 *
 * Note: One i2b2 fact corresponds to one CDM diagnosis. Other
 *       than filtering, there are no cardinality changes
 */
create or replace view pcornet_diagnosis as
select  rownum  DIAGNOSISID
     , obs.patient_num PATID
     , obs.encounter_num ENCOUNTERID
     , nvl(enc.ENC_TYPE, 'NI') ENC_TYPE
     , enc.ADMIT_DATE
     , obs.provider_id PROVIDERID
     , dx_meta.DX
     , dx_meta.DX_TYPE
     , nvl(src.dx_source, 'NI') DX_SOURCE
     , (select claim from dx_origin_enum) DX_ORIGIN
     , nvl(px.pdx, 'NI') PDX
     , obs.concept_Cd RAW_DX
     , obs.upload_id RAW_DX_TYPE
     , obs.sourcesystem_cd RAW_DX_SOURCE
     , obs.instance_num raw_dx_origin
     , null RAW_PDX
from "&&I2B2STAR".observation_fact obs
join dx_meta on dx_meta.concept_cd = obs.concept_cd
-- ISSUE: prove that these left-joins match at most once.
left join dx_source_meta src on src.modifier_cd = obs.modifier_cd
left join pdx_meta px on px.modifier_cd = obs.modifier_cd
left join encounter enc on obs.encounter_num = enc.encounterid
;
-- select * from pcornet_diagnosis

/*Check that the view is type-compatible with the table. */
-- insert into "&&PCORNET_CDM".diagnosis select * from pcornet_diagnosis where 1=0;


create or replace view dx_by_enc_type as
with qty as
  (select enc_type
  , count( *) count_dx
  , count(distinct encounterid) count_enc
  from diagnosis
  group by enc_type
  )
select enc_type
, count_dx
, count_enc
, round(count_dx / (greatest(1, count_enc)), 2) dx_per_enc
from qty
order by enc_type ;

-- select * from dx_by_enc_type;

/** Procedures
 */

whenever sqlerror continue; drop table px_meta; whenever sqlerror exit;

create table px_meta as
select c_basecode concept_cd
     , SUBSTR(pr.pcori_basecode, INSTR(pr.pcori_basecode, ':') + 1, 11) px
     -- C4 and HC got merged as CH in CDM 3.1
     , replace(replace(SUBSTR(pr.c_fullname, length('\PCORI\PROCEDURE\%'), 2),
                       'C4', 'CH'), 'HC', 'CH') px_type
     , c_name
from grousemetadata.pcornet_proc pr
where pr.c_fullname like '\PCORI\PROCEDURE\%'
  and pr.c_synonym_cd = 'N'
  and pcori_basecode is not null
  and pr.c_basecode not like 'PROCEDURE:%'
  and pr.m_applied_path = '@'
  and pr.c_visualattributes like 'L%'
;
create unique index pdx_meta_concept on px_meta (concept_cd);

create or replace view px_source_enum as
select 'OD' "Order"
     , 'BI' Billing
     , 'CL' Claim
     , other.*
from cdm_other_enum other;

create or replace view pcornet_procedures as
-- Instance num should be unique within patient_num and upload_id.
-- There isn't room for all three; since we can recover patient_num and upload_id
-- from other columns, hash them and truncate, hoping that's good enough.
select substr(obs.instance_num || ' ' || ora_hash(obs.upload_id || ' ' || obs.patient_num), 1, 18) PROCEDURESID
     , obs.patient_num PATID
     , obs.encounter_num ENCOUNTERID
     , nvl(vd.inout_cd, 'NI') ENC_TYPE
     , vd.start_date ADMIT_DATE
     , vd.PROVIDERID
     , obs.start_date PX_DATE
     , px_meta.PX
     , px_meta.PX_TYPE
     , (select Claim from px_source_enum) PX_SOURCE
     , substr(px_meta.c_name, 1, 50) RAW_PX
     , obs.upload_id RAW_PX_TYPE
from "&&I2B2STAR".observation_fact obs
join px_meta on px_meta.concept_cd = obs.concept_cd
left join "&&I2B2STAR".visit_dimension vd on obs.patient_num = vd.patient_num
                                         and obs.encounter_num = vd.encounter_num
;

/*Check that the view is type-compatible with the table. */
-- insert into "&&PCORNET_CDM".procedures select * from pcornet_procedures where 1=0;
explain plan for
select * from pcornet_procedures
where patid between 1 and 10000
;

/**
 * ref Table IVB. Procedure Records Per Encounter and Per Patient, Overall and by Encounter Type
 */
create or replace view px_per_enc_by_type as
select enc_type
     , to_char(count(*), '99G999G999G999') "PROCEDURES records"
     , to_char(count(known_px_type), '99G999G999G999') "known PX_TYPE"
     , to_char(count(distinct encounterid), '99G999G999G999') "ENCOUNTER records"
     , round(count(*) / count(distinct encounterid), 2) "PX per encounter"
     , round(count(known_px_type) / count(distinct encounterid), 2) "known PX_TYPE per encounter"
from (
  select proceduresid
       , case when px_type in ('09', '10', '11', 'CH', 'LC', 'ND', 'RE') then px_type else null end known_px_type
       , encounterid
       , case when enc_type in ('NI', 'UN', 'OT') then 'Missing / Other' else enc_type end enc_type
  from procedures
) px
group by enc_type
;
-- select* from px_per_enc_by_type;

create or replace view cms_dx_dstats as
select &&design_digest design_digest from dual;

select 1 up_to_date
from cms_dx_dstats where design_digest = &&design_digest;
