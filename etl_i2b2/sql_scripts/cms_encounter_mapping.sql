/** cms_encounter_mapping - map CMS claims to i2b2 encounters

We have two sorts of mappings:
  1. one encounter_num for each hospital encounter (medpar_all row)
  2. for each patient day:
     - if the patient day is (temporally) subsumed by a hospital encounter,
       we map to that hospital encounter's encounter_num; else
     - allocated an encounter_num for that patient day.

These substitution variables are provided by calling tasks, but for convenience in sqldeveloper:
define I2B2STAR = NIGHTHERONDATA;
define cms_source_cd = '''ccwdata.org''';

Reasonable performance probably depends on indexes such as:
create index medpar_bene on "&&CMS_RIF".medpar_all (bene_id);

*/
select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql';
select bene_cd from cms_key_sources where 'dep' = 'cms_keys.pls';

-- for debugging: truncate table "&&I2B2STAR".encounter_mapping;
alter table "&&I2B2STAR".encounter_mapping disable constraint encounter_mapping_pk;
whenever sqlerror continue;
drop index "&&I2B2STAR".encounter_mapping_pk;
whenever sqlerror exit;
alter index "&&I2B2STAR".em_encnum_idx unusable;
alter index "&&I2B2STAR".em_idx_encpath unusable;
alter index "&&I2B2STAR".em_uploadid_idx unusable;


  insert /*+ append */
  into "&&I2B2STAR".encounter_mapping
    (
      encounter_ide
    , encounter_ide_source
    , project_id
    , encounter_num
    , patient_ide
    , patient_ide_source
    , encounter_ide_status
    , upload_date
    , update_date
    , download_date
    , import_date
    , sourcesystem_cd
    , upload_id
    )
select /*+ index(medpar) */ medpar.medpar_id encounter_ide
  , key_sources.medpar_cd encounter_ide_source
  , :project_id project_id
  , "&&I2B2STAR".sq_up_encdim_encounternum.nextval encounter_num
  , medpar.bene_id patient_ide
  , key_sources.bene_cd patient_ide_source
  , i2b2_status.active encounter_ide_status
  , sysdate upload_date
  , medpar.ltst_clm_acrtn_dt update_date
  , :download_date
  , sysdate import_date
  , &&cms_source_cd sourcesystem_cd
  , :upload_id upload_id
  from "&&CMS_RIF".medpar_all medpar
  cross join cms_key_sources key_sources
  cross join i2b2_status
  where medpar.bene_id between coalesce(:bene_id_first, medpar.bene_id)
                           and coalesce(:bene_id_last, medpar.bene_id);

commit;  -- avoid ORA-12838: cannot read/modify an object after modifying it in parallel

create or replace view cms_medpar_mapping
as
  select emap.encounter_ide as medpar_id
  , emap.encounter_num
  from "&&I2B2STAR".encounter_mapping emap
  join cms_key_sources key_sources
  on key_sources.medpar_cd = emap.encounter_ide_source ;


create unique index "&&I2B2STAR".encounter_mapping_pk on "&&I2B2STAR".encounter_mapping(encounter_ide,
  encounter_ide_source, project_id, patient_ide, patient_ide_source) nologging;
  alter table "&&I2B2STAR".encounter_mapping enable constraint encounter_mapping_pk;


/** patient_day mappings rolled up to medpar */
insert
  /*+ append */
into "&&I2B2STAR".encounter_mapping
  (
    encounter_ide
  , encounter_ide_source
  , project_id
  , encounter_num
  , patient_ide
  , patient_ide_source
  , encounter_ide_status
  , upload_date
  , update_date
  , download_date
  , import_date
  , sourcesystem_cd
  , upload_id
  )
with bc_chunk as
  (select /*+ index(clm) */ bene_id
  , clm_from_dt
  , nch_wkly_proc_dt
  from "&&CMS_RIF".bcarrier_claims clm
  where bene_id is not null
    and bene_id between coalesce(:bene_id_first, bene_id)
                    and coalesce(:bene_id_last, bene_id)
  )
, medpar_chunk as
  (select /*+ index(medpar) */ medpar.medpar_id
  , bene_id
  , admsn_dt
  , dschrg_dt
  , cmm.encounter_num
  from "&&CMS_RIF".medpar_all medpar
  join cms_medpar_mapping cmm
  on cmm.medpar_id = medpar.medpar_id
  where bene_id is not null
    and bene_id between coalesce(:bene_id_first, bene_id)
                    and coalesce(:bene_id_last, bene_id)
  )
select fmt_patient_day(pat_day.bene_id, pat_day.clm_from_dt) encounter_ide
, key_sources.patient_day_cd encounter_ide_source
, :project_id project_id
, coalesce(medpar_encounter_num, "&&I2B2STAR".sq_up_encdim_encounternum.nextval) encounter_num
, pat_day.bene_id patient_ide
, key_sources.bene_cd patient_ide_source
, i2b2_status.active encounter_ide_status
, sysdate upload_date
, pat_day.update_date
, :download_date
, sysdate import_date
,
  &&cms_source_cd sourcesystem_cd
, :upload_id upload_id
from
  (select pat_day.bene_id
  , pat_day.clm_from_dt
  , min(medpar.encounter_num) medpar_encounter_num
  , max(nch_wkly_proc_dt) update_date
  from bc_chunk pat_day
  left join medpar_chunk medpar
  on medpar.bene_id       = pat_day.bene_id
    and medpar.admsn_dt  <= pat_day.clm_from_dt
    and medpar.dschrg_dt >= pat_day.clm_from_dt
  group by pat_day.bene_id
  , pat_day.clm_from_dt
  ) pat_day

cross join cms_key_sources key_sources
cross join i2b2_status ;

commit;


create or replace view cms_patient_day_mapping
as
  select emap.encounter_ide as patient_day
  , emap.encounter_num
  from "&&I2B2STAR".encounter_mapping emap
  join cms_key_sources key_sources
  on key_sources.patient_day_cd = emap.encounter_ide_source ;


-- Test for completeness.

select
  case
    when medpar   = 1
      and pat_day = 1 then 1
    else 0
  end complete
from
  (select
    (select 1 from cms_medpar_mapping where rownum = 1
    ) medpar
  ,(select 1 from cms_patient_day_mapping where rownum = 1
    ) pat_day
  from dual
  ) ;
