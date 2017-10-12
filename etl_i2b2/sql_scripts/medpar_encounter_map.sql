/** medpar_encounter_map - map CMS medpar events to i2b2 encounters

Seel also cms_encounter_mapping for patient-day rollup.

Reasonable performance probably depends on indexes such as:
create index medpar_bene on "&&CMS_RIF".medpar_all (bene_id);
*/

select medpar_cd from cms_key_sources where 'dep' = 'cms_keys.pls';
select design_digest from obs_fact_pipe_design where 'dep' = 'obs_fact_pipe.sql';

-- for debugging: truncate table "&&I2B2STAR".encounter_mapping;
-- ISSUE: push index disable/enable into medpar_mapper method?
alter table "&&I2B2STAR".encounter_mapping disable constraint encounter_mapping_pk;
whenever sqlerror continue;
drop index "&&I2B2STAR".encounter_mapping_pk;
whenever sqlerror exit;
alter index "&&I2B2STAR".em_encnum_idx unusable;
alter index "&&I2B2STAR".em_idx_encpath unusable;
alter index "&&I2B2STAR".em_uploadid_idx unusable;

/* ISSUE: MedparMapping task id doesn't depend on starting sequence number,
    so MigratePendingUploads can get messed up.

create synonym encounter_mapping for grousedata.encounter_mapping;
*/

with io as (
 select clock_access('medpar_encounter_map clock') clock,
        medpar_mapper(upload_id => :upload_id) mm
 from dual
)
select progress.* from io, table(
medpar_upload_progress(
    io.mm,
    download_date => :download_date,
    project_id  => :project_id,
    source_info => '&&CMS_RIF' || '.MEDPAR_ALL',
    medpar_data => cursor(
      select medpar_id, bene_id, ltst_clm_acrtn_dt
      from "&&CMS_RIF".medpar_all
      /* where rownum < 100000 */
      ),
    clock => io.clock,
    chunk_size => 50000)
) progress;


alter index "&&I2B2STAR".em_idx_encpath rebuild;
alter index "&&I2B2STAR".em_encnum_idx rebuild;
alter index "&&I2B2STAR".em_uploadid_idx rebuild;

create or replace view cms_medpar_mapping
as
  select /*+ index(emap)*/ emap.encounter_ide as medpar_id
  , emap.encounter_num
  from "&&I2B2STAR".encounter_mapping emap
  join cms_key_sources key_sources
  on key_sources.medpar_cd = emap.encounter_ide_source ;


/* Test for completeness: any records with a relevant download_date?

ISSUE: Jenkins/Docker environment seems to run in UTC, throwing off download_date.
*/
select 1 task_upload_found
from dual where exists (
  select 1 from "&&I2B2STAR".encounter_mapping emap
  where emap.download_date >= :download_date
  and rownum = 1
)
;
