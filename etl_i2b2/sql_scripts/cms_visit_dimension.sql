select domain from cms_ccw where 'dep' = 'cms_ccw_spec.sql';

truncate table "&&I2B2STAR".visit_dimension;

insert into NightHeronData.visit_dimension (
          encounter_num
        , active_status_cd
        , patient_num  
        , start_date
        , end_date
        , import_date
        , upload_id
        , download_date
        , sourcesystem_cd
        )
select 
      en_map.encounter_num
    , cms_vd.encounter_ide_status
    , pat_map.patient_num
    , cms_vd.start_date
    , cms_vd.end_date
    , sysdate as import_date
    , :upload_id  -- ISSUE: express dependency?
    , cms_vd.download_date
    , cms_vd.sourcesystem_cd
from cms_visit_dimension cms_vd
    join (
    select encounter_ide clm_id
        , patient_ide bene_id
        , encounter_num
    from "&&I2B2STAR".encounter_mapping en_map  -- ISSUE: express dependency?
        join cms_ccw
        on encounter_ide_source = cms_ccw.domain
        and patient_ide_source = cms_ccw.domain
        join i2b2_status
        on encounter_ide_status = i2b2_status.active
        ) en_map
    join (
    select patient_ide bene_id
        , patient_num
    from "&&I2B2STAR".patient_mapping pat_map  -- ISSUE: express dependency?
        join cms_ccw
        on patient_ide_source = cms_ccw.domain
        join i2b2_status
        on patient_ide_status = i2b2_status.active
        ) pat_map
    on cms_vd.bene_id = pat_map.bene_id
    ;
