

insert /*+ append */
into
  "&&I2B2STAR".observation_fact
  (
    encounter_num
  , patient_num
  , concept_cd
  , provider_id
  , start_date
  , modifier_cd
  , instance_num
  , valtype_cd
  , tval_char
  , nval_num
  , valueflag_cd
  , quantity_num
  , units_cd
  , end_date
  , location_cd
  -- , observation_blob
  , confidence_num
  , update_date
  , download_date
  , import_date
  , sourcesystem_cd
  , upload_id
  )
  select
  enc_map.encounter_num
  , pat_map.patient_num
  , f.concept_cd
  , f.provider_id
  , f.start_date
  , f.modifier_cd
  , f.instance_num
  , f.valtype_cd
  , f.tval_char
  , f.nval_num
  , f.valueflag_cd
  , f.quantity_num
  , f.units_cd
  , f.end_date
  , f.location_cd
  , f.confidence_num
  , f.update_date
  , :download_date
  , sysdate import_date
  , f.sourcesystem_cd
  , :upload_id
  from &&fact_view f
  join "&&I2B2STAR".encounter_mapping enc_map
    on f.encounter_ide = enc_map.encounter_ide
    and f.encounter_ide_source = enc_map.encounter_ide_source
  join bene_id_mapping pat_map on pat_map.bene_id = f.bene_id
  where f.bene_id between :bene_id_lo and :bene_id_hi
;


/* ISSUE: this assumes at most one OK record per transform name.
          Add a constraint to say as much?
*/
select
  count( *) loaded_record
from
  "&&I2B2STAR".observation_fact f
where
  f.upload_id =
  (select
    max(upload_id) -- cheating?
  from
    "&&I2B2STAR".upload_status
  where
    transform_name = '&&fact_view'
  ) ;
