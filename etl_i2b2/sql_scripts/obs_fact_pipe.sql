/*
ref 13 Using Pipelined and Parallel Table Functions
from Database Data Cartridge Developer's Guide
https://docs.oracle.com/cd/B28359_01/appdev.111/b28425/pipe_paral_tbl.htm
*/

create table obs_fun (
pat integer,
attr varchar2(64),
val integer);

create or replace package cms_fact_pipeline
is

type obs_cur_t
is
  ref
  cursor
    -- @@ return dconnolly.observation_fact%rowtype
    return obs_fun%rowtype
    ;
  
type cms_fact_t
is
  record
  (
    medpar_id VARCHAR2(15 BYTE), -- cms_deid.medpar_all.medpar_id%type,
    bene_id VARCHAR2(15 BYTE), -- cms_deid.mbsf_ab_summary.bene_id%type,
    concept_cd grousedata.observation_fact.concept_cd%type,
    provider_id grousedata.observation_fact.provider_id%type,
    start_date grousedata.observation_fact.start_date%type,
    modifier_cd grousedata.observation_fact.modifier_cd%type,
    instance_num grousedata.observation_fact.instance_num%type,
    valtype_cd grousedata.observation_fact.valtype_cd%type,
    tval_char grousedata.observation_fact.tval_char%type,
    nval_num grousedata.observation_fact.nval_num%type,
    valueflag_cd grousedata.observation_fact.valueflag_cd%type,
    quantity_num grousedata.observation_fact.quantity_num%type,
    units_cd grousedata.observation_fact.units_cd%type,
    end_date grousedata.observation_fact.end_date%type,
    location_cd grousedata.observation_fact.location_cd%type
    -- , observation_blob
    ,
    confidence_num grousedata.observation_fact.confidence_num%type
    -- ISSUE: supertype for these general audit columns?
    ,
    update_date grousedata.observation_fact.update_date%type,
    -- download_date grousedata.observation_fact.download_date%type,
    import_date grousedata.observation_fact.import_date%type,
    sourcesystem_cd grousedata.observation_fact.sourcesystem_cd%type
    -- upload_id grousedata.observation_fact.upload_id%type
    ) ;

type obs_fact_t
is
  record
  (
    encounter_num grousedata.observation_fact.encounter_num%type,
    patient_num grousedata.observation_fact.patient_num%type,
    concept_cd grousedata.observation_fact.concept_cd%type,
    provider_id grousedata.observation_fact.provider_id%type,
    start_date grousedata.observation_fact.start_date%type,
    modifier_cd grousedata.observation_fact.modifier_cd%type,
    instance_num grousedata.observation_fact.instance_num%type,
    valtype_cd grousedata.observation_fact.valtype_cd%type,
    tval_char grousedata.observation_fact.tval_char%type,
    nval_num grousedata.observation_fact.nval_num%type,
    valueflag_cd grousedata.observation_fact.valueflag_cd%type,
    quantity_num grousedata.observation_fact.quantity_num%type,
    units_cd grousedata.observation_fact.units_cd%type,
    end_date grousedata.observation_fact.end_date%type,
    location_cd grousedata.observation_fact.location_cd%type
    -- , observation_blob
    ,
    confidence_num grousedata.observation_fact.confidence_num%type
    -- ISSUE: supertype for these general audit columns?
    ,
    update_date grousedata.observation_fact.update_date%type,
    download_date grousedata.observation_fact.download_date%type,
    import_date grousedata.observation_fact.import_date%type,
    sourcesystem_cd grousedata.observation_fact.sourcesystem_cd%type,
    upload_id grousedata.observation_fact.upload_id%type) ;

end cms_fact_pipeline;
  

create or replace type progress_event is object (
 -- todo: start time
  row_count int,
  description varchar2(128)
);
create TYPE Progress_Event_Set AS TABLE OF progress_event;

create or replace function obs_progress(
    obs_data cms_fact_pipeline.obs_cur_t,
    chunk_size int := 10000)
  return progress_event_set pipelined
is
  pragma autonomous_transaction;
type obs_chunk_t
is
  table of obs_data%rowtype index by binary_integer;
  in_obs_chunk obs_chunk_t ;
  out_event progress_event := progress_event(0, 'several records') ;
begin
  loop
    exit
  when obs_data%notfound;
    fetch obs_data bulk collect
    into in_obs_chunk limit chunk_size;
    forall i in 1..in_obs_chunk.count
    insert
    into obs_fun
      (
        pat
      , attr
      , val
      )
      values
      (
        in_obs_chunk(i) .pat
      , in_obs_chunk(i) .attr
      , in_obs_chunk(i) .val
      ) ;
    commit;
    out_event.row_count := in_obs_chunk.count;
    pipe row(out_event) ;
  end loop;
end;
/

with cases as
  ( select rownum x from dual connect by rownum <= 100
  )
, test_data as
  (select round((cases.x + 1) / 3) pat
  , decode(mod(cases.x, 3), 0, 'age', 1, 'height', 2, 'diagnosis') attr
  , cases.x val
  from cases
  )
select *
from table(obs_progress(obs_data => cursor
  (select * from test_data
  ), chunk_size => 5)) ;


select * from obs_fun
;

truncate table obs_fun
;

/**
create or replace function obs_fact_map(cms_obs cms_fact_pipeline.cms_fact_t)
return cms_fact_pipeline.obs_fact_t
pipelined
is
  out_fact obs_fact_t := cms_fact_pipeline.obs_fact_t();
begin
  
begin
  select
    pat_day_medpar_rollup(f.medpar_id, f.bene_id, f.start_date) encounter_num
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
  join bene_id_mapping pat_map on pat_map.bene_id = f.bene_id
end;
*/




create type id_mapping_t
as
  object
  (
    encounter_ide grousedata.encounter_mapping.encounter_ide%type,
    encounter_num grousedata.encounter_mapping.encounter_num%type) ;
/
insert
into nightherondata.encounter_mapping
  (
    nightherondata.encounter_mapping.encounter_ide%type
  , nightherondata.encounter_mapping.encounter_ide_source
  , nightherondata.encounter_mapping.project_id
  , nightherondata.encounter_mapping.encounter_num
  , nightherondata.encounter_mapping.patient_ide
  , nightherondata.encounter_mapping.patient_ide_source
  , nightherondata.encounter_mapping.encounter_ide_status
  , nightherondata.encounter_mapping.upload_date
  , nightherondata.encounter_mapping.update_date
  , nightherondata.encounter_mapping.download_date
  , nightherondata.encounter_mapping.import_date
  , nightherondata.encounter_mapping.sourcesystem_cd
  , nightherondata.encounter_mapping.upload_id
  )
  values
  (
    :v0
  , :v1
  , :v2
  , :v3
  , :v4
  , :v5
  , :v6
  , :v7
  , :v8
  , :v9
  , :v10
  , :v11
  , :v12
  ) ;

create or replace type mapping_set_t
as
  table of nightherondata.encounter_mapping%rowtype;
/

type refcur_t
is
  ref
  cursor
    return dconnolly.medpar_all%rowtype;
/

create package oblig_cur_pkg
is
type refcur_t
is
  ref
  cursor
    return dconnolly.medpar_all%rowtype ;
  end oblig_cur_pkg;

grant select any sequence to dconnolly;


create or replace function medpar_map(
    p in oblig_cur_pkg.refcur_t)
  return mappint_set_t pipelined
  authid current_user -- hmm... http://stackoverflow.com/questions/996198/execute-immediate-within-a-stored-procedure-keeps-giving-insufficient-priviliges
is
  out_rec id_mapping_t := id_mapping_t(null, null) ;
  in_rec p%rowtype;
begin
  loop
    fetch p into in_rec;
    exit
  when p%notfound;
    out_rec.map_key := in_rec.medpar_id;
    out_rec.map_val := nightherondata.sq_up_encdim_encounternum.nextval;
    pipe row(out_rec) ;
  end loop;
close p;
return;
end;
/

select * from
table(medpar_map(cursor(select * from dconnolly.medpar_all where rownum < 10)))
;