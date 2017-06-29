/* obs_fact_pipe - Pipelined bulk insert of observation facts.

ref 13 Using Pipelined and Parallel Table Functions
from Database Data Cartridge Developer's Guide
https://docs.oracle.com/cd/B28359_01/appdev.111/b28425/pipe_paral_tbl.htm

ISSUE: is  authid current_user needed?
ref http://stackoverflow.com/questions/996198/execute-immediate-within-a-stored-procedure-keeps-giving-insufficient-priviliges

handy:

with cases as
  ( select rownum x from dual connect by rownum <= 100
  )
, test_data as
  (select round((cases.x + 1) / 3) pat
  , decode(mod(cases.x, 3), 0, 'age', 1, 'height', 2, 'diagnosis') attr
  , cases.x val
  from cases
  )
select * from test_data;

*/

select bene_id from bene_id_mapping where 'dep' = 'cms_patient_mapping.sql';


/** Package cms_fact_pipeline provides ref cursor types etc.

A ref cursor type declaration inside a package is the documented
(and only known) way to pass `select ...` data to a function.
*/
create or replace package cms_fact_pipeline
is

/** obs_fact_t is (the useful parts of) the i2b2 observation_fact table record type.
*/
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
    location_cd grousedata.observation_fact.location_cd%type,
    -- , observation_blob
    confidence_num grousedata.observation_fact.confidence_num%type,
    update_date grousedata.observation_fact.update_date%type,
    download_date grousedata.observation_fact.download_date%type,
    import_date grousedata.observation_fact.import_date%type,
    sourcesystem_cd grousedata.observation_fact.sourcesystem_cd%type,
    upload_id grousedata.observation_fact.upload_id%type) ;

  /** obs_cur_t is a i2b2 observation fact ref cursor type.
  */
type obs_fact_cur_t
is
  ref
  cursor
    return obs_fact_t ;

/** obs_fact_set is a collection of facts,
as returned by pipelined functions
*/
type obs_fact_set
is
  table of obs_fact_t;


/** cms_fact_t is like an i2b2 observation fact,
but the encounter_num and patient_num are not known;
only the bene_id, start_date, and (optionally) the medpar_id.

Use obs_fact_map (below) to convert to obs_fact_t.
*/
  type cms_fact_t
is
  record
  (
    bene_id   varchar2(15 byte), -- cms_deid.mbsf_ab_summary.bene_id%type,
    medpar_id varchar2(15 byte), -- cms_deid.medpar_all.medpar_id%type,
    -- note novel ordering
    concept_cd grousedata.observation_fact.concept_cd%type,
    start_date grousedata.observation_fact.start_date%type,
    modifier_cd grousedata.observation_fact.modifier_cd%type,
    instance_num grousedata.observation_fact.instance_num%type,
    valtype_cd grousedata.observation_fact.valtype_cd%type,
    tval_char grousedata.observation_fact.tval_char%type,
    nval_num grousedata.observation_fact.nval_num%type,
    end_date grousedata.observation_fact.end_date%type,
    -- , observation_blob
    -- ISSUE: supertype for these general audit columns?
    update_date grousedata.observation_fact.update_date%type,
    -- download_date grousedata.observation_fact.download_date%type,
    -- import_date grousedata.observation_fact.import_date%type,
    sourcesystem_cd grousedata.observation_fact.sourcesystem_cd%type,
    provider_id grousedata.observation_fact.provider_id%type,
    valueflag_cd grousedata.observation_fact.valueflag_cd%type,
    quantity_num grousedata.observation_fact.quantity_num%type,
    units_cd grousedata.observation_fact.units_cd%type,
    location_cd grousedata.observation_fact.location_cd%type,
    confidence_num grousedata.observation_fact.confidence_num%type
    -- upload_id grousedata.observation_fact.upload_id%type
  ) ;

-- See obs_cur_t above.
type cms_fact_cur_t
is
  ref
  cursor
    return cms_fact_t ;


  end cms_fact_pipeline;
/


/** obs_fact_map provides encounter, patient nums for CMS facts.

REQUIRES: bene_id_mapping
See also pat_day_medpar_rollup.
*/
create or replace function obs_fact_map(
    cms_obs_cur cms_fact_pipeline.cms_fact_cur_t,
    download_date date,
    upload_id     int)
  return cms_fact_pipeline.obs_fact_set pipelined
is
  f cms_obs_cur%rowtype;
  out_obs cms_fact_pipeline.obs_fact_t;
begin
  loop
    exit
  when cms_obs_cur%notfound;
    fetch cms_obs_cur into f;

    out_obs.encounter_num := pat_day_medpar_rollup(f.medpar_id, f.bene_id, f.start_date) ;
    select patient_num
    into out_obs.patient_num
    from bene_id_mapping pat_map
    where pat_map.bene_id    = f.bene_id;
    out_obs.concept_cd      := f.concept_cd;
    out_obs.provider_id     := f.provider_id;
    out_obs.start_date      := f.start_date;
    out_obs.modifier_cd     := f.modifier_cd;
    out_obs.instance_num    := f.instance_num;
    out_obs.valtype_cd      := f.valtype_cd;
    out_obs.tval_char       := f.tval_char;
    out_obs.nval_num        := f.nval_num;
    out_obs.valueflag_cd    := f.valueflag_cd;
    -- out_obs.quantity_num    := f.quantity_num;
    out_obs.units_cd        := f.units_cd;
    out_obs.end_date        := f.end_date;
    -- out_obs.location_cd     := f.location_cd;
    -- out_obs.confidence_num  := f.confidence_num;
    out_obs.update_date     := f.update_date;
    out_obs.download_date   := download_date;
    out_obs.sourcesystem_cd := f.sourcesystem_cd;
    out_obs.upload_id       := upload_id;

    pipe row(out_obs) ;
  end loop;
close cms_obs_cur;
end obs_fact_map;
/

/* eyeball it for testing:

select *
from table(obs_fact_map(
             cms_obs_cur => cursor(select * from cms_maxdata_ps_facts where rownum < 2000),
             download_date => sysdate,
             upload_id  => 1)) ;
*/



/** A progress_event indicates progress in a long-running operation.
*/
create or replace type progress_event
is
  object
  (
    -- todo: start time
    row_count   int,
    description varchar2(128)) ;
/
/** A progress_event_set facilitates pipelining.
*/
create or replace type progress_event_set
as
  table of progress_event;
/


/** obs_load_progress loads observation facts and reports progress

ISSUE: observation_fact_898 is hard-coded.
*/
create or replace function obs_load_progress(
    obs_data cms_fact_pipeline.obs_fact_cur_t,
    chunk_size    int := 2000)
  return progress_event_set pipelined
is
  pragma autonomous_transaction;
  out_event progress_event := progress_event(0, 'several records@@') ;
type obs_chunk_t
is
  table of obs_data%rowtype index by binary_integer;
  obs_chunk obs_chunk_t ;
begin
  loop
    exit
  when obs_data%notfound;
    fetch obs_data bulk collect
    into obs_chunk limit chunk_size;
    forall i in 1..obs_chunk.count
    insert
    into observation_fact_898
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
      values
      (
        obs_chunk(i) .encounter_num
      , obs_chunk(i) .patient_num
      , obs_chunk(i) .concept_cd
      , obs_chunk(i) .provider_id
      , obs_chunk(i) .start_date
      , obs_chunk(i) .modifier_cd
      , obs_chunk(i) .instance_num
      , obs_chunk(i) .valtype_cd
      , obs_chunk(i) .tval_char
      , obs_chunk(i) .nval_num
      , obs_chunk(i) .valueflag_cd
      , obs_chunk(i) .quantity_num
      , obs_chunk(i) .units_cd
      , obs_chunk(i) .end_date
      , obs_chunk(i) .location_cd
      , obs_chunk(i) .confidence_num
      , obs_chunk(i) .update_date
      , obs_chunk(i) .download_date
      , sysdate -- import_date
      , obs_chunk(i) .sourcesystem_cd
      , obs_chunk(i) .upload_id
      ) ;
    commit;
    out_event.row_count := obs_chunk.count;
    pipe row(out_event) ;
  end loop;
end;
/


truncate table observation_fact_898;

with max_mapped as
  (select *
  from table(obs_fact_map( cms_obs_cur => cursor
    (select * from cms_maxdata_ps_facts where rownum < 2000
    ), download_date => sysdate, upload_id => 1))
  )
select *
from table(obs_progress( obs_data => cursor
  (select * from max_mapped
  ), chunk_size => 50)) ;

select count(*) from observation_fact_898;

select * from observation_fact_898;

