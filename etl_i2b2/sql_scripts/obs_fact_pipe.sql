/* obs_fact_pipe - Pipelined bulk insert of observation facts.

ref 13 Using Pipelined and Parallel Table Functions
from Database Data Cartridge Developer's Guide
https://docs.oracle.com/cd/B28359_01/appdev.111/b28425/pipe_paral_tbl.htm

ISSUE: is  authid current_user needed?
ref http://stackoverflow.com/questions/996198/execute-immediate-within-a-stored-procedure-keeps-giving-insufficient-priviliges

ISSUE: TODO: PARALLEL_ENABLE and PARTITION BY
https://docs.oracle.com/cd/B28359_01/appdev.111/b28425/pipe_paral_tbl.htm#i1004978

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
    observation_blob grousedata.observation_fact.observation_blob%type,
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
    cms_obs_cur cms_fact_pipeline.cms_fact_cur_t)
  return cms_fact_pipeline.obs_fact_set pipelined
  parallel_enable(partition cms_obs_cur by any)
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
    -- out_obs.download_date   := download_date;
    out_obs.sourcesystem_cd := f.sourcesystem_cd;
    -- out_obs.upload_id       := upload_id;

    pipe row(out_obs) ;
  end loop;
close cms_obs_cur;
end obs_fact_map;
/

/* eyeball it for testing:

select *
from table(obs_fact_map(
             cms_obs_cur => cursor(select * from cms_maxdata_ps_facts where rownum < 2000))) ;
*/


/** A progress_event indicates progress in a long-running operation.

handy:
drop type progress_event force;

drop table upload_progress;
create table upload_progress (
    start_time  timestamp,
    upload_id   int,
    source_info varchar(128),
    dest_table  varchar(64),
    row_count   int,
    dur interval day to second,
    detail      varchar2(128));
*/

create or replace type progress_event
is
  object
  (
    start_time  timestamp,
    upload_id   int,
    source_info varchar(128),
    dest_table  varchar(64),
    row_count   int,
    dur interval day to second,
    detail varchar2(128),
    member function krows_per_min return float,
    member function toString return varchar2) ;
/
create or replace type body progress_event is
  member function krows_per_min return float is
    dur_sec   float := sysdate + (
      self.dur * 24 * 60 * 60
    ) - sysdate;
  begin
    if dur_sec = 0 then
      return null;
    end if;
    return round(
      (
        self.row_count / 1000
      ) / (
        dur_sec / 60
      )
    , 2
    );
  end;

  member function tostring return varchar2
    is
  begin
    return to_char(
      self.start_time
    , 'YYYY-MM-DD HH24:MI:SS')
     ||  ' '
     ||  self.upload_id
     ||  ': '
     ||  self.row_count
     ||  ' rows ->'
     ||  self.dest_table
     ||  ' in '
     ||  self.dur
     ||  ' = '
     ||  self.krows_per_min ()
     ||  ' krow/min';
  end;

end;


/*
select progress_event(
    clock_access('bulk load clock').fine_time()
  , 101
  , 'mbsf'
  , 'observation_fact_101'
  , 123456789
  , (systimestamp + interval '1' minute) - systimestamp
  , null
  ).tostring() event
from dual;
*/

/** A progress_event_set facilitates pipelining.
*/
create or replace type progress_event_set
as
  table of progress_event;
/


/** Reify access to the clock rather than using ambient authority.
*/
create or replace type clock_access as object (
  label varchar2(128),
  member function fine_time return timestamp
);
/
create or replace type body clock_access
is
  member function fine_time
  return timestamp
is
begin
  return current_timestamp;
end;
end;
/




/** obs_load_progress loads observation facts and reports progress
*/
create or replace function obs_load_progress(
    source_info varchar2,
    obs_data cms_fact_pipeline.obs_fact_cur_t,
    clock clock_access,
    download_date date, upload_id int,
    detail varchar2 := '',
    chunk_size    int := 2000)
  return progress_event_set pipelined
is
  pragma autonomous_transaction;
  dest_table varchar2(64) := 'observation_fact_' || upload_id;
  out_event progress_event := progress_event(clock.fine_time(), upload_id,
                                             source_info, dest_table,
                                             null, null, detail) ;
type obs_chunk_t
is
  table of obs_data%rowtype index by binary_integer;
  obs_chunk obs_chunk_t ;
  import_date date := clock.fine_time();
begin
  loop
    exit
  when obs_data%notfound;
    out_event.start_time := clock.fine_time();

    fetch obs_data bulk collect
    into obs_chunk limit chunk_size;

    for i in 1..obs_chunk.count loop
      obs_chunk(i).import_date := import_date;
    end loop;

    forall i in 1..obs_chunk.count
    -- Not supported:
    -- Record inserts and updates using the EXECUTE IMMEDIATE statement
    -- https://docs.oracle.com/database/121/LNPLS/composites.htm#GUID-EC8E43E9-8356-4256-857A-D8109F2CF324
    execute immediate '
    insert into ' || dest_table || '
      values
      (
        :encounter_num
      , :patient_num
      , :concept_cd
      , :provider_id
      , :start_date
      , :modifier_cd
      , :instance_num
      , :valtype_cd
      , :tval_char
      , :nval_num
      , :valueflag_cd
      , :quantity_num
      , :units_cd
      , :end_date
      , :location_cd
      , :observation_blob
      , :confidence_num
      , :update_date
      , :download_date
      , sysdate -- import_date
      , :sourcesystem_cd
      , :upload_id
      )   '
    using
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
      , obs_chunk(i) .observation_blob
      , obs_chunk(i) .confidence_num
      , obs_chunk(i) .update_date
      , download_date
      , obs_chunk(i) .sourcesystem_cd
      , upload_id;
    commit;
    out_event.row_count := obs_chunk.count;
    out_event.dur       := clock.fine_time() - out_event.start_time;

    insert into upload_progress values (
      out_event.start_time,
      out_event.upload_id,
      out_event.source_info,
      out_event.dest_table,
      out_event.row_count,
      out_event.dur,
      out_event.detail);
    commit;

    pipe row(out_event) ;
  end loop;
end;
/


truncate table observation_fact_100;

with max_mapped as
  (select *
  from table(obs_fact_map(
    cms_obs_cur => cursor(select * from cms_maxdata_ps_facts /*where rownum < 20000*/)))
  )
select *
from table(obs_load_progress(
  source_info => 'cms_maxdata_ps_facts',
  obs_data => cursor(select * from max_mapped),
  clock => clock_access('bulk load clock'),
  download_date => sysdate,
  upload_id => 100,
  chunk_size => 10000)) ;

select count(*) from observation_fact_100;

select * from observation_fact_100;

create table observation_fact_101 nologging compress as
select * from observation_fact where 1 = 0;
truncate table observation_fact_101;
truncate table upload_progress;

with mbsf_mapped as
  (select *
  from table(obs_fact_map(
    cms_obs_cur => cursor(select * from cms_mbsf_facts /*where rownum < 20000*/)))
  )
select *
from table(obs_load_progress(
  source_info => 'cms_mbsf_facts',
  obs_data => cursor(select * from mbsf_mapped),
  clock => clock_access('bulk load clock'),
  download_date => sysdate,
  upload_id => 101,
  chunk_size => 50000)) ;


select progress_event(
    p.start_time
  , p.upload_id
  , p.source_info
  , p.dest_table
  , p.row_count
  , p.dur
  , p.detail
  ).tostring() event
from upload_progress p;
