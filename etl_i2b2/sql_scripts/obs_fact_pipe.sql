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

select bene_id from bene_id_mapping where 'dep' = 'cms_patient_mapping.sql'
/


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


type medpar_t
is
  record
  (
    medpar_id cms_deid.medpar_all.medpar_id%type,
    bene_id cms_deid.medpar_all.bene_id%type,
    ltst_clm_acrtn_dt cms_deid.medpar_all.ltst_clm_acrtn_dt%type) ;

type medpar_cur_t
is
  ref
  cursor
    return medpar_t;

  end cms_fact_pipeline;
/


create or replace type bene_id_mapper
is
  object
  (
    upload_id          int,
    patient_ide_source varchar2(50 byte), -- grousedata.patient_mapping.patient_ide_source%type
    constructor
  function bene_id_mapper(
      self in out nocopy bene_id_mapper,
      upload_id int)
    return self
  as
    result,
    member function lookup(
      bene_id varchar2)
    return integer) ;
/
create or replace type body bene_id_mapper
is
constructor
function bene_id_mapper(
    self in out nocopy bene_id_mapper,
    upload_id int)
  return self
as
  result
is
begin
  self.upload_id := upload_id;
  select bene_cd
  into self.patient_ide_source
  from cms_key_sources;
  return;
end;

  member function lookup(
    bene_id varchar2)
  return integer
is
  patient_num int;
begin
  select patient_num
  into patient_num
  from "&&I2B2STAR".patient_mapping pat_map
  where patient_ide_source = self.patient_ide_source
    and patient_ide        = bene_id;
  return patient_num;
end;
end;
/


/** medpar_mapper - i2b2 encounter mappings from CMS medpar_id

This encapsulates access to encounter_mapping, sq_up_encdim_encounternum
in the i2b2 star schema.

*/
create or replace type medpar_mapper
as
  object
  (
    upload_id            int,
    sourcesystem_cd      varchar2(64),
    encounter_ide_source varchar2(64),
    patient_ide_source   varchar2(64),
    constructor
  function medpar_mapper(
      self in out nocopy medpar_mapper,
      upload_id int)
    return self
  as
    result,
    member function enc_key_alloc(
      dummy int := null)
    return int,
    
/* Find encounter_num for patient day.

If obs_date falls within a MEDPAR for the_bene_id, use the (positive) encounter_num for that MEDPAR.
Otherwise, use a (negative) hash of the bene_id and date.
ISSUE: collision risk.
*/
    member function pat_day_rollup(
      the_medpar_id varchar2,
      the_bene_id   varchar2,
      obs_date      date)
    return integer) ;
/
create or replace type body medpar_mapper
is

  constructor
function medpar_mapper(
    self in out nocopy medpar_mapper,
    upload_id int)
  return self
as
  result
is
begin
  self.sourcesystem_cd := &&cms_source_cd;
  self.upload_id := upload_id;
  select medpar_cd
  into self.encounter_ide_source
  from cms_key_sources;
  select bene_cd
  into self.patient_ide_source
  from cms_key_sources;
  return;
end;

member function enc_key_alloc(
    dummy int := null)
  return int
is
begin
  return "&&I2B2STAR".sq_up_encdim_encounternum.nextval;
end;

member function pat_day_rollup(
    the_medpar_id varchar2,
    the_bene_id   varchar2,
    obs_date      date)
  return integer
is
  the_encounter_num integer;
begin

with the_medpar as
  (select coalesce(the_medpar_id,(select min(medpar_id) medpar_id
    from "&&CMS_RIF".medpar_all medpar
    where medpar.bene_id = the_bene_id
      and obs_date between medpar.admsn_dt and medpar.dschrg_dt
    )) medpar_id
  from dual
  )
, the_emap as
  (select min(emap.encounter_num) encounter_num
  from cms_medpar_mapping emap
  join the_medpar
  on the_medpar.medpar_id = emap.medpar_id
  )
select coalesce(the_emap.encounter_num, - abs(ora_hash(fmt_patient_day(the_bene_id, obs_date))))
into the_encounter_num
from the_emap;
return the_encounter_num;
end;
end;
/

/** medpar_upload_progress - insert mappings in chunks and report progress

ISSUE: insert into encounter_mapping should be done in a method on medpar_mapper, but
       I'm running into the issue of referring to a type from a package in a method decl:
       Error(6,15): PLS-00201: identifier 'CMS_FACT_PIPELINE.MEDPAR_T' must be declared
ISSUE: ambient: insert into upload_progress (logging ocap exception?)
*/
create or replace function medpar_upload_progress(
    mm medpar_mapper,
    download_date date,
    project_id           varchar2,
    source_info varchar2,
    medpar_data cms_fact_pipeline.medpar_cur_t,
    clock clock_access,
    detail        varchar2 := '',
    chunk_size    int      := 2000)
  return progress_event_set pipelined
is
  pragma autonomous_transaction;
  dest_table varchar2(128) := '&&I2B2STAR' || '.ENCOUNTER_MAPPING';
  out_event progress_event := progress_event(clock.fine_time(), mm.upload_id, source_info, dest_table, null, null,
  detail) ;
  import_date date := clock.fine_time() ;

type enc_chunk_t
is
  table of medpar_data%rowtype index by binary_integer;
  enc_chunk enc_chunk_t ;
  active varchar2(8);

begin
  select active into active from i2b2_status;
  loop
    exit
  when medpar_data%notfound;
    out_event.start_time := clock.fine_time() ;

    fetch medpar_data bulk collect
    into enc_chunk limit chunk_size;

    forall i in 1..enc_chunk.count
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
      values
      (
        enc_chunk(i) .medpar_id
      , mm.encounter_ide_source
      , project_id
      , mm.enc_key_alloc()
      , enc_chunk(i) .bene_id
      , mm.patient_ide_source
      , active
      , import_date
      , enc_chunk(i) .ltst_clm_acrtn_dt
      , download_date
      , import_date
      , mm.sourcesystem_cd
      , mm.upload_id
      ) ;
    out_event.row_count := enc_chunk.count;
    out_event.dur       := clock.fine_time() - out_event.start_time;

    insert
    into upload_progress values
      (
        out_event.start_time
      , out_event.upload_id
      , out_event.source_info
      , out_event.dest_table
      , out_event.row_count
      , out_event.dur
      , out_event.detail
      ) ;
    commit;

    pipe row(out_event) ;
  end loop;
end;
/

/*testing...
with io as (
 select clock_access('medpar_encounter_map clock') clock,
        medpar_mapper(upload_id => :upload_id) mm
 from dual
)
select * from io, table(
medpar_upload_progress(
    io.mm,
    download_date => sysdate,
    project_id  => 'GROUSE',
    source_info => 'MEDPAR_ALL',
    medpar_data => cursor(
      select /*+ index(medpar) *@@/ medpar_id
                   , bene_id
                   , ltst_clm_acrtn_dt
             from "&&CMS_RIF".medpar_all
             where rownum < 10000
      ),
    clock => io.clock,
    chunk_size => 5000)
);
*/

/** obs_fact_map provides encounter, patient nums for CMS facts.
*/
create or replace function obs_fact_map(
    mm medpar_mapper,
    bm bene_id_mapper,
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

    out_obs.encounter_num   := mm.pat_day_rollup(f.medpar_id, f.bene_id, f.start_date) ;
    out_obs.patient_num     := bm.lookup(f.bene_id);
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

drop type progress_event force
/
create type progress_event
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
/


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

ISSUE: reify access to observation_fact_NNN a la medpar_mapper?
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
    insert /*+ append */ into ' || dest_table || '
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


/* Integration testing:

truncate table observation_fact_100;

with max_mapped as
  (select *
  from table(obs_fact_map(
    cms_obs_cur => cursor(select * from cms_maxdata_ps_facts
                          -- where rownum < 20000
    )))
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
    cms_obs_cur => cursor(select * from cms_mbsf_facts
                          -- where rownum < 20000
                          )))
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
from upload_progress p
order by p.start_time desc;
*/


create or replace view obs_fact_pipe_design as select &&design_digest design_digest from dual
/

select 1 complete
from obs_fact_pipe_design
where design_digest = &&design_digest
/
