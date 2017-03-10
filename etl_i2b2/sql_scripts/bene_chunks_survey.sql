/** bene_chunks_survey - survey bene_id values and group into chunks

ISSUE: in some medicaid records, bene_id is null and we need to use msis_id, state_cd.
       cf deid/bene_id_mapping.sql MAXDATA_PS, MAXDATA_IP, MAXDATA_LT, MAXDATA_OT,  MAXDATA_RX
*/

select chunk_num from bene_chunks where 'dep' = 'bene_chunks_create.sql';

insert into bene_chunks (
        bene_id_source
        , chunk_qty
        , chunk_num
        , chunk_size
        , bene_id_first
        , bene_id_last
)
select :bene_id_source
    , :chunk_qty
    , chunk_num
    , count(*) chunk_size
    , min(bene_id) bene_id_first
    , max(bene_id) bene_id_last
    from (
    select bene_id, ntile(:chunk_qty) over (order by bene_id) as chunk_num
    from (select distinct bene_id from "&&CMS_RIF".&&bene_id_source)
    ) group by chunk_num
order by chunk_num
    ;

/** bene_id_check_outside_mbsf

This query lets us verify that few, if any, bene_ids are not covered by MBSF_AB_SUMMARY.
As of this writing, All bene_ids from tables other than MAXDATA_OT, MAXDATA_PS, MAXDATA_RX, MAXDATA_IP
were subsumed by MBSF_AB_SUMMARY and these tables had just 1 chunk (2K-23K bene_ids)
that was not subsumed.
*/
create or replace view bene_id_check_outside_mbsf as
  with mbsf as (
    select min(bene_id_first) id_first
    ,      max(bene_id_last) id_last
    from bene_chunks
    where bene_id_source = 'MBSF_AB_SUMMARY'
  ) select mbsf.*
  ,      bc.*
  from bene_chunks bc
  ,    mbsf
  where bc.bene_id_source != 'MBSF_AB_SUMMARY'
  and
    bc.bene_id_first < mbsf.id_first
  or
    bc.bene_id_last > mbsf.id_last;


select case
    when (select count(distinct chunk_num)
    from bene_chunks
    -- ISSUE: bind variables aren't available from complete()
    where bene_id_source = '&&bene_id_source'
        and chunk_qty = &&chunk_qty) = &&chunk_qty
    then 1
    else 0
    end complete
from dual;
