/** bene_chunks_survey - survey bene_id values and group into chunks
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
select 'MBSF_AB_SUMMARY'
    , :chunk_qty
    , chunk_num
    , count(*) chunk_size
    , min(bene_id) bene_id_first
    , max(bene_id) bene_id_last
    from (
    select bene_id, ntile(:chunk_qty) over (order by bene_id) as chunk_num
    from (select distinct bene_id
          from "&&CMS_RIF".mbsf_ab_summary
          /* Eliminate null case so that index can be used. */
          where bene_id is not null)
    ) group by chunk_num
order by chunk_num
    ;

update bene_chunks
set bene_id_first = null
where chunk_num   = 1;
update bene_chunks
set bene_id_last = null
where chunk_num  =
  (select max(chunk_num) from bene_chunks
  ) ;

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
    where bene_id_source = 'MBSF_AB_SUMMARY'
        and chunk_qty = &&chunk_qty) = &&chunk_qty
    then 1
    else 0
    end complete
from dual;
