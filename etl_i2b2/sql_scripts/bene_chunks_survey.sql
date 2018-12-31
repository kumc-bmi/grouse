/** bene_chunks_survey - survey bene_id values and group into chunks
*/

select chunk_num from bene_chunks where 'dep' = 'bene_chunks_create.sql';

insert into bene_chunks (
          chunk_qty
        , chunk_num
        , bene_id_qty
        , bene_id_first
        , bene_id_last
)
select :chunk_qty
    , chunk_num
    , count(distinct bene_id) bene_id_qty
    , min(bene_id) bene_id_first
    , max(bene_id) bene_id_last
    from (
    select bene_id
         , ntile(:chunk_qty) over (order by bene_id) as chunk_num
    from (select distinct bene_id
          from (
            /* Eliminate null case so that index can be used. */
            select /*+ parallel(12) */ bene_id from "&&CMS_RIF".mbsf_abcd_summary where bene_id is not null
	    union all
            select /*+ parallel(12) */ bene_id from "&&CMS_RIF".maxdata_ps where bene_id is not null
          ))
    ) group by chunk_num, :chunk_qty
order by chunk_num
    ;

-- TODO: factor in CMSExtract download date and/or schema name into completion test.
select case
    when (select count(distinct chunk_num)
    from bene_chunks
    where chunk_qty = &&chunk_qty) = &&chunk_qty
    then 1
    else 0
    end complete
from dual;
