/** bene_chunks_survey - survey bene_id values and group into chunks
*/

select chunk_num from bene_chunks where 'dep' = 'bene_chunks_create.sql';

delete from bene_chunks where bene_id_source = '&&source_table';

insert into bene_chunks (
        bene_id_source
        , chunk_qty
        , chunk_num
        , chunk_size
        , bene_id_first
        , bene_id_last
)
select '&&source_table'
    , :chunk_qty
    , chunk_num
    , count(*) chunk_size
    , min(bene_id) bene_id_first
    , max(bene_id) bene_id_last
    from (
    select bene_id, ntile(:chunk_qty) over (order by bene_id) as chunk_num
    from (select /*+ parallel(16) */ distinct bene_id
          from "&&CMS_RIF"."&&source_table"
          /* Eliminate null case so that index can be used. */
          where bene_id is not null)
    ) group by chunk_num
order by chunk_num
    ;

select case
    when (select count(distinct chunk_num)
    from bene_chunks
    where bene_id_source = '&&source_table'
        and chunk_qty = &&chunk_qty) = &&chunk_qty
    then 1
    else 0
    end complete
from dual;
