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
    from "&&CMS_RIF".&&bene_id_source
    ) group by chunk_num
order by bene_id_first
    ;


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
