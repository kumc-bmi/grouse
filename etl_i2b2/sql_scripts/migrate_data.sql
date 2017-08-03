/** obs_fact_append -- append data from a workspace table.
*/

insert /*+ parallel(&&parallel_degree) append */ into &&dest_table
select * from &&source_table ;
commit ;


select /*+ parallel(&&parallel_degree) */ 1 complete
from &&dest_table
where &&test_column = (
    select &&test_column v1 from &&source_table
    where rownum = 1
)
and rownum = 1 ;
