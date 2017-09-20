/** concept_dimension_fill - fill concept dimension from metadata table

Refs:

 - i2b2 Ontology design
 - i2b2 CRC design

*/

insert into "&&I2B2STAR".concept_dimension(
  concept_path,
  concept_cd,
  name_char,
  update_date,
  download_date,
  import_date,
  sourcesystem_cd,
  upload_id
  )
select
  ib.c_dimcode,
  max(ib.c_basecode),
  max(ib.c_name),
  max(update_date),
  min(download_date),
  sysdate,
  max(sourcesystem_cd),
  :upload_id
from "&&I2B2META"."&&ONT_TABLE_NAME" ib
where ib.c_basecode is not null and lower(ib.c_tablename) = 'concept_dimension'
group by ib.c_dimcode, sysdate, :upload_id
;


/***
 * Have we already filled the concept_dimension from this metadata table?
 *
 * Check a few of the concepts with the greatest c_hlevel.
 */
with cmeta as (
  select *
  from "&&I2B2META"."&&ONT_TABLE_NAME" meta
  where lower(meta.c_tablename) = 'concept_dimension'
)
, few as (
  select c_basecode, c_name, c_dimcode
  from cmeta
  where cmeta.c_hlevel = (select max(c_hlevel) from cmeta)
  and rownum < 5
)
select count(*) complete
from "&&I2B2STAR".concept_dimension cd
join few on few.c_dimcode = cd.concept_path
;
