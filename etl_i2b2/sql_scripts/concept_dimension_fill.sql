/** concept_dimension_fill - fill concept dimension from metadata table

Refs:

 - i2b2 Ontology design
 - i2b2 CRC design

*/

delete from "&&I2B2STAR".concept_dimension cd
where cd.concept_path in (
  select c_dimcode
  from "&&I2B2META"."&&ONT_TABLE_NAME" ib
  where ib.c_basecode is not null and lower(ib.c_tablename) = 'concept_dimension'
)
;

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
 * Check a dozen of the most recently imported concepts.
 */
with cmeta as (
  select *
  from "&&I2B2META"."&&ONT_TABLE_NAME" meta
  where lower(meta.c_tablename) = 'concept_dimension'
  order by import_date desc
)
, few as (
  select c_basecode, c_name, c_dimcode, import_date
  from cmeta
  where rownum <= 12
)
select count(*) / 12 complete
from "&&I2B2STAR".concept_dimension cd
join few on few.c_dimcode = cd.concept_path
        and few.import_date <= cd.import_date
;
