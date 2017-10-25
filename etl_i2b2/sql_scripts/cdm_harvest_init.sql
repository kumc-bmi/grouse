/** cdm_harvest_init - Initialize PCORNet CDM ETL tracking.

ref 2017-01-06-PCORnet-Common-Data-Model-v3dot1-parseable.xlsx

Note: Columns "assigned by DRN OC" (e.g. datamartid) are left null.

*/

create or replace view harvest_enum as
select '01' not_present
     , '02' present
     , 'NI' no_information
     , 'UN' unknown
     , 'OT' other
from dual;

delete from "&&PCORNET_CDM".harvest;

insert into "&&PCORNET_CDM".harvest
  (datamart_platform, cdm_version) values
  ('02' /*Oracle*/  , 3.1);
commit;

select count(*) complete
from "&&PCORNET_CDM".harvest
where (select present from harvest_enum) is not null;
