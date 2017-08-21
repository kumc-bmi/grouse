/** cms_patient_mapping - view of CMS beneficiaries

Patient mappings are generated upstream (see ../../deid).
*/

select bene_cd from cms_key_sources where 'dep' = 'cms_keys.pls';

create or replace view bene_id_mapping
as
  (select patient_ide bene_id
  , patient_num
  from "&&I2B2STAR".patient_mapping pat_map
  cross join cms_key_sources key_sources
  where patient_ide_source = key_sources.bene_cd
  ) ;


select 1 complete
from bene_id_mapping
where rownum = 1  -- at least 1 row is populated
;
