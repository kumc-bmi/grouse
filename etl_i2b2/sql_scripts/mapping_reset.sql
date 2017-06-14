/** mapping_reset - reset encounter mappings, sequences

Patient mappings are generated upstream (see ../../deid), so we don't
reset those.
*/

select bene_cd from cms_key_sources where 'dep' = 'cms_keys.pls';

whenever sqlerror continue;
drop sequence "&&I2B2STAR".sq_up_encdim_encounternum;
whenever sqlerror exit;
create sequence "&&I2B2STAR".sq_up_encdim_encounternum cache 1024;

-- ISSUE: truncate ... fails with: ORA-01031: insufficient privileges
delete from "&&I2B2STAR".encounter_mapping;

select 1 complete
from "&&I2B2STAR".upload_status up
where up.transform_name = :task_id
and up.load_status = 'OK';
