/* date_shift_2015_part -- shift dates for 1 upload of facts from CMS_DEID_2015
*/

select patient_num, shift_35, shift_45 from bc_shift_345 where 'dep' = 'date_shift_normalize.sql';

create table observation_fact_s35_&&upload_id as select * from observation_fact_&&upload_id;

merge into observation_fact_s35_&&upload_id obs
using bc_shift_345 s345 on (obs.patient_num = s345.patient_num)
when matched then
  update set obs.start_date = obs.start_date + coalesce(shift_35, shift_45, 0),
             obs.end_date = obs.end_date + coalesce(shift_35, shift_45, 0),
             obs.update_date = obs.update_date + coalesce(shift_35, shift_45, 0);
commit;

-- complete?
-- ISSUE: doesn't handle the case where the create table succeeded but not the merge
select case when out_qty = in_qty and in_qty > 0 then in_qty else 0 end complete
from (
  select (select count(*) from observation_fact_&&upload_id) in_qty
       , (select count(*) from observation_fact_s35_&&upload_id) out_qty
  from dual
);
