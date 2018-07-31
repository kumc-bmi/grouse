/** date_shift_normalize - organize date shift info by patient_num

ISSUE: TODO: 2014, including bc_shift_34 as per ticket:5300#comment:23

*/

select bene_id_deid, date_shift_days from cms_deid.dt_sft_diff_1113_15_all where 1 = 0;
select bene_id_deid, date_shift_days from cms_deid.dt_sft_diff_14_15_all where 1 = 0;

whenever sqlerror continue;
drop table bc_shift_35;
drop index bc_shift_35_pk;
drop table bc_shift_45;
drop index bc_shift_45_pk;
drop table bc_shift_345;
drop index bc_shift_345_pk;
whenever sqlerror exit;


create table bc_shift_35 as
  select /*+ parallel */ to_number(bene_id_deid) patient_num, DATE_SHIFT_DAYS
  from cms_deid.dt_sft_diff_1113_15_all
  where bene_id_deid in (select distinct to_char(patient_num) from site_cohorts)
  ;
create unique index bc_shift_35_pk on bc_shift_35 (patient_num);

create table bc_shift_45 as
  select /*+ parallel */ to_number(bene_id_deid) patient_num, DATE_SHIFT_DAYS
  from cms_deid.dt_sft_diff_14_15_all
  where bene_id_deid in (select distinct to_char(patient_num) from site_cohorts)
  ;
create unique index bc_shift_45_pk on bc_shift_45 (patient_num);

create table bc_shift_345 as
select coalesce(s35.patient_num, s45.patient_num) patient_num, s35.DATE_SHIFT_DAYS shift_35, s45.DATE_SHIFT_DAYS shift_45
from (
  select /*+ parallel */ to_number(bene_id_deid) patient_num, DATE_SHIFT_DAYS
  from cms_deid.dt_sft_diff_1113_15_all
  where bene_id_deid in (select distinct to_char(patient_num) from site_cohorts)
) s35
full outer join
(
  select /*+ parallel */ to_number(bene_id_deid) patient_num, DATE_SHIFT_DAYS
  from cms_deid.dt_sft_diff_14_15_all
  where bene_id_deid in (select distinct to_char(patient_num) from site_cohorts)
) s45 on s35.patient_num = s45.patient_num
;
create unique index bc_shift_345_pk on bc_shift_345 (patient_num);


/* complete? */
select (select count(*) from bc_shift_345) complete
from dual
;
