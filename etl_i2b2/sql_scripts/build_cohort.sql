/** build_cohort - create i2b2 patient set in batch mode

## CohortRIF Workflow utilities:

-- clobber CohortRIF output
delete from mbsf_abcd_summary;
delete from medpar_all;
delete from bcarrier_claims_k;
delete from bcarrier_line_k;
delete from outpatient_base_claims_k;
delete from outpatient_revenue_center_k;
delete from pde;
commit;


-- QA: look for missing patient_mapping etc.
select site_schema, result_instance_id, start_date, task_id, count(distinct patient_num)
from site_cohorts
group by site_schema, result_instance_id, start_date, task_id
order by start_date desc;

select case
       when pd.patient_num is not null then 'CDM (CMS)'
       when sc.patient_num < 22000000 then 'CMS'
       end crosswalk
     , sc.*
from site_cohorts sc
left join patient_dimension pd on pd.patient_num = sc.patient_num
where site_schema in ('BLUEHERONDATA_KUMC_CALAMUS', 'BLUEHERONDATA_UTSW')
order by sc.patient_num
;

*/

-- dependencies
select patient_num from "&&I2B2_STAR_SITE".observation_fact where 1=0;
select :task_id
     , :inclusion_concept_cd -- e.g. 'SEER_SITE:26000' '\i2b2\naaccr\SEER Site\Breast\'
     , :dx_date_min
     , :result_instance_id        -- caller gets QT_SQ_QRI_QRIID.nextval
     , :query_instance_id
     , :query_master_id  -- caller gets QT_SQ_QM_QMID.nextval
     , :query_name, :user_id, :project_id
from dual;


insert
into "&&I2B2_STAR".qt_query_master
  (
    query_master_id
  , name
  , user_id
  , group_id
  --, master_type_cd
  --, plugin_id
  , create_date
  --, delete_date
  --, delete_flag
  , generated_sql
  --, request_xml
  --, i2b2_request_xml
  --, pm_xml
  )
  values
  (
    :query_master_id  -- caller gets QT_SQ_QM_QMID.nextval
  , :query_name
  , :user_id
  , :project_id
  , current_timestamp
  , '
  select distinct patient_num
  from "&&I2B2_STAR_SITE".observation_fact obs
  where obs.concept_cd in (' || :inclusion_concept_cd || ')
  and start_date >= ''' || :dx_date_min || ''''
  ) ;


insert into "&&I2B2_STAR".qt_query_instance
select :query_instance_id
, qm.query_master_id
, qm.user_id
, qm.group_id
, null batch_mode
, sysdate start_date
, null end_date
, null delete_flag
, (select status_type_id from "&&I2B2_STAR".QT_QUERY_STATUS_TYPE where name = 'PROCESSING')
, :task_id message
from "&&I2B2_STAR".qt_query_master qm
where query_master_id=:query_master_id
;


insert into "&&I2B2_STAR".qt_query_result_instance

select :result_instance_id
, :query_instance_id
, (select min(result_type_id) from "&&I2B2_STAR".QT_QUERY_RESULT_TYPE where name='PATIENTSET') result_type_id
, null -- q_stats.qty set_size
, sysdate start_date
, null end_date
, null delete_flag
, (select STATUS_TYPE_ID from "&&I2B2_STAR".QT_QUERY_STATUS_TYPE where NAME = 'PROCESSING') status_type_id
, null message
, :task_id description
, null -- real_set_size
, 'date shifted @@which patient_num' obfusc_method
from dual;


insert into "&&I2B2_STAR".qt_patient_set_collection
select "&&I2B2_STAR".QT_SQ_QPR_PCID.nextval
, :result_instance_id
, rownum set_index
, cohort.patient_num
from (
  select distinct patient_num
  from "&&I2B2_STAR_SITE".observation_fact obs
  where obs.concept_cd in (:inclusion_concept_cd,
                           -- KLUDGE for MCW
  		       	      'NAACCR|400:C500'
                  , 'NAACCR|400:C501'
                  , 'NAACCR|400:C502'
                  , 'NAACCR|400:C503'
                  , 'NAACCR|400:C504'
                  , 'NAACCR|400:C505'
                  , 'NAACCR|400:C506'
                  , 'NAACCR|400:C507'
                  , 'NAACCR|400:C508')
  and start_date >= :dx_date_min -- '2011-01-01'
) cohort
;

update "&&I2B2_STAR".qt_query_result_instance
set set_size = (
  select count(*) qty
  from "&&I2B2_STAR".qt_patient_set_collection
  where result_instance_id = :result_instance_id
  )
, status_type_id = (select STATUS_TYPE_ID from "&&I2B2_STAR".QT_QUERY_STATUS_TYPE where NAME = 'FINISHED')
where result_instance_id = :result_instance_id
;

/*
-- about patient sets
select qm.query_master_id
     , qm.name
     , qri.set_size
     , qri.obfusc_method
     , qm.user_id, qm.create_date
     , qm.generated_sql
     , qri.query_instance_id
     , qri.result_instance_id
from qt_query_result_instance qri
join qt_query_instance qi on qi.query_instance_id = qri.query_instance_id
join qt_query_master qm on qm.query_master_id = qi.query_master_id
order by qm.query_master_id desc;
*/


create or replace view site_cohorts as
with upload_ok as (
  select upload_id, load_date, end_date
       , trim(both '''' from source_cd) site_schema
       , substr(transform_name, -11) task_id
  from upload_status up
  where load_status = 'OK'
)
, psets as (
  select result_instance_id, set_size, start_date
       , substr(ri.description, -11) task_id
  from qt_query_result_instance ri
)
select upload_ok.upload_id, upload_ok.site_schema, psets.*, pcol.patient_num, pcol.set_index
from upload_ok
join psets on psets.task_id = upload_ok.task_id
join qt_patient_set_collection pcol on pcol.result_instance_id = psets.result_instance_id
order by load_date desc, set_index
;


-- done already?
-- also recorded in upload_status
select set_size complete
from "&&I2B2_STAR".qt_query_result_instance
where description = :task_id and set_size > 0
;


