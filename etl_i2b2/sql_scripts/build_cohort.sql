/** build_cohort - create i2b2 patient set in batch mode

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
into qt_query_master
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
, (select status_type_id from QT_QUERY_STATUS_TYPE where name = 'PROCESSING')
, :task_id message
from qt_query_master qm
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
, :task_id message
, null description
, null -- real_set_size
, 'date shifted @@which patient_num' obfusc_method
from dual;


insert into "&&I2B2_STAR".qt_patient_set_collection
select QT_SQ_QPR_PCID.nextval
, :result_instance_id
, rownum set_index
, cohort.patient_num
from (
  select distinct patient_num
  from "&&I2B2_STAR_SITE".observation_fact obs
  where obs.concept_cd in (:inclusion_concept_cd)
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
where qm.query_master_id = 22;
*/
