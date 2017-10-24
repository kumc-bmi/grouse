/** cdm_harvest_init - Initialize PCORNet CDM ETL tracking.

ref 2017-01-06-PCORnet-Common-Data-Model-v3dot1-parseable.xlsx

Note: Columns "assigned by DRN OC" (e.g. datamartid) are left null.

ISSUE: We assume the session owner owns the PCORNet datamart;
       i.e. this is run by the Analyst, not in the job control
       environment.

*/

whenever sqlerror continue;
drop table harvest;
whenever sqlerror exit;

CREATE TABLE harvest(
	NETWORKID varchar(10) NULL,
	NETWORK_NAME varchar(20) NULL,
	DATAMARTID varchar(10) NULL,
	DATAMART_NAME varchar(20) NULL,
	DATAMART_PLATFORM varchar(2) NULL,
	CDM_VERSION numeric(8, 2) NULL,
	DATAMART_CLAIMS varchar(2) default 'NI',
	DATAMART_EHR varchar(2) default 'NI',
	BIRTH_DATE_MGMT varchar(2) default 'NI',
	ENR_START_DATE_MGMT varchar(2) default 'NI',
	ENR_END_DATE_MGMT varchar(2) default 'NI',
	ADMIT_DATE_MGMT varchar(2) default 'NI',
	DISCHARGE_DATE_MGMT varchar(2) default 'NI',
	PX_DATE_MGMT varchar(2) default 'NI',
	RX_ORDER_DATE_MGMT varchar(2) default 'NI',
	RX_START_DATE_MGMT varchar(2) default 'NI',
	RX_END_DATE_MGMT varchar(2) default 'NI',
	DISPENSE_DATE_MGMT varchar(2) default 'NI',
	LAB_ORDER_DATE_MGMT varchar(2) default 'NI',
	SPECIMEN_DATE_MGMT varchar(2) default 'NI',
	RESULT_DATE_MGMT varchar(2) default 'NI',
	MEASURE_DATE_MGMT varchar(2) default 'NI',
	ONSET_DATE_MGMT varchar(2) default 'NI',
	REPORT_DATE_MGMT varchar(2) default 'NI',
	RESOLVE_DATE_MGMT varchar(2) default 'NI',
	PRO_DATE_MGMT varchar(2) default 'NI',
	REFRESH_DEMOGRAPHIC_DATE date NULL,
	REFRESH_ENROLLMENT_DATE date NULL,
	REFRESH_ENCOUNTER_DATE date NULL,
	REFRESH_DIAGNOSIS_DATE date NULL,
	REFRESH_PROCEDURES_DATE date NULL,
	REFRESH_VITAL_DATE date NULL,
	REFRESH_DISPENSING_DATE date NULL,
	REFRESH_LAB_RESULT_CM_DATE date NULL,
	REFRESH_CONDITION_DATE date NULL,
	REFRESH_PRO_CM_DATE date NULL,
	REFRESH_PRESCRIBING_DATE date NULL,
	REFRESH_PCORNET_TRIAL_DATE date NULL,
	REFRESH_DEATH_DATE date NULL,
	REFRESH_DEATH_CAUSE_DATE date NULL
)
;

create or replace view harvest_enum as
select '01' not_present
     , '02' present
     , 'NI' no_information
     , 'UN' unknown
     , 'OT' other
from dual;

insert into harvest
  (datamart_platform, cdm_version) values
  ('02' /*Oracle*/  , 3.1);
commit;

select count(*) complete
from harvest
where (select present from harvest_enum) is not null;
