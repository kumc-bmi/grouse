load data infile  harvest.dsv
truncate into table harvest
fields terminated by '|'
(
networkid
network_name
datamartid
datamart_name
datamart_platform
cdm_version
datamart_claims
datamart_ehr
birth_date_mgmt DATE 'YYYY-MM-DD'
enr_start_date_mgmt DATE 'YYYY-MM-DD'
enr_end_date_mgmt DATE 'YYYY-MM-DD'
admit_date_mgmt DATE 'YYYY-MM-DD'
discharge_date_mgmt DATE 'YYYY-MM-DD'
px_date_mgmt DATE 'YYYY-MM-DD'
rx_order_date_mgmt DATE 'YYYY-MM-DD'
rx_start_date_mgmt DATE 'YYYY-MM-DD'
rx_end_date_mgmt DATE 'YYYY-MM-DD'
dispense_date_mgmt DATE 'YYYY-MM-DD'
lab_order_date_mgmt DATE 'YYYY-MM-DD'
specimen_date_mgmt DATE 'YYYY-MM-DD'
result_date_mgmt DATE 'YYYY-MM-DD'
measure_date_mgmt DATE 'YYYY-MM-DD'
onset_date_mgmt DATE 'YYYY-MM-DD'
report_date_mgmt DATE 'YYYY-MM-DD'
resolve_date_mgmt DATE 'YYYY-MM-DD'
pro_date_mgmt DATE 'YYYY-MM-DD'
refresh_demographic_date DATE 'YYYY-MM-DD'
refresh_enrollment_date DATE 'YYYY-MM-DD'
refresh_encounter_date DATE 'YYYY-MM-DD'
refresh_diagnosis_date DATE 'YYYY-MM-DD'
refresh_procedures_date DATE 'YYYY-MM-DD'
refresh_vital_date DATE 'YYYY-MM-DD'
refresh_dispensing_date DATE 'YYYY-MM-DD'
refresh_lab_result_cm_date DATE 'YYYY-MM-DD'
refresh_condition_date DATE 'YYYY-MM-DD'
refresh_pro_cm_date DATE 'YYYY-MM-DD'
refresh_prescribing_date DATE 'YYYY-MM-DD'
refresh_pcornet_trial_date DATE 'YYYY-MM-DD'
refresh_death_date DATE 'YYYY-MM-DD'
refresh_death_cause_date DATE 'YYYY-MM-DD'
death_date_mgmt DATE 'YYYY-MM-DD'
medadmin_stop_date_mgmt DATE 'YYYY-MM-DD'
medadmin_start_date_mgmt DATE 'YYYY-MM-DD'
obsclin_date_mgmt DATE 'YYYY-MM-DD'
obsgen_date_mgmt DATE 'YYYY-MM-DD'
refresh_med_admin_date DATE 'YYYY-MM-DD'
refresh_obs_clin_date DATE 'YYYY-MM-DD'
refresh_obs_gen_date DATE 'YYYY-MM-DD'
refresh_provider_date DATE 'YYYY-MM-DD'
)