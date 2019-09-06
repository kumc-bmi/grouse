load data infile  med_admin.dsv
truncate into table med_admin
fields terminated by '|'
(
medadminid
patid
encounterid
prescribingid
medadmin_providerid
medadmin_start_date DATE 'YYYY-MM-DD'
medadmin_start_time
medadmin_stop_date DATE 'YYYY-MM-DD'
medadmin_stop_time
medadmin_type
medadmin_code
medadmin_dose_admin
medadmin_dose_admin_unit
medadmin_route
medadmin_source
raw_medadmin_med_name
raw_medadmin_code
raw_medadmin_dose_admin
raw_medadmin_dose_admin_unit
raw_medadmin_route
)