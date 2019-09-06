load data infile  vital.dsv
truncate into table vital
fields terminated by '|'
(
vitalid
patid
encounterid
measure_date DATE 'YYYY-MM-DD'
measure_time
vital_source
ht
wt
diastolic
systolic
original_bmi
bp_position
smoking
tobacco
tobacco_type
raw_vital_source
raw_diastolic
raw_systolic
raw_bp_position
raw_smoking
raw_tobacco
raw_tobacco_type
)