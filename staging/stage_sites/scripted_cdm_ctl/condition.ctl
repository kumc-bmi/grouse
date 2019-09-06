load data infile  condition.dsv
truncate into table condition
fields terminated by '|'
(
conditionid
patid
encounterid
report_date DATE 'YYYY-MM-DD'
resolve_date DATE 'YYYY-MM-DD'
onset_date DATE 'YYYY-MM-DD'
condition_status
condition
condition_type
condition_source
raw_condition_status
raw_condition
raw_condition_type
raw_condition_source
)