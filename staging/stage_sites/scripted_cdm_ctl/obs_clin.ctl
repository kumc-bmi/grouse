load data infile  obs_clin.dsv
truncate into table obs_clin
fields terminated by '|'
(
obsclinid
patid
encounterid
obsclin_providerid
obsclin_date DATE 'YYYY-MM-DD'
obsclin_time
obsclin_type
obsclin_code
obsclin_result_qual
obsclin_result_text
obsclin_result_snomed
obsclin_result_num
obsclin_result_modifier
obsclin_result_unit
raw_obsclin_name
raw_obsclin_code
raw_obsclin_type
raw_obsclin_result
raw_obsclin_modifier
raw_obsclin_unit
)