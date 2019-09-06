load data infile  obs_gen.dsv
truncate into table obs_gen
fields terminated by '|'
(
obsgenid
patid
encounterid
obsgen_providerid
obsgen_date DATE 'YYYY-MM-DD'
obsgen_time
obsgen_type
obsgen_code
obsgen_result_qual
obsgen_result_text
obsgen_result_num
obsgen_result_modifier
obsgen_result_unit
obsgen_table_modified
obsgen_id_modified
raw_obsgen_name
raw_obsgen_code
raw_obsgen_type
raw_obsgen_result
raw_obsgen_unit
)