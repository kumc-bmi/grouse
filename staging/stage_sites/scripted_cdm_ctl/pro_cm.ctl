load data infile  pro_cm.dsv
truncate into table pro_cm
fields terminated by '|'
(
pro_cm_id
patid
encounterid
pro_loinc
pro_date DATE 'YYYY-MM-DD'
pro_time
pro_method
pro_mode
pro_cat
raw_pro_code
raw_pro_response
pro_item_fullname
pro_item_loinc
pro_item_name
pro_item_text
pro_item_version
pro_measure_count_scored
pro_measure_fullname
pro_measure_loinc
pro_measure_name
pro_measure_scaled_tscore
pro_measure_score
pro_measure_seq
pro_measure_standard_error
pro_measure_theta
pro_measure_version
pro_response_num
pro_response_text
pro_type
)