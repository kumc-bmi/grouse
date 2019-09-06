load data infile  lab_result_cm.dsv
truncate into table lab_result_cm
fields terminated by '|'
(
lab_result_cm_id
patid
encounterid
lab_name
specimen_source
lab_loinc
priority
result_loc
lab_px
lab_px_type
lab_order_date DATE 'YYYY-MM-DD'
specimen_date DATE 'YYYY-MM-DD'
specimen_time
result_date DATE 'YYYY-MM-DD'
result_time
result_qual
result_num
result_modifier
result_unit
norm_range_low
norm_modifier_low
norm_range_high
norm_modifier_high
abn_ind
raw_lab_name
raw_lab_code
raw_panel
raw_result
raw_unit
raw_order_dept
raw_facility_code
result_snomed
)