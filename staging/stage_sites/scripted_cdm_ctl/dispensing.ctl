load data infile  dispensing.dsv
truncate into table dispensing
fields terminated by '|'
(
dispensingid
patid
prescribingid
dispense_date DATE 'YYYY-MM-DD'
ndc
dispense_sup
dispense_amt
raw_ndc
dispense_dose_disp
dispense_dose_disp_unit
dispense_route
raw_dispense_dose_disp
raw_dispense_dose_disp_unit
raw_dispense_route
)