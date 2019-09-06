load data infile  diagnosis.dsv
truncate into table diagnosis
fields terminated by '|'
(
diagnosisid
patid
encounterid
enc_type
admit_date DATE 'YYYY-MM-DD'
providerid
dx
dx_type
dx_source
dx_origin
pdx
dx_poa
raw_dx
raw_dx_type
raw_dx_source
raw_pdx
raw_dx_poa
)