load data infile  procedures.dsv
truncate into table procedures
fields terminated by '|'
(
proceduresid
patid
encounterid
enc_type
admit_date DATE 'YYYY-MM-DD'
providerid
px_date DATE 'YYYY-MM-DD'
px
px_type
px_source
ppx
raw_px
raw_px_type
raw_ppx
)