load data infile  enrollement.dsv
truncate into table enrollement
fields terminated by '|'
(
patid
enr_start_date DATE 'YYYY-MM-DD'
enr_end_date DATE 'YYYY-MM-DD'
chart
enr_basis
)