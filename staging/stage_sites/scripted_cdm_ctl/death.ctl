load data infile  death.dsv
truncate into table death
fields terminated by '|'
(
patid
death_date DATE 'YYYY-MM-DD'
death_date_impute
death_source
death_match_confidence
)