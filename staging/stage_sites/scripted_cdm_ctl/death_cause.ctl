load data infile  death_cause.dsv
truncate into table death_cause
fields terminated by '|'
(
patid
death_cause
death_cause_code
death_cause_type
death_cause_source
death_cause_confidence
)