load data infile  demographic.dsv
truncate into table demographic
fields terminated by '|'
(
patid
birth_date DATE 'YYYY-MM-DD'
birth_time
sex
sexual_orientation
gender_identity
hispanic
race
biobank_flag
pat_pref_language_spoken
raw_sex
raw_sexual_orientation
raw_gender_identity
raw_hispanic
raw_race
raw_pat_pref_language_spoken
)