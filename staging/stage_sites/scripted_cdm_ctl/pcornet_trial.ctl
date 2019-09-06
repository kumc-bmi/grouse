load data infile  pcornet_trial.dsv
truncate into table pcornet_trial
fields terminated by '|'
(
patid
trialid
participantid
trial_siteid
trial_enroll_date DATE 'YYYY-MM-DD'
trial_end_date DATE 'YYYY-MM-DD'
trial_withdraw_date DATE 'YYYY-MM-DD'
trial_invite_code
)