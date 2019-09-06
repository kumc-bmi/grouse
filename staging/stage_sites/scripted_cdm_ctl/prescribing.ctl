load data infile  prescribing.dsv
truncate into table prescribing
fields terminated by '|'
(
prescribingid
patid
encounterid
rx_providerid
rx_order_date DATE 'YYYY-MM-DD'
rx_order_time
rx_start_date DATE 'YYYY-MM-DD'
rx_end_date DATE 'YYYY-MM-DD'
rx_dose_ordered
rx_dose_ordered_unit
rx_quantity
rx_dose_form
rx_refills
rx_days_supply
rx_frequency
rx_prn_flag
rx_route
rx_basis
rxnorm_cui
rx_source
rx_dispense_as_written
raw_rx_med_name
raw_rx_frequency
raw_rxnorm_cui
raw_rx_quantity
raw_rx_ndc
raw_rx_dose_ordered
raw_rx_dose_ordered_unit
raw_rx_route
raw_rx_refills
)