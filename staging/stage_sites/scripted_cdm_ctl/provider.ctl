load data infile  provider.dsv
truncate into table provider
fields terminated by '|'
(
providerid
provider_sex
provider_specialty_primary
provider_npi
provider_npi_flag
raw_provider_specialty_primary
raw_prov_name
raw_prov_type
)