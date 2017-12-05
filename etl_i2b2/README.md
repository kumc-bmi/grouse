# ETL from CMS RIF to i2b2 for GROUSE

The GROUSE staging process results in tables that mirror the structure
of CMS Research Identifiable Files (RIF); for example the
[Master Beneficiary Summary File][mbsf]. We use the demographic
information there to load the i2b2 `patient_dimension` table. For the
structure of i2b2 tables, see the
[i2b2 Data Repository (CRC) Cell Design Document][CRC].

[mbsf]: https://www.resdac.org/cms-data/files/mbsf
[CRC]: https://www.i2b2.org/software/files/PDF/current/CRC_Design.pdf

Note that the GROUSE staging preserves the RIF structure but
obfuscates patient identifiers and dates.


## Usage

To build sections of an i2b2 style fact table in an analyst's schema,
invoke the `CMSRIFLoad` task following [luigi][] conventions:

    luigi --workers=24 --module cms_pd CMSRIFLoad

Then use `etl_tasks.MigratePendingUploads` to install the data in the
production i2b2 `observation_fact` table and `cms_pd.PatientDimension`
to load the `patient_dimension` table.

[luigi]: https://github.com/spotify/luigi

**TODO**: HHA etc. in cms_pd.CMSRIFLoad
**TODO**: ENROLLMENT, DEATH in cms_i2p.I2P

Troubleshooting is discussed in [CONTRIBUTING][].

### Data Characterization

Use `cms_pd.Demographics` to produce a report (in `.csv` format) akin
to Table 1A from the [PCORNet CDM][CDM] Emperical Data
Characterization (EDC).


[CDM]: http://www.pcornet.org/pcornet-common-data-model/


## Installation, Dependencies and Docker

Our deployment platform is docker, using
[stockport/luigi-taskrunner](https://hub.docker.com/r/stockport/luigi-taskrunner/):

    docker run --rm \
           -v`/bin/pwd`:/luigi/tasks \
           -v`/bin/pwd`:/etc/luigi \
           -ePASSKEY \
           stockport/luigi-taskrunner \
	       --local-scheduler --workers 4 --module cms_etl GrouseETL

If you want to run outside docker, See `requirements.txt`.

### luigid (optional)

    docker run --name luigid -p8082:8082 -v/STATE:/luigi/state -v/CONFIG:/etc/luigi -t stockport/luigid


## Configuration

See [client.cfg](client.cfg).

## Design and Development

For design info and project coding style, see [CONTRIBUTING][].

[CONTRIBUTING]: CONTRIBUTING.md
