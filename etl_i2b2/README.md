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

To load demographics and diagnoses into an i2b2 star schema from the
CMS RIF tables, (once installed and configured as below), invoke the
main `GrouseETL` task following [luigi][] conventions:

    luigi --local-scheduler --module cms_etl GrouseETL

[luigi]: https://github.com/spotify/luigi

**TODO**: Lots; e.g. diagnoses from `medpar_all`

Troubleshooting is discussed in [CONTRIBUTING][].

### Data Characterization

Reports akin to tables from the [PCORNet CDM][CDM] Emperical Data
Characterization (EDC) are produced as .csv file byproducts.

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

See [luigi.cfg.example](luigi.cfg.example).

## Design and Development

For design info and project coding style, see [CONTRIBUTING][].

[CONTRIBUTING]: CONTRIBUTING.md
