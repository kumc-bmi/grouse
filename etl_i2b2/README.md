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
obfuscates some identifiers and dates.


## Usage

To load an i2b2 star schema from the CMS RIF tables, (once
dependencies below are satisfied), invoke the main `GrouseETL` task
following [luigi][] conventions:

    (grouse-etl)$ PYTHONPATH=. luigi --module cms_etl GrouseETL \
                                     --local-scheduler

Or:

    (grouse-etl)$ ./luigid_run.sh
    (grouse-etl)$ PYTHONPATH=. luigi --workers 4 --module cms_etl GrouseETL


[luigi]: https://github.com/spotify/luigi

**ISSUE**: why won't luigi find modules in the current directory?


### Data Characterization

Reports akin to tables from the [PCORNet CDM][CDM] Emperical Data
Characterization (EDC) are produced as .csv file byproducts.

[CDM]: http://www.pcornet.org/pcornet-common-data-model/


### Dependencies


The usual python `requirements.txt` conventions *TODO: cite* apply:

    $ virtualenv ~/pyenv/grouse-etl
    $ . ~/pyenv/grouse-etl/bin/activate
    (grouse-etl)$ pip install -r requirements.txt


## Design and Development

For design info and project coding style, see [CONTRIBUTING][].

[CONTRIBUTING]: CONTRIBUTING.md
