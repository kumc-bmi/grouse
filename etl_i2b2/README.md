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

To load demographics and diagnoses into an i2b2 star schema from the
CMS RIF tables, (once dependencies below are satisfied), invoke the
main `GrouseETL` task following [luigi][] conventions:

    (grouse-etl)$ luigi --local-scheduler \
	                    --module cms_etl GrouseETL

[luigi]: https://github.com/spotify/luigi

**ISSUE**: why won't luigi find modules in the current directory?
           Use `PYTHONPATH=. luigi ...` if necessary.

**TODO**: Lots; e.g. diagnoses from `medpar_all`

### Data Characterization

Reports akin to tables from the [PCORNet CDM][CDM] Emperical Data
Characterization (EDC) are produced as .csv file byproducts.

[CDM]: http://www.pcornet.org/pcornet-common-data-model/


## Design and Development

For design info and project coding style, see [CONTRIBUTING][].

[CONTRIBUTING]: CONTRIBUTING.md


## Configuration

see `luigi.cfg.example`


## Dependencies and Docker Usage

Our deployment platform is docker:

    docker build -t grouse-etl .
    docker run --rm -e"PASSKEY=$PASSKEY" -v/CONFIG/DIR:/etc/luigi -t grouse-etl \
	       --local-scheduler --workers 4 --module cms_etl GrouseETL

If you want to run outside docer, See `requirements.txt`.

### ssh tunnel to Oracle DB host

The `kingsquare/tunnel` image is handy:

    docker run --rm --name lsnr -v $SSH_AUTH_SOCK:/ssh-agent -t kingsquare/tunnel \
        *:7521:localhost:1521 USERNAME@ORAHOST

Then use `oracle://lsnr:7521/SID` in your `.cfg` file.

Then add a `--link` to it when you run the task:

    docker run --rm --link=lsnr ... -t grouse-etl ...

### luigid (optional)

    docker run --name luigid -p8082:8082 -v/STATE:/luigi/state -v/CONFIG:/etc/luigi -t stockport/luigid
