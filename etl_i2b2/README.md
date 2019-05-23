# ETL from CMS RIF to i2b2, PCORNet for GROUSE

The GROUSE staging process results in tables that mirror the structure
of CMS Research Identifiable Files (RIF); for example the
[Master Beneficiary Summary File][mbsf]. (*Note that the GROUSE
staging preserves the RIF structure but obfuscates patient identifiers
and dates.*)

We transform the CMS RIF data and load it into an i2b2 data
repository, following the
[i2b2 Data Repository (CRC) Cell Design Document][CRC].

[mbsf]: https://www.resdac.org/cms-data/files/mbsf
[CRC]: https://www.i2b2.org/software/files/PDF/current/CRC_Design.pdf

We transform the i2b2 data and load it into the [PCORNet Common Data
Model (CDM)][CDM], v3.1.

[CDM]: http://www.pcornet.org/pcornet-common-data-model/


## Usage

To build sections of an i2b2 style fact table in an analyst's schema,
once installation and configuration (below) is done, invoke the
`CMSRIFLoad` task following [luigi][] conventions:

    luigi --workers=24 --module cms_pd CMSRIFLoad

`CMSRIFLoad` consists of a `MedicareYear` and `MedicaidYear` task for
2014, 2015, etc.; each `MedicareYear` and `MedicaidYear`  task includes
  - demographics (`MBSFUpload`, `MAXPSUpload` resp.)
  - outpatient claims (carrier claims, `MAXDATA_OT`)
  - inpatient claims (`MEDPAR`)
  - prescription drugs (`PDE`, `MAXRxUpload`)

The dependencies look something like this:

<img src="https://github.com/kumc-bmi/grouse/raw/master/etl_i2b2/CMSRIFLoad%20Task%20Dependencies.png" 
alt="CMSRIFLoad subtasks"/>

Then use `etl_tasks.MigratePendingUploads` to install the data in the
production i2b2 `observation_fact` table. Use
`cms_pd.PatientDimension` and `cms_pd.VisitDimLoad` to load the
`patient_dimension` and `visit_dimension` tables from
`observation_fact`.

[luigi]: https://github.com/spotify/luigi

**TODO**: HHA etc. in cms_pd.CMSRIFLoad

Troubleshooting is discussed in [CONTRIBUTING][].

### PCORNet CDM and Data Characterization

Use `cms_pd.Demographics` to produce a report (in `.csv` format) akin
to Table 1A from the [PCORNet CDM][CDM] Emperical Data
Characterization (EDC). The task uses SQL scripts to create two views:

  1. `pcornet_demographic`, a view of the i2b2 `patient_dimension` table
     transformed for the PCORNet CDM
  2. `demographic_summary`, a view to simulate table IA in the
     emperical data characterization (EDC) report.

Provided the summary looks plausibe, use `cms_i2p.I2P` to materialize
the `pcornet_demographic` view as a `DEMOGRAPHIC` table and likewise
the other CDM tables.

[CDM]: http://www.pcornet.org/pcornet-common-data-model/

**TODO**: ENROLLMENT, DEATH in `cms_i2p.I2P`

## Installation, Dependencies and Docker

Our deployment platform is docker, using
[stockport/luigi-taskrunner](https://hub.docker.com/r/stockport/luigi-taskrunner/):

    docker run --rm \
           -v`/bin/pwd`:/luigi/tasks \
           -v`/bin/pwd`:/etc/luigi \
           -ePASSKEY \
           stockport/luigi-taskrunner \
	       --local-scheduler --workers 4 --module etl_tasks MigratePendingUploads

If you want to run outside docker, See `requirements.txt`.

### luigid (optional)

    docker run --name luigid -p8082:8082 -v/STATE:/luigi/state -v/CONFIG:/etc/luigi -t stockport/luigid


## Configuration

See [client.cfg](client.cfg).

## Design and Development

For design info and project coding style, see [CONTRIBUTING][].

[CONTRIBUTING]: CONTRIBUTING.md
