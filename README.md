# Greater Plains Collaborative Reusable Unified Study Environment (GROUSE)

The [Greater Plains Collaborative (GPC)][GPC], led by
[KUMC Medical Informatics][miea], is a
[PCORI Clinical Data Research Network (CDRN)][CDRN].  As explained in
the [GROUSE][] executive summary:

> ... we seek to expand the data completeness of our patients’ health
> care processes and outcomes and understand the information gain for
> our complete and comprehensive population by comparing correlations
> between the Medicare and Medicaid claims data with the data in our
> CDRN that includes the electronic health record and billing data
> from each of our component health systems, clinical registry data
> (e.g. hospital tumor registries), private payer claims data, and
> patient-reported outcomes, as available, and work with our GPC and
> CDRN investigators to answer specific cohort questions to achieve
> our overarching aims.

This software supports the project as follows:

  1. GPC sites will send the NewWave-GDIT (Research Data Distribution
     Center under contract with [CMS][]) encrypted beneficiary identifiers
     (e.g., SSN, HICs, name, DOB) along with a randomly generated ID
     for each of their patients.
  2. NewWave-GDIT will generate the finder files that contain mappings
     between the random IDs and CMS IDs and send the crosswalk files
     and CMS data back to KUMC.
	 - [grouse_tables.csv](grouse_tables.csv) summarizes the CMS data
     - [staging](staging/README.md) supports decrypting/loading the CMS
        data into Oracle from the encrypted archives.
  3. KUMC will integrate the crosswalk and CMS data with individual
     site EMR data (limited data set) through linking the random
     IDs. This will allow KUMC to achieve record linkage of patients’s
     EMR and claims data without obtaining or retaining actual patient
     level PII from individual GPC sites.
     - [deid](deid/README.md) supports creating a de-identified
       copy of the CMS tables.
     - [site_integration](site_integration/README.md) supports
	   integrating site EMR data
  4. The merged dataset will be de-identified and made available to
     project team members for running analyses or using the i2b2
     client.
     - [etl_i2b2](etl_i2b2/README.md) supports building an [i2b2][] star
       schema from (de-identified) CMS data.

![GROUSE record linkage diagram](https://informatics.gpcnetwork.org/trac/Project/raw-attachment/wiki/GROUSE/grouse-record-linkage-diagram.png)

[//]: # TODO: add obfuscate.py, which supports step 1.

## Platform

The platform used at the
[University of Kansas Medical Center Division of Medical Informatics][miea] includes:

* [Linux (SLES 12)](https://www.suse.com/products/server/)
* [Python](https://www.python.org/)
* [Oracle database 12c](https://www.oracle.com/database/index.html)
* [Jenkins](https://jenkins.io/) (to run scripts in a repeatable way while maintaining logs)

[GROUSE]: https://informatics.gpcnetwork.org/trac/Project/wiki/GROUSE
[GPC]: http://gpcnetwork.org/
[CDRN]: http://www.pcori.org/research-results/pcornet-national-patient-centered-clinical-research-network/clinical-data-and-0
[miea]: http://www.kumc.edu/miea.html
[i2b2]: https://www.i2b2.org/
[ResDAC]: http://www.resdac.org/about-resdac/our-services
[CMS]: https://www.cms.gov/
