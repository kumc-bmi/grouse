This repository contains scripts to help manage health insurance claims data from the [Center for Medicare and Medicaid Services ​(CMS)](https://www.cms.gov/) obtained through the [Research Data Assistance Center (​ResDAC)](http://www.resdac.org/about-resdac/our-services) at the University of Minnesota.

This work is part of the [Greater Plains Collaborative Reusable Unified Study Environment (GROUSE)](https://informatics.gpcnetwork.org/trac/Project/wiki/GROUSE) project run by the [Greater Plains Collaborative (GPC)](http://gpcnetwork.org/) - a [PCORI Clinical Data Research Network (CDRN)](http://www.pcori.org/research-results/pcornet-national-patient-centered-clinical-research-network/clinical-data-and-0).

At the [University of Kansas Medical Center Division of Medical Informatics](http://www.kumc.edu/ea-mi/medical-informatics.html) we have utilized these scripts with the following environment/tools:

* [Linux (SLES 12)](https://www.suse.com/products/server/)
* [Python 2.7x](https://www.python.org/)
* [Oracle database 12c](https://www.oracle.com/database/index.html)
* [Jenkins](https://jenkins.io/) (to run scripts in a repeatable way while maintaining logs)

The code is organized as follows:

  - [staging](staging/README.md) contains code to decrypt/load the CMS
    data into Oracle from the encrypted archives.
  - [deid](deid/README.md) contains code to create a de-identified
    copy of the CMS tables.
  - [etl_i2b2](etl_i2b2/README.md) contains code to load an [i2b2][] star
    schema from CMS data.

[i2b2]: https://www.i2b2.org/
