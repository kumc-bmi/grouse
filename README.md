This repository contains scripts to help manage health insurance claims data from the [Center for Medicare and Medicaid Services ​(CMS)](https://www.cms.gov/) obtained through the [Research Data Assistance Center (​ResDAC)](http://www.resdac.org/about-resdac/our-services) at the University of Minnesota.

This work is part of the [Greater Plains Collaborative Reusable Unified Study Environment (GROUSE)](https://informatics.gpcnetwork.org/trac/Project/wiki/GROUSE) project run by the [Greater Plains Collaborative (GPC)](http://gpcnetwork.org/) - a [PCORI Clinical Data Research Network (CDRN)](http://www.pcori.org/research-results/pcornet-national-patient-centered-clinical-research-network/clinical-data-and-0).

At the [University of Kansas Medical Center Division of Medical Informatics](http://www.kumc.edu/ea-mi/medical-informatics.html) we have utilized these scripts with the following environment/tools:

* [Linux (SLES 12)](https://www.suse.com/products/server/)
* [Python 2.7x](https://www.python.org/)
* [Oracle database 12c](https://www.oracle.com/database/index.html)
* [Jenkins](https://jenkins.io/) (to run scripts in a repeatable way while maintaining logs)

##### Staging (Loading claims/meta data into Oracle)
###### [decrypt_all.py](staging/decrypt_all.py)
Decrypt files provided by the CMS contractor.  There may be over 100 self-decrypting archives in the directory hierarchy included in the CMS delivery package.  Rather than entering the decryption key over and over this script recursively searches for the relevant files, marks them as executable, takes the decryption key from an environment variable and puts the decrypted files in the current working directory.
```
cd /path/to/decrypted/files # Presumably empty until after the following script runs
python /path/to/decrypt_all.py /path/to/encrypted/files DECRYPT_KEY_ENV_VAR_NAME [--dry-run]
```

##### [parse_fts.py](parse_fts.py)
Parse decrypted File Transfer Summary documents included in the CMS delivery package to automatically generate:

* Create/drop table Oracle [Data Definition Language (DDL) Statements](https://docs.oracle.com/database/121/SQLRF/statements_1001.htm#SQLRF30001)
* [Oracle control files](https://docs.oracle.com/database/121/ADMIN/control.htm#ADMIN006) for loading the data into Oracle with [SQL*Loader](https://docs.oracle.com/cd/B28359_01/server.111/b28319/ldr_concepts.htm#g1013706)
* [Shell script](https://www.gnu.org/software/bash/) to call `sqlldr` with each data file and control file to load the data into Oracle.
```
python parse_fts.py /path/to/decrypted/files
```
Then, something like the following could be used to drop/create the required tables:
```
sqlplus $SQLLDR_USER/$SQLLDR_PASSWORD@$ORACLE_SID <<EOF

set echo on;

whenever sqlerror continue;
start oracle_drop.sql --This .sql is generated using parse_fts.py

whenever oserror exit 9;
whenever sqlerror exit sql.sqlcode;

select case when qty > 0 then 1/0 else 1 end no_tables from (
  select count(*) qty from (
    select * from user_tables
    )
  );

start oracle_create.sql --This .sql is generated using parse_fts.py
EOF
```
After that, the tables could be loaded with something like the following:
```
bash sqlldr_all.sh  # This shell script is generated with parse_fts.py
```
Note that the above shell script assumes that `$SQLLDR_USER`, `$SQLLDR_PASSWORD`, `$ORACLE_SID` are provided in the environment.

###### [parse\_ref_sets.py](staging/parse_ref_sets.py)
Much like `parse_fts.py`, this script parses the Code Reference Sets Excel file provided in the delivery package and generates Oracle create/drop table scripts, control files, and shell scripts to load the data via `sqlldr`.

Something like the following could be used to drop/create the necessary tables and load the Excel sheets data into Oracle:
```
python parse_ref_sets.py /path/to/code_reference_sets.xlsx
```
```
# Drop/create the reference tables
sqlplus $SQLLDR_USER/$SQLLDR_PASSWORD@$ORACLE_SID <<EOF

set echo on;

whenever sqlerror continue;
start oracle_drop_ref.sql --This .sql is generated using parse_ref_sets.py

whenever oserror exit 9;
whenever sqlerror exit sql.sqlcode;

start oracle_create_ref.sql --This .sql is generated using parse_ref_sets.py
EOF
```
```
# Load the data
bash sqlldr_all_ref.sh --This .sql is generated using parse_ref_sets.py
```
