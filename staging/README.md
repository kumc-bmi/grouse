## CMS Staging (Loading claims/meta data into Oracle)
### [decrypt_all.py](decrypt_all.py)
Decrypt files provided by the CMS contractor.  There may be over 100 self-decrypting archives in the directory hierarchy included in the CMS delivery package.  Rather than entering the decryption key over and over this script recursively searches for the relevant files, marks them as executable, takes the decryption key from an environment variable and puts the decrypted files in the current working directory.
```
cd /path/to/decrypted/files # Presumably empty until after the following script runs
python /path/to/decrypt_all.py /path/to/encrypted/files DECRYPT_KEY_ENV_VAR_NAME [--dry-run]
```

### [parse_fts.py](parse_fts.py)
Parse decrypted File Transfer Summary documents included in the CMS delivery package to automatically generate:

* Create/drop table Oracle [Data Definition Language (DDL) Statements](https://docs.oracle.com/database/121/SQLRF/statements_1001.htm#SQLRF30001)
* [Oracle control files](https://docs.oracle.com/database/121/ADMIN/control.htm#ADMIN006) for loading the data into Oracle with [SQL*Loader](https://docs.oracle.com/cd/B28359_01/server.111/b28319/ldr_concepts.htm#g1013706)
* [Shell script](https://www.gnu.org/software/bash/) to call `sqlldr` with each data file and control file to load the data into Oracle.
* [CSV](https://en.wikipedia.org/wiki/Comma-separated_values) file (`table_column_description.csv`) containing rows with the table name, column name, and column description from the .fts file.  This is used later to add comments to the automatically generated de-identification SQL code.
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

start oracle_create.sql --This .sql is generated using parse_fts.py
EOF
```
After that, the tables could be loaded with something like the following:
```
bash sqlldr_all.sh  # This shell script is generated with parse_fts.py
```
Note that the above shell script assumes that `$SQLLDR_USER`, `$SQLLDR_PASSWORD`, `$ORACLE_SID` are provided in the environment.

### [parse\_ref_sets.py](parse_ref_sets.py)
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

### [sample\_data.py](sample_data.py)
Create CMS files with some subset of data.  This is useful for quick-iteration testing (for example, de-identification that can take many hours against the full data set).
```
python sample_data.py /path/to/decrypted/cms/files /path/to/empty/sample/directory <number of rows for each sample file>
```