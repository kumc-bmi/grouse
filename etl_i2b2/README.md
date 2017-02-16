# ETL from CMS RIF to i2b2 for GROUSE

GROUSE = *ISSUE look up / cite*
CMS = Center for Medicare Somethingmubmle *ISSUE look up / cite*
RIF = Research Identifyable Files (but note: in GROUSE, they're further obfuscated)


## Usage

To load an i2b2 star schema from the CMS RIF tables, (once dependencies
below are satisfied):

    (grouse-etl)$ PYTHONPATH=. luigi --module cms_etl GrouseETL \
                                     --local-scheduler

Or:

    (grouse-etl)$ ./luigid_run.sh
    (grouse-etl)$ PYTHONPATH=. luigi --workers 4 --module cms_etl GrouseETL

**ISSUE**: why won't luigi find modules in the current directory?

### Data Characterization

Reports akin to tables from the PCORNet Emperical Data
Characterization (EDC) are produced as .csv file byproducts.


### Dependencies

The usual python `requirements.txt` conventions *TODO: cite* apply:

    $ virtualenv ~/pyenv/grouse-etl
    $ . ~/pyenv/grouse-etl/bin/activate
    (grouse-etl)$ pip install -r requirements.txt
