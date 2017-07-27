## Norms @@

Code should pass tests, style checks, and static type checking:

    $ nosetests && flake8 . && mypy .

## ETL Task and SQL Script Design

The main luigi task is `cms_etl.GrouseETL` and the main modules are:

  - *cms_etl* -- Load an i2b2 star schema from CMS RIF data
  - *etl_tasks* -- Source-agnostic Luigi ETL Task support
  - *script_lib* -- library of SQL scripts
  - *sql_syntax* -- break SQL scripts into statements, etc.

The `GrouseETL` task requires the `Demographics` task, which uses a
few SQL scripts:

  - *cms_dem_txform* - view CMS demographics from an i2b2 lens
  - *cms_dem_load* - load CMS demographics into i2b2 patient dimension
  - *cms_dem_dstats* - Descriptive statistics for CMS Demographics

The output is:

  - data loaded in the i2b2 `patient_dimension` table
  - descriptive statistics in a .csv file
  - intermediate views and tables

The `GrouseRollback` task truncates all the inserted rows and deletes
all the intermediate views and tables.


## SQL Script Library Design, Style and Conventions

Each script should start with a header comment and some
dependency-checking queries. See `script_lib.py` for details.

SQL is written in lowercase, indented 2 spaces, 120 maximum line
length. More details are in the evolving `kumc-bmi-sql-style.xml`
sqldeveloper style profile.

  - *TODO(NG)*: line length in the top level README.md

  - *ISSUE*: sqldeveloper 3 vs. 4 style files?


## Pandas@@ IOU TODO ISSUE @@

### Dry SQL: views of magic strings and numbers

Collect manifest constants in `select ... from dual` views; for
example: `(select active from i2b2_status)` rather than `'A'`.

While doing sub-selects or cross joins with constant views is a little
awkward, it's an idiom we have used for some time and it does seem to
work.

Alternatives considered:

  - PL/SQL inherits a lot from Ada, but not Ada's discriminated types.
  - PL/SQL has object types similar to Java, but exploration
    into scala-style with a subclass for each member showed poor support
    for singletons.
  - PL/SQL has packages of constant functions, but postgres does not
    have packages, so the `pkg.fn` client syntax is not portable.

## Python doctest for story telling and unit testing

Each python module header should tell a story using [doctest][],
i.e. examples that are also unit tests.

You can run them one module at a time:

    (grouse-etl)$ python -m doctest sql_script.py -v
	Trying:
	    Script.cms_patient_mapping.title
	Expecting:
	    'map CMS beneficiaries to i2b2 patients'
	ok
	...
	22 tests in 13 items.
	22 passed and 0 failed.
	Test passed.

Or install [nose][] and run all modules at once:

  (grouse-etl)% nosetests --with-doctest
  ......................
  ----------------------------------------------------------------------
  Ran 22 tests in 0.658s
  
  OK

[doctest]: http://docs.python.org/2/library/doctest.html
[nose]: https://pypi.python.org/pypi/nose/


## Python code style

We appreciate object capability discipline and the "don't call us,
we'll call you" style that facilitates unit testing with mocks.

  - *ISSUE*: Luigi's design doesn't seem to support this idiom.
             Constructors are implict and tasks parameters have to be
             serializable, which works against the usual closure
             object pattern.  Also, the task cache is global mutable
             state.

We avoid mutable state, preferring functional style.

  - *NOTE*: PEP8 tools warn against assinging a lambda to a name,
            suggesting `def` instead. We're fine with it; hence
            `ignore = E731` in `setup.cfg`.


We follow PEP8. The first line of a module or function docstring
should be a short description; if it is blank, either the item is in
an early stage of development or the name and doctests are supposed to
make the purpose obvious.

  - *NOTE*: with static type annotations, the 79 character line
            length limit is awkward; hence we use 99 in `setup.cfg`.

  - *ISSUE*: Dan didn't realize until recently that PEP8 recommends
             triple double quotes over triple single quotes for
             docstrings. He's in the habit of using single quotes
			 to minimize use of the shift key.


## Luigi Troubleshooting

**ISSUE**: why won't luigi find modules in the current directory?
           Use `PYTHONPATH=. luigi ...` if necessary.

Most diagnostics are self-explanatory; `etl_tasks` includes
`SQLScriptError` and `ConnectionProblem` exception classes intended to
improve diagnostics

One challenging diagnostic is:

    RuntimeError: Unfulfilled dependency at run time: DiagnosesLoad_oracle___dconnol_CMS_DEID_SAMPLE_1438246788671_bd6231c982

It seems to indicate that the `.complete()` test on a required task
fails even after that task has been `.run()`. For example, the `select
count(*)` completion test in a load script might have failed because
of incorrect join constraints.
