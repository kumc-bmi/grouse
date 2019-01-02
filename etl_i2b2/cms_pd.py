"""cms_pd -- CMS ETL using pandas

As detailed in the example log below, the steps of an `DataLoadTask` are:

 - Allocate an upload_id and insert an upload_status record
 - Connect and select a rows from the input table bounded by bene_id;
   logging the execution plan first.
 - For each chunk of several thousand of such rows:
   - stack diagnoses and pivot facts
   - map patients and encounters
   - bulk insert into observation_fact_N where N is the upload_id

 - encounter_num: see pat_day_rollup
 - instance_num: preserves correlation with source record ("subencounter")
                 low 3 digits are dx/px number

17:37:02 00 [1] upload job: <oracle://me@db-server/sgrouse>...
17:37:02 00.002542 [1, 2] scalar select I2B2.sq_uploadstatus_uploadid.nextval...
1720 for CarrierClaimUpload #1 of 64; 121712 bene_ids
17:37:02 00.005157 [1, 2] scalar select I2B2.sq_uploadstatus_uploadid.nextval.
17:37:02 00.010149 [1, 3] execute INSERT INTO "I2B2".upload_status ...
17:37:02 00.003866 [1, 3] execute INSERT INTO "I2B2".upload_status .
17:37:02 INFO  1720 for CarrierClaimUpload #-1 of -1; 121712 bene_ids 1...
17:37:02 00.042701 [1, 4] UP#1720: ETL chunk from CMS_DEID.bcarrier_claims...
17:37:02 00.043673 [1, 4, 5] get facts...
17:37:02 00.196869 [1, 4, 5, 6] execute explain plan for SELECT * ...
17:37:02 00.007902 [1, 4, 5, 6] execute explain plan for SELECT * .
17:37:02 00.205344 [1, 4, 5, 7] execute SELECT PLAN_TABLE_OUTPUT line FROM TABLE(DBMS_XPLAN.DISPLAY())...
17:37:02 00.013324 [1, 4, 5, 7] execute SELECT PLAN_TABLE_OUTPUT line FROM TABLE(DBMS_XPLAN.DISPLAY()).
17:37:02 INFO get chunk [1031320, 1]
query: SELECT *
WHERE "CMS_DEID".bcarrier_claims.bene_id BETWEEN :bene_id_1 AND :bene_id_2 plan:
Plan hash value: 2999476641

---------------------------------------------------------------------------------------------------------------------
| Id  | Operation                            | Name                | Rows  | Bytes | Cost (%CPU)| Time     | Inst   |
---------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT REMOTE              |                     |  1113K|   199M|  2013K  (1)| 00:01:19 |        |
|*  1 |  FILTER                              |                     |       |       |            |          |        |
|   2 |   TABLE ACCESS BY INDEX ROWID BATCHED| BCARRIER_CLAIMS     |  1113K|   199M|  2013K  (1)| 00:01:19 | SGROU~ |
|*  3 |    INDEX RANGE SCAN                  | CMS_IX_BCACLA_BENID |  2009K|       |  5406   (1)| 00:00:01 | SGROU~ |
---------------------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

   1 - filter(:BENE_ID_2>=:BENE_ID_1)
   3 - access("BCARRIER_CLAIMS"."BENE_ID">=:BENE_ID_1 AND "BCARRIER_CLAIMS"."BENE_ID"<=:BENE_ID_2)

Note
-----
   - fully remote statement

17:37:02 00.224509 [1, 4, 5, 8] UP#1720: select from CMS_DEID.bcarrier_claims...
17:37:09 06.749958 [1, 4, 5, 8] UP#1720: select from CMS_DEID.bcarrier_claims
 + 100000 rows = 100000 for 1399 (1.15%) of 121712 bene_ids.
17:37:09 06.975701 [1, 4, 5, 9] stack diagnoses from 100000 CMS_DEID.bcarrier_claims records...
17:37:12 02.963609 [1, 4, 5, 9] stack diagnoses from 100000 CMS_DEID.bcarrier_claims records 333580 diagnoses.
17:37:12 09.940432 [1, 4, 5, 10] pivot facts from 100000 CMS_DEID.bcarrier_claims records...
17:37:22 10.808200 [1, 4, 5, 10] pivot facts from 100000 CMS_DEID.bcarrier_claims records 2767740 total observations.
17:37:23 21.472786 [1, 4, 5, 11] mapping 2767740 facts...
17:37:23 21.474279 [1, 4, 5, 11, 12] read_sql select patient_ide bene_id, patient_num from I2B2.patient_mapping
{'bene_id_last': '1001496', 'bene_id_first': '1', 'patient_ide_source': 'ccwdata.org(BENE_ID)'}...
17:37:23 00.315361 [1, 4, 5, 11, 12] read_sql select patient_ide bene_id, patient_num from I2B2.patient_mapping
{'bene_id_last': '1001496', 'bene_id_first': '1', 'patient_ide_source': 'ccwdata.org(BENE_ID)'}.
17:37:24 22.876878 [1, 4, 5, 11, 13] read_sql select medpar.medpar_id, medpar.bene_id, emap.encounter_num
{'bene_id_last': '1001496', 'bene_id_first': '1', 'encounter_ide_source': 'ccwdata.org(MEDPAR_ID)'}...
17:37:25 00.389943 [1, 4, 5, 11, 13] read_sql select medpar.medpar_id, medpar.bene_id, emap.encounter_num
{'bene_id_last': '1001496', 'bene_id_first': '1', 'encounter_ide_source': 'ccwdata.org(MEDPAR_ID)'}.
17:37:41 17.737663 [1, 4, 5, 11] mapping 2767740 facts pmap: 16627 emap: 2121.
17:37:43 40.980482 [1, 4, 5] get facts 2767740 facts.
17:37:43 41.025014 [1, 4, 14] UP#1720: bulk insert 2767740 rows into observation_fact_1720...
17:38:17 34.165037 [1, 4, 14] UP#1720: bulk insert 2767740 rows into observation_fact_1720.
17:38:17 01:15.148141 [1, 4] UP#1720: ETL chunk from CMS_DEID.bcarrier_claims.
17:38:17 01:15.191470 [1] upload job: <oracle://me@db-server/sgrouse>


Pivoting CMS RIF Data to i2b2 Observation Facts
-----------------------------------------------

We curate information about relevant columns, e.g. for MEDPAR_ALL::

    >>> CMSVariables.curated_info
    'metadata/active_columns.csv'
    >>> col_info = MEDPAR_Upload.active_col_data()
    >>> col_info[['Status', 'column_name', 'valtype_cd', 'dxpx', 'ix']][::4].set_index('column_name')
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
                            Status valtype_cd       dxpx    ix
    column_name
    bene_id                      A          T         NaN   NaN
    bene_age_cnt                 A          N         NaN   NaN
    prvdr_num_spcl_unit_cd       A          @         NaN   NaN
    ltst_clm_acrtn_dt          NaN          D         NaN   NaN
    src_ip_admsn_cd              A          @         NaN   NaN
    dschrg_dt                 i2b2          D         NaN   NaN
    ...
    dgns_vrsn_cd_1               A          @   DGNS_VRSN   1.0
    dgns_vrsn_cd_5               A          @   DGNS_VRSN   5.0
    ...
    dgns_4_cd                    A          @     DGNS_CD   4.0
    dgns_8_cd                    A          @     DGNS_CD   8.0
    ...
    srgcl_prcdr_vrsn_cd_20       A          @  PRCDR_VRSN  20.0
    srgcl_prcdr_vrsn_cd_24       A          @  PRCDR_VRSN  24.0
    srgcl_prcdr_3_cd             A          @    PRCDR_CD   3.0
    srgcl_prcdr_7_cd             A          @    PRCDR_CD   7.0
    srgcl_prcdr_11_cd            A          @    PRCDR_CD  11.0
    srgcl_prcdr_15_cd            A          @    PRCDR_CD  15.0
    srgcl_prcdr_19_cd            A          @    PRCDR_CD  19.0
    srgcl_prcdr_23_cd            A          @    PRCDR_CD  23.0
    srgcl_prcdr_prfrm_1_dt       A          D    PRCDR_DT   1.0
    srgcl_prcdr_prfrm_5_dt       A          D    PRCDR_DT   5.0
    ...

Let's double-check the breakdown by `valtype_cd`::

    >>> col_info[~col_info.Status.isnull()].groupby('valtype_cd')[['column_name']].count()
    ... # doctest: +NORMALIZE_WHITESPACE
                column_name
    valtype_cd
    @                   194
    D                    31
    N                    38
    T                     5

Suppose we read a block of MEDPAR_ALL records::

    >>> rif_data, col_info, simple_cols = _RIFTestData.build(MEDPAR_Upload)
    >>> all(rif_data.columns == col_info.column_name)
    True
    >>> rif_data.set_index(['bene_id', 'medpar_id'])[['bene_age_cnt', 'utlztn_day_cnt']]
    ... # doctest: +NORMALIZE_WHITESPACE
                                  bene_age_cnt  utlztn_day_cnt
    bene_id         medpar_id
    47PZ1AN7X       5086687R53              67            1258
    31EY60688L      8V4FZ36                 81             427
    5R0PECV         Z7N452HIJN4             86            5326
    LMJ541WFYP2D26  17Z0BX5                 47            4504
    0353RF08208HK1N 875JJPV91C13            39            1840

In the simple case, we make each column value an i2b2 observation fact
using the column name as the concept code. The instance num is used to
correlate observations from the same source (MEDPAR_ALL) record::

    >>> pd.set_option('display.width', 120)  # cf. setup.cfg

    >>> obs_num = MEDPAR_Upload.pivot_valtype(
    ...     Valtype.numeric, rif_data, MEDPAR_Upload.table_name, simple_cols)
    >>> len(obs_num)
    170
    >>> obs_num.sort_values(['instance_num', 'concept_cd']
    ...     ).set_index(['bene_id', 'medpar_id', 'instance_num'])[
    ...                                ['start_date', 'provider_id', 'concept_cd', 'nval_num']][::13][:6]
    ... # doctest: +NORMALIZE_WHITESPACE
                                       start_date      provider_id               concept_cd nval_num
    bene_id   medpar_id  instance_num
    47PZ1AN7X  5086687R53 0             1997-01-08       N5CV2LX74U     ADMSN_DEATH_DAY_CNT:     3575
                          0             1997-01-08       N5CV2LX74U             ER_CHRG_AMT:     5655
                          0             1997-01-08       N5CV2LX74U    PROFNL_FEES_CHRG_AMT:     3590
    31EY60688L 8V4FZ36    1000          1970-09-11  9P0WBJ3I62GR86I      BENE_PRMRY_PYR_AMT:     6049
                          1000          1970-09-11  9P0WBJ3I62GR86I     MDCL_SUPLY_CHRG_AMT:     2649
                          1000          1970-09-11  9P0WBJ3I62GR86I  STAY_FINL_ACTN_CLM_CNT:     9949

    >>> obs_txt = MEDPAR_Upload.pivot_valtype(
    ...     Valtype.text, rif_data, MEDPAR_Upload.table_name, simple_cols)
    >>> len(obs_txt)
    10
    >>> obs_txt.sort_values(['instance_num', 'concept_cd']
    ...     ).set_index(['bene_id', 'medpar_id', 'instance_num'])[
    ...     ['start_date', 'provider_id', 'concept_cd', 'tval_char']][:4]
    ... # doctest: +NORMALIZE_WHITESPACE
                                        start_date      provider_id       concept_cd       tval_char
    bene_id   medpar_id  instance_num
    47PZ1AN7X  5086687R53 0             1997-01-08       N5CV2LX74U       PRVDR_NUM:       8086SOV68
                          0             1997-01-08       N5CV2LX74U  UNIQ_TRKNG_NUM:     ENQXWNYGMC2
    31EY60688L 8V4FZ36    1000          1970-09-11  9P0WBJ3I62GR86I       PRVDR_NUM:  IGLHF9R0Q3TPD2
                          1000          1970-09-11  9P0WBJ3I62GR86I  UNIQ_TRKNG_NUM:      232L0C379F

    >>> obs_dt = MEDPAR_Upload.pivot_valtype(
    ...     Valtype.date, rif_data, MEDPAR_Upload.table_name, simple_cols)
    >>> len(obs_dt)
    20
    >>> obs_dt.sort_values(['instance_num', 'concept_cd']
    ...     ).set_index(['bene_id', 'medpar_id', 'instance_num'])[
    ...                                ['start_date', 'provider_id', 'concept_cd', 'tval_char']][:8]
    ... # doctest: +NORMALIZE_WHITESPACE
                                        start_date      provider_id                concept_cd   tval_char
    bene_id   medpar_id  instance_num
    47PZ1AN7X  5086687R53 0             1984-11-26       N5CV2LX74U            BENE_DEATH_DT:  1984-11-26
                          0             1987-04-09       N5CV2LX74U  BENE_MDCR_BNFT_EXHST_DT:  1987-04-09
                          0             1977-10-06       N5CV2LX74U        SNF_QUALN_FROM_DT:  1977-10-06
                          0             1992-05-03       N5CV2LX74U        SNF_QUALN_THRU_DT:  1992-05-03
    31EY60688L 8V4FZ36    1000          1996-09-13  9P0WBJ3I62GR86I            BENE_DEATH_DT:  1996-09-13
                          1000          1977-06-12  9P0WBJ3I62GR86I  BENE_MDCR_BNFT_EXHST_DT:  1977-06-12
                          1000          1978-02-09  9P0WBJ3I62GR86I        SNF_QUALN_FROM_DT:  1978-02-09
                          1000          1979-11-19  9P0WBJ3I62GR86I        SNF_QUALN_THRU_DT:  1979-11-19

The `concept_cd` consists of the value as well as the column name for coded values::

    >>> obs_coded = MEDPAR_Upload.pivot_valtype(
    ...     Valtype.coded, rif_data, MEDPAR_Upload.table_name, simple_cols)
    >>> obs_coded.sort_values(['instance_num', 'concept_cd']
    ...     ).set_index(['bene_id', 'medpar_id', 'instance_num'])[
    ...     ['start_date', 'provider_id', 'concept_cd']][::12][:6]
    ... # doctest: +NORMALIZE_WHITESPACE
                                        start_date      provider_id                  concept_cd
    bene_id   medpar_id  instance_num
    47PZ1AN7X  5086687R53 0             1997-01-08       N5CV2LX74U            ADMSN_DAY_CD:DX9
                          0             1997-01-08       N5CV2LX74U            ICU_IND_CD:VY112
                          0             1997-01-08       N5CV2LX74U  RDLGY_OTHR_IMGNG_IND_SW:MR
    31EY60688L 8V4FZ36    1000          1970-09-11  9P0WBJ3I62GR86I      DRG_OUTLIER_STAY_CD:3Z
                          1000          1970-09-11  9P0WBJ3I62GR86I             PA_IND_CD:SFGZ4
                          1000          1970-09-11  9P0WBJ3I62GR86I        SS_LS_SNF_IND_CD:NA4


Multiple Diagnoses, Procedures per record
=========================================

Multiple diagnoses per record are represented by groups of related
columns::

    >>> dx_cols = MEDPAR_Upload.vrsn_cd_groups(col_info, kind='DGNS', aux='DGNS_IND')
    >>> dx_cols
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
                       column_name_vrsn    column_name     column_name_ind
    mod_grp    ix
    ADMTG_DGNS 0.0   admtg_dgns_vrsn_cd  admtg_dgns_cd                 NaN
    DGNS       1.0       dgns_vrsn_cd_1      dgns_1_cd   poa_dgns_1_ind_cd
               2.0       dgns_vrsn_cd_2      dgns_2_cd   poa_dgns_2_ind_cd
               3.0       dgns_vrsn_cd_3      dgns_3_cd   poa_dgns_3_ind_cd
    ...
    DGNS_E     1.0     dgns_e_vrsn_cd_1    dgns_e_1_cd                 NaN
               2.0     dgns_e_vrsn_cd_2    dgns_e_2_cd                 NaN
               3.0     dgns_e_vrsn_cd_3    dgns_e_3_cd                 NaN
    ...
               12.0   dgns_e_vrsn_cd_12   dgns_e_12_cd                 NaN

Likewise groups of columns for procedures::

    >>> px_cols = MEDPAR_Upload.vrsn_cd_groups(col_info, kind='PRCDR', aux='PRCDR_DT')
    >>> px_cols
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
                            column_name_vrsn        column_name           column_name_dt
    mod_grp     ix
    SRGCL_PRCDR 1.0    srgcl_prcdr_vrsn_cd_1   srgcl_prcdr_1_cd   srgcl_prcdr_prfrm_1_dt
                2.0    srgcl_prcdr_vrsn_cd_2   srgcl_prcdr_2_cd   srgcl_prcdr_prfrm_2_dt
                3.0    srgcl_prcdr_vrsn_cd_3   srgcl_prcdr_3_cd   srgcl_prcdr_prfrm_3_dt
    ...
                25.0  srgcl_prcdr_vrsn_cd_25  srgcl_prcdr_25_cd  srgcl_prcdr_prfrm_25_dt

The diagnosis codes have separate version columns.::

    >>> rif_data.set_index(['bene_id', 'medpar_id'])[dx_cols.loc[('ADMTG_DGNS', 0.0)].dropna()]
    ... # doctest: +NORMALIZE_WHITESPACE
                                 admtg_dgns_vrsn_cd admtg_dgns_cd
    bene_id         medpar_id
    47PZ1AN7X       5086687R53                    9         59197
    31EY60688L      8V4FZ36                       9         37258
    5R0PECV         Z7N452HIJN4                   9         10409
    LMJ541WFYP2D26  17Z0BX5                      10         62215
    0353RF08208HK1N 875JJPV91C13                  9          3479

Some dianoses have present-on-admission flags::

    >>> rif_data.set_index(['bene_id', 'medpar_id'])[dx_cols.loc[('DGNS', 1.0)]]
    ... # doctest: +NORMALIZE_WHITESPACE
                                 dgns_vrsn_cd_1 dgns_1_cd poa_dgns_1_ind_cd
    bene_id         medpar_id
    47PZ1AN7X       5086687R53                9     48261
    31EY60688L      8V4FZ36                   9     42169                 U
    5R0PECV         Z7N452HIJN4               9     26392                 Z
    LMJ541WFYP2D26  17Z0BX5                  10     51147
    0353RF08208HK1N 875JJPV91C13              9     35407                 Y

When we make i2b2 observation facts, we combine diagnosis version and
code into `concept_cd` (inserting the decimal point as expected by the
i2b2 ontologies we use) and use the low-order 3 digits to distinguish
diagnoses::

    >>> obs_dx = MEDPAR_Upload.dx_data(rif_data, MEDPAR_Upload.table_name, dx_cols)
    >>> obs_dx.sort_values('instance_num').set_index(['bene_id', 'admsn_dt', 'instance_num'])[
    ...                             ['provider_id', 'dgns_vrsn', 'dgns_cd', 'concept_cd']][::5][:8]
    ... # doctest: +NORMALIZE_WHITESPACE
                                             provider_id dgns_vrsn dgns_cd   concept_cd
    bene_id         admsn_dt   instance_num
    47PZ1AN7X  1997-01-08 0                  N5CV2LX74U         9   59197   ICD9:591.97
                          5                  N5CV2LX74U         9   18389   ICD9:183.89
                          28                 N5CV2LX74U         9   26358   ICD9:263.58
    31EY60688L 1970-09-11 1002          9P0WBJ3I62GR86I        10   54236  ICD10:542.36
                          1027          9P0WBJ3I62GR86I         9   22382   ICD9:223.82
                          1032          9P0WBJ3I62GR86I        10   37241  ICD10:372.41
    5R0PECV    1998-07-06 2003             6EW010SWQ9M6         9    8470    ICD9:847.0
                          2008             6EW010SWQ9M6         9   12227   ICD9:122.27

We include primary diagnosis and admitting diagnosis info in `modifier_cd`::

    >>> obs_dx.sort_values('instance_num').set_index(['bene_id', 'start_date', 'instance_num'])[
    ...                             ['concept_cd', 'mod_grp', 'x', 'modifier_cd']][::10]
    ... # doctest: +NORMALIZE_WHITESPACE
                                               concept_cd     mod_grp    x    modifier_cd
    bene_id         start_date instance_num
    47PZ1AN7X       1997-01-08 0              ICD9:591.97  ADMTG_DGNS  0.0  DX:ADMTG_DGNS
                               28             ICD9:263.58      DGNS_E  3.0      DX:DGNS_E
    31EY60688L      1970-09-11 1027           ICD9:223.82      DGNS_E  2.0      DX:DGNS_E
    5R0PECV         1998-07-06 2003            ICD9:847.0        DGNS  3.0        DX:DGNS
                               2029           ICD9:494.44      DGNS_E  4.0      DX:DGNS_E
    LMJ541WFYP2D26  1980-10-16 3004          ICD10:831.90        DGNS  4.0        DX:DGNS
                               3033          ICD10:582.14      DGNS_E  8.0      DX:DGNS_E
    0353RF08208HK1N 1992-05-18 4028           ICD9:611.71      DGNS_E  3.0      DX:DGNS_E

Procedures follow the same pattern::

    >>> rif_data.loc[0, 'srgcl_prcdr_2_cd'] = None  # sometimes procedure data is missing

    >>> obs_px = MEDPAR_Upload.px_data(rif_data, MEDPAR_Upload.table_name, px_cols)
    >>> obs_px.sort_values('instance_num').set_index(['bene_id', 'admsn_dt', 'instance_num'])[
    ...                             ['start_date', 'prcdr_vrsn', 'prcdr_cd', 'concept_cd']]
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
                                             start_date prcdr_vrsn prcdr_cd   concept_cd
    bene_id         admsn_dt   instance_num
    47PZ1AN7X       1997-01-08 0             2004-07-17         98    A880Y  ICD9:A8.80Y
                               2             2009-09-26       1WLR    47KI3  ICD9:47.KI3
                               3             2007-10-14      ZDM84       5T      ICD9:5T
    ...


Medicaid Inpatient
==================

Again, we use our curated column data to build test data and pivot the
coded data::

    >>> rif_data, col_info, simple_cols = _RIFTestData.build(MAXDATA_IP_Upload)

Medicaid diagnoses have an implicit diagnosis code version::

    >>> dx_cols = MAXDATA_IP_Upload.vrsn_cd_groups(col_info, kind='DGNS', aux='DGNS_IND')
    >>> obs_dx = MAXDATA_IP_Upload.dx_data(rif_data, MAXDATA_IP_Upload.table_name, dx_cols)
    >>> obs_dx.sort_values('instance_num').set_index(['bene_id', 'instance_num'])[
    ...                             ['start_date', 'provider_id', 'dgns_cd', 'x', 'concept_cd']][::3][:6]
    ... # doctest: +NORMALIZE_WHITESPACE
                           start_date      provider_id dgns_cd    x   concept_cd
    bene_id   instance_num
    47PZ1AN7X 0            1980-03-09  LX74U2086SOV686    OPD4  1.0   ICD9:OPD.4
              3            1980-03-09  LX74U2086SOV686   8H6VB  4.0  ICD9:8H6.VB
              6            1980-03-09  LX74U2086SOV686      9F  7.0      ICD9:9F
    4OKC5DG   1000         1993-03-17  2N047RRRAK4AS0S    FT1F  1.0   ICD9:FT1.F
              1003         1993-03-17  2N047RRRAK4AS0S      97  4.0      ICD9:97
              1006         1993-03-17  2N047RRRAK4AS0S     X3S  7.0     ICD9:X3S


The MAXDATA_IP table has only one procedure date column::

    >>> px_cols = MAXDATA_IP_Upload.vrsn_cd_groups(col_info, kind='PRCDR', aux='PRCDR_DT')
    >>> px_cols
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
                column_name_vrsn column_name   column_name_dt
    mod_grp ix
    PRCDR   1.0   prcdr_cd_sys_1  prcdr_cd_1  prncpl_prcdr_dt
            2.0   prcdr_cd_sys_2  prcdr_cd_2              NaN
            3.0   prcdr_cd_sys_3  prcdr_cd_3              NaN
            4.0   prcdr_cd_sys_4  prcdr_cd_4              NaN
            5.0   prcdr_cd_sys_5  prcdr_cd_5              NaN
            6.0   prcdr_cd_sys_6  prcdr_cd_6              NaN


    >>> obs_px = MAXDATA_IP_Upload.px_data(rif_data, MAXDATA_IP_Upload.table_name, px_cols)
    >>> obs_px.sort_values('instance_num').set_index(['bene_id', 'instance_num'])[
    ...                             ['start_date', 'prcdr_vrsn', 'prcdr_cd', 'concept_cd']][::2]
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
                                start_date prcdr_vrsn prcdr_cd   concept_cd
    bene_id       instance_num
    47PZ1AN7X     0             1988-09-16         TJ       3Z      ICD9:3Z
                  2             1988-09-16        4TL     DHE5   ICD9:DH.E5
                  4             1988-09-16        4J7     0A1D   ICD9:0A.1D
    4OKC5DG       1000          1989-10-25       Z8PD      GZ0    ICD9:GZ.0
                  1002          1989-10-25       A7LR    ZDM84  ICD9:ZD.M84
                  1004          1989-10-25      K7466     15ZV   ICD9:15.ZV
    ...

"""

from io import StringIO
import logging
from random import Random
from typing import (
    Any, Iterable, Iterator, List, Dict, Optional as Opt,
    Tuple, Type, TypeVar, cast)
import enum

import cx_ora_fix; cx_ora_fix.patch_version()  # noqa: E702

import luigi
import numpy as np  # type: ignore
import pandas as pd  # type: ignore
import pkg_resources as pkg
import sqlalchemy as sqla

from cms_etl import FromCMS, DBAccessTask, BeneIdSurvey, PatientMapping, MedparMapping
from etl_tasks import (
    LoggedConnection, LogState,
    SqlScriptTask, ReportTask, UploadTarget, UploadTask,
    make_url, log_plan
)
import param_val as pv
from param_val import IntParam, StrParam
from script_lib import Script
from sql_syntax import Params

log = logging.getLogger(__name__)
T = TypeVar('T')
ListParam = pv._valueOf([1000], luigi.ListParameter)


class CMSRIFLoad(luigi.WrapperTask):
    medicare_years = ListParam()
    medicaid_years = ListParam()

    def requires(self) -> List[luigi.Task]:
        care = [MedicareYear(year=y) for y in self.medicare_years]  # type: List[luigi.Task]
        caid = [MedicaidYear(year=y) for y in self.medicaid_years]  # type: List[luigi.Task]
        return care + caid


class _LoadTask(FromCMS, DBAccessTask):
    @property
    def label(self) -> str:
        raise NotImplementedError

    @property
    def input_label(self) -> str:
        raise NotImplementedError

    def output(self) -> luigi.Target:
        return self._upload_target()

    def _upload_target(self) -> 'UploadTarget':
        return UploadTarget(self._make_url(self.account),
                            self.project.upload_table,
                            self.task_id, self.source,
                            echo=self.echo)

    def run(self) -> None:
        upload = self._upload_target()
        with upload.job(self,
                        label=self.label,
                        user_id=make_url(self.account).username) as conn_id_r:
            lc, upload_id, result = conn_id_r
            self.load(lc, upload, upload_id, result)

    def load(self, lc: LoggedConnection, upload: 'UploadTarget', upload_id: int, result: Params) -> None:
        raise NotImplementedError('subclass must implement')

    def with_admin(self, detail: pd.DataFrame, upload_id: int,
                   lc: LoggedConnection, table_info: sqla.Table) -> pd.DataFrame:
        current_time = pd.read_sql(sqla.select([sqla.func.current_timestamp()]),
                                   lc._conn).iloc[0][0]
        out = detail[[col.name for col in table_info.columns
                      if col.name in detail.columns.values]].copy()
        out['sourcesystem_cd'] = self.source.source_cd.replace("'", '')  # kludgy
        out['download_date'] = self.source.download_date
        out['upload_id'] = upload_id
        out['import_date'] = current_time
        return out


class DataLoadTask(_LoadTask):
    def load(self, lc: LoggedConnection, upload: 'UploadTarget', upload_id: int, result: Params) -> None:
        # Use create table as ... select * from ... to be sure columns are compatible
        # for partition exchange.
        lc.execute('''
        create table observation_fact_{upload_id} as
        select * from {i2b2star}.observation_fact where 1=0
        '''.format(upload_id=upload_id, i2b2star=self.project.star_schema))
        fact_table = sqla.Table('observation_fact_%s' % upload_id,
                                sqla.MetaData(), auto_load=True, autoload_with=lc._conn)
        fact_dtype = {c.name: c.type for c in fact_table.columns
                      if not c.name.endswith('_blob')}
        bulk_rows = 0
        obs_fact_chunks = self.obs_data(lc, upload_id)
        while 1:
            with lc.log.step('UP#%(upload_id)d: %(event)s from %(input)s',
                             dict(event='ETL chunk', upload_id=upload_id,
                                  input=self.input_label)):
                with lc.log.step('%(event)s',
                                 dict(event='get facts')) as step1:
                    try:
                        obs_fact_chunk, pct_in = next(obs_fact_chunks)
                    except StopIteration:
                        break
                    step1.msg_parts.append(' %(fact_qty)s facts')
                    step1.argobj.update(dict(fact_qty=len(obs_fact_chunk)))
                with lc.log.step('UP#%(upload_id)d: %(event)s %(rowcount)d rows into %(into)s',
                                 dict(event='bulk insert',
                                      upload_id=upload_id,
                                      into=fact_table.name,
                                      rowcount=len(obs_fact_chunk))) as insert_step:
                    obs_fact_chunk = _check_start_date(obs_fact_chunk,
                                                       threshold=(0.01, cast(logging.Logger, lc.log)))
                    obs_fact_chunk.to_sql(name=fact_table.name,
                                          con=lc._conn,
                                          dtype=fact_dtype,
                                          if_exists='append', index=False)
                    bulk_rows += len(obs_fact_chunk)
                    insert_step.argobj.update(dict(rowsubtotal=bulk_rows))
                    insert_step.msg_parts.append(
                        ' (subtotal: %(rowsubtotal)d)')

                # report progress via the luigi scheduler and upload_status table
                _start, elapsed, elapsed_ms = lc.log.elapsed()
                eta = lc.log.eta(pct_in)
                message = ('UP#%(upload_id)d %(pct_in)0.2f%% eta %(eta)s '
                           'loaded %(bulk_rows)d rows @%(rate_out)0.2fK/min %(elapsed)s') % dict(
                    upload_id=upload_id, pct_in=pct_in, eta=eta.strftime('%a %d %b %H:%M'),
                    bulk_rows=bulk_rows, elapsed=elapsed,
                    rate_out=bulk_rows / 1000.0 / (elapsed_ms / 1000000.0 / 60))
                # self.set_status_message(message)
                lc.execute(upload.table.update()
                           .where(upload.table.c.upload_id == upload_id)
                           .values(loaded_record=bulk_rows, end_date=eta,
                                   message=message))
        result[upload.table.c.loaded_record.name] = bulk_rows

    def obs_data(self, lc: LoggedConnection, upload_id: int) -> Iterator[Tuple[pd.DataFrame, float]]:
        raise NotImplementedError


def read_sql_step(sql: str, lc: LoggedConnection,
                  params: Opt[Params]=None, show_lines: int=1) -> pd.DataFrame:
    with lc.log.step('%(event)s %(sql1)s' + ('\n%(params)s' if params else ''),
                     dict(event='read_sql', sql1=_nlines(str(sql), show_lines), params=params)):
        return pd.read_sql(sql, lc._conn, params=params or {})


def _nlines(s: str, n: int) -> str:
    return '\n'.join(s.strip().split('\n')[:n])


class BeneMapped(DataLoadTask):
    '''Choose patient_num based on bene_id.
    '''
    def requires(self) -> List[luigi.Task]:
        return [PatientMapping()]

    def complete(self) -> bool:
        return (self.output().exists() and
                all(task.complete() for task in self.requires()))

    def ide_source(self, key_cols: str) -> str:
        source_cd = self.source.source_cd[1:-1]  # strip quotes
        return source_cd + key_cols

    def patient_mapping(self, lc: LoggedConnection,
                        bene_range: Tuple[int, int],
                        debug_plan: bool=False,
                        key_cols: str='(BENE_ID)') -> pd.DataFrame:
        q = '''select patient_ide bene_id, patient_num from %(I2B2STAR)s.patient_mapping
        where patient_ide_source = :patient_ide_source
        and patient_ide between :bene_id_first and :bene_id_last
        ''' % dict(I2B2STAR=self.project.star_schema)

        params = dict(patient_ide_source=self.ide_source(key_cols),
                      bene_id_first=bene_range[0],
                      bene_id_last=bene_range[1])  # type: Params
        if debug_plan:
            log_plan(lc, event='patient_mapping', sql=q, params=params)
        return read_sql_step(q, lc, params=params)


class MedparMapped(BeneMapped):
    '''Choose encounters from medpar (inpatient) if available else
    patient-day.
    '''

    def requires(self) -> List[luigi.Task]:
        return BeneMapped.requires(self) + [
            MedparMapping()]

    def encounter_mapping(self, lc: LoggedConnection,
                          bene_range: Tuple[int, int],
                          debug_plan: bool=False,
                          key_cols: str='(MEDPAR_ID)') -> pd.DataFrame:
        q = '''select medpar.medpar_id, medpar.bene_id, emap.encounter_num
                    , medpar.admsn_dt, medpar.dschrg_dt
        from %(CMS_RIF)s.medpar_all medpar
        join %(I2B2STAR)s.encounter_mapping emap on emap.encounter_ide = medpar.medpar_id
        where medpar.bene_id between :bene_id_first and :bene_id_last
          and emap.patient_ide between :bene_id_first and :bene_id_last
          and emap.encounter_ide_source = :encounter_ide_source
        order by medpar.medpar_id, emap.encounter_num
        ''' % dict(I2B2STAR=self.project.star_schema,
                   CMS_RIF=self.source.cms_rif)

        params = dict(encounter_ide_source=self.ide_source(key_cols),
                      bene_id_first=bene_range[0],
                      bene_id_last=bene_range[1])  # type: Params

        if debug_plan:
            log_plan(lc, event='patient_mapping', sql=q, params=params)

        out = read_sql_step(q, lc, params=params)
        dups = out.medpar_id.duplicated()
        if any(dups):
            lc.log.warn('duplicates: %d out of %d', len(dups[dups]), len(out))  # type: ignore
            out = out[~dups].reset_index()
        return out

    @classmethod
    def pat_day_rollup(cls, data: pd.DataFrame, medpar_mapping: pd.DataFrame) -> pd.DataFrame:
        """
        :param data: with bene_id, start_date, and optionally medpar_id
        :param medpar_mapping: with medpar_id, encounter_num, admsn_dt, dschrg_dt

        Note medpar_mapping.sql ensures encounter_num > 0 when assigned to a medpar_id.
        """
        out = data.reset_index().copy()
        out['start_day'] = pd.to_datetime(out.start_date, unit='D')
        pat_day = out[['bene_id', 'start_day']].drop_duplicates()

        # assert(medpar_mapping is 1-1 from medpar_id to encounter_num)
        pat_enc = pat_day.merge(medpar_mapping, on='bene_id', how='left')

        pat_enc = pat_enc[(pat_enc.start_day >= pat_enc.admsn_dt) &
                          (pat_enc.start_day <= pat_enc.dschrg_dt)]
        pat_enc = pat_enc.set_index(['bene_id', 'start_day'])  # [['encounter_num', 'medpar_id']]
        pat_enc = pat_enc[~pat_enc.index.duplicated(keep='first')]
        out = out.merge(pat_enc, how='left', left_on=['bene_id', 'start_day'], right_index=True)
        if len(out) != len(data):
            log.warn('pat_day_rollup lost: %d',
                     len(data) - len(out))

        # ISSUE: hash is not portable between python and Oracle
        fallback = - cls.fmt_patient_day(out).apply(hash).abs()
        out.encounter_num = out.encounter_num.fillna(fallback)

        return out

    @classmethod
    def fmt_patient_day(cls, df: pd.DataFrame) -> pd.Series:
        return df.start_date.dt.strftime('%Y-%m-%d') + ' ' + df.bene_id

    def with_mapping(self, data: pd.DataFrame,
                     pmap: pd.DataFrame, emap: pd.DataFrame) -> pd.DataFrame:
        """
        @param pmap: patient_num for every bene_id in data
        """
        obs_qty = len(data)
        obs = data.merge(pmap, on=CMSVariables.bene_id)
        if len(obs) != obs_qty:
            log.warn('patient_mapping on bene_id lost: %d: %s...',
                     obs_qty - len(obs),
                     data[~(data.bene_id.isin(obs.bene_id))].head(3))

        if 'medpar_id' in data.columns.values:
            obs = obs.merge(emap[['medpar_id', 'encounter_num']], on='medpar_id', how='left')
        else:
            obs = self.pat_day_rollup(obs, emap)
        if len(obs) != obs_qty:
            log.warn('encounter_mapping on bene_id lost: %d',
                     obs_qty - len(obs))

        if 'provider_id' in obs.columns.values:
            obs.provider_id = obs.provider_id.where(~obs.provider_id.isnull(), '@')
        else:
            obs['provider_id'] = '@'

        return obs


class CMSVariables(object):
    r'''CMS Variables are more or less the same as SQL columns.

    We curate active columns (variables):

    >>> CMSVariables.active_columns('PDE')[
    ...     ['Status', 'table_name', 'column_name', 'description']].head(2)
        Status table_name column_name                   description
    914      A        pde      PDE_ID          Encrypted 723 PDE ID
    915    dim        pde     BENE_ID  Encrypted 723 Beneficiary ID

    We relate columns to i2b2 `valtype_cd` typically by SQL type but
    subclasses may use `valtype_override` to map column_name (matched
    by regexp) to valtype_cd.

    Columns in the range of `cls.i2b2_map` get valtype_cd = np.nan, indicating
    that they don't serve as facts.
    '''
    i2b2_map = {
        'patient_ide': 'bene_id',
        'start_date': 'clm_from_dt',
        'end_date': 'clm_thru_dt',
        'update_date': 'nch_wkly_proc_dt'}

    bene_id = 'bene_id',
    medpar_id = 'medpar_id'

    pdx = 'prncpal_dgns_cd'

    """Tables all have less than 10^3 columns."""
    max_cols_digits = 3

    """Columns shorter than this are treated as codes. """
    code_max_len = 7

    valtype_override = []  # type: List[Tuple[str, str]]
    concept_scheme_override = {'hcpcs_cd': 'HCPCS'}
    _mute_unused_warning = Dict

    curated_info = 'metadata/active_columns.csv'
    _active_columns = pkg.resource_string(__name__, curated_info)

    @classmethod
    def active_columns(cls, table_name: str,
                       extras: Iterable[str]=[],
                       active: str='A') -> pd.DataFrame:
        col_info = pd.read_csv(StringIO(cls._active_columns.decode('utf-8')))
        return col_info[(col_info.table_name == table_name.lower()) &
                        (~col_info.Status.isnull() |
                         col_info.column_name.str.lower().isin(extras))]

    @classmethod
    def column_properties(cls, info: pd.DataFrame) -> pd.DataFrame:
        '''Relate columns (variables) to i2b2 `valtype_cd`.
        '''
        info['valtype_cd'] = [col_valtype(c).value for c in info.column.values]

        for cd, pat in cls.valtype_override:
            info.valtype_cd = info.valtype_cd.where(~ info.column_name.str.match(pat, as_indexer=True), cd)
        info.loc[info.column_name.isin(cls.i2b2_map.values()), 'valtype_cd'] = np.nan

        return info.drop('column', 1)


def rif_modifier(table_name: str) -> str:
    return 'CMS_RIF:' + table_name.upper()


@enum.unique
class Valtype(enum.Enum):
    """cf section 3.2 Observation_Fact of i2b2 CRC Design
    """
    coded = '@'
    text = 'T'
    date = 'D'
    numeric = 'N'


@enum.unique
class NumericOp(enum.Enum):
    """cf section 3.2 Observation_Fact of i2b2 CRC Design
    """
    eq = 'E'
    not_eq = 'NE'
    lt = 'L'
    lt_or_eq = 'LE'
    gt = 'G'
    gt_or_eq = 'GE'


@enum.unique
class PDX(enum.Enum):
    """cf. PCORNet CDM"""
    primary = '1'
    secondary = '2'


def col_valtype(col: sqla.Column) -> Valtype:
    """Determine valtype_cd based on measurement level
    """
    return (
        Valtype.numeric
        if isinstance(col.type, sqla.types.Numeric) else
        Valtype.date
        if isinstance(col.type, (sqla.types.Date, sqla.types.DateTime)) else
        Valtype.text if (isinstance(col.type, sqla.types.String) and
                         col.type.length > CMSVariables.code_max_len) else
        Valtype.coded
    )


def col_groups(col_info: pd.DataFrame, suffixes: List[str],
               prefix: str='column_name') -> pd.DataFrame:
    '''Group columns that contribute to the same `concept_cd`.

    @suffixes: e.g. ['_cd', '_vrsn'] when getting dgns_cd, dgns_vrsn
    @param col_info: as from column_data, filtered to only the relevant
                     columns, in interleaved order
                     (dgns_cd_1, dgns_vrsn_1, dgns_cd_2, ...)
    @return: a data frame with one row per group (e.g. 8 rows for dgns_cd_1 thru dgns_cd_8)
             and one column per suffix, named prefix + suffixes[col_ix]
             e.g. [column_name_cd, column_name_vrsn]
    '''
    out = None
    for ix, suffix in enumerate(suffixes):
        cols = col_info[ix::len(suffixes)].reset_index()[['column_name']]
        if out is None:
            out = cols
        else:
            out = out.merge(cols, left_index=True, right_index=True)
    if out is None:
        raise TypeError('no suffixes?')
    out.columns = [prefix + s for s in suffixes]
    return out


def fmt_dx_codes(dgns_vrsn: pd.Series, dgns_cd: pd.Series,
                 decimal_pos: int=3) -> pd.Series:
    #   I found null dgns_vrsn e.g. one record with ADMTG_DGNS_CD = V5789
    #   so let's default to the IDC9 case
    scheme = 'ICD' + dgns_vrsn.where(~dgns_vrsn.isnull(), '9')
    decimal = np.where(dgns_cd.str.len() > decimal_pos, '.', '')
    before = dgns_cd.str.slice(stop=decimal_pos)
    after = dgns_cd.str.slice(start=decimal_pos)
    return scheme + ':' + before + decimal + after


def fmt_px_codes(prcdr_cd: pd.Series, prcdr_vrsn: pd.Series,
                 hcpcs_vrsns: List[str]=['CPT', 'HCPCS', '01', '06', '10'],
                 other_vrsns: List[str]=['14', '15', '16']) -> pd.Series:
    '''Format procedure codes to match PCORNET_PROC.

    What CMS calls a HCPCS code, PCORNET_PROC calls a HCPCS code only
    if it starts with a letter.

    CMS leaves decimals implicit, where PCORNET_PROC expects them to appear.

    Medicaid uses a two digit PRCDR_CD_SYS; e.g. '01' for CPT-4.
    Codes 10-87 are documented as 'OTHER SYSTEMS', but '10' seems to be HCPCS.

    >>> px = pd.DataFrame.from_records([
    ...   ['HCPCS', '99213'],
    ...   ['HCPCS', 'G8553'],
    ...   ['9',     '1234' ],
    ...   ['HCPCS', '90718'],
    ...   ['01',    '90718'],
    ...   ['06',    'J3301'],
    ...   ['10',    'T1019'],
    ...   ['14',    '001MT'],
    ...  ], columns=['vrsn', 'cd'])
    >>> pd.DataFrame(dict(concept_cd=fmt_px_codes(px.cd, px.vrsn)))
          concept_cd
    0      CPT:99213
    1    HCPCS:G8553
    2     ICD9:12.34
    3      CPT:90718
    4      CPT:90718
    5    HCPCS:J3301
    6    HCPCS:T1019
    7  PROC|14:001MT
    '''
    assert all(~prcdr_cd.isnull())
    is_other = prcdr_vrsn.isin(other_vrsns)
    is_hcpcs = ~is_other & prcdr_vrsn.isin(hcpcs_vrsns)
    is_cpt = is_hcpcs & ~prcdr_cd.str.match('^[A-Z]', as_indexer=True)
    cpt = 'CPT:' + prcdr_cd[is_cpt]
    hcpcs = 'HCPCS:' + prcdr_cd[is_hcpcs & ~is_cpt]

    icd9 = prcdr_cd[~is_hcpcs & ~is_other]
    icd9 = icd9.where(icd9.str.len() <= 2,
                      icd9.str[:2] + '.' + icd9.str[2:])
    icd9 = 'ICD9:' + icd9
    other = 'PROC|' + prcdr_vrsn[is_other] + ':' + prcdr_cd[is_other]
    return icd9.append([cpt, hcpcs, other])[prcdr_cd.index]


class CMSRIFUpload(MedparMapped, CMSVariables):
    year = IntParam()
    bene_id_first = IntParam()
    bene_id_last = IntParam()
    bene_id_qty = IntParam(significant=False, default=-1)
    group_num = IntParam(significant=False, default=-1)
    group_qty = IntParam(significant=False, default=-1)

    chunk_size = IntParam(default=3000, significant=False)
    # label doesn't overlap with RIF columns
    src_ix = sqla.literal_column('rownum', type_=sqla.types.Integer).label('src_ix')
    chunk_rowcount = 1  # updated to useful value in `chunks()` method

    table_name = 'PLACEHOLDER'

    obs_id_vars = ['patient_ide', 'encounter_ide', 'start_date', 'end_date', 'update_date', 'provider_id']
    obs_value_cols = ['update_date', 'start_date', 'end_date']

    @property
    def label(self) -> str:
        return ('%(task_family)s #%(group_num)s of %(group_qty)s;'
                ' %(bene_id_qty)s bene_ids' %
                dict(self.to_str_params(), task_family=self.task_family))

    @property
    def input_label(self) -> str:
        return self.qualified_name()

    def qualified_name(self, name: Opt[str] = None) -> str:
        return '%s.%s' % (self.source.cms_rif, name or self.table_name)

    def table_info(self, lc: LoggedConnection) -> sqla.MetaData:
        return self.source.table_details(lc, [self.table_name])

    def source_query(self, meta: sqla.MetaData) -> sqla.sql.expression.Select:
        # ISSUE: order_by(t.c.bene_id)?
        t = meta.tables[self.qualified_name()].alias('rif')
        return (sqla.select([self.src_ix] + self.active_source_cols(t))  # type: ignore
                .where(t.c.bene_id.between(self.bene_id_first, self.bene_id_last)))

    @classmethod
    def active_source_cols(cls, t: sqla.Table) -> List[sqla.Column]:
        names = list(cls.active_col_data().column_name)
        return [c for c in t.columns if c.name in names]

    @classmethod
    def active_col_data(cls) -> pd.DataFrame:
        info = CMSVariables.active_columns(
            cls.table_name, extras=cls.i2b2_map.values())
        info.column_name = info.column_name.str.lower()
        return info

    def chunks(self, lc: LoggedConnection,
               chunk_size: int=1000) -> pd.DataFrame:
        '''Get data from `source_query` in chunks.

        .. note:: Here we use "chunk" in the pandas sense of
                  breaking up sql query results; the query
                  we're breaking up covers a "chunk" in the sense
                  of breaking up the CMS RIF data into
                  chunks of beneficiaries.
        '''
        params = dict(bene_id_first=self.bene_id_first,
                      bene_id_last=self.bene_id_last)
        meta = self.table_info(lc)
        q = self.source_query(meta)
        log_plan(lc, event='get chunk', query=q, params=params)
        # How many rows for this whole chunk of beneficiaries?
        self.chunk_rowcount = lc.scalar(sqla.select([sqla.func.count()]).select_from(q))
        return pd.read_sql(q, lc._conn, params=params, chunksize=chunk_size)

    def column_data(self, lc: LoggedConnection) -> pd.DataFrame:
        meta = self.table_info(lc)
        q = self.source_query(meta)

        return pd.DataFrame([dict(column_name=col.name,
                                  data_type=col.type,
                                  column=col)
                             for col in q.columns
                             if col.name != self.src_ix.name])

    def obs_data(self, lc: LoggedConnection, upload_id: int) -> Iterator[Tuple[pd.DataFrame, float]]:
        cols = self.column_properties(self.column_data(lc))
        chunks = self.chunks(lc, chunk_size=self.chunk_size)
        subtot_in = 0

        bene_range = (self.bene_id_first, self.bene_id_last)
        with lc.log.step('%(event)s %(bene_range)s',
                         dict(event='mapping', bene_range=bene_range)) as map_step:
            pmap = self.patient_mapping(lc, bene_range)
            map_step.argobj.update(pmap_len=len(pmap))
            map_step.msg_parts.append(' pmap: %(pmap_len)d')
            emap = self.encounter_mapping(lc, bene_range)
            map_step.argobj.update(emap_len=len(emap))
            map_step.msg_parts.append(' emap: %(emap_len)d')

        [fact_t] = self.project.table_details(lc, ['observation_fact']).tables.values()
        while 1:
            with lc.log.step('UP#%(upload_id)d: %(event)s from %(source_table)s',
                             dict(event='select', upload_id=upload_id,
                                  source_table=self.qualified_name())) as s1:
                try:
                    data = next(chunks).set_index(self.src_ix.name)
                except StopIteration:
                    break
                subtot_in, pct_in = self._input_progress(data, subtot_in, s1)

            obs, simple_cols = self.custom_obs(lc, data, cols)

            with lc.log.step('%(event)s from %(records)d %(source_table)s records',
                             dict(event='pivot facts', records=len(data),
                                  source_table=self.qualified_name())) as pivot_step:
                for valtype in Valtype:
                    obs_v = self.pivot_valtype(valtype, data, self.table_name, simple_cols)
                    if len(obs_v) > 0:
                        obs = obs_v if obs is None else obs.append(obs_v)
                if obs is None:
                    continue
                pivot_step.argobj.update(dict(obs_len=len(obs)))
                pivot_step.msg_parts.append(' %(obs_len)d total observations')

                mapped = self.with_mapping(obs, pmap, emap)
                lc.log.info('after mapping by %s: %d',
                            'medpar_id' if 'medpar_id' in obs.columns.values else 'bene_id and start_date',
                            len(mapped))
            obs_fact = self.with_admin(mapped, upload_id, lc, fact_t)

            yield obs_fact, pct_in

    def _input_progress(self, data: pd.DataFrame,
                        subtot_in: int,
                        s1: LogState) -> Tuple[int, float]:
        subtot_in += len(data)
        pct_in = 100.0 * subtot_in / self.chunk_rowcount
        s1.argobj.update(rows_in=len(data), subtot_in=subtot_in, pct_in=pct_in,
                         chunk_rowcount=self.chunk_rowcount)
        s1.msg_parts.append(
            ' + %(rows_in)d rows = %(subtot_in)d (%(pct_in)0.2f%%) of %(chunk_rowcount)d')
        return subtot_in, pct_in

    @classmethod
    def _map_cols(cls, obs: pd.DataFrame, i2b2_cols: List[str],
                  required: bool=False) -> pd.DataFrame:
        """
        Note: cls.i2b2_map may map more than one rif col to an i2b2 col.
        So we don't bother to get rid of the old column.
        """
        out = obs.copy()
        for c in i2b2_cols:
            if required or c in cls.i2b2_map:
                out[c] = obs[cls.i2b2_map[c]]

        return out

    def custom_obs(self, lc: LoggedConnection,
                   data: pd.DataFrame, cols: pd.DataFrame) -> Tuple[Opt[pd.DataFrame], pd.DataFrame]:
        return None, cols

    @classmethod
    def pivot_valtype(cls, valtype: Valtype, rif_data: pd.DataFrame,
                      table_name: str, col_info: pd.DataFrame) -> pd.DataFrame:
        '''Unpivot columns of (wide) rif_data with a given i2b2 valtype to (long) i2b2 facts.

        The `provider_id`, `start_date` etc. are mapped using CMSVariables.i2b2_map.

        @param table_name: used with `rif_modifier` to make fact `modifier_cd`
        @param col_info: with column_name, valtype_cd columns

        See also `DataFrame.melt`__.

        __ https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.melt.html
        '''
        id_vars = _no_dups([cls.i2b2_map[v] for v in cls.obs_id_vars if v in cls.i2b2_map])
        ty_cols = list(col_info[col_info.valtype_cd == valtype.value].column_name)
        ty_data = rif_data[id_vars + ty_cols].copy()

        # please excuse one line of code duplication...
        spare_digits = CMSVariables.max_cols_digits
        ty_data['instance_num'] = ty_data.index * (10 ** spare_digits)

        # i2b2 numeric (and text?) constraint searches only match modifier_cd = '@'
        # so only use rif_modifer() on coded values.
        ty_data['modifier_cd'] = '@'

        obs = pd.melt(ty_data, id_vars=id_vars + ['instance_num', 'modifier_cd'],
                      var_name='column').dropna(subset=['value'])

        V = Valtype
        obs['valtype_cd'] = valtype.value

        scheme = obs.column
        for target, replacement in cls.concept_scheme_override.items():
            scheme = scheme.where(scheme != target, replacement)
        scheme = scheme.str.upper() + ':'

        if valtype == V.coded:
            obs['concept_cd'] = scheme + obs.value
            obs['tval_char'] = None  # avoid NaN, which causes sqlalchemy to choke
        else:
            obs['concept_cd'] = scheme
            if valtype == V.numeric:
                obs['nval_num'] = obs.value
                obs['tval_char'] = NumericOp.eq.value
            elif valtype == V.text:
                obs['tval_char'] = obs.value
            elif valtype == V.date:
                obs['tval_char'] = obs.value.astype('<U')  # format yyyy-mm-dd...
            else:
                raise TypeError(valtype)

        if valtype == V.date:
            obs['start_date'] = obs['end_date'] = obs.value
        else:
            obs = cls._map_cols(obs, ['start_date', 'end_date'])

        obs = cls._map_cols(obs, ['update_date'], required=True)
        obs = cls._map_cols(obs, ['provider_id', 'quantity_num', 'confidence_num'])

        return obs


def _no_dups(seq: List[T]) -> List[T]:
    from typing import Set, Callable, Any
    # ack: https://stackoverflow.com/a/480227/7963
    seen = set()  # type: Set[T]
    seen_add = seen.add  # type: Callable[[T], Any]
    return [x for x in seq if not (x in seen or seen_add(x))]
    Set, Callable, Any  # mute unused import warning


def obs_stack(rif_data: pd.DataFrame,
              rif_table_name: str, projections: pd.DataFrame,
              id_vars: List[str], value_vars: List[str]) -> pd.DataFrame:
    '''
    :param projections: columns to project (e.g. diagnosis code and version);
                        order matches value_vars
    :param id_vars: a la pandas.melt (no dups allowed)
    :param value_vars: a la melt; data column (e.g. dgns_cd) followed by dgns_vrsn etc.
    '''
    assert id_vars == _no_dups(id_vars)
    assert len(projections) >= 1

    spare_digits = CMSVariables.max_cols_digits

    out = None
    for ix, ((mod_grp, x), rif_cols) in enumerate(projections.iterrows()):
        value_cols = list(rif_cols.dropna())
        obs = rif_data[id_vars + value_cols].copy()

        instance_num = obs.index * (10 ** spare_digits) + ix
        obs = obs.set_index(id_vars)
        # value_cols is shorter than value_vars when there's no POA flag
        obs.columns = value_vars[:len(value_cols)]  # e.g. icd_dgns_cd11 -> dgns_cd
        obs['instance_num'] = instance_num

        obs = obs.dropna(subset=value_vars[:2])
        obs['mod_grp'] = mod_grp
        obs['x'] = x

        if out is None:
            out = obs
        else:
            out = out.append(obs)

    if out is None:
        raise TypeError('no projections?')

    return out


class date_trunc(sqla.sql.functions.GenericFunction):  # type: ignore
    type = sqla.types.DateTime
    name = 'trunc'


class _ByExtractYear(CMSRIFUpload):
    bene_enrollmt_ref_yr = IntParam(default=2013)  # ISSUE: dead code? affects task ids, though

    i2b2_map = dict(
        patient_ide='bene_id',
        start_date='start_date',  # start of year
        end_date='extract_dt',    # end of year
        update_date='download_date')

    def source_query(self, meta: sqla.MetaData) -> sqla.sql.expression.Select:
        t = meta.tables[self.qualified_name()].alias('rif')
        download_col = sqla.literal(self.source.download_date).label('download_date')
        start_date = date_trunc(t.c.extract_dt, 'year').label('start_date')
        return (sqla.select([self.src_ix, start_date, t.c.extract_dt, download_col] +  # type: ignore
                            self.active_source_cols(t))
                .where(t.c.bene_id.between(self.bene_id_first, self.bene_id_last)))


class MBSFUpload(_ByExtractYear):
    table_name = 'mbsf_abcd_summary'


class MAXPSUpload(_ByExtractYear):
    '''
    >>> col_info = MAXPSUpload.active_col_data()
    >>> col_info.set_index('column_name')[['DATA_TYPE', 'valtype_cd']].head()
    ... # doctest: +NORMALIZE_WHITESPACE
                      DATA_TYPE valtype_cd
    column_name
    bene_id            VARCHAR2          T
    state_cd           VARCHAR2          @
    el_state_case_num  VARCHAR2          T
    max_yr_dt            NUMBER          N
    el_dob                 DATE          D

    '''
    table_name = 'maxdata_ps'

    valtype_override = [
        ('@', '.*_cd$')  # e.g. EL_AGE_GRP_CD
    ]

    coltype_override = [
        (sqla.String(CMSVariables.code_max_len - 1), '_cd')
    ]

    @classmethod
    def active_source_cols(cls, t: sqla.Table) -> List[sqla.Column]:
        '''Override (cast) DB column types (e.g. coded columns with numeric types).
        '''
        names = list(cls.active_col_data().column_name)
        info = [c for c in t.columns if c.name in names]
        for desired_type, suffix in cls.coltype_override:
            info = [sqla.sql.expression.cast(col, desired_type).label(col.name)  # type: ignore
                    if col.name.endswith(suffix) else col
                    for col in info]
        return info


class _DxPxCombine(CMSRIFUpload):
    valtype_dx = '@dx'
    valtype_px = '@px'

    valtype_override = [
        (valtype_dx, r'.*(_dgns_|rsn_visit)'),
        (valtype_dx, r'^dgns_'),
        (valtype_px, r'.*prcdr_')
    ]

    @classmethod
    def vrsn_cd_groups(cls, col_info: pd.DataFrame, kind: str, aux: str,
                       ix_cols: List[str]=['mod_grp', 'ix'],
                       column_name: str='column_name') -> pd.DataFrame:
        # ISSUE: enum for kind, aux?
        suffixes = ['_vrsn', '_cd'] + [aux.replace(kind, '').lower()]
        cd_cols = col_info[(col_info.dxpx == kind + '_CD') &
                           (col_info.valtype_cd == '@')][ix_cols + [column_name]]
        vrsn_cols = col_info[col_info.dxpx == kind + '_VRSN'][ix_cols + [column_name]]
        if len(vrsn_cols) > 0:
            groups = pd.merge(vrsn_cols, cd_cols, on=ix_cols, suffixes=[suffixes[0], ''])
        else:
            groups = cd_cols
        dt_cols = col_info[col_info.dxpx == aux][ix_cols + [column_name]]
        if len(dt_cols) > 0:
            groups = pd.merge(groups, dt_cols, on=ix_cols, how='left', suffixes=['', suffixes[2]])
        return groups.set_index(ix_cols)

    def custom_obs(self, lc: LoggedConnection,
                   data: pd.DataFrame, cols: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # curated column info
        col_info = self.active_col_data()
        # order col_info like db cols
        col_info = col_info.set_index('column_name').loc[cols.column_name].reset_index()
        dx_g = self.vrsn_cd_groups(col_info, kind='DGNS', aux='DGNS_IND')
        px_g = self.vrsn_cd_groups(col_info, kind='PRCDR', aux='PRCDR_DT')
        simple_cols = cols[(~col_info.Status.isnull()) &
                           ~cols.column_name.isin(self.i2b2_map.values()) &
                           col_info.dxpx.isnull()]

        with lc.log.step('%(event)s from %(records)d %(source_table)s records',
                         dict(event='stack dx, px', records=len(data),
                              source_table=self.qualified_name())) as stack_step:
            obs = None
            obs_dx = self.dx_data(data, self.table_name, dx_g)
            if obs_dx is not None:
                stack_step.msg_parts.append(' %(dx_len)d diagnoses')
                stack_step.argobj.update(dict(dx_len=len(obs_dx)))
                obs = obs_dx
            obs_px = self.px_data(data, self.table_name, px_g)
            if obs_px is not None:
                stack_step.msg_parts.append(' %(px_len)d procedures')
                stack_step.argobj.update(dict(px_len=len(obs_px)))
                if obs is None:
                    obs = obs_px
                else:
                    obs = obs.append(obs_px)
        return obs, simple_cols

    @classmethod
    def dx_data(cls, rif_data: pd.DataFrame,
                table_name: str, dx_cols: pd.DataFrame,
                log: logging.Logger=log,
                vrsn_default: str='9') -> pd.DataFrame:
        """Combine diagnosis columns i2b2 style

        :param vrsn_default: for MAXDATA_IP, default to IDC9
        """
        if len(dx_cols) < 1:
            return None
        id_vars = _no_dups([cls.i2b2_map[v]
                            for v in cls.obs_id_vars if v in cls.i2b2_map])

        # MAXDATA_IP has no version column; just a code column.
        if len(dx_cols.columns) == 1:
            value_vars = ['dgns_cd']
        else:
            value_vars = ['dgns_vrsn', 'dgns_cd', 'dgns_poa_ind']

        obs = obs_stack(rif_data, table_name, dx_cols,
                        id_vars=id_vars,
                        value_vars=value_vars).reset_index()
        obs['valtype_cd'] = Valtype.coded.value

        if 'dgns_vrsn' not in obs.columns:
            obs['dgns_vrsn'] = vrsn_default

        obs['concept_cd'] = fmt_dx_codes(obs.dgns_vrsn, obs.dgns_cd)

        # We don't need this after all, do we?
        # poa_suffix = np.where(obs.dgns_poa_ind.isnull() | (obs.dgns_poa_ind == ' '),
        #                       '', '+POA:' + obs.dgns_poa_ind)
        pdx_suffix = np.where(obs.x == 1, '+PDX', '')
        obs['modifier_cd'] = 'DX:' + obs.mod_grp + pdx_suffix

        obs = cls._map_cols(obs, cls.obs_value_cols, required=True)
        obs = cls._map_cols(obs, ['provider_id'])

        return _check_start_date(obs, threshold=(0.01, log))

    @classmethod
    def px_data(cls, data: pd.DataFrame, table_name: str, px_cols: pd.DataFrame,
                log: logging.Logger=log,
                default_vrsn: str='HCPCS', exclude_vrsn: List[str]=['88', '99'],
                px_source_mod: str='PX_SOURCE:CL',
                obs_value_cols: List[str]=['provider_id', 'update_date']) -> pd.DataFrame:
        """Combine procedure columns i2b2 style

        Forward-fill `start_date` because MAXDATA_IP has may procedure
        code columns but only one procedure date column.

        :param exclude_vrsn: indication that there was no procedure observed
        """
        if len(px_cols) < 1:
            return None

        # BCARRIER_LINE has no procedure version column.
        if any('v' in col for col in px_cols.columns):
            value_vars = ['prcdr_vrsn', 'prcdr_cd', 'prcdr_dt']
        else:
            value_vars = ['prcdr_cd', 'prcdr_dt']

        obs = obs_stack(data, table_name, px_cols,
                        id_vars=_no_dups([cls.i2b2_map[v]
                                          for v in cls.obs_id_vars if v in cls.i2b2_map]),
                        value_vars=value_vars).reset_index()
        obs['valtype_cd'] = Valtype.coded.value
        obs['modifier_cd'] = px_source_mod
        if 'prcdr_vrsn' not in obs.columns:
            obs['prcdr_vrsn'] = default_vrsn
        obs = obs[~obs.prcdr_vrsn.isin(exclude_vrsn)]
        obs['concept_cd'] = fmt_px_codes(obs.prcdr_cd, obs.prcdr_vrsn)

        if 'prcdr_dt' in obs.columns:
            obs = obs.rename(columns=dict(prcdr_dt='start_date')).sort_values('instance_num')
            obs.start_date.fillna(method='ffill', inplace=True)
        else:
            obs['start_date'] = np.nan
        if 'start_date' in cls.i2b2_map:
            obs.start_date.fillna(obs[cls.i2b2_map['start_date']], inplace=True)
        # ISSUE: impute PX end date... from where?
        # obs['end_date'] = obs.start_date

        obs = cls._map_cols(obs, obs_value_cols)

        return _check_start_date(obs, threshold=(0.01, log))


class MEDPAR_Upload(_DxPxCombine):
    table_name = 'medpar_all'

    design_version = IntParam(default=len([
        'MEDPAR: pivot DX using curated column info',
        'MEDPAR_ALL: get encounter_num from medpar_id',
        'MEDPAR_ALL: procedure data, codes (#4889)',
        'DRG_CD:xxx -> DRG:xxx',
        'MSDRG: scheme per PCORNET_ENC',
        'InpatientStays: prcdr_vrsn / prcdr_cd were switched',
        'px_source mod per PCORNET_PROC',
    ]))

    i2b2_map = dict(
        patient_ide='bene_id',
        encounter_ide='medpar_id',
        start_date='admsn_dt',
        end_date='dschrg_dt',
        provider_id='org_npi_num',
        update_date='ltst_clm_acrtn_dt')

    # MSDRG per PCORNET_ENC metadata
    concept_scheme_override = dict(_DxPxCombine.concept_scheme_override,
                                   drg_cd='MSDRG')


def _check_start_date(obs: pd.DataFrame,
                      threshold: Opt[Tuple[float, logging.Logger]]=None) -> pd.DataFrame:
    tot = len(obs)
    if tot:
        bad = obs[obs.start_date.isnull()]
        if len(bad) > 0:
            if threshold is None:
                raise ValueError(bad.head())
            val, log = threshold
            if len(bad) * 1.0 / tot > val:
                raise ValueError(bad.head())
            else:
                log.warn('ignoring %d (%f%%) records with null start_date',
                         len(bad), 100.0 * len(bad) / tot)
            obs = obs[~obs.start_date.isnull()]
    return obs


class MAXDATA_IP_Upload(_DxPxCombine):
    table_name = 'maxdata_ip'

    design_version = IntParam(default=len([
        'no dgns_vrsn columns'
    ]))

    i2b2_map = dict(
        patient_ide='bene_id',
        start_date='srvc_bgn_dt',
        provider_id='npi',
        end_date='srvc_end_dt',
        update_date='srvc_end_dt')

    concept_scheme_override = dict(_DxPxCombine.concept_scheme_override,
                                   drg_rel_group='DRG')


class CarrierClaimUpload(_DxPxCombine):
    table_name = 'bcarrier_claims_k'

    i2b2_map = dict(
        patient_ide='bene_id',
        start_date='clm_from_dt',
        end_date='clm_thru_dt',
        update_date='nch_wkly_proc_dt')


class CarrierLineUpload(_DxPxCombine):
    '''Carrier Claim Line details

    Especially procedures:

    >>> rif_data, col_info, simple_cols = _RIFTestData.build(CarrierLineUpload)
    >>> px_cols = CarrierLineUpload.vrsn_cd_groups(col_info, kind='PRCDR', aux='PRCDR_DT')
    >>> px_cols
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
                column_name     column_name_dt
    mod_grp ix
    PRCDR   1.0    hcpcs_cd  line_1st_expns_dt

    >>> obs_px = CarrierLineUpload.px_data(rif_data, CarrierLineUpload.table_name, px_cols)
    >>> obs_px.sort_values('instance_num') .set_index(['bene_id', 'clm_thru_dt', 'instance_num'])[
    ...                             ['start_date', 'prcdr_vrsn', 'prcdr_cd', 'concept_cd']]
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
                                              start_date prcdr_vrsn prcdr_cd  concept_cd
    bene_id         clm_thru_dt instance_num
    47PZ1AN7X       2002-07-17  0             1971-05-13      HCPCS      DX9   HCPCS:DX9
    Y11284TLK49E566 1982-04-26  1000          1983-03-18      HCPCS      4R0    CPT:4R0
    97WM8844276M5   1988-02-26  2000          2002-02-24      HCPCS     721U   CPT:721U
    3HOY34RGW       1978-01-09  3000          1991-09-26      HCPCS       74     CPT:74
    12APH9HOR74G8QC 1996-07-10  4000          1983-09-14      HCPCS       V4   HCPCS:V4

    Regression test for LINE_ICD_DGNS_CD:
    >>> dx_cols = CarrierLineUpload.vrsn_cd_groups(col_info, kind='DGNS', aux='IND')
    >>> dx_cols
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
                      column_name_vrsn       column_name
    mod_grp ix
    LINE    0.0  line_icd_dgns_vrsn_cd  line_icd_dgns_cd

    '''
    table_name = 'bcarrier_line_k'
    claim_table_name = CarrierClaimUpload.table_name

    i2b2_map = dict(
        patient_ide='bene_id',
        # performance of joining with bcarrier_claims is disastrous
        # start_date='clm_from_dt',
        start_date='clm_thru_dt',
        end_date='clm_thru_dt',
        # ISSUE: carrier line start/end date columns?
        # start_date='line_1st_expns_dt',
        # end_date='line_last_expns_dt',
        provider_id='prf_physn_npi',
        update_date='line_last_expns_dt')

    valtype_override = _DxPxCombine.valtype_override + [
        ('@', 'line_ndc_cd')
    ]
    concept_scheme_override = dict(_DxPxCombine.concept_scheme_override,
                                   line_ndc_cd='NDC')

    def _table_info_too_slow(self, lc: LoggedConnection) -> sqla.MetaData:
        return self.source.table_details(lc, [self.table_name, self.claim_table_name])

    def _source_query_too_slow(self, meta: sqla.MetaData) -> sqla.sql.expression.Select:
        line = meta.tables[self.qualified_name()].alias('line')
        claim = meta.tables[self.qualified_name(self.claim_table_name)].alias('claim')
        return (sqla.select([claim.c.clm_from_dt, line])  # type: ignore
                .select_from(line.join(claim, line.c.clm_id == claim.c.clm_id))
                .where(sqla.and_(
                    line.c.bene_id.between(self.bene_id_first, self.bene_id_last),
                    claim.c.bene_id.between(self.bene_id_first, self.bene_id_last))))


class OutpatientClaimUpload(_DxPxCombine):
    table_name = 'outpatient_base_claims_k'
    i2b2_map = dict(
        patient_ide='bene_id',
        start_date='clm_from_dt',
        end_date='clm_thru_dt',
        update_date='nch_wkly_proc_dt',
        provider_id='at_physn_npi')


class OutpatientRevenueUpload(_DxPxCombine):
    table_name = 'outpatient_revenue_center_k'
    i2b2_map = dict(
        patient_ide='bene_id',
        start_date='clm_thru_dt',
        end_date='clm_thru_dt',
        update_date='rev_cntr_dt',
        provider_id='rndrng_physn_npi')


class MAXDATA_OT_Upload(_DxPxCombine):
    '''Medicaid Other Therapies

    Diagnoses and procedures:

    >>> rif_data, col_info, simple_cols = _RIFTestData.build(MAXDATA_OT_Upload)
    >>> dx_cols = MAXDATA_OT_Upload.vrsn_cd_groups(col_info, kind='DGNS', aux='DGNS_IND')
    >>> obs_dx = MAXDATA_OT_Upload.dx_data(rif_data, MAXDATA_OT_Upload.table_name, dx_cols)
    >>> obs_dx.sort_values('instance_num').set_index(['bene_id', 'instance_num'])[
    ...                             ['start_date', 'provider_id', 'dgns_cd', 'x', 'concept_cd']][::3][:6]
    ... # doctest: +NORMALIZE_WHITESPACE
                               start_date    provider_id dgns_cd    x   concept_cd
    bene_id       instance_num
    47PZ1AN7X     0            1994-06-14        WU2086S   4341J  1.0  ICD9:434.1J
    VY11284TLK49E 1001         1990-05-04  ZRWK3B0XOKC5D    MF3A  2.0   ICD9:MF3.A
    0WKD167       3000         2013-05-04       C0U47KI3      4N  1.0      ICD9:4N
    2X1W3C12A     4001         2000-12-28        X5MV10I     E46  2.0     ICD9:E46

    >>> px_cols = MAXDATA_OT_Upload.vrsn_cd_groups(col_info, kind='PRCDR', aux='PRCDR_DT')
    >>> obs_px = MAXDATA_OT_Upload.px_data(rif_data, MAXDATA_OT_Upload.table_name, px_cols)
    >>> obs_px.sort_values('instance_num').set_index(['bene_id', 'instance_num'])[
    ...                             ['start_date', 'provider_id', 'prcdr_cd', 'x', 'concept_cd']][::3][:6]
    ... # doctest: +NORMALIZE_WHITESPACE
                           start_date provider_id prcdr_cd    x concept_cd
    bene_id   instance_num
    47PZ1AN7X 0            1994-06-14     WU2086S       7R  1.0    ICD9:7R
    0WKD167   3000         2013-05-04    C0U47KI3      Q43  1.0  ICD9:Q4.3
    '''
    table_name = 'maxdata_ot'
    i2b2_map = dict(
        patient_ide='bene_id',
        start_date='srvc_bgn_dt',
        end_date='srvc_end_dt',
        update_date='srvc_end_dt',
        provider_id='npi')


class DrugEventUpload(CMSRIFUpload):
    table_name = 'pde'
    i2b2_map = dict(
        # Include all CDM DISPENSING data in each fact
        quantity_num='qty_dspnsd_num',
        confidence_num='days_suply_num',

        patient_ide='bene_id',
        start_date='srvc_dt',
        end_date='srvc_dt',
        update_date='srvc_dt',
        provider_id='prscrbr_id')

    obs_id_vars = CMSRIFUpload.obs_id_vars + ['quantity_num', 'confidence_num']

    valtype_override = [
        ('@', 'prod_srvc_id')
    ]
    concept_scheme_override = {
        'prod_srvc_id': 'NDC'
    }


class DrugEventSAFUpload(DrugEventUpload):
    table_name = 'pde_saf'


class MAXRxUpload(CMSRIFUpload):
    table_name = 'maxdata_rx'
    i2b2_map = dict(
        quantity_num='qty_srvc_units',
        confidence_num='days_supply',

        patient_ide='bene_id',
        start_date='prscrptn_fill_dt',
        end_date='prscrptn_fill_dt',
        update_date='prscrptn_fill_dt',
        provider_id='npi')

    obs_id_vars = CMSRIFUpload.obs_id_vars + ['quantity_num', 'confidence_num']
    valtype_override = [
        ('@', 'ndc')
    ]


class _BeneIdGrouped(luigi.WrapperTask):
    year = IntParam()
    group_tasks = cast(List[Type[CMSRIFUpload]], [])  # abstract

    def requires(self) -> List[luigi.Task]:
        deps = []  # type: List[luigi.Task]
        for group_task in self.group_tasks:
            survey = BeneIdSurvey()
            deps += [survey]
            results = survey.results()
            if results:
                deps += [
                    group_task(
                        year=self.year,
                        group_num=ntile.chunk_num,
                        group_qty=len(results),
                        bene_id_qty=ntile.bene_id_qty,
                        bene_id_first=ntile.bene_id_first,
                        bene_id_last=ntile.bene_id_last)
                    for ntile in results
                ]
        return deps


class MedicareYear(_BeneIdGrouped):
    year = IntParam()
    group_tasks = [
        MBSFUpload,
        CarrierClaimUpload, CarrierLineUpload,
        DrugEventUpload,
        OutpatientClaimUpload, OutpatientRevenueUpload,
        MEDPAR_Upload
    ]


class MedicaidYear(_BeneIdGrouped):
    year = IntParam()
    group_tasks = [
        MAXPSUpload,
        MAXRxUpload,
        MAXDATA_OT_Upload
    ]


def obj_string(df: pd.DataFrame,
               clobs: List[str]=[],
               pad: int=4) -> Dict[str, sqla.types.String]:
    '''avoid CLOBs'''
    df = df.reset_index()
    obj_cols = [col for (col, ty) in df.dtypes.items()
                if ty.kind == 'O' and col not in clobs]
    dt = {col: sqla.types.String(np.nanmax([df[col].str.len().max() + pad, pad]))
          for col in obj_cols}
    # log.debug('no clobs? %s', dt)
    return dt


class LoadDataFile(DBAccessTask):
    table_name = StrParam()
    directory = StrParam(default='metadata', significant=False)

    def complete(self) -> bool:
        table = sqla.Table(self.table_name, sqla.MetaData())
        return table.exists(bind=self._dbtarget().engine)  # type: ignore

    def run(self) -> None:
        data = pd.read_csv('%s/%s.csv' % (self.directory, self.table_name))  # ISSUE: ambient
        with self.connection('load data file') as lc:
            data.to_sql(self.table_name, lc._conn, if_exists='replace',
                        dtype=obj_string(data))


class PatientDimension(FromCMS, UploadTask):
    script = Script.cms_patient_dimension

    def requires(self) -> List[luigi.Task]:
        curated = LoadDataFile(table_name='cms_pcornet_map')
        return SqlScriptTask.requires(self) + [curated]


class Demographics(ReportTask):
    script = Script.cms_dem_dstats
    report_name = 'demographic_summary'

    def requires(self) -> List[luigi.Task]:
        pdim = PatientDimension()
        report = SqlScriptTask(script=self.script,
                               param_vars=pdim.vars_for_deps)  # type: luigi.Task
        return [report] + cast(List[luigi.Task], [pdim])


class VisitDimLoad(luigi.WrapperTask, FromCMS, DBAccessTask):
    pat_group_qty = IntParam(default=20, significant=False)

    def requires(self) -> List[luigi.Task]:
        pd = PatientDimension()  # ISSUE: requirements depend on requirements

        with self.connection('partition patients') as q:
            groups = self.project.patient_groups(q, self.pat_group_qty)

        return [cast(luigi.Task, pd)] + [
            VisitDimForPatGroup(patient_num_lo=lo,
                                patient_num_hi=hi,
                                pat_group_qty=qty,
                                pat_group_num=num)
            for (qty, num, lo, hi) in groups]


class VisitDimForPatGroup(_LoadTask):
    patient_num_lo = IntParam()
    patient_num_hi = IntParam()
    pat_group_qty = IntParam(significant=False)
    pat_group_num = IntParam(significant=False)
    chunk_size = IntParam(100000, significant=False)
    parallel_degree = IntParam(default=20, significant=False)

    # Only one task should insert into visit_dimension at a time.
    resources = {'visit_dimension': 1}

    view = 'cms_visit_dimension'
    prep_script = Script.cms_visit_dimension

    @property
    def label(self) -> str:
        return '%s: %d of %s (%d to %d)' % (self.task_family,
                                            self.pat_group_num, self.pat_group_qty,
                                            self.patient_num_lo, self.patient_num_hi)

    def requires(self) -> List[luigi.Task]:
        return [
            PatientDimension(),
            VisitCodesCache(),
            SqlScriptTask(script=self.prep_script,
                          param_vars=self.vars_for_deps),
        ]

    def load(self, lc: LoggedConnection, upload: 'UploadTarget', upload_id: int, result: Params) -> None:
        [vdim] = self.project.table_details(lc, ['visit_dimension']).tables.values()
        dtype = {c.name: c.type for c in vdim.columns
                 if not c.name.endswith('_blob')}

        q = 'select /*+ parallel({parallel_degree}) */ * from {view} where patient_num between :lo and :hi'.format(
            parallel_degree=self.parallel_degree, view=self.view)
        pat_range = dict(lo=self.patient_num_lo, hi=self.patient_num_hi)  # type: Params
        log_plan(lc, event=self.view, sql=q,
                 params=pat_range)

        # clean up from any earlier failed attempts
        lc.execute("delete from {i2b2_star}.{dim_table} where patient_num between :lo and :hi".format(
            i2b2_star=vdim.schema, dim_table=vdim.name), params=pat_range)
        lc.execute('commit')

        chunks = pd.read_sql(q, lc._conn, params=pat_range, chunksize=self.chunk_size)
        subtot = 0
        while 1:
            with lc.log.step('UP#%(upload_id)d: %(event)s x%(chunk_size)d into %(i2b2_star)s.%(dim_table)s',
                             dict(event='visit chunk', chunk_size=self.chunk_size,
                                  upload_id=upload_id, i2b2_star=vdim.schema, dim_table=vdim.name)) as step:
                try:
                    visit_chunk = next(chunks)
                except StopIteration:
                    break
                visit_chunk = self.with_admin(visit_chunk, upload_id, lc, vdim)
                with self.connection('insert visits') as writing:
                    visit_chunk.to_sql(schema=vdim.schema, name=vdim.name,
                                       con=writing._conn,
                                       dtype=dtype,
                                       if_exists='append', index=False)
                    writing.execute('commit')
                subtot += len(visit_chunk)
                step.msg_parts.append(' %(row_subtot)s rows')
                step.argobj.update(dict(row_subtot=subtot))


class VisitCodesCache(_LoadTask):
    '''Cache (materialize) visit codes view in a table.

    Use UPLOAD_STATUS track completion status.
    '''
    prep_script = Script.cms_visit_dimension
    table = StrParam(default='cms_enc_codes_t')
    # TODO: parallel_degree should have significant=False
    # and view should be design-time, not a parameter; but
    # changing them will invalidate existing results, so for
    # now, let's leave them as is.
    parallel_degree = IntParam(default=20)
    view = StrParam(default='cms_enc_codes_v')

    @property
    def label(self) -> str:
        return 'cache %s as %s' % (self.view, self.table)

    def requires(self) -> List[luigi.Task]:
        return [
            self.project,  # I2B2 project
            SqlScriptTask(script=self.prep_script,
                          param_vars=self.vars_for_deps),
        ]

    steps = [
        'delete from {table}',  # ISSUE: lack of truncate privilege is a pain.
        'commit',
        'insert /*+ parallel({parallel_degree}) append */ into {table} select * from {view}',
        'commit',
    ]

    def load(self, work: LoggedConnection, upload: 'UploadTarget', upload_id: int, result: Params) -> None:
        log_plan(work, event=self.view, sql='select * from ' + self.view, params={})
        for step in self.steps:
            work.execute(step.format(view=self.view, table=self.table,
                                     parallel_degree=self.parallel_degree))


class _RIFTestData(object):
    @classmethod
    def build(cls, task_family: Type[CMSRIFUpload], qty: int=5) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        col_info = task_family.active_col_data()
        rng = Random(1)
        rif_data = _RIFTestData.arb_records(5, rng, col_info)
        simple_cols = col_info[~col_info.Status.isnull() &
                               ~col_info.column_name.isin(task_family.i2b2_map.values()) &
                               col_info.dxpx.isnull()]
        return rif_data, col_info, simple_cols

    @classmethod
    def arb_records(cls, qty: int, rng: Random, col_info: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame([
            {col.column_name: cls.arb_value(rng, col.valtype_cd, col.column_name)
             for _, col in col_info.iterrows()}
            for _ in range(qty)
        ])[col_info.column_name]

    @classmethod
    def arb_value(cls, rng: Random, valtype_cd: str, column_name: str) -> Any:
        import datetime
        from re import search
        randint = rng.randint

        def only_so_many(x):  # type: ignore
            try:
                grp = int(search(r'_(\d+)', column_name).group(1))  # type: ignore
                if grp ** 2 > randint(2, 12 ** 2):
                    return None
            except Exception:
                pass
            return x

        if 'poa_' in column_name and '_ind_cd' in column_name:
            return rng.choice('YNUW1ZX ')
        if 'dgns_' in column_name and 'vrsn_' in column_name:
            return only_so_many('10' if randint(1, 10) == 10 else '9')  # type: ignore
        if 'dgns_' in column_name:
            return '%d%d' % (randint(10, 88), randint(40, 500))
        if 'yr_num' in column_name:
            return str(randint(2011, 2013))
        if 'age_cnt' in column_name:
            return randint(3, 89)

        pick_date = lambda: datetime.date(randint(1970, 2014), randint(1, 12), randint(1, 28))
        pick_letter = lambda: chr(randint(ord('A'), ord('Z')))
        pick_digit = lambda: chr(randint(ord('0'), ord('9')))
        pick_alnum = lambda: pick_letter() if randint(0, 1) else pick_digit()   # type: ignore
        f = {
            'D': pick_date,
            '@': lambda: ''.join(pick_alnum() for _ in range(randint(2, 5))),   # type: ignore
            'T': lambda: ''.join(pick_alnum() for _ in range(randint(7, 15))),  # type: ignore
            'N': lambda: randint(100, 10000)
        }[valtype_cd]
        return f()  # type: ignore
