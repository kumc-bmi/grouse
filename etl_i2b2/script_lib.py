r'''script_lib -- library of SQL scripts

Scripts are pkg_resources, i.e. design-time constants.

Each script should have a title, taken from the first line::

    >>> Script.cms_patient_mapping.title
    'map CMS beneficiaries to i2b2 patients'

    >>> text = Script.cms_patient_mapping.value
    >>> lines = text.split('\n')
    >>> print(lines[0])
    /** cms_patient_mapping - map CMS beneficiaries to i2b2 patients

TODO: copyright, license blurb enforcement

We can separate the script into statements::

    >>> statements = Script.cms_patient_mapping.statements()
    >>> print(next(s for s in statements if 'insert' in s))
    ... #doctest: +ELLIPSIS
    insert /*+ append */
      into "&&I2B2STAR".patient_mapping
    ...

A bit of sqlplus syntax is supported for ignoring errors in just part
of a script:

    >>> Script.sqlerror('whenever sqlerror exit')
    False
    >>> Script.sqlerror('whenever sqlerror continue')
    True
    >>> Script.sqlerror('select 1 + 1 from dual') is None
    True

Dependencies between scripts are declared as follows::

    >>> print(next(decl for decl in statements if "'dep'" in decl))
    ... #doctest: +ELLIPSIS
    select active from i2b2_status where 'dep' = 'i2b2_crc_design.sql'

    >>> Script.cms_patient_mapping.deps()
    ... #doctest: +ELLIPSIS
    [<Script(i2b2_crc_design)>, <Package(cms_keys)>]

The `.pls` extension indicates a dependency on a package rather than a script::

    >>> _sorted = lambda s: sorted(s, key=lambda x: x.name)
    >>> _sorted(Script.cms_dem_txform.deps())
    [<Package(cms_keys)>, <Script(i2b2_crc_design)>]

We statically detect relevant effects; i.e. tables and views created::

    >>> Script.i2b2_crc_design.created_objects()
    [view i2b2_status]

as well as tables inserted into::

    >>> variables={I2B2STAR: 'I2B2DEMODATA',
    ...            CMS_RIF: 'CMS_DEID',
    ...            'cms_source_cd': "'ccwdata.org'", 'fact_view': 'F'}
    >>> Script.cms_facts_load.inserted_tables(variables)
    ['"I2B2DEMODATA".observation_fact']

To insert in chunks by bene_id, use the relevant params in your insert
statement:

    >>> sorted(ChunkByBene.required_params)
    ['bene_id_first', 'bene_id_last']

    >>> chunked = Script.cms_encounter_mapping
    >>> from sql_syntax import param_names
    >>> [ix for (ix, s) in enumerate(chunked.statements())
    ... if ChunkByBene.required_params <= set(param_names(s)) ]
    [8, 11]

A survey of bene_ids from the relevant tables is assumend::

    >>> sorted(ChunkByBene.sources_from(chunked))
    ['BCARRIER_CLAIMS', 'MEDPAR_ALL']

Groups exhaust chunks::
    >>> [ChunkByBene.group_chunks(1000, 1, n) for n in range(1, 2)]
    [(1, 1000)]
    >>> [ChunkByBene.group_chunks(398, 2, n) for n in range(1, 3)]
    [(1, 200), (201, 398)]
    >>> [ChunkByBene.group_chunks(500, 3, n) for n in range(1, 4)]
    [(1, 167), (168, 334), (335, 500)]

TODO: indexes.
ISSUE: truncate, delete, update aren't reversible.

The last statement should be a scalar query that returns non-zero to
signal that the script is complete:

    >>> print(statements[-1])
    select 1 complete
    from "&&I2B2STAR".patient_mapping
    where rownum = 1

The completion test may depend on a digest of the script and its dependencies:

    >>> design_digest = Script.cms_dem_txform.digest()
    >>> last = Script.cms_dem_txform.statements(variables)[-1].strip()
    >>> print(last.replace(str(design_digest), '123...'))
    select 1 up_to_date
    from cms_dem_txform where design_digest = 123...

'''

from itertools import groupby
from typing import Dict, FrozenSet, Iterable, List, Optional, Sequence, Text, Tuple, Type
import enum
import re
import abc

import pkg_resources as pkg
from sqlalchemy.engine import RowProxy

from sql_syntax import (
    Environment, Params, StatementInContext, ObjectId, SQL, Name,
    iter_statement, iter_blocks, substitute,
    created_objects, inserted_tables)

I2B2STAR = 'I2B2STAR'  # cf. &&I2B2STAR in sql_scripts
CMS_RIF = 'CMS_RIF'

ScriptStep = Tuple[int, Text, SQL]
Filename = str   # issue: are filenames bytes?


class SQLMixin(enum.Enum):
    @property
    def sql(self) -> SQL:
        from typing import cast
        return cast(SQL, self.value)  # hmm...

    @abc.abstractmethod
    def parse(self, text: SQL) -> Iterable[StatementInContext]:
        raise NotImplementedError

    def each_statement(self,
                       variables: Optional[Environment]=None) -> Iterable[ScriptStep]:
        for line, comment, statement in self.parse(self.sql):
            ss = substitute(statement, self._all_vars(variables))
            yield line, comment, ss

    def _all_vars(self, variables: Optional[Environment]) -> Optional[Environment]:
        '''Add design_digest to variables.
        '''
        if variables is None:
            return None
        return dict(variables, design_digest=str(self.digest()))

    def statements(self,
                   variables: Optional[Environment]=None) -> Sequence[Text]:
        return list(stmt for _l, _c, stmt
                    in self.each_statement(variables=variables))

    def created_objects(self) -> List[ObjectId]:
        return []

    def inserted_tables(self,
                        variables: Environment={}) -> List[Name]:
        return []

    @property
    def title(self) -> Text:
        line1 = self.sql.split('\n', 1)[0]
        if not (line1.startswith('/** ') and ' - ' in line1):
            raise ValueError('%s missing title block' % self)
        return line1.split(' - ', 1)[1]

    def deps(self) -> List['SQLMixin']:
        # TODO: takewhile('select' in script)
        return [child
                for sql in self.statements()
                for child in Script._get_deps(sql)]

    def dep_closure(self) -> List['SQLMixin']:
        # TODO: takewhile('select' in script)
        return [self] + [descendant
                         for sql in self.statements()
                         for child in Script._get_deps(sql)
                         for descendant in child.dep_closure()]

    def digest(self) -> int:
        '''Hash the text of this script and its dependencies.

        >>> nodeps = Script.i2b2_crc_design
        >>> nodeps.digest() == hash(frozenset([nodeps.value]))
        True

        >>> complex = Script.cms_dem_txform
        >>> complex.digest() != hash(frozenset([complex.value]))
        True
        '''
        return hash(frozenset(s.sql for s in self.dep_closure()))

    @classmethod
    def _get_deps(cls, sql: Text) -> List['SQLMixin']:
        '''
        >>> ds = Script._get_deps(
        ...     "select col from t where 'dep' = 'oops.sql'")
        Traceback (most recent call last):
            ...
        KeyError: 'oops'

        >>> Script._get_deps(
        ...     "select col from t where x = 'name.sql'")
        []
        '''
        m = re.search(r"select \S+ from \S+ where 'dep' = '([^']+)'", sql)
        if not m:
            return []
        name, ext = m.group(1).rsplit('.', 1)
        choices = Script if ext == 'sql' else Package if ext == 'pls' else []
        deps = [s for s in choices if s.name == name]  # type: ignore # mypy issue #2305
        if not deps:
            raise KeyError(name)
        return deps

    @classmethod
    def sqlerror(cls, s: SQL) -> Optional[bool]:
        if s.strip().lower() == 'whenever sqlerror exit':
            return False
        elif s.strip().lower() == 'whenever sqlerror continue':
            return True
        return None


class ScriptMixin(SQLMixin):
    def parse(self, text: SQL) -> Iterable[StatementInContext]:
        return iter_statement(text)

    def created_objects(self) -> List[ObjectId]:
        return [obj
                for _l, _comment, stmt in iter_statement(self.sql)
                for obj in created_objects(stmt)]

    def inserted_tables(self,
                        variables: Optional[Environment]={}) -> List[Name]:
        return [obj
                for _l, _comment, stmt in iter_statement(self.sql)
                for obj in inserted_tables(
                        substitute(stmt, self._all_vars(variables)))]


class Script(ScriptMixin, enum.Enum):
    '''Script is an enum.Enum of contents.

    ISSUE: It's tempting to consider separate libraries for NAACCR,
           NTDS, etc., but that doesn't integrate well with the
           generic luigi.EnumParameter() in etl_tasks.SqlScriptTask.

    '''
    [
        # Keep sorted
        bene_chunks_create,
        bene_chunks_survey,
        cms_dem_dstats,
        cms_dem_txform,
        cms_dx_dstats,
        cms_dx_txform,
        cms_enc_dstats,
        cms_enc_txform,
        cms_encounter_mapping,
        cms_facts_load,
        cms_patient_dimension,
        cms_patient_mapping,
        cms_visit_dimension,
        i2b2_crc_design,
        mapping_reset,
        synpuf_txform,
    ] = [
        pkg.resource_string(__name__,
                            'sql_scripts/' + fname).decode('utf-8')
        for fname in [
                'bene_chunks_create.sql',
                'bene_chunks_survey.sql',
                'cms_dem_dstats.sql',
                'cms_dem_txform.sql',
                'cms_dx_dstats.sql',
                'cms_dx_txform.sql',
                'cms_enc_dstats.sql',
                'cms_enc_txform.sql',
                'cms_encounter_mapping.sql',
                'cms_facts_load.sql',
                'cms_patient_dimension.sql',
                'cms_patient_mapping.sql',
                'cms_visit_dimension.sql',
                'i2b2_crc_design.sql',
                'mapping_reset.sql',
                'synpuf_txform.sql',
        ]
    ]

    def __repr__(self) -> str:
        return '<%s(%s)>' % (self.__class__.__name__, self.name)


class PackageMixin(SQLMixin):
    def parse(self, txt: SQL) -> Iterable[StatementInContext]:
        return iter_blocks(txt)


class Package(PackageMixin, enum.Enum):
    [
        # Keep sorted
        cms_keys,
    ] = [
        pkg.resource_string(__name__,
                            'sql_scripts/' + fname).decode('utf-8')
        for fname in [
                'cms_keys.pls',
        ]
    ]

    def __repr__(self) -> str:
        return '<%s(%s)>' % (self.__class__.__name__, self.name)


class ChunkByBene(object):
    # We previously used lo, hi, but they sort as hi, lo,
    # which is hard to read, so we use first, last
    required_params = frozenset(['bene_id_first', 'bene_id_last'])

    lookup_sql = '''
    select chunk_num, chunk_size
         , bene_id_first, bene_id_last
    from bene_chunks
    where chunk_qty = :qty
    and chunk_num between :first and :last
    order by chunk_num
    '''

    bene_id_table = 'MBSF_AB_SUMMARY'

    bene_id_tables = '''
    BCARRIER_CLAIMS BCARRIER_LINE HHA_BASE_CLAIMS HHA_CONDITION_CODES
    HHA_OCCURRNCE_CODES HHA_REVENUE_CENTER HHA_SPAN_CODES
    HHA_VALUE_CODES HOSPICE_BASE_CLAIMS HOSPICE_CONDITION_CODES
    HOSPICE_OCCURRNCE_CODES HOSPICE_REVENUE_CENTER HOSPICE_SPAN_CODES
    HOSPICE_VALUE_CODES MAXDATA_IP MAXDATA_LT MAXDATA_OT MAXDATA_PS
    MAXDATA_RX MBSF_AB_SUMMARY MBSF_D_CMPNTS MEDPAR_ALL MEDPAR_ALL
    OUTPATIENT_BASE_CLAIMS OUTPATIENT_CONDITION_CODES
    OUTPATIENT_OCCURRNCE_CODES OUTPATIENT_REVENUE_CENTER
    OUTPATIENT_SPAN_CODES OUTPATIENT_VALUE_CODES PDE PDE_SAF
    '''.strip().split()

    @classmethod
    def group_chunks(cls, qty: int, group_qty: int, group_num: int) -> Tuple[int, int]:
        per_group = qty // group_qty + 1
        first = (group_num - 1) * per_group + 1
        last = min(qty, first + per_group - 1)
        return first, last

    @classmethod
    def sources_from(cls, script: Script) -> FrozenSet[Name]:
        return frozenset(
            t
            for statement in script.statements()
            for t in cls.bene_id_tables
            if '"&&{0}".{1}'.format(CMS_RIF, t.lower()) in statement)

    @classmethod
    def result_chunks(cls, result: List[RowProxy]) -> Tuple[List[Params], List[int]]:
        chunks = [dict(bene_id_first=row.bene_id_first,
                       bene_id_last=row.bene_id_last)
                  for row in result]
        sizes = [row.chunk_size for row in result]
        return chunks, sizes


def _object_to_creators(libs: List[Type[SQLMixin]]) -> Dict[ObjectId, List[SQLMixin]]:
    '''Find creator scripts for each object.

    "There can be only one."
    >>> creators = _object_to_creators([Script, Package])
    >>> [obj for obj, scripts in creators.items()
    ...  if len(scripts) > 1]
    []
    '''
    fst = lambda pair: pair[0]
    snd = lambda pair: pair[1]

    # mypy doesn't know enums are iterable
    # https://github.com/python/mypy/issues/2305
    from typing import cast

    objs = sorted(
        [(obj, s)
         for lib in libs for s in cast(Iterable[SQLMixin], lib)
         for obj in s.created_objects()],
        key=fst)
    by_obj = groupby(objs, key=fst)
    return dict((obj, list(map(snd, places))) for obj, places in by_obj)


_redefined_objects = [
    obj for obj, scripts in _object_to_creators([Script, Package]).items()
    if len(scripts) > 1]
assert _redefined_objects == [], _redefined_objects
