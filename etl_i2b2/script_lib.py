import re

import enum
import pkg_resources as pkg

from sql_syntax import (
    iter_statement, substitute,
    created_objects, inserted_tables)

I2B2STAR = 'I2B2STAR'  # cf. &&I2B2STAR in sql_scripts


class Script(enum.Enum):
    [
        cms_ccw_spec,
        cms_dem_dstats,
        cms_dem_load,
        cms_dem_txform,
        cms_patient_dimension,
        cms_patient_mapping,
        i2b2_crc_design
    ] = [
        (fname, pkg.resource_string(__name__, 'sql_scripts/' + fname))
        for fname in [
                'cms_ccw_spec.sql',
                'cms_dem_dstats.sql',
                'cms_dem_load.sql',
                'cms_dem_txform.sql',
                'cms_patient_dimension.sql',
                'cms_patient_mapping.sql',
                'i2b2_crc_design.sql'
        ]
    ]

    def __repr__(self):
        return '<%s(%s)>' % (self.__class__.__name__, self.name)

    def statements(self,
                   variables=None):
        _name, text = self.value
        return [
            substitute(statement, variables)
            for _line, _comment, statement in iter_statement(text)]

    def created_objects(self):
        '''
        >>> Script.cms_ccw_spec.created_objects()
        [(<Script(cms_ccw_spec)>, ('view', 'cms_ccw'))]

        TODO: indexes
        '''
        _name, text = self.value
        return [(dep, obj)
                for dep in ([self] + self.deps())
                for (_name, text) in [dep.value]
                for _l, _comment, stmt in iter_statement(text)
                for obj in created_objects(stmt)]

    def inserted_tables(self,
                        variables={}):
        '''
        >>> Script.cms_patient_mapping.inserted_tables(
        ...     variables={'I2B2STAR': 'i2b2demodata'})
        [(<Script(cms_patient_mapping)>, '"i2b2demodata".patient_mapping')]
        '''
        return [(dep, obj)
                for dep in ([self] + self.deps())
                for (_name, text) in [dep.value]
                for _l, _comment, stmt in iter_statement(text)
                for obj in inserted_tables(substitute(stmt, variables))]

    def title(self):
        _name, text = self.value
        title = text.split('\n', 1)[0]
        if ' - ' in title:
            title = title.split(' - ', 1)[1]
        return title

    def deps(self):
        # TODO: takewhile('select' in script)
        return [script
                for sql in self.statements()
                for script in Script._get_deps(sql)]

    @classmethod
    def _get_deps(cls, sql):
        '''
        >>> ds = Script._get_deps(
        ...     "select col from t where 'dep' = 'cms_dem_txform.sql'")
        >>> ds == [Script.cms_dem_txform]
        True

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
        name = m.group(1).replace('.sql', '')
        deps = [s for s in Script if s.name == name]
        if not deps:
            raise KeyError(name)
        return deps
