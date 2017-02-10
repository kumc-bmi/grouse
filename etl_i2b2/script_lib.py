import re

import enum
import pkg_resources as pkg


class Script(enum.Enum):
    [
        cms_ccw_spec,
        cms_dem_dstats,
        cms_dem_load,
        cms_dem_txform,
        cms_patient_mapping,
        grouse_project,
        i2b2_crc_design
    ] = [
        (fname, pkg.resource_string(__name__, 'sql_scripts/' + fname))
        for fname in [
                'cms_ccw_spec.sql',
                'cms_dem_dstats.sql',
                'cms_dem_load.sql',
                'cms_dem_txform.sql',
                'cms_patient_mapping.sql',
                'grouse_project.sql',
                'i2b2_crc_design.sql'
        ]
    ]

    def __repr__(self):
        return '<%s(%s)>' % (self.__class__.__name__, self.name)

    def statements(self,
                   variables=None,
                   separator=';\n'):
        _name, text = self.value
        return [
            Script._substitute(part.strip(), variables)
            for part in text.split(separator)
            if part.strip()]

    def title(self):
        _name, text = self.value
        title = text.split('\n', 1)[0]
        if ' - ' in title:
            title = title.split(' - ', 1)[1]
        return title

    @classmethod
    def _substitute(cls, sql, variables):
        '''Evaluate substitution variables in the style of Oracle sqlplus.
        '''
        if variables is None:
            return sql
        sql_esc = sql.replace('%', '%%')  # escape %, which we use specially
        return re.sub('&&(\w+)', r'%(\1)s', sql_esc) % variables

    @classmethod
    def params_of(cls, s, p):
        '''
        >>> Script.params_of('select 1+1 from dual', {'x':1, 'y':2})
        {}
        >>> Script.params_of('select 1+:y from dual', {'x':1, 'y':2})
        {'y': 2}
        '''
        return dict([(k, v) for k, v in p.iteritems()
                     if ':' + k in s])

    def deps(self):
        # TODO: takewhile('select' in script)
        return [script
                for sql in self.statements()
                for script in Script._get_deps(sql)]

    @classmethod
    def _get_deps(cls, sql):
        '''
        >>> ds = Script._get_deps(
        ...     "select col from t where 'dep' = 'grouse_project.sql'")
        >>> ds == [Script.grouse_project]
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
