import cx_Oracle as cx

# kludge to avoid...
# ValueError: invalid literal for int() with base 10: '0b2'
# .../sqlalchemy/dialects/oracle/cx_oracle.py in <listcomp>(.0)
#    707 
#    708         if hasattr(self.dbapi, 'version'):
#--> 709             self.cx_oracle_ver = tuple([int(x) for x in
#    710                                         self.dbapi.version.split('.')])

cx.version = cx.version.replace('b2', '')  
