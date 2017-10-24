'''cms_i2p -- i2b2 to PCORNet CDM optimized for CMS
'''

from etl_tasks import SqlScriptTask
from script_lib import Script


class HarvestInit(SqlScriptTask):
    script = Script.cdm_harvest_init
