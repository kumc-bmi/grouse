'''pcornet_ont - Luigi Tasks to help load the SCILHS PCORNet Ontology

'''

from datetime import datetime
from typing import List, cast

import luigi

from etl_tasks import DBAccessTask, SourceTask
from etl_tasks import DBTarget, SchemaTarget, TimeStampParameter
from ont_load import MetaToConcepts
from param_val import StrParam


class PCORNetConcepts(luigi.WrapperTask):
    meta_tables = [
        'PCORNET_DEMO',
        'PCORNET_DIAG',
        'PCORNET_ENC',
        'PCORNET_ENROLL',
        'PCORNET_LAB',
        'PCORNET_MED',
        'PCORNET_PROC',
        'PCORNET_VITAL',
    ]

    def requires(self) -> List[luigi.Task]:
        return [
            PCORNetMetaToConcepts(ont_table_name=table)
            for table in self.meta_tables]


class PCORNetMetaToConcepts(MetaToConcepts):
    @property
    def i2b2meta(self) -> str:
        return self.source.pcorimetadata  # type: ignore

    @property
    def source(self) -> SourceTask:
        return TerminologyDumpImport()


class TerminologyDumpImport(SourceTask, DBAccessTask):
    '''
      - Shrine Terminology Files - Oracle Reeder Apr 10
        Reeder at UTSouthwestern
        Mon Apr 10 10:39:05 CDT 2017
        http://listserv.kumc.edu/pipermail/gpc-dev/2017q2/003805.html
      - ​basic SNOW ontology alignment: Demographics, Diagnoses
        GPC:ticket:525#comment:12 Apr 17
        https://informatics.gpcnetwork.org/trac/Project/ticket/525#comment:12
      - PCORIMETADATA_3_24_2017.dmp
    '''
    # March 24 is really last modified date.
    # Jun  6 09:17 is when I downloaded it and imported it.
    download_date = cast(datetime, TimeStampParameter(default=datetime(2017, 3, 24)))
    source_cd = "'PCORNET_CDM'"

    pcorimetadata = StrParam(default='PCORIMETADATA')
    table_eg = 'pcornet_demo'

    def _dbtarget(self) -> DBTarget:
        return SchemaTarget(self._make_url(self.account),
                            schema_name=self.pcorimetadata,
                            table_eg=self.table_eg,
                            echo=self.echo)
