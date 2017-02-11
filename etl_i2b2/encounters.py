from etl_tasks import UploadTask, ReportTask
from demographics import PatientMappingTask
from demographics import _GrouseTask, _GrouseWrapper, _make_from  # TODO: refactor

from script_lib import Script


class EncounterMappingTask(UploadTask, _GrouseWrapper):
    script = Script.cms_encounter_mapping


class VisitDimensionTask(UploadTask, _GrouseWrapper):
    script = Script.cms_visit_dimension

    def requires(self):
        return [
            _make_from(PatientMappingTask, self),
            _make_from(EncounterMappingTask, self)]


class EncounterReport(ReportTask, _GrouseTask):
    # TODO: Encounter report a la Table IIID.
    # TODO: script = Script.cms_dem_dstats
    report_name = 'encounters_per_visit_patient'

    def requires(self):
        data = _make_from(VisitDimensionTask, self)
        # report = _make_from(SqlScriptTask, self,
        #                     script=self.script)
        return data  # [data, report]


# TODO: Rollback
# EncounterReport.script,
# VisitDimensionTask.script,
# EncounterMappingTask.script,
