import sys
import inro.modeller as _modeller
import inro.emme.desktop.app as _app
import inro.emme.core.exception as _exception
import itertools as _itertools
import traceback as _traceback
import datetime
import os
from shutil import copyfile

class UpdateEMMEDatabasePath(_modeller.Tool()):
    version = "1.0.0" # this is the version
    default_path = ""
    tool_run_message = ""
    BKRCastFolder = _modeller.Attribute(_modeller.InstanceType)

    def page(self):
        pb = _modeller.ToolPageBuilder(self, title="BKRCast Network Interface",
                     description="Update EMME database path",
                     branding_text="Modeling and Analysis Group -- City of Bellevue Transportation")
        pb.add_select_file("BKRCastFolder", "directory", "", self.default_path, title = "Select the BKRCast folder")

        if self.tool_run_message != "":
            pb.tool_run_status(self.tool_run_msg_status)

        return pb.render()

    @_modeller.method(return_type=_modeller.UnicodeType)
    def tool_run_msg_status(self):
        return self.tool_run_message

    @property
    def current_scenario(self):
        return _modeller.Modeller().desktop.data_explorer().primary_scenario.core_scenario

    @property
    def current_emmebank(self):
        return self.current_scenario.emmebank

    def run(self):
        self.tool_run_message = ""
        try:
            self.__call__()
            run_message = "Network exported"
            self.tool_run_message += _modeller.PageBuilder.format_info(run_message)
        except Exception, e:
            self.tool_run_message += _modeller.PageBuilder.format_exception(e, _traceback.format_exc(e))

    @_modeller.logbook_trace(name="BKRCast Update EMME Database Path", save_arguments=True)
    def __call__(self):

        #if not os.path.exists('projects'):
        #    self.tool_run_message = "projects folder does not exist"
        #    print self.tool_run_message
        #    _modeller.logbook_write(self.tool_run_message)
        #    raise Exception(self.tool_run_message)

        #if not os.path.exists('Banks'):
        #    self.tool_run_message = "banks folder does not exist"
        #    print self.tool_run_message
        #    _modeller.logbook_write(self.tool_run_message)
        #    raise Exception(self.tool_run_message)

        for folders in os.walk(self.BKRCastFolder):
            foldername = self.getFoldername(self.BKRCastFolder)
            projname = os.path.join(folders[0], foldername + ".emp")
            print projname

            project = _app.start_dedicated(True, "tool", projname)



    def getFoldername(self, fullpath):
        path = fullpath
        print path
        if fullpath[-1] == '/' or fullpath[-1] == '\\':
            length = len(path)
            path = path[:length-1]
            print path
        name = os.path.basename(path)
        print name
        return name