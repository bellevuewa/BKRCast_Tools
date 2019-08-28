#//////////////////////////////////////////////////////////////////////////////
#////                                                                       ///
#////                                                                       ///
#//// adjacent_nodes.py                                                     ///
#////                                                                       ///
#////                                                                       ///
#////                                                                       ///
#//// Copyright 2011 INRO.                                                  ///
#////                                                                       ///
#//// Copyright (C) INRO, 2011. All rights reserved.                        ///
#//// The right to use and/or distribute this script, which employs         ///
#//// proprietary INRO programming interfaces, is granted exclusively       ///
#//// to Emme Licensees provided the following conditions are met:          ///
#//// 1.This script cannot be sold for a fee (but it can be used and        ///
#////   distributed without charge within consulting projects).             ///
#//// 2.The user is aware that this script is not a part of the Emme        ///
#////   software licence and there is no explicit or implied warranty       ///
#////   or support provided with this script.                               ///
#//// 3.This copyright notice must be preserved.                            ///
#////                                                                       ///
#//////////////////////////////////////////////////////////////////////////////

import math as _math
import traceback as _traceback
import os

import inro.modeller as _m
import inro.emme.core.exception as _except


class AdjacentNodes(_m.Tool()):
    
    north_angle = _m.Attribute(float)
    export_file = _m.Attribute(unicode)
    scenario = _m.Attribute(_m.InstanceType)
    tool_run_msg = ""
    
    def __init__(self):
        self.north_angle = 90

    def page(self):
        pb = _m.ToolPageBuilder(self,
            title="List adjacent nodes",
            description="Exports the nodes specified by a selection expression to "
                "a text file, with a list of the adjacent nodes "
                "sorted North South East and West.<br>"
                "Note: can only export nodes with four or less adjacent nodes.",
            branding_text="INRO - Emme")
        
        if self.tool_run_msg != "":
            pb.tool_run_status(self.tool_run_msg_status)
        pb.add_text_box("north_angle", size=10,
            title="North Angle:",
            note="Angle which is North, counterclockwise relative to the horizontal axis.")
        pb.add_select_file("export_file", window_type="save_file", 
            start_path=os.path.dirname(_m.Modeller().desktop.project_file_name()),
            title="Export file:")

        if not self.scenario:
            self.scenario = _m.Modeller().desktop.data_explorer().\
                            primary_scenario.core_scenario

        pb.add_select_scenario("scenario", title="Scenario:")
        
        return pb.render()
    
    def run(self):
        self.tool_run_msg = ""
        try:
            self(self.north_angle, 
                 self.export_file, self.scenario)
            self.tool_run_msg = _m.PageBuilder.format_info("Tool completed.")
        except Exception, e:
            self.tool_run_msg += _m.PageBuilder.format_exception(
                e, _traceback.format_exc(e))
            raise

    @_m.logbook_trace(name="List nodes and directionally sorted adjacent nodes",
        save_arguments=True)
    def __call__(self, north_angle, export_file, scenario): 
        network = scenario.get_network()
        classify = ClassifyAdjacentNodes(north_angle, network)
        with open(export_file, 'w') as f:
            f.write("ui3    Node   N      S      E      W\n")
            for node in network.nodes():
                if (node.data3 > 0):
                    #if node.number == 2458:
                    #    break
                    adj = classify(node)
                    adj["I"] = node.number
                    f.write('%d ' % node.data3)
                    f.write(" ".join(["%6s" % adj.get(d, "") for d in "INSEW"]))
                    f.write("\n")   
                    print(" ".join(["%6s" % adj.get(d, "") for d in "INSEW"]))
        return 
        
    @_m.method(return_type=_m.UnicodeType)
    def tool_run_msg_status(self):
        return self.tool_run_msg
        
class ClassifyAdjacentNodes(object):

    def __init__(self, north_angle, network):
        self.directions = "NWSE"        # counter clockwise order, starting with North
        self.expected_angles = [(x + north_angle) % 360 for x in range(0, 360, 90)]
        self.emme_network = network

    def __call__(self, at_node):
        out_link_nodes = set([])
        for l in at_node.outgoing_links():
            if self.emme_network.mode('a') in l.modes:
                out_link_nodes.add(l.j_node)

        in_link_nodes = set([])
        for l in at_node.incoming_links():
            if self.emme_network.mode('a') in l.modes:
                in_link_nodes.add(l.i_node)
                
        adjacent_nodes = out_link_nodes.union(in_link_nodes)
        if len(adjacent_nodes) > 4:
            print("More than 4 adjacent nodes at: %s." % at_node.id)

        directional_nodes = {}
        for node in adjacent_nodes:
            closest = self._closest_direction(at_node, node)
            directional_nodes[closest] = node
            print(closest, " ", node.number)
        if len(adjacent_nodes) != len(directional_nodes):
            print("Multiple nodes mapped to the same direction at: %s." % at_node.id)
        return directional_nodes
        
    def _closest_direction(self, at_node, dir_node):
        angle = _math.degrees(_math.atan2(dir_node.y - at_node.y, 
                                          dir_node.x - at_node.x))
        print(dir_node.number, " ", angle)
        angle_names = zip(self.expected_angles, self.directions)
        angle_diffs = [(self._normalize(a - angle), n) for a, n in angle_names]
        angle_diffs.sort()
        for atuple in angle_diffs:
            print atuple[0], atuple[1]
        return angle_diffs[0][1]
        
    def _normalize(self, ang_diff):
        v = abs(ang_diff) % 360
        if v > 180:
            return 360 - v
        return v
              
