# Copyright © 2018 Exatel S.A.
# Contact: opensource@exatel.pl
# LICENSE: GPL-3.0-or-later, See COPYING file
# Author: Tomasz Fortuna

"""
Our algorithms crawl street graphs node by node and execute some actions at
the nodes. When the ways are very long (kilometers) without having any
intersections, the accuracy of the algorithms suffer. To improve accuracy
without adding additional complications we've altered the import algorithm to
split the overly long ways by inserting artificial nodes.
"""

import math


def m2deg(meters):
    """
    Convert length in meters to degrees in a simplified way, linearized around
    center of Poland: ~ latitude 52.0393, longitude 19.4866.

    It should work well enough in Europe/USA for the way splitter.

    0.0001° = 11.11949266456835m (latitudinal) -> 111194.92664568349m
    0.0001° =  6.83982215626177m (longitudinal) -> 68398.22156261769m
    0.0001° + 0.0001° = 0.0001414213562373095m = perpendicular = 13.054737m -> 92310.9306m

    Average: 90634.692934m
    """
    meters_per_degree = 90634.692934
    return meters / meters_per_degree


def deg2m(degrees):
    """Inverse of m2deg"""
    meters_per_degree = 90634.692934
    return degrees * meters_per_degree


class WaySplitter:
    def __init__(self, way_nodes, way_intersections, max_meters):
        self.way_nodes = way_nodes
        self.way_intersections = way_intersections

        self.max_meters = max_meters
        self.max_degrees = m2deg(max_meters)

    def split(self, node_lst):
        """
        Split way (represented by list of nodes) until no chunk exceeds given
        length.

        All coordinates are in lon/lat order.
        """
        # Current length in degrees.
        length = 0
        # Generated split ways.
        split_ways = []
        # Currently aggregated points
        current_way = []
        # Last added point for measuring distance.
        node_prev = None
        node_cur = None

        for node_cur_id in node_lst:
            node_prev = node_cur
            node_cur = self.way_nodes[node_cur_id]

            # Special case: first node
            if node_prev is None:
                current_way.append(node_cur_id)
                node_prev = node_cur
                continue

            # taxi metric - not good for vector calculations
            distance = math.sqrt((node_prev[0] - node_cur[0])**2 +
                                 (node_prev[1] - node_cur[1])**2)

            if length + distance <= self.max_degrees:
                # No length overflow: just add to the current way.
                current_way.append(node_cur_id)
                length += distance
                continue

            # Chunk length overflow.

            # Optimization: try to stick to existing nodes if possible.
            if len(current_way) >= 2 and distance <= self.max_degrees:
                # Split way on this node.
                """
                Case 0:
                    l1        l2
                P1 ---- P2 ---------- P3
                l1 < max_degrees,
                l2 < max_degrees,
                l1+l2 > max_degrees

                Result, two ways:
                P1 ---- P2   P2 ---------- P3
                """
                split_ways.append(current_way)
                current_way = current_way[-1:]
                current_way.append(node_cur_id)
                length = distance

                # New split point is an intersection
                self.way_intersections.add(current_way[0])
                continue

            # Real splitting with adding new points follows.
            """
            Case 1:
                l1                    l2
            P1 ---- P2 -------------------------------- P3
            l1 < max_degrees, but l1+l2 > max_degrees
            Result, two ways:
            P1 ---- P2 ------------- Art1  Art1 ------------------- P3

            Case 2:            l1
            P1 ------------------------------------- P2
            with l1>max_degrees

            Result:
            P1 ---- Art1  Art1 ---- Art2  Art2 ----- P2

            """
            # Artificial node required.
            # Construct a direction vector
            vector = self.unit_vector(node_prev, node_cur, distance)
            # We need one, or more artificial points.
            times = int((length + distance) / self.max_degrees)

            for i in range(0, times):
                # Create node, as far as possible
                # node_new = node_cur + vector * max_degrees
                to_go = self.max_degrees - length
                node_new = tuple(
                    prev + v * to_go
                    for prev, v in zip(node_prev, vector)
                )
                art_id = self.new_intersection(node_new, original_id=node_cur_id, cnt=i)

                # Finish current_way, and start new one.
                current_way.append(art_id)
                split_ways.append(current_way)

                # Start new one from the artificial point
                current_way = current_way[-1:]

                node_prev = node_new
                length = 0

            # Finally - add the last point which caused all the fus.
            current_way.append(node_cur_id)

        # Last one
        assert len(current_way) > 1
        split_ways.append(current_way)

        return split_ways

    @staticmethod
    def unit_vector(start, stop, distance):
        """
        Calculate a unit (length=1) vector showing direction from "start" node to "stop"
        node.
        """
        return (
            (stop[0] - start[0])/distance,
            (stop[1] - start[1])/distance
        )

    def new_intersection(self, coords, original_id, cnt):
        """
        Register a new point. Base ID on source point.
        Args:
            coords: coordinates (lon, lat)
            original_id: ID of original point
            cnt: new node number

        Returns:
            ID of new point
        """
        while True:
            new_id = original_id * 10000 + cnt
            if new_id not in self.way_nodes:
                break
            cnt += 10
        self.way_nodes[new_id] = coords
        self.way_intersections.add(new_id)
        return new_id
