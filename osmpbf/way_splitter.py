import math

# DEBUG TODO REMOVE
import geojson

def m2deg(meters):
    """
    Convert length in meters to degrees in a simplified way, linearized around
    center of Poland: ~ latitude 52.0393, longitude 19.4866.

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
            #print("  node_id {} current: {} prev: {}".format(node_cur_id,
            # node_cur, node_prev))

            # Special case: first node
            if node_prev is None:
                current_way.append(node_cur_id)
                node_prev = node_cur
                continue

            # taxi metric - not good for vector calculations
            distance = math.sqrt((node_prev[0] - node_cur[0])**2 +
                                 (node_prev[1] - node_cur[1])**2)

            #print("  len: {:.2f} distance: {:.2f}".format(deg2m(length),
            # deg2m(distance)))

            if length + distance <= self.max_degrees:
                # No length overflow: just add to current way.
                current_way.append(node_cur_id)
                length += distance
                continue

            # Chunk length overflow.

            # Optimization: try to stick to existing nodes.
            if len(current_way) >= 2 and distance <= self.max_degrees:
                # Split way on this node.
                """
                Case 1:
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
                node_prev = node_cur

                # New split point is an intersection
                self.way_intersections.add(current_way[0])

                #print("  len overflow: broke optimistically: {}".format(split_ways[-1]))
            else:
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
                #print("  len overflow: art point needed")

                # TODO: Factor out  as a func.
                # TODO: implement Node as a class with math.
                # Artificial node required.
                # Construct a direction vector
                vector = node_cur[0] - node_prev[0], node_cur[1] - node_prev[1]
                # normalize vector length to 1.
                vector = [x / distance for x in vector]
                # We need one, or more artificial points.
                times = int((length + distance) / self.max_degrees)
                #print("  splitting {} times".format(times))

                for i in range(0, times):
                    # Create node, as far as possible
                    # node_new = node_cur + vector * max_degrees
                    node_new = tuple(
                        prev + v * self.max_degrees * i
                        for prev, v in zip(node_prev, vector)
                    )
                    art_id = node_cur_id * 10000 + i
                    #print("  new_node: {} -> {}".format(art_id, node_new))
                    self.way_nodes[art_id] = node_new

                    # Finish current_way, and start new one.
                    current_way.append(art_id)
                    split_ways.append(current_way)

                    # Start new one from the artificial point
                    current_way = current_way[-1:]
                    length = 0

                    # New split point is an intersection
                    self.way_intersections.add(current_way[0])

                    #print("    art point added, split {}".format(split_ways[-1]))

                # Finally - add the last point which caused all the fus.
                current_way.append(node_cur_id)
                node_prev = node_cur

            # End of overflow handling

        # Last one
        if len(current_way) > 1:
            split_ways.append(current_way)

        # DEBUG
        """
        coords = shapely.geometry.LineString(
            self.way_nodes[nid]
            for nid in node_lst
        )
        print()
        print()
        print()
        print(geojson.dumps(coords))

        collection = shapely.geometry.collection.GeometryCollection([
            shapely.geometry.LineString(
                self.way_nodes[nid]
                for nid in way
            )
            for way in split_ways
        ])
        print("  ", geojson.dumps(collection))
        """
        return split_ways


