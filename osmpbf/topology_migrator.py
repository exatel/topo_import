"""
(C) 2018 Exatel SA
Author: Tomasz Fortuna <bla@thera.be>
License: GPLv3
"""
import enum
import math
import shapely.geometry
import shapely.wkt
import shapely.wkb
# DEBUG TODO REMOVE
import geojson
from .parsepbf import Node

class WayMapping(enum.Enum):
    """
    Map OSM object type (based on tags) into "tag id".

    Only highway tags listed here are imported.

    https://wiki.openstreetmap.org/wiki/Key:highway
    """
    motorway = 100
    motorway_link = 101
    motorway_junction = 102
    trunk = 200
    trunk_link = 201
    primary = 300
    primary_link = 301
    secondary = 400
    secondary_link = 401

    tertiary = 500
    tertiary_link = 501

    # Less important than tertiary, but with *known* classification
    unclassified = 600

    residential = 700
    living_street = 701

    service = 900

    # Foresty-road
    # track = 1000

    # Unknown classification
    road = 1100

    # To be removed
    #path = 2000
    #cycleway = 2100
    #footway = 2100

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

class TopologyMigrator:
    """
    Output - topology which allows to migrate from start_id to any_id.

    Two tables:
    vertices:
      - geometry for indexing and querying
      - lat, lon for usage without converting
    ways:
      - start/end point
      - geom - used for querying.
      - tag - route type.
      - length in meters

    To calculate length I need lat/lon of... ALL POINTS - not only intersections.
    """
    CHUNK_SIZE=1000

    def __init__(self, conn, max_meters=None):
        self.conn = conn
        # Points within routable graph.
        # Step 1: aggregate ids, and info if are intersection nodes or not.
        # {node_id: None, ...}, and then:
        # {node_id: (lon, lat), ...}

        self.way_intersections = set()
        self.way_nodes = {}

        self.ways_ignored = 0
        self.ways_found = 0

        self.ways_buffer = []
        self.nodes_buffer = []

        self.max_meters = max_meters

    def create_db(self):
        """
        Create topology model
        """
        script = """
        DROP TABLE IF EXISTS r_nodes;
        DROP TABLE IF EXISTS r_ways;

        CREATE TABLE r_nodes (
          id bigint,
          lon double precision,
          lat double precision,
          geom geometry(Point, 4326)
        );

        CREATE TABLE r_ways (
          -- Single OSM way can be split into multiple routable ones.
          id bigint,
          -- Original OSM ID
          id_osm bigint,
          type integer,
          source bigint,
          target bigint,
          lon1 double precision,
          lat1 double precision,
          lon2 double precision,
          lat2 double precision,
          name text,
          length double precision,
          geom geometry(LineString, 4326)
        );
        """
        with self.conn.cursor() as cursor:
            cursor.execute(script)
        self.conn.commit()

    def index_db(self):
        "Index database"
        sql_index = [
            "CREATE INDEX ON r_ways USING gist(geom)",
            "CREATE INDEX ON r_nodes USING gist(geom)",

            "CREATE UNIQUE INDEX ON r_nodes USING btree(id)",
            "CREATE UNIQUE INDEX ON r_ways USING btree(id)",
            "CREATE INDEX ON r_ways USING btree(id_osm)",
        ]
        with self.conn.cursor() as cursor:
            for i, sql in enumerate(sql_index):
                print("Creating index {}/{}".format(i+1, len(sql_index)))
                cursor.execute(sql)
                self.conn.commit()

    def filter_way(self, way):
        "Return True if we should filter this way out (not a street)"
        highway = way.tags.get(b'highway')
        if highway is None:
            return True

        highway = highway.decode('ascii')
        try:
            _ = WayMapping[highway]
            return False
        except KeyError:
            self.ways_ignored += 1
            return True

    def node_optimisation_cb(self, way):
        """
        PBF files have nodes first, and then ways. We are doing two passes over the
        file to read only the required nodes and limit RAM usage.

        This way-callback is used in first pass to aggregate IDs of nodes used in ways
        and to detect intersections in the middle of the ways.
        """
        if self.filter_way(way):
            self.ways_ignored += 1
            return
        self.ways_found += 1

        # Mark all nodes and intersections for aggregating data
        for node_id in way.nodes:
            # If the node was already marked - it is an intersection.
            if node_id in self.way_nodes:
                self.way_intersections.add(node_id)

            # Mark as used.
            self.way_nodes[node_id] = None

        # Always mark beginning/end as intersection nodes
        self.way_intersections.add(way.nodes[0])
        self.way_intersections.add(way.nodes[-1])


    def node_cb(self, node):
        """
        Node-callback used in second pass to aggregate lat/lon of all relevant
        nodes appearing in ways.
        """
        # If node is part of a way - store lat/lon
        if node.node_id in self.way_nodes:
            self.way_nodes[node.node_id] = (node.lon, node.lat)

    def split_long(self, node_lst, max_meters=25):
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

        max_degrees = m2deg(max_meters)

        for node_cur_id in node_lst:
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

            if length + distance > max_degrees:
                # Chunk length overflow.

                # Optimization: try to stick to existing nodes.
                if len(current_way) >= 2 and distance <= max_degrees:
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
                    times = int((length + distance) / max_degrees)
                    #print("  splitting {} times".format(times))

                    for i in range(0, times):
                        # Create node, as far as possible
                        # node_new = node_cur + vector * max_degrees
                        node_new = tuple(
                            prev + v * max_degrees * i
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
                        #print("    art point added, split {}".format(split_ways[-1]))

                    # Finally - add the last point which caused all the fus.
                    current_way.append(node_cur_id)
                    node_prev = node_cur


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

    def way_cb(self, way):
        """
        Way-callback used in second pass to split and migrate ways.

        We split ways on intersections and then artificially split ways if they
        are too long.
        """
        if self.filter_way(way):
            return

        # Extract basics
        highway = way.tags[b'highway']
        tag = WayMapping[highway.decode('ascii')]

        name = way.tags.get(b'name', b'').decode('utf-8')

        # Single way should be split if there's an intersection with another
        # route in the middle - T-shaped.

        # List of split ways created from input way.
        # Single node is always shared between split ways
        # [[node_id1, node_id2, ..., node_idX], [node_idX, node_idX+1, ...]]
        split_ways = []

        # 1) Built ways and split them on intersections.
        cur_way = [way.nodes[0]]
        for node_id in way.nodes[1:]:
            if node_id in self.way_intersections:
                # Split at this point
                cur_way.append(node_id)
                split_ways.append(cur_way)
                cur_way=[node_id]
            else:
                # Just aggregate - no intersection here.
                cur_way.append(node_id)

        # 2) Split too long split_ways even further.
        if self.max_meters is not None:
            short_ways = []
            for split_way in split_ways:
                short_ways += self.split_long(split_way,
                                              max_meters=self.max_meters)
        else:
            short_ways = split_ways
        # 3) Store ways in DB
        for i, nodes in enumerate(short_ways):
            geom = [self.way_nodes[node_id] for node_id in nodes]
            line_string = shapely.geometry.LineString(geom)
            cur_id = way.way_id * 10000 + i
            data = (
                cur_id,
                way.way_id,
                tag.value,
                nodes[0], nodes[-1],
                geom[0][0], geom[0][1],
                geom[-1][0], geom[-1][1],
                name,
                shapely.wkb.dumps(line_string),
            )

            self.ways_buffer.append(data)

        if len(self.ways_buffer) > self.CHUNK_SIZE:
            # Mass import.
            self.flush()


    def flush(self):
        """
        Flush buffers into the DB

        TODO: COPY would be faster. Yet executemany seems to do the trick.
        """
        if self.ways_buffer:
            sql = ("INSERT INTO r_ways (id, id_osm, type,  source, target, "
                   "lon1, lat1, lon2, lat2,  name,  geom) VALUES "
                   "(%s, %s, %s,  %s, %s, "
                   "%s, %s, %s, %s,  %s,"
                   "ST_GeomFromWKB(%s::geometry, 4326))")
            with self.conn.cursor() as cursor:
                cursor.executemany(sql, self.ways_buffer)
                self.ways_buffer = []
                self.conn.commit()

        if self.nodes_buffer:
            sql = ("INSERT INTO r_nodes (id, lon, lat,  geom) VALUES "
                   "(%s, %s, %s, "
                   "ST_GeomFromWKB(%s::geometry, 4326))")
            with self.conn.cursor() as cursor:
                cursor.executemany(sql, self.nodes_buffer)
                self.nodes_buffer = []
                self.conn.commit()

    def import_nodes(self):
        """
        Import cached intersection nodes into DB.

        TODO: This can be merged with 2nd-pass node-callback.
        """
        print("Import {} nodes into DB".format(len(self.way_intersections)))
        for node_id in self.way_intersections:
            lon, lat = self.way_nodes[node_id]
            point = shapely.geometry.Point([lon, lat])
            data = (
                node_id,
                lon, lat,
                shapely.wkb.dumps(point),
            )
            self.nodes_buffer.append(data)

            if len(self.nodes_buffer) > self.CHUNK_SIZE:
                # Mass import.
                self.flush()

    def finish(self):
        """
        Final touches - flush caches, convert length and create indexes.
        """
        print("Flush cache")
        self.flush()

        print("Calculate length in meters")
        with self.conn.cursor() as cursor:
            sql = "UPDATE r_ways SET length=ST_Length(geom::geography)"
            cursor.execute(sql)
        self.conn.commit()

        self.index_db()
