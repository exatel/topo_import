# Copyright © 2018 Exatel S.A.
# Contact: opensource@exatel.pl
# LICENSE: GPL-3.0-or-later, See COPYING file
# Author: Tomasz Fortuna

"""
Migrates street topology from OSM pbf file into postgresql in a way which
enables easy routing.

TODO: Migrate to pyosmium.
"""

import enum
import shapely.geometry
import shapely.wkt
import shapely.wkb
from .way_splitter import WaySplitter


class WayMapping(enum.Enum):
    """
    Map OSM object type (based on tags) into "tag id".

    Only highway tags listed here are imported.

    ref: https://wiki.openstreetmap.org/wiki/Key:highway
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

    # To be removed probably.
    # path = 2000
    # cycleway = 2100
    # footway = 2100


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
    CHUNK_SIZE=5000

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

        if max_meters is not None:
            self.way_splitter = WaySplitter(self.way_nodes,
                                            self.way_intersections,
                                            max_meters)
        else:
            self.way_splitter = None

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

        if self.way_splitter is not None:
            short_ways = []
            for split_way in split_ways:
                short_ways += self.way_splitter.split(split_way)
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
