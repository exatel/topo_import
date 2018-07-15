OSM Routing Importer
====================

Imports OpenStreetMap data from a binary PBF file format into PostgreSQL in a
way which allows to do a topological routing.

Program was created because osm2pgrouting (C++) was choking on a full country
import. For a single Masovian voivodeship: osm2pgrouting uses 10 minutes and
14GB of RAM. This project needs 2 minutes and 1GB of RAM - with a similar end
result (same length of migrated ways, less unnecessary splits).

Model
-----

Two tables are used: 

Ways table:

     Column |           Type
    --------|---------------------------
     id     | bigint  # Generated ID, as OSM ways are split at intersections
     id_osm | bigint  # Original (duplicated) osm_id
     type   | integer # Type based on highway tag
     source | bigint  # source node_id of intersection 
     target | bigint  # target node_id
     lon1   | double precision # start/target coords
     lat1   | double precision
     lon2   | double precision
     lat2   | double precision
     name   | text             # Way name - if any
     length | double precision # Length in meters
     geom   | geometry(LineString,4326) # Geometry

Nodes table, contains only nodes on intersections of the ways.

     Column |         Type
    --------|----------------------
     id     | bigint  # OSM ID of a node
     lon    | double precision
     lat    | double precision
     geom   | geometry(Point,4326)



License & Credits
-----------------

PBF file parsing is based on a project found at:
http://pbf.raggedred.net/parsepbf.tar.bz2
Original readme in osmpbf/readme.txt.

Program was heavily rewritten to use callback-way approach instead of RAM-heavy
caching of everything.

Therefore the code is licensed under GPLv3 license.
