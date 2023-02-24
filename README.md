OSM Routing Importer
====================

Imports OpenStreetMap data from a binary PBF file format into PostgreSQL in a
way which allows to do a topological routing.

Program was created because osm2pgrouting (C++) was choking on a full country
import. For a single Masovian voivodeship: osm2pgrouting uses 10 minutes and
14GB of RAM. This project needs 2 minutes and 1GB of RAM - with a similar end
result (same length of migrated ways, less unnecessary splits).

This later turned out to be easy to extend and allowed us to easily modify the
import to add a way splitting.

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


If using the way splitter the new, artificial, nodes are given IDs equal to the
original node ID * 10000 + internal counter.


Address exporter
----------------

Address exporter exports address data from OSM PBF file using pyosmium library.

Example call:
`./topo_import.py --address-import --pbf ~/poland-latest.osm.pbf --cache-mem`
once exported, the IPython embed console will show and can store the data with:
`extractor.save_to_csv("output.csv")`

With `--cache-mem` will require around 12GB of free RAM to extract from Poland.
Without this option uses a node-cache in file that can take around 30GB of
space.

License & Credits
-----------------

This program is licensed under GNU GPLv3 license.


The PBF file parsing is based on a GPLv3 parsepbf project, found originally at:
http://pbf.raggedred.net/parsepbf.tar.bz2
Original readme is located in osmpbf/readme.txt.

Library was heavily rewritten to use a callback approach during reading to ease
the RAM usage and refresh the code a bit.
