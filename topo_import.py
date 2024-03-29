#!/usr/bin/env python3

# Copyright © 2018 Exatel S.A.
# Contact: opensource@exatel.pl
# LICENSE: GPL-3.0-or-later, See COPYING file
# Author: Tomasz Fortuna

import argparse
import os.path
import sys
from time import time

import psycopg2
import psycopg2.extras
from IPython import embed

from osmpbf import AddressExtractor, GeometryMatcher, PBFParser, StreetMatcher, TopologyMigrator


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--pbf", required=True, help="pbf file to import")
    p.add_argument("--host", default="127.0.0.1", type=str,
                   help="postgresql database address")
    p.add_argument("--db", default="pgroute", type=str,
                   help="database name")

    p.add_argument("--topo-import", action="store_true",
                   help="Import topology into postgresql DB")
    p.add_argument("--address-import", action="store_true",
                   help="Import address data")
    p.add_argument("--cache-mem", action="store_true",
                   help="Cache nodes/points in memory instead of a file")

    p.add_argument("--port", default=None,
                   help="database connection port")
    p.add_argument("--username", required=False,
                   help="database connection username")

    # TODO: Use file or ask instead
    p.add_argument("--password", required=False,
                   help="database connection password (WARN: will be listed in process list)")

    p.add_argument("--max-meters", default=None,
                   required=False,
                   type=int,
                   help="split ways exceeding X meters")

    p.add_argument("--output-path", nargs="?", default=None, type=str, const="output.csv",
                   help="Use with address import."
                        "The path for the csv file where the output from the address importer"
                        "should be written."
                        "If not specified, an ipython embed will open at the end.")

    args = p.parse_args()

    if args.topo_import == args.address_import:
        p.error("Pass either --topo-import or --address-import")

    if args.topo_import and (args.username is None or args.password is None):
        p.error("--topo-import requires DB username/password")

    if not os.path.exists(args.pbf):
        print("The binary file %s cannot be found" % args.pbf)
        sys.exit(1)

    return args


def connect(args):
    print("Connect to database")
    conn = psycopg2.connect(host=args.host,
                            dbname=args.db,
                            port=args.port,
                            user=args.username,
                            password=args.password,
                            cursor_factory=psycopg2.extras.NamedTupleCursor)

    return conn


def address_import(args):
    """Import address geolocalization data."""
    extractor = AddressExtractor()

    if args.cache_mem:
        idx = 'flex_mem'
    else:
        idx = 'sparse_file_array,node-cache.data'

    try:
        extractor.start_inner = time()
        extractor.apply_file(args.pbf, locations=True, idx=idx)
        took = time() - extractor.start_inner
        print(f"Aggregating places took {took:.1f} seconds")
        print(f"Final stats {dict(extractor.stats)}")

        extractor.start_inner = time()
        matcher = GeometryMatcher(extractor)
        print("Starting Relations geometry matcher")
        matcher.apply_file(args.pbf, locations=True, idx=idx)
        took = time() - extractor.start_inner
        print(f"Matching Relations geometry took {took:.1f} seconds")
        print(f"Matcher stats {dict(matcher.stats)}")

        extractor.start_inner = time()
        matcher = StreetMatcher(extractor)
        print("Starting street matcher")
        matcher.apply_file(args.pbf, locations=True, idx=idx)
        took = time() - extractor.start_inner
        print(f"Matching streets took {took:.1f} seconds")
        print(f"Matcher stats {dict(matcher.stats)}")

        extractor.start_inner = time()
        print("Starting matching cities (areas) to places.")
        extractor.finish()
        took = time() - extractor.start_inner
        print(f"Matching addresses to administrative boundaries took {took:.1f} seconds")
        print(f"Total extract time is {extractor.took:.1f}s")
    except KeyboardInterrupt:
        print("Starting shell after interrupt")
        print(dict(extractor.stats))

    if args.output_path:
        extractor.save_to_csv(args.output_path)
        sys.exit(0)

    # Allow manipulation of loaded data before quitting.
    embed()


def topo_import(args):
    conn = connect(args)
    # 1) First pass - migrate ways and a aggregate node ids
    migrator = TopologyMigrator(conn, args.max_meters)

    print("Create an empty scheme")
    migrator.create_db()

    print()
    print("1st-pass: Aggregate node ids of intersections and parts of ways:")
    with open(args.pbf, "rb") as fpbf:
        # While going through ways (way_callback) call node_optimisation_cb
        # function to gather node ids and intersections required later.
        p = PBFParser(fpbf,
                      way_callback=migrator.node_optimisation_cb)

        if not p.parse():
            print("Error while parsing the file")
            return

    print()
    print("2nd-pass: Gather node coordinates and import ways:")
    with open(args.pbf, "rb") as fpbf:
        # node_callback will simply aggregate latitude and longitude
        # of previously marked nodes in RAM.

        # way_callback aggregates way data with all the geometry and stores in
        # the DB as it reads them. It holds most logic as it can split imported
        # ways into smaller parts.
        p = PBFParser(fpbf,
                      node_callback=migrator.node_cb,
                      way_callback=migrator.way_cb)

        if not p.parse():
            print("Error while parsing the file")
            return

    print()

    # Import intersection nodes
    migrator.import_nodes()

    # Convert length and index data
    migrator.finish()


def main():
    # the main part of the program starts here
    # extract the command line options

    args = parse_args()

    print("Loading the PBF file: %s" % args.pbf)
    if args.topo_import:
        topo_import(args)
    if args.address_import:
        address_import(args)


if __name__ == '__main__':
    main()
