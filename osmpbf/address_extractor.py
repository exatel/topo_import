# Copyright © 2018 Exatel S.A.
# Contact: opensource@exatel.pl
# LICENSE: GPL-3.0-or-later, See COPYING file
# Author: Tomasz Fortuna

import enum
from dataclasses import dataclass
import shapely.geometry
from shapely.geometry.polygon import Polygon
import shapely.wkt
import shapely.wkb


@dataclass
class Address:
    city: str
    postcode: str
    street: str
    housenumber: str
    city_simc: str


class AddressExtractor:
    """
    """

    def __init__(self):
        ##
        # Pass 0
        # ways/some nodes required to parse required relations,
        # or later to geolocate buildings made of ways
        self.required_ways = set()
        self.required_nodes = set()
        self.relations = {}

        ##
        # Pass 1 cache

        # Nodes that had an address
        self.addressed_nodes = {}

        # Most likely buildings.
        self.addressed_ways = {}

        # {way_id: [node_id, node_id, ...], ...}
        self.ways_buffer = {}

        ##
        # Pass 2

        # Store nodes that are required to resolve ways positions
        self.nodes_buffer = {}

    def way_cb_pass_1(self, way):
        """
        PBF files have nodes first, and then ways. We are doing multiple passes.

        This aggregates all node IDs that are required to locate buildings

        Maybe to locate streets?
        """
        tags = way.tags

        if way.way_id in self.required_ways:
            # Required for administrative relation
            self.ways_buffer[way.way_id] = way.nodes
            for node_id in way.nodes:
                self.required_nodes.add(node_id)

        if b'building' not in tags:
            return

        address = self.tags_to_address(tags)
        for node_id in way.nodes:
            if node_id in self.addressed_nodes:
                found = self.addressed_nodes[node_id]
                if found['addr'] != address:
                    print("PROBABLY NOT CERTAIN - REMOVE?")
                    print(way)
                    print(found)
                    print()
                # Ok, it's found already - move on.
                return

        # TODO: Add only first node; should be enough to get lat/lon accurately enough.
        self.required_nodes.add(way.nodes[0])
        self.addressed_ways[way.way_id] = {
            'name': tags.get(b'name', b'').decode('utf-8'),
            'amenity': tags.get(b'amenity', b'').decode('utf-8'),
            'addr': address,
            'node_id': way.nodes[0],

            # Will be filled in pass 2
            'geo': None,
        }

    def relation_cb_pass_0(self, relation):
        """
        Gather administrative relations
        """
        tags = relation.tags
        boundary = tags.get(b'boundary', None)
        if boundary is None:
            return

        admin_level = int(tags.get(b'admin_level', 99))
        # 1 continent, 2 is country, Then, for countries it differs.
        # PL: 4 - voivodship, 6 - powiat, 7 - gmina

        if admin_level <= 4 or admin_level >= 10:
            return

        if b'religion' in tags:
            # Church administration split
            return

        name = tags.get(b'name', b'').decode('utf-8')
        reltype = tags.get(b'type', '').decode('utf-8')
        simc = tags.get(b'teryt:simc', None)
        terc = tags.get(b'teryt:terc', None)
        # wieś, przysiółek
        terc_type = tags.get(b'terc:typ', b'').decode('utf-8')
        if simc:
            simc = int(simc)
        if terc:
            terc = int(terc)
        # Cities usually have population.
        has_population = b'population' in tags
        # official_name, short_name

        if name.startswith("gmina "):
            # Duplicate?
            # TODO: Handle
            return

        quality = 0
        if terc_type or simc:
            quality += 3
        if has_population:
            quality += 1

        #k = [k.decode('utf-8') for k in tags]
        # print(f"{admin_level:2}, TT:{terc_type:5s} RT:{reltype:10s} N:{name:20s} T:{terc} S:{simc} {k}")

        polygon = []
        for member in relation.members:
            if member['type'] == 'way' and member['role'] == b'outer':
                self.required_ways.add(member['id'])
                polygon.append(member['id'])
            if member['type'] == 'node' and member['role'] == b'admin_centre':
                self.required_nodes.add(member['id'])

        self.relations[relation.relation_id] = {
            'name': name,
            'quality': quality,
            'level': admin_level,
            'poly': polygon,
            # Will be created later
            'shape': None,
        }

    def tags_to_address(self, tags):
        "Convert OSM tags to address handling nulls"
        return Address(
            housenumber=tags.get(b'addr:housenumber', b'').decode('utf-8'),
            city=tags.get(b'addr:city', b'').decode('utf-8'),
            street=tags.get(b'addr:street', b'').decode('utf-8'),
            postcode=tags.get(b'addr:postcode', b'').decode('utf-8'),
            city_simc=tags.get(b'addr:city:simc', b'').decode('utf-8'),
        )

    def node_cb_pass_1(self, node):
        """
        Store all nodes with address (buildings, ATMs, other)
        """
        # If node is part of a way - store lat/lon
        #if node.node_id in self.way_nodes:
            #self.way_nodes[node.node_id] = (node.lon, node.lat)

        tags = node.tags

        if b'addr:housenumber' not in tags:
            # TODO: What if other addr: are available?
            return

        address = self.tags_to_address(tags)
        self.addressed_nodes[node.node_id] = {
            'name': tags.get(b'name', b'').decode('utf-8'),
            'amenity': tags.get(b'amenity', b'').decode('utf-8'),
            'addr': address,
            'geo': (node.lon, node.lat),
        }

    def node_cb_pass_2(self, node):
        """
        Node-callback used in second pass to aggregate lat/lon of all relevant
        nodes appearing in ways.
        """
        # If node is part of a way (building) - store lat/lon
        if node.node_id in self.required_nodes:
            self.nodes_buffer[node.node_id] = (node.lon, node.lat)

    def pass_2_finish(self):
        "Finish mapping between ways and nodes in pass 2"
        for way in self.addressed_ways.values():
            # PBF is broken if there's no matching node.
            geo = self.nodes_buffer[way['node_id']]
            way['geo'] = geo

        for rel_id, relation in self.relations.items():
            points = []
            for way_id in relation['poly']:
                if way_id not in self.ways_buffer:
                    print(f"WARNING: Relation {rel_id} related to nonexistant way {way_id}")
                    continue
                for node_id in self.ways_buffer[way_id]:
                    points.append(self.nodes_buffer[node_id])
            relation['shape'] = Polygon(points)
            del relation['poly']

    def finish(self):
        """
        Final touches - flush caches, convert length and create indexes.
        """
            # point = shapely.geometry.Point([lon, lat])
                 # shapely.wkb.dumps(point),
