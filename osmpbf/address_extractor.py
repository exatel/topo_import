# Copyright © 2018 Exatel S.A.
# Contact: opensource@exatel.pl
# LICENSE: GPL-3.0-or-later, See COPYING file
# Author: Tomasz Fortuna

import time
from dataclasses import dataclass
from collections import defaultdict
from typing import Optional

import shapely.geometry
import shapely.wkt
import shapely.wkb

import osmium
import rtree


@dataclass
class Address:
    """Complete address with additional metadata"""
    city: str
    postcode: str
    street: str
    housenumber: str
    city_simc: str


@dataclass
class Area:
    """Area (from way or from relation). Marks administrative boundary - eg. city"""
    aid: str
    name: str
    quality: int
    level: int
    geo: object
    centroid: shapely.geometry.Point


@dataclass
class Place:
    """Addressed place, from way or from a node"""
    pid: str
    name: str
    addr: Address
    amenity: Optional[str]
    geo: object
    # From node or from a way?
    from_way: bool
    # Best matched street distance
    street_distance: float


class AddressExtractor(osmium.SimpleHandler):
    """Extract addresses from OSM database using pyosmium."""

    # Approximation
    ONE_DEGREE_IN_M = 110000

    def __init__(self):
        # Nodes that had an address
        self.places: list[Place] = []

        # Many nodes don't fancy a street. Index them so we can match streets as we read them
        self.address_idx = rtree.Index(interleaved=True)

        # Administrative areas by ID
        self.areas: Area = []

        self.stats = defaultdict(lambda: 0)

        self.start = time.time()

        # Factory that creates WKT from an osmium geometry
        self.wktfab = osmium.geom.WKTFactory()

        super().__init__()

    def match_street(self, way):
        """
        Try to name closests points to the street.

        FIXME: Doing it live while finding buildings marked as ways might cause
        some not to mark the building streets correctly. That would have to be
        a second pass to cover them all.
        """
        tags = way.tags
        name = tags.get('name', '')
        # Village names might have empty name and it's ok.

        # TODO: Can it be done more directly?
        wkt = self.wktfab.create_linestring(way)
        geo = shapely.wkt.loads(wkt)
        for place_idx in self.address_idx.nearest(geo.centroid.coords[0], 100):
            place = self.places[place_idx]
            distance = geo.distance(place.geo)
            if distance > 0.01:
                # Over some distance it doesn't matter. Let's assume it's not on this street
                break
            if distance < place.street_distance:

                if place.addr.street:
                    self.stats['place_street_override'] += 1
                else:
                    self.stats['place_street_new'] += 1
                place.street_distance = distance
                place.street = name
            else:
                self.stats['place_street_no_override'] += 1

    def way(self, way):
        """Read ways with cached nodes to get geo information."""
        tags = way.tags
        self.stats['ways'] += 1

        if self.stats['ways'] % 10000 == 0:
            print("Reading ways", dict(self.stats), time.time() - self.start)

        if 'highway' in tags:
            self.stats['way_street'] += 1
            self.match_street(way)
            return

        # TODO: We can export all the STREETS to the elasticsearch with full geo.
        # And then use it to find street nearest to the building.
        if 'building' not in tags:
            self.stats['way_not_building'] += 1
            return

        if 'addr:housenumber' not in tags:
            self.stats['way_no_housenumber'] += 1
            return

        address = self.tags_to_address(tags)

        try:
            geo = shapely.geometry.Point(way.nodes[0].location.lon, way.nodes[0].location.lat)
        except osmium._osmium.InvalidLocationError:
            self.stats['way_with_invalid_location'] += 1
            geo = None

        place = Place(
            pid=f'w{way.id}',
            name=tags.get('name', ''),
            amenity=tags.get('amenity', ''),
            addr=address,
            geo=geo,
            from_way=True,
            street_distance=99999,
        )
        self.index_address(place)

    def node(self, node):
        """Store all nodes with address (buildings, ATMs, other)."""
        self.stats['nodes'] += 1

        if self.stats['nodes'] % 1000000 == 0:
            took = time.time() - self.start
            per_s = self.stats['nodes'] / took
            print(f"Reading nodes {dict(self.stats)} in {took:.1f}s, {per_s:.1f}/s")

        tags = node.tags
        if b'addr:housenumber' not in tags:
            # TODO: What if other addr: are available?
            self.stats['node_no_housenumber'] += 1
            return

        address = self.tags_to_address(tags)
        place = Place(
            pid=f'n{node.id}',
            name=tags.get('name', ''),
            amenity=tags.get('amenity', ''),
            addr=address,
            geo=shapely.geometry.Point(node.location.lon, node.location.lat),
            from_way=False,
            street_distance=99999,
        )
        self.index_address(place)

    def index_address(self, place):
        "Add address to indices"
        idx = len(self.places)
        self.places.append(place)
        if not place.addr.street:
            self.address_idx.insert(idx, place.geo.coords[0])
            self.stats['no_street_idx'] += 1

    def area(self, area):
        """Parse areas (administrative boundaries)."""
        self.stats['areas'] += 1
        tags = area.tags

        if self.stats['areas'] % 50000 == 0:
            print("Reading areas", dict(self.stats), time.time() - self.start)

        boundary = tags.get('boundary', None)
        if boundary is None:
            self.stats['areas_not_boundary'] += 1
            return

        if boundary != 'administrative':
            self.stats['areas_not_administrative'] += 1
            return

        admin_level = int(tags.get('admin_level', "99"))
        # 1 continent, 2 is country, Then, for countries it differs.
        # PL: 4 - voivodship, 6 - powiat, 7 - gmina

        if admin_level <= 4 or admin_level >= 10:
            self.stats['areas_bad_level'] += 1
            return

        if 'religion' in tags:
            # Church administration split
            self.stats['areas_religion'] += 1
            return

        try:
            wkt = self.wktfab.create_multipolygon(area)
        except RuntimeError:
            print("Problem with reading multipolygon from area, ignoring", area)
            self.stats['area_with_runtime_error'] += 1
            return

        geo = shapely.wkt.loads(wkt)
        centroid = geo.centroid

        name = tags.get('name', '')
        reltype = tags.get('type', '')
        simc = tags.get('teryt:simc', None)
        terc = tags.get('teryt:terc', None)
        # wieś, przysiółek
        terc_type = tags.get('terc:typ', '')
        if simc:
            simc = int(simc)
        if terc:
            terc = int(terc)

        # Cities usually have population.
        has_population = 'population' in tags
        # official_name, short_name

        if name.startswith("gmina "):
            self.stats['areas_gmina'] += 1

        if name.startswith("powiat "):
            self.stats['areas_powiat'] += 1

        quality = 0
        if terc_type or simc:
            quality += 3
        if has_population:
            quality += 1

        print(f"{admin_level:2}, TT:{terc_type:5s} RT:{reltype:10s} N:{name:20s} T:{terc} S:{simc}")

        self.areas.append(Area(
            aid=f'a{area.id}',
            name=name,
            quality=quality,
            level=admin_level,
            geo=geo,
            centroid=centroid,
        ))

    def tags_to_address(self, tags):
        """Convert OSM tags to address handling nulls."""
        return Address(
            housenumber=tags.get('addr:housenumber', ''),
            city=tags.get('addr:city', ''),
            street=tags.get('addr:street', ''),
            postcode=tags.get('addr:postcode', ''),
            city_simc=tags.get('addr:city:simc', ''),
        )

    def find_area(self, geo):
        """Find area naively."""
        found_area = None
        for area in self.areas:
            boundary = area.geo
            if boundary.contains(geo):
                self.stats['matched_areas'] += 1
                found_area = area
        if not found_area:
            self.stats['unmatched_by_areas'] += 1
        return found_area

    def finish(self):
        """
        Match cities (areas) to places (ways/nodes).

        Create a list of points without cities (unmatched)
        Create index for area.
        Go throught all unmatched points and try to match closest area.

        0.5 degrees is max distance within Warsaw administrative region.
        """
        # Go through the addressed nodes and determine it's city using administrative areas

        ridx = rtree.Index(interleaved=True)
        # Coord form for interleaved:
        # [xmin, ymin, ..., kmin, xmax, ymax, ..., kmax].

        self.places.sort(key=lambda place: (place.addr.city,
                                            place.addr.street,
                                            place.addr.housenumber))

        print("Building rtree index")
        for i, area in enumerate(self.areas):
            ridx.insert(i, area.geo.bounds)

        unmatched = [place for place in self.places if not place.addr.city]

        start = time.time()

        for pos, place in enumerate(unmatched):
            parents = [
                self.areas[idx]
                for idx in ridx.intersection(place.geo.coords[0])
            ]

            # From highest level (7) to lowest (9)
            parents.sort(key=lambda ar: ar.level, reverse=True)

            if pos % 20000 == 0:
                took = time.time() - start
                print(f"Matching to cities {pos}/{len(unmatched)} in "
                      f"{took:.1f} {pos/took:.1f}/s {dict(self.stats)}")

            for parent in parents:
                # Additional check, as bounding boxes are not perfect
                if parent.geo.contains(place.geo):
                    # print(f"Placing {place.addr} in {parent.name} LVL:{parent.level}")
                    place.addr.city = area.name

                    distance = place.geo.distance(area.centroid)
                    self.stats['max_area_distance'] = max(distance, self.stats['max_area_distance'])
                    self.stats[f'matched_area_lvl{parent.level}'] += 1
                    if parent.level == 8:
                        # Those are usually cities. Those should override the 9 level.
                        # TODO: What if multiple 8 levels match?
                        break
                else:
                    # print("NOT WITHIN")
                    self.stats['bounding_box_but_no_match'] += 1

            if not parents:
                self.stats['place_without_region'] += 1
                continue

        unmatched = [un for un in unmatched if not un.addr.city]
        print(f"DIDN'T MATCH {len(unmatched)} entries finally")
        return
