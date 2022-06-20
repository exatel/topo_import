# Copyright © 2022 Exatel S.A.
# Contact: opensource@exatel.pl
# LICENSE: GPL-3.0-or-later, See COPYING file
# Author: Tomasz Fortuna

"""
Extracts addresses from country PBF file.

Handles:
- Buildings (places) that are addressed nodes.
- Buildings that are closed ways.
- Ways/nodes might be tagged with streets/cities or not.
- Buildings without cities are matched to administrative boundary.
- Buildings without streets are matched to the nearest streets.

It's tuned currently for Poland (administrative levels for example; 8 seems to
be city, but 9 is applicable to addresses). Should be easy to change.

Example extract from Poland:
- 8033831 places total
- 1573 places without city
- 5297743 places with city and named street
- 7778386 with city and a street matched to a way (including unnamed)
- 2651073 streets matched to a way (170k named)

extractor.stats:
 'addr_no_city': 2929308,
 'addr_no_city_with_place': 2900615,
 'addr_no_city_with_street': 42663,
 'addr_no_street': 2904957,
 'addr_no_street_with_place': 2904730,
 'addr_with_place_and_street': 14509,
 'area_with_runtime_error': 4,
 'areas': 20019609,
 'areas_bad_level': 2218,
 'areas_gmina': 2174,
 'areas_not_administrative': 17084,
 'areas_not_boundary': 19971369,
 'areas_powiat': 314,
 'bounding_box_but_no_match': 63086,
 'matched_area_lvl5': 2304,
 'matched_area_lvl6': 27067,
 'matched_area_lvl7': 27067,
 'matched_area_lvl8': 25908,
 'matched_area_lvl9': 102,
 'max_area_distance': 0.5317941698374715,
 'no_street_idx': 2904957,
 'node_no_housenumber': 182717327,
 'nodes': 186336433,
 'place_without_region': 41
 'way_no_housenumber': 11742782,
 'way_not_building': 9693906,
 'ways': 25851413,

matcher.stats:
 'ignore_street_type': 3251281
 'place_street_keep_named': 103605,
 'place_street_new': 3775515,
 'place_street_no_override': 2983263,
 'place_street_override': 177246,
 'street_close_enough': 6936024,
 'street_too_far': 2835054,
 'streets': 4386685,
 'unknown_street': 508552,
 'ways': 25851413,
"""

import time
import csv
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
    """Complete address with additional metadata."""

    city: str
    postcode: str
    street: str
    housenumber: str
    city_simc: str


@dataclass
class Area:
    """Area (from way or relation). Administrative boundary - eg. city."""

    aid: str
    name: str
    quality: int
    level: int
    geo: object
    centroid: shapely.geometry.Point


@dataclass
class Place:
    """Addressed place, from way or from a node."""
    pid: str
    name: str
    addr: Address
    amenity: Optional[str]
    geo: object

    # Best matched street distance. Will be large for directly addressed
    # streets.
    street_distance: float
    # When street name is copied from a way, this is a way ID
    street_id: str = None

    # City was set from the administrative area.
    city_from_area: bool = False


class AddressExtractor(osmium.SimpleHandler):
    """Extract addresses from OSM database using pyosmium."""

    # Approximation
    ONE_DEGREE_IN_M = 110000

    def __init__(self):
        # Nodes that had an address
        self.places: list[Place] = []

        # Many nodes don't fancy a street. Index them so we can match streets as we read them
        # We CAN'T sort places while this index is in use.
        self.address_idx = rtree.Index(interleaved=True)

        # Administrative areas by ID
        self.areas: Area = []

        self.stats = defaultdict(lambda: 0)

        self.start = time.time()

        # Factory that creates WKT from an osmium geometry
        self.wktfab = osmium.geom.WKTFactory()

        self.start = time.time()
        self.took = None

        super().__init__()

    def save_to_csv(self, filename):
        """Save places to CSV file."""
        fields = [
            'pid', 'name', 'city', 'postcode', 'street', 'housenumber',
            'simc', 'amenity', 'lon', 'lat', 'street_distance', 'city_from_area'
        ]
        start = time.time()
        with open(filename, "w") as csvf:
            writer = csv.writer(csvf)
            writer.writerow(fields)
            for idx, place in enumerate(self.places):
                data = [
                    place.pid, place.name, place.addr.city, place.addr.postcode,
                    place.addr.street, place.addr.housenumber, place.addr.city_simc,
                    place.amenity, place.geo.coords[0][0], place.geo.coords[0][1],
                    place.street_distance, "1" if place.city_from_area else "0"
                ]
                writer.writerow(data)
                if idx % 100000 == 0:
                    took = time.time() - start
                    print(f"Stored {idx} in {took:.1f}; {idx/took:.1f}/s")

    def way(self, way):
        """Read ways with cached nodes to get geo information."""
        tags = way.tags
        self.stats['ways'] += 1

        if self.stats['ways'] % 10000 == 0:
            print("Reading ways", dict(self.stats), time.time() - self.start)

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
            wkt = self.wktfab.create_linestring(way)
            geo = shapely.wkt.loads(wkt)
        except osmium._osmium.InvalidLocationError:
            self.stats['way_with_invalid_location'] += 1
            geo = None

        place = Place(
            pid=f'w{way.id}',
            name=tags.get('name', ''),
            amenity=tags.get('amenity', ''),
            addr=address,
            geo=geo.centroid,
            # For places without streets will be relaxed later to the nearest street (in degrees).
            street_distance=360,
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
            street_distance=360,
        )
        self.index_address(place)

    def index_address(self, place):
        """Add address to indices."""
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

        # print(f"{admin_level:2}, TT:{terc_type:5s} RT:{reltype:10s} "
        #       f"N:{name:20s} T:{terc} S:{simc}")

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
        place = tags.get('addr:place', '')
        street = tags.get('addr:street', '')
        city = tags.get('addr:city', '')
        # Enumerate different cases in stats
        if not city:
            self.stats['addr_no_city'] += 1
            if place:
                self.stats['addr_no_city_with_place'] += 1
            if street:
                self.stats['addr_no_city_with_street'] += 1

        if not street:
            self.stats['addr_no_street'] += 1
            if place:
                self.stats['addr_no_street_with_place'] += 1
        else:
            if place:
                # This is an error according to:
                # https://wiki.openstreetmap.org/wiki/Key:addr:place
                self.stats['addr_with_place_and_street'] += 1

        return Address(
            housenumber=tags.get('addr:housenumber', ''),
            # Fall back to place, which seems to work sometimes in Poland
            city=city or place,
            street=street,
            postcode=tags.get('addr:postcode', ''),
            city_simc=tags.get('addr:city:simc', ''),
        )

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

        # Indices in address_idx will be invalid after the sort.
        del self.address_idx
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
            parents.sort(key=lambda ar: ar.level, reverse=False)

            if pos % 5000 == 0:
                took = time.time() - start
                print(f"Matching to cities {pos}/{len(unmatched)} in "
                      f"{took:.1f} {pos/took:.1f}/s {dict(self.stats)}")

            for parent in parents:
                # Additional check, as bounding boxes are not perfect
                if parent.geo.contains(place.geo):
                    place.addr.city = parent.name
                    place.city_from_area = True

                    distance = place.geo.distance(parent.centroid)
                    self.stats['max_area_distance'] = max(distance,
                                                          self.stats['max_area_distance'])
                    self.stats[f'matched_area_lvl{parent.level}'] += 1
                    if parent.level == 8:
                        # Those are usually cities. Those should override the 9 level.
                        # TODO: What if multiple 8 levels match?
                        break
                else:
                    self.stats['bounding_box_but_no_match'] += 1

            if not parents:
                self.stats['place_without_region'] += 1
                continue

        self.took = time.time() - self.start
        return


class StreetMatcher(osmium.SimpleHandler):
    """Find addresses without streets and try to match it."""

    # Max 200m from street
    MAX_DISTANCE = 0.002

    def __init__(self, extractor):
        self.extractor = extractor
        self.start = time.time()
        self.stats = defaultdict(lambda: 0)
        self.wktfab = osmium.geom.WKTFactory()
        super().__init__()

    def way(self, way):
        """Try to name closests points to the street."""
        tags = way.tags
        self.stats['ways'] += 1

        if self.stats['ways'] % 50000 == 0:
            took = time.time() - self.start
            print(f"StreetMatcher reading ways in "
                  f"{took:.1f}s {self.stats['ways']/took:.1f}/s")
            print("  ", dict(self.stats))

        if 'highway' not in tags:
            return

        way_type = tags.get("highway")

        self.stats['streets'] += 1
        name = tags.get('name', '')

        if way_type in {"footway", "track", "sidewalk", "pedestrian",
                        "cycleway", "service", "construction", "path"}:
            self.stats['ignore_street_type'] += 1
            return

        if not name:
            self.stats['unknown_street'] += 1
        # Village names might have empty name and it's ok.

        # TODO: Can it be done more directly?
        try:
            wkt = self.wktfab.create_linestring(way)
            geo = shapely.wkt.loads(wkt)
        except osmium._osmium.InvalidLocationError:
            self.stats['way_with_invalid_location'] += 1
            return

        # Do intersection with street bounding box to find all possible houses.
        # Then iterate over possibilities and measure actual distance and bind
        # to the closest street.
        residential = (
            geo.bounds[0] - self.MAX_DISTANCE, geo.bounds[1] - self.MAX_DISTANCE,
            geo.bounds[2] + self.MAX_DISTANCE, geo.bounds[3] + self.MAX_DISTANCE
        )
        for place_idx in self.extractor.address_idx.intersection(residential):
            place = self.extractor.places[place_idx]

            distance = geo.distance(place.geo)
            if distance > self.MAX_DISTANCE:
                # Over some distance it doesn't matter. Let's assume it's not on this street
                self.stats['street_too_far'] += 1
                continue
            self.stats['street_close_enough'] += 1

            if distance < place.street_distance:
                # Street is closer than the previous one.
                if place.addr.street:
                    self.stats['place_street_override'] += 1
                    if not name:
                        # Don't replace named with unnamed
                        self.stats['place_street_keep_named'] += 1
                        continue
                else:
                    self.stats['place_street_new'] += 1
                place.addr.street = name
                place.street_distance = distance
                place.street_id = way.id
            else:
                self.stats['place_street_no_override'] += 1
