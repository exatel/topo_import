# -*- coding: utf-8 -*-
#
#       parsepbf.py
#
#       Copyright 2011 Chris Hill <osm@raggedred.net>
#
#       This program is free software; you can redistribute it and/or modify
#       it under the terms of the GNU General Public License as published by
#       the Free Software Foundation; either version 3 of the License, or
#       (at your option) any later version.
#
#       This program is distributed in the hope that it will be useful,
#       but WITHOUT ANY WARRANTY; without even the implied warranty of
#       MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#       GNU General Public License for more details.
#
#       You should have received a copy of the GNU General Public License
#       along with this program. If not, see <http://www.gnu.org/licenses/>.

#       Altered heavily in 2018 by Exatel.

from time import time
from . import fileformat_pb2
from . import osmformat_pb2
from struct import unpack
import zlib

from collections import namedtuple

Way = namedtuple('Way', 'way_id, tags, nodes')
Node = namedtuple('Node', 'node_id, lon, lat, tags')
Relation = namedtuple('Relation', 'relation_id, members, tags')

class PBFParser:
    """Manage the process of parsing an osm.pbf file"""

    def __init__(self, filehandle,
                 node_callback=None, way_callback=None, relation_callback=None):
        """PBFParser constuctor"""
        self.fpbf = filehandle
        self.blobhead = fileformat_pb2.BlobHeader()
        self.blob = fileformat_pb2.Blob()
        self.hblock = osmformat_pb2.HeaderBlock()
        self.primblock = osmformat_pb2.PrimitiveBlock()

        self.node_callback = node_callback
        self.way_callback = way_callback
        self.relation_callback = relation_callback

        # Aggregated stats
        self.cnt = {
            'node': 0,
            'way': 0,
            'rel': 0,
            'block': 0,
        }
        # Stats at previous status
        self.last_cnt = self.cnt.copy()
        self.last_status = time()

        self.membertype = {
            0:'node',
            1:'way',
            2:'relation'
        }

    def init(self):
        """Check the file headers"""
        # read the blob header
        if self.read_pbf_blob_header() == False:
            return False

        #read the blob
        if self.read_blob() == False:
            return False

        #check the contents of the first blob are supported
        self.hblock.ParseFromString(self.BlobData)
        for rf in self.hblock.required_features:
            if rf in ("OsmSchema-V0.6","DenseNodes"):
                pass
            else:
                print("not a required feature %s"%(rf))
                return False
        return True

    def parse(self):
        """work through the data extracting OSM objects"""
        if not self.init():
            return False

        while self.read_next_block():
            self.cnt['block'] += 1
            for pg in self.primblock.primitivegroup:
                if self.node_callback is not None and pg.dense.id:
                    self.process_dense(pg.dense)
                if self.node_callback is not None and pg.nodes:
                    self.process_nodes(pg.nodes)
                if self.way_callback is not None and pg.ways:
                    self.process_ways(pg.ways)
                if self.relation_callback is not None and pg.relations:
                    self.process_rels(pg.relations)

            if time() - self.last_status > 5:
                self.show_stat()

        print("Summary:")
        self.show_stat()
        return True

    def show_stat(self):
        "Show statistics"
        took = time() - self.last_status
        per_second = {
            k: (cur - self.last_cnt[k]) / took
            for k, cur in self.cnt.items()
        }
        msg = (
            "nodes/ways/rels/blocks: "
            "{0[node]} / {0[way]} / {0[rel]} / {0[block]}; "
            "per second: {1[node]:.1f} / {1[way]:.1f} / "
            "{1[rel]:.1f} / {1[block]:.1f}"
        )
        msg = msg.format(
            self.cnt, per_second,
        )
        print(msg)

        self.last_cnt = self.cnt.copy()
        self.last_status = time()

    def read_pbf_blob_header(self):
        """Read a blob header, store the data for later"""
        size = self.read_int()
        if size <= 0:
            return False

        if self.blobhead.ParseFromString(self.fpbf.read(size)) == False:
            return False
        return True

    def read_blob(self):
        """Get the blob data, store the data for later"""
        size = self.blobhead.datasize
        if self.blob.ParseFromString(self.fpbf.read(size)) == False:
            return False
        if self.blob.raw_size > 0:
            # uncompress the raw data
            self.BlobData = zlib.decompress(self.blob.zlib_data)
            #print "uncompressed BlobData %s"%(self.BlobData)
        else:
            #the data does not need uncompressing
            self.BlobData = raw
        return True

    def read_next_block(self):
        """read the next block. Block is a header and blob, then extract the block"""
        # read a BlobHeader to get things rolling. It should be 'OSMData'
        if self.read_pbf_blob_header() == False:
            return False

        if self.blobhead.type != "OSMData":
            print("Expected OSMData, found %s"%(self.blobhead.type))
            return False

        # read a Blob to actually get some data
        if self.read_blob() == False:
            return False

        # extract the primative block
        self.primblock.ParseFromString(self.BlobData)
        return True

    def process_dense(self, dense):
        """process a dense node block"""
        NANO = 1000000000
        #DenseNode uses a delta system of encoding os everything needs to start at zero
        lastID = 0
        lastLat = 0
        lastLon = 0
        tagloc = 0
        #cs = 0
        #ts = 0
        #uid = 0
        #user = 0
        gran = float(self.primblock.granularity)
        latoff = float(self.primblock.lat_offset)
        lonoff = float(self.primblock.lon_offset)
        for i in range(len(dense.id)):
            lastID += dense.id[i]
            lastLat += dense.lat[i]
            lastLon += dense.lon[i]
            lat = float(lastLat*gran + latoff) / NANO
            lon = float(lastLon*gran + lonoff) / NANO
            #user += dense.denseinfo.user_sid[i]
            #uid += dense.denseinfo.uid[i]
            #vs = dense.denseinfo.version[i]
            #ts += dense.denseinfo.timestamp[i]
            #cs += dense.denseinfo.changeset[i]
            #suser = self.primblock.stringtable.s[user]
            #tm = ts*self.primblock.date_granularity/1000
            node = Node(node_id=lastID, lon=lon, lat=lat, tags={})
            if tagloc<len(dense.keys_vals):  # don't try to read beyond the end of the list
                while dense.keys_vals[tagloc]!=0:
                    ky = dense.keys_vals[tagloc]
                    vl = dense.keys_vals[tagloc+1]
                    tagloc += 2
                    sky = self.primblock.stringtable.s[ky]
                    svl = self.primblock.stringtable.s[vl]
                    node.tags[sky] = svl
            tagloc += 1

            self.cnt['node'] += 1
            self.node_callback(node)

    def process_nodes(self, nodes):
        NANO = 1000000000
        gran = float(self.primblock.granularity)
        latoff = float(self.primblock.lat_offset)
        lonoff = float(self.primblock.lon_offset)
        for nd in nodes:
            nodeid = nd.id
            lat = float(nd.lat*gran+latoff)/NANO
            lon = float(nd.lon*gran+lonoff)/NANO
            #vs = nd.info.version
            #ts = nd.info.timestamp
            #uid = nd.info.uid
            #user = nd.info.user_sid
            #cs = nd.info.changeset
            #tm = ts*self.primblock.date_granularity/1000
            node = Node(node_id=lastID, lon=lon, lat=lat, tags={})

            for i in range(len(nd.keys)):
                ky = nd.keys[i]
                vl = nd.vals[i]
                sky = self.primblock.stringtable.s[ky]
                svl = self.primblock.stringtable.s[vl]
                node.tags[sky] = svl

            self.cnt['node'] += 1
            self.node_callback(node)

    def process_ways(self, ways):
        """process the ways in a block, extracting id, nds & tags"""
        for wy in ways:
            wayid = wy.id
            #vs = wy.info.version
            #ts = wy.info.timestamp
            #uid = wy.info.uid
            #user = self.primblock.stringtable.s[wy.info.user_sid]
            #cs = wy.info.changeset
            #tm = ts*self.primblock.date_granularity/1000
            way = Way(way_id=wayid, nodes=[], tags={})
            ndid = 0
            for nd in wy.refs:
                ndid += nd
                way.nodes.append(ndid)
            for i in range(len(wy.keys)):
                ky = wy.keys[i]
                vl = wy.vals[i]
                sky = self.primblock.stringtable.s[ky]
                svl = self.primblock.stringtable.s[vl]
                way.tags[sky] = svl

            self.cnt['way'] += 1
            self.way_callback(way)

    def process_rels(self, rels):
        for rl in rels:
            relid = rl.id
            vs = rl.info.version
            #ts = rl.info.timestamp
            #uid = rl.info.uid
            #user = self.primblock.stringtable.s[rl.info.user_sid]
            #cs = rl.info.changeset
            #tm = ts*self.primblock.date_granularity/1000
            rel = Relation(relation_id=relid, members=[], tags={})
            memid = 0
            for i in range(len(rl.memids)):
                role = rl.roles_sid[i]
                memid+=rl.memids[i]
                memtype = self.membertype[rl.types[i]]
                memrole = self.primblock.stringtable.s[role]
                member = {
                    'id': memid,
                    'type': memtype,
                    'role': memrole,
                }
                rel.members.append(member)
            for i in range(len(rl.keys)):
                ky = rl.keys[i]
                vl = rl.vals[i]
                sky = self.primblock.stringtable.s[ky]
                svl = self.primblock.stringtable.s[vl]
                rel.tags[sky] = svl

            self.cnt['rel'] += 1
            self.relation_callback(rel)

    def read_int(self):
        """
        read an integer in network byte order and change to
        machine byte order. Return -1 if eof
        """
        be_int = self.fpbf.read(4)
        if len(be_int) == 0:
            return -1
        else:
            le_int = unpack('!L',be_int)
            return le_int[0]
