parsepbf.py
===========

pbf files are binary files that contain OpenStreetMap data. Binary files
have the advantage of being much more compact than the usual XML format 
files even when they have been compressed. Binary files can also be 
faster to read and write. 

OSM .pbf files are based on the Google Protocol buffer formats. More 
information can be found in the OSM Wiki. See 
http:// wiki.openstreetmap.org/wiki/pbf

parsepbf.py is intended as a starting point for anyone who wants to use
.pbf files in Python. The program parses a .pbf file storing the nodes
ways and relations in python lists. Optionally the program can save the 
resulting lists as an XML file.

Installation
============
Copy the files into a folder of your choice. On linux systems you may 
like to set the execute flag to allow the python script to be run 
directly. The files osm.py, fileformat_pb2.py and osmformat_pb2.py are
required to be in the same folder as parsepbf.py or on the PYTHONPATH.

Syntax
======

parsepbf.py [options] pbffilename 

see parsepbf.py -h for more information

feedback
========
please send any feedback to osm at raggededred dot net
