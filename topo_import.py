#!/usr/bin/env python3

import sys
import os.path

from optparse import OptionParser

from osmpbf import PBFParser

def main():
    # the main part of the program starts here
    # extract the command line options
    parser = OptionParser(usage="%prog PBFfile [options]", version="%prog 1.3")
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose",
                      default=True,
                      help="don't print status messages to stdout")
    (options, args) = parser.parse_args()

    if len(args) != 1 :
        print("You must enter the binary filename (*.pbf)")
        sys.exit(1)

    PBFFile = args[0] # the left over stuff when the options have been extracted

    if  not os.path.exists(PBFFile) :
        print("The binary file %s cannot be found" % (PBFFile))
        sys.exit(1)

    if options.verbose :
        print("Parse a binary OSM file")

    if options.verbose :
        print("Loading the PBF file: %s"%(PBFFile))

    # options sorted out, so now process the file
    # open the file and xml out file if needed
    with open(PBFFile, "rb") as fpbf:
        # create the parser object
        p = PBFParser(fpbf)

        #check the file head
        if not p.init():
            print("Header error")
            sys.exit(1)

        # parse the rest of the file
        print("PARSE")
        p.parse()




if __name__ == '__main__':
    main()
