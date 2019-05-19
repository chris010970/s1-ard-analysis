#!/usr/bin/env python

import os
import sys
import argparse

from pyroSAR.auxdata import dem_autoload, dem_create
from pyroSAR.gamma import par2hdr, dem
from pyroSAR.gamma.api import diff

from spatialist import Vector

# parse command line arguments
def parseArguments(args=None):

    parser = argparse.ArgumentParser(description='data-prepare-dem')

    parser.add_argument('aoi', action="store")
    parser.add_argument('path', action="store")

    parser.add_argument('-d', '--dem',
                        help='dem type',
                        default='SRTM 1Sec HGT' )

    parser.add_argument('-s', '--srs',
                        type=int,
                        help='utm spatial reference',
                        default=32760 )

    parser.add_argument('-r', '--res',
                        type=int,
                        help='resolution',
                        default=20 )

    return parser.parse_args(args)


# parse arguments
args = parseArguments( sys.argv[1:] )

# check shape file exists
if ( os.path.exists( args.aoi ) ):

    if not os.path.isdir(args.path):
        os.makedirs(args.path)

    # reproject dem to projection / aoi as scene
    with Vector(args.aoi) as site:
        dem.dem_autocreate(geometry=site, 
                       demType='SRTM 1Sec HGT', 
                       outfile=args.path + '/base-4326', 
                       t_srs=4326,
                       buffer=0.00 )


    sys.exit()





    dem_id = args.dem.replace(' ', '-')
    dem_base = 'dem_snap_{}'.format(dem_id)

    dem_snap = os.path.join( args.path, dem_base + '.tif')
    if not os.path.isfile( dem_snap ):

        # compile vrt of tiles covering aoi
        with Vector(args.aoi) as site:
            vrt = dem_autoload(geometries=[site],
                               demType=args.dem,
                               vrt='/vsimem/{}.vrt'.format(dem_base),
                               buffer=0.1,
                               username='chris.williams@sa.catapult.org.uk', 
                               password='&VxEos1970' )

        # create a DEM GTiff file from the VRT
        dem_create(src=vrt, dst=dem_snap, t_srs=args.srs, tr=(args.res, args.res), geoid_convert=True)
        print ( 'Created SNAP dem file: ' + dem_snap )

    # create gamma dem files
    dem_base = 'dem_gamma_{}'.format( dem_id )
    dem_gamma = os.path.join( args.path, dem_base )

    if not os.path.isfile(dem_gamma):

        parfile = dem_gamma + '.par'
        if not os.path.isfile(parfile):

            # note - hardcoded utm wgs84 with 4326 datum - UTM zone (60), false northing (north = 0, south hemisphere = 1000000 ) 
            inlist = ['UTM', 'WGS84', 1, 60, 10000000, os.path.basename(dem_gamma), '', '', '', '', '',
                        '-{0} {0}'.format(args.res), '']

            #inlist = ['UTM', 'WGS84', 1, 32, 0, os.path.basename(dem_gamma), '', '', '', '', '',
            #      '-{0} {0}'.format(args.res), '']

            # create gamma dem parameter file - pass inlist as stdin 
            diff.create_dem_par(DEM_par=parfile, inlist=inlist )

            # hook up parameter file with geotiff
            diff.dem_import(input_DEM=dem_snap,
                            DEM=dem_gamma,
                            DEM_par=parfile)

            # convert parameter file to envi
            par2hdr(parfile, parfile.replace('.par', '.hdr'))
            print ( 'Created GAMMA dem file: ' + dem_gamma )


