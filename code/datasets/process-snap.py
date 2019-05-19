#!/usr/bin/env python

import os
import sys
import argparse
import shutil

from pyroSAR import snap

from osgeo import gdal
from datetime import datetime

# import shared functions
sys.path.insert(0, '/sac/bin/util')
import ps
import fio
import parser

# validate scene file and coverage
def getSceneList( args ):

    scene_list = []

    # single file argument
    if args.file is not None:
        scene_list.append( args.file )

    else:

        # generate file list
        filelist = fio.getFileList( '*.zip', args.path )
        if len( filelist ) > 0:

            # parse datetime from pathname
            for obj in filelist:
                dt = parser.getDateTime( obj )

                # get files satisfying constraints
                if dt.timestamp() >= args.start.timestamp() and dt.timestamp() <= args.end.timestamp():
                    scene_list.append( obj )

    return sorted( scene_list )


# validate scene file and coverage
def checkScene( pathname ):

    sceneOK = False

    # decompress product file
    path = os.path.dirname( pathname )

    out, err, code = ps.execute( 'unzip', [ "-o", "-d", path, pathname ] )
    if ( code <= 1 ):

        filelist = fio.getFileList( '*.tiff', path )
        if len( filelist ) > 0:

            # open scene and extract gcps
            in_ds = gdal.Open( filelist[ 0 ] )        
            gcps = in_ds.GetGCPs()

            min_x = 180;  max_x = -180;
            for gcp in gcps:
            
                min_x = min ( min_x, gcp.GCPX )
                max_x = max ( max_x, gcp.GCPX )

            # large longitude difference when crossing antimeridian 
            if max_x - min_x < 10:                
                sceneOK = True
            else:
                print ( '... scene crosses antimeridan - skipping: {}'.format ( pathname ) )
            
    # housekeeping of raw zip sub-folder
    zip_path = os.path.splitext( pathname )[ 0 ]
    if ( os.path.exists( zip_path ) ):
        shutil.rmtree( zip_path )

    return sceneOK


# parse command line arguments
def parseArguments(args=None):

    # valid datetime args
    def valid_date(s):
        try:
            return datetime.strptime(s, "%d/%m/%Y")

        except ValueError:
            msg = "Not a valid date: '{0}'.".format(s)
            raise argparse.ArgumentTypeError(msg)


    # parse arguments
    parser = argparse.ArgumentParser(description='data-prepare-snap')

    parser.add_argument('product', action="store")

    parser.add_argument('-d', '--dem',
                        help='dem filename',
                        default=None )

    parser.add_argument('-r', '--res',
                        type=int,
                        help='resolution',
                        default=20 )

    parser.add_argument('-f', '--file',
                        help='scene filename',
                        default=None )

    parser.add_argument('-p', '--path',
                        help='scene root path',
                        default='/data/raw/fiji' )

    parser.add_argument('-s', '--start',
                        help='start date DD/MM/YYYY',
                        default=None,
                        type=valid_date )

    parser.add_argument('-e', '--end',
                        help='end date DD/MM/YYYY',
                        default=None,
                        type=valid_date )

    return parser.parse_args(args)


# parse arguments and check existence of dem file
args = parseArguments( sys.argv[1:] )
if os.path.exists( args.path ):

    # generate scene list from arguments
    scene_list = getSceneList( args )
    if len ( scene_list ) > 0:

        for scene in scene_list:

            print ( 'processing scene: ' + scene )

            # construct paths and logger
            raw_path = os.path.dirname( scene )
            ard_path = raw_path.replace( 'raw', 'ard' ) + "/snap";

            # check scene content and credentials
            if True: # checkScene( scene ):

                out_path = os.path.join( ard_path, args.product )

                snap.geocode(infile=scene,
                             outdir=out_path, 
                             t_srs=32632, tr=args.res,
                             shapefile='/data/S1_ARD/code/aoi/testsite_alps.shp', 
                             cleanup=False,
                             export_extra=['incidenceAngleFromEllipsoid',
                                           'localIncidenceAngle',
                                           'projectedLocalIncidenceAngle', 'DEM' ], groupsize=1 )

            else:
                print ( 'scene crosses anti-meridian: ' + scene )

    else:
        print ( 'no scenes found: ' + args.path )

else:
    print ( 'file does not exist: ' + args.dem )

