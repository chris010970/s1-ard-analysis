#!/usr/bin/env python

import os
import sys
import argparse
import shutil

from pyroSAR.auxdata import dem_autoload, dem_create
from pyroSAR.gamma import par2hdr, geocode
from pyroSAR.gamma.api import diff

from osgeo import gdal
from datetime import datetime

# import shared functions
sys.path.insert(0, '/sac/bin/util')
import ps
import fio
import parser
import reproject

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
    parser = argparse.ArgumentParser(description='data-prepare-dem')

    parser.add_argument('dem', action="store")
    parser.add_argument('product', action="store")

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
if os.path.exists( args.dem ):

    # generate scene list from arguments
    scene_list = getSceneList( args )
    if len ( scene_list ) > 0:

        for scene in scene_list:

            print ( 'processing scene: ' + scene )

            # construct paths and logger
            raw_path = os.path.dirname( scene )
            ard_path = raw_path.replace( 'raw', 'ard' ) + "/gamma";

            # check scene content and credentials
            if checkScene( scene ):

                out_path = os.path.join( ard_path, args.product )

                # geocode scene
                geocode(scene=scene,
                    dem=args.dem,
                    tempdir=os.path.join(out_path, 'process'),
                    outdir=out_path,
                    targetres=args.res,
                    scaling='db',
                    func_geoback=1, 
                    # cleanup=False, 
                    export_extra=['inc_geo', 'ls_map_geo'] )

                # reproject imagery to epsg:3460
                image_list = fio.getFileList ( 'S1*.tif', out_path )
                options = reproject.getTransform( image_list[ 0 ], { 't_epsg' : 32760, 'res_x' : 20, 'res_y' : 20  } )

                for img_pathname in image_list:

                    warp_pathname = img_pathname.replace( '.tif', '_warp.tif' )
                    reproject.toEpsg( img_pathname, warp_pathname, options, [ 'TILED=YES', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256' ] )

            else:
                print ( 'scene crosses anti-meridian: ' + scene )

    else:
        print ( 'no scenes found: ' + args.path )

else:
    print ( 'file does not exist: ' + args.dem )

