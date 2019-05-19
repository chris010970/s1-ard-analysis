#!/usr/bin/env python

import os
import sys
import fnmatch
import argparse
import shutil

from osgeo import gdal
from xml.dom import minidom
from datetime import datetime

# import shared functions
sys.path.insert(0, '/sac/bin/util')
import ps
import fio
import parser

# get configuration
def updateImages( path_list ):

    # initialise driver and options
    options = [ 'TILED=YES', 'BLOCKXSIZE=256', 'BLOCKYSIZE=256', 'INTERLEAVE=PIXEL' ]
    driver = gdal.GetDriverByName( "GTiff" )

    # open each image
    for obj in path_list:

        # create copy with tiled options
        src_ds = gdal.Open( obj )
        new_ds = driver.CreateCopy( obj + '.tmp', src_ds, options = options )
        
        new_ds.FlushCache()
        new_ds = None

        # overwrite existing file
        shutil.move( obj + '.tmp', obj )

    return

# compile cross-referenced imagery into multiband vrt file 
def compileVrt( scene, product ):

    out_pathname = None

    # generate file list
    filelist = fio.getFileList( '*', scene )
    if len( filelist ) > 0:

        # need to guarantee consistent band ordering 
        sorted_list = []

        # for each band
        bands = product.getElementsByTagName('band')
        for band in bands:

            if ( band.hasAttribute( "filename" ) ):

                for obj in filelist:

                    # add entry to sort list if configuration matches argument
                    if fnmatch.fnmatch ( os.path.basename( obj ), str( band.attributes[ "filename" ].value ) ):

                        sorted_list.append( obj )
                        break;

    # build vrt on validation of successful sort
    if len ( sorted_list ) == len ( bands ):

        out_pathname = scene + '/' + product.attributes[ "name" ].value + '.vrt'
        updateImages( sorted_list )

        vrt = gdal.BuildVRT( out_pathname, sorted_list, options=gdal.BuildVRTOptions(separate=True) )
        vrt = None 

    return out_pathname


# get scene list
def getSceneList( args, product ):

    path_list = []

    # generate file list
    filelist = fio.getPathList( product.attributes[ "path" ].value, args.path )
    if len( filelist ) > 0:

        # parse datetime from pathname
        for obj in filelist:
            dt = parser.getDateTime( obj )

            # get files satisfying constraints
            if dt.timestamp() >= args.start.timestamp() and dt.timestamp() <= args.end.timestamp():
                path_list.append( obj )

    return sorted( path_list )


# get configuration
def getProduct( args ):

    # open configuration file
    xmldoc = minidom.parse( args.cfg )
    product_list = xmldoc.getElementsByTagName('product')

    for obj in product_list:

        # identify match 
        if ( obj.hasAttribute( "name" ) ):
            if args.product in str( obj.attributes[ "name" ].value ):
                product = obj
                break;

    return product


# parse command line arguments
def parseArguments(args=None):

    # valid datetime args
    def valid_date(s):
        try:
            return datetime.strptime(s, "%d/%m/%Y")

        except ValueError:
            msg = "Not a valid date: '{0}'.".format(s)
            raise argparse.ArgumentTypeError(msg)

    # parse configuration
    parser = argparse.ArgumentParser(description='ingest raster datasets into postgis database')

    parser.add_argument('path', action="store")
    parser.add_argument('cfg', action="store")
    parser.add_argument('product', action="store")

    # optional temporal filter
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
if os.path.exists( args.cfg ):

    # retrieve product configuration 
    product = getProduct( args )
    if product is not None:

        # generate scene list from arguments
        scene_list = getSceneList( args, product )
        if len ( scene_list ) > 0:

            # for each scene
            for scene in scene_list:
                print ( 'processing scene: ' + scene )

                # generate multi-band vrt 
                out_pathname = compileVrt( scene, product )
                if out_pathname is not None:

                    # execute ingestion
                    out, err, code = ps.execute( '/sac/bin/DBIngest-Raster.py', [ out_pathname, args.cfg, product.attributes[ "name" ].value ] )
                    code = 0
                    if code != 0:
                        print( str( err ) )
                    else:
                        print( '... ok!' )
            
                else:
                    # missing files
                    print( '... unable to create vrt for scene: ' + scene )

        else:
            # configuration issue
            print( '... no files added to scene list for path: ' + args.path )

    else:
        # unable to find product
        print( '... unable to find product: ' + args.product )

else:
    # configuration issue
    print( '... unable to find configuration file: ' + args.cfg )


