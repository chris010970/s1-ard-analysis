#!/usr/bin/env python

import os
import sys
import math
import time
import argparse
import psycopg2

import numpy as np
import datetime as dt

from threading import Thread
from psycopg2.extensions import AsIs

# landcover types
landcover_types = { 'forest', 'grassland', 'sugarcane', 'evergreen' }

# slope thresholds
slope= { 'flat' : { 'min' : 0.0, 'max' : 15.0 }, 'steep' : { 'min' : 20.0, 'max' : 1000.0 } }

# generate random selection of points coincident with landcover class and aoi
def populateTable( args, ctype ):

    # get connection
    conn = psycopg2.connect("dbname='{}' user='sac' host='localhost' password='sac'".format( args.database ) )
    cur = conn.cursor()

    # construct bbox filter if aoi defined
    sub_filter = ''
    if args.aoi is not None:
        sub_filter = "ST_Intersects ( geom, ST_Transform( ST_MakeEnvelope( {}, {}, {}, {}, 4326 ), {} ) )".format( args.aoi[ 0 ], args.aoi[ 1 ], args.aoi[ 2 ], args.aoi[ 3 ], args.aoi[ 4 ] )

    # add slope thresholds if configured
    if len( args.slope ) > 0:

        if len( sub_filter ) > 0:
            sub_filter += " AND "

        sub_filter += "slope >= {} AND slope <= {}".format( slope[ args.slope ][ 'min' ], slope[ args.slope ][ 'max' ] )

    # get schema name   
    schema = 'sample'
    if len ( args.slope ) > 0:
        schema += '_' + args.slope

    # construct query
    query = "CREATE TABLE %s.%s AS WITH pts AS ( SELECT geom, slope FROM landcover_poi.%s ORDER BY RANDOM() ) " \
                " SELECT geom, slope FROM pts WHERE " \
                    + sub_filter + " ORDER BY RANDOM() LIMIT %s;"

    try:

        # create sample schema if not exists
        cur.execute( "CREATE SCHEMA IF NOT EXISTS %s;", ( [ AsIs( schema ) ] ) )
        conn.commit()

        # delete sample table if not exists
        cur.execute( "DROP TABLE IF EXISTS %s.%s;" , ( AsIs( schema ), AsIs( ctype ) ) )
        conn.commit()

        # execute query
        cur.execute( query, ( AsIs( schema ), AsIs( ctype ), AsIs( ctype ), AsIs( args.samples ) ) )
        conn.commit()
        
    # handle exception
    except psycopg2.Error as e:
        print ( e.pgerror )    

    # close connection
    print ( cur.query )
    conn.close()

    return


# parse command line arguments
def parseArguments(args=None):

    parser = argparse.ArgumentParser(description='sample')

    # optional arguments
    parser.add_argument('-d', '--database',
                        help='database',
                        default='fiji')

    parser.add_argument('-n', '--samples',
                        type=int,
                        help='number of point samples to compute statistics per scene',
                        default='10000')

    parser.add_argument('-a', '--aoi',  
                        nargs=5,                     
                        help='latitude / longitude bbox to constrain statistical analysis (xmin ymin xmax ymax target-epsg)' )

    parser.add_argument('-c', '--landcover',  
                        nargs='+',                     
                        help='landcover options (forest, grassland)',
                        default=[ 'forest', 'grassland' ] )

    parser.add_argument('-s', '--slope',  
                        help='slope option (flat, steep, none)',
                        default='' )

    return parser.parse_args(args)


# parse arguments
args = parseArguments( sys.argv[1:] )

# generate sample points for landcover classes
for ctype in landcover_types:
    if ctype in args.landcover:
        populateTable( args, ctype )


