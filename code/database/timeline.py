#!/usr/bin/env python

import os
import sys
import math
import time
import argparse

import psycopg2
import numpy as np
import datetime as dt

from psycopg2.extensions import AsIs

# landcover types
landcover_types = { 'forest', 'grassland', 'sugarcane', 'evergreen' }

# create table utilised for temporal signature plots
def populateTable( plist ):

    # get connection
    conn = psycopg2.connect("dbname='{}' user='sac' host='localhost' password='sac'".format( plist[ 'db' ] ) )
    cur = conn.cursor()

    # compute backscatter statistics mapped to acquisition datetime - filter by orbit direction
    query = ''; param_list = ()
    if plist[ 'orbit' ] == 'ASCENDING' or plist[ 'orbit' ] == 'DESCENDING':   

        query = "CREATE TABLE IF NOT EXISTS timeline.%s_%s_%s AS ( " \
                    "WITH pts AS ( SELECT DATE(a.fdate) fdate, a.vv gamma_vv, a.vh gamma_vh, b.vv snap_vv, b.vh snap_vh, c.orbitdirection FROM result_gamma.%s_%s a " \
                        "INNER JOIN result_snap.%s_%s b ON a.fdate = b.fdate AND ST_Equals( a.geom, b.geom ) " \
                            "INNER JOIN meta c ON a.fdate = c.fdate ) " \
                                "SELECT fdate, AVG(gamma_vv) gamma_vv_mean, STDDEV(gamma_vv) gamma_vv_stddev, AVG(gamma_vh) gamma_vh_mean, STDDEV(gamma_vh) gamma_vh_stddev, " \
                                    "AVG(snap_vv) snap_vv_mean, STDDEV(snap_vv) snap_vv_stddev, AVG(snap_vh) snap_vh_mean, STDDEV(snap_vh) snap_vh_stddev FROM pts " \
                                        "WHERE orbitdirection = '%s' GROUP BY fdate ORDER BY fdate );"

        param_list = ( AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ), AsIs( plist[ 'orbit' ] ),
                            AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ), 
                                AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ), 
                                    AsIs( plist[ 'orbit' ] ) )

    else:

        # compute backscatter statistics mapped to acquisition datetime
        query = "CREATE TABLE IF NOT EXISTS timeline.%s_%s AS ( " \
                    "WITH pts AS ( SELECT DATE(a.fdate) fdate, a.vv gamma_vv, a.vh gamma_vh, b.vv snap_vv, b.vh snap_vh FROM result_gamma.%s_%s a " \
                        "INNER JOIN result_snap.%s_%s b ON a.fdate = b.fdate AND ST_Equals( a.geom, b.geom ) ) " \
                            "SELECT fdate, AVG(gamma_vv) gamma_vv_mean, STDDEV(gamma_vv) gamma_vv_stddev, AVG(gamma_vh) gamma_vh_mean, STDDEV(gamma_vh) gamma_vh_stddev, " \
                                "AVG(snap_vv) snap_vv_mean, STDDEV(snap_vv) snap_vv_stddev, AVG(snap_vh) snap_vh_mean, STDDEV(snap_vh) snap_vh_stddev FROM pts " \
                                    "GROUP BY fdate ORDER BY fdate );"
                    
        param_list = ( AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ), 
                            AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ), 
                                AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ) )

    try:

        # create sample schema if not exists
        cur.execute( "CREATE SCHEMA IF NOT EXISTS timeline;"  )
        conn.commit()

        # execute query
        cur.execute( query, param_list )        
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

    parser = argparse.ArgumentParser(description='process-plots-time')

    # mandatory arguments
    parser.add_argument(    'product',
                            help='product',
                            action="store")

    parser.add_argument('-d', '--database',
                        help='database',
                        default='fiji')

    parser.add_argument('-o', '--orbit',  
                        help='orbit direction (ascending, descending)',
                        default='both' )

    parser.add_argument('-c', '--landcover',  
                        nargs='+',                     
                        help='landcover options (forest, grassland)',
                        default=[ 'forest', 'grassland' ] )

    return parser.parse_args(args)


# parse arguments
args = parseArguments( sys.argv[1:] )
plist = { 'orbit' : args.orbit.upper(), 'product' : args.product, 'db' : args.database }

# for each landcover class 
for ctype in landcover_types:

    if ctype in args.landcover:

        # create and populate pgsql table
        plist[ 'landcover' ] = ctype
        populateTable( plist )

