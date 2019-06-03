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

# landcover classes
landcover_types = { 'forest', 'grassland', 'sugarcane', 'evergreen' }

# execute query
def populateTable( plist ):

    # get connection
    conn = psycopg2.connect("dbname='{}' user='sac' host='localhost' password='sac'".format( plist[ 'db' ] ) )
    cur = conn.cursor()

    # switch on optional orbit direction parameter
    query = ''; param_list = ()
    if plist[ 'orbit' ] == 'ASCENDING' or plist[ 'orbit' ] == 'DESCENDING':   

        # join gamma and snap result tables - compute error statistics - filter on orbit
        query = "CREATE TABLE IF NOT EXISTS %s.%s_%s_%s AS ( " \
                    "WITH pts AS ( " \
                        "SELECT a.fdate, ST_AsText(a.geom) geom, a.vv gamma_vv, a.vh gamma_vh, b.vv snap_vv, b.vh snap_vh, a.vv-b.vv vv_error, a.vh-b.vh vh_error, c.filename, c.orbitdirection, c.relativeorbitnumber FROM %s_gamma.%s_%s a " \
                            "INNER JOIN %s_snap.%s_%s b ON a.fdate = b.fdate AND ST_Equals( a.geom, b.geom ) " \
                                "INNER JOIN meta c ON a.fdate = c.fdate ) " \
                                    "SELECT geom, avg(gamma_vv) gamma_vv_mean, stddev(gamma_vv) gamma_vv_stddev, avg( gamma_vh ) gamma_vh_mean, stddev( gamma_vh) gamma_vh_stddev, avg( snap_vv ) snap_vv_mean, stddev( snap_vv ) snap_vv_stddev, avg( snap_vh ) snap_vh_mean, stddev( snap_vh ) snap_vh_stddev, avg(vv_error) vv_error_mean, stddev( vv_error ) vv_error_stddev, avg( vh_error ) vh_error_mean, stddev( vh_error) vh_error_stddev  " \
                                        "FROM pts WHERE orbitdirection = '%s' GROUP BY geom );"

        param_list = ( AsIs( plist[ 'out_schema' ] ), AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ), AsIs( plist[ 'orbit' ].lower() ),
                            AsIs( plist[ 'in_schema' ] ), AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ), 
                                AsIs( plist[ 'in_schema' ] ), AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ),
                                    AsIs( plist[ 'orbit' ] ) )

    else:

        # join gamma and snap result tables - compute error statistics
        query = "CREATE TABLE IF NOT EXISTS %s.%s_%s AS ( " \
                    "WITH pts AS (  " \
                        "SELECT a.fdate, ST_AsText(a.geom) geom, a.vv gamma_vv, a.vh gamma_vh, b.vv snap_vv, b.vh snap_vh, a.vv-b.vv vv_error, a.vh-b.vh vh_error FROM %s_gamma.%s_%s a " \
                            "INNER JOIN %s_snap.%s_%s b ON a.fdate = b.fdate AND ST_Equals( a.geom, b.geom ) ) " \
                                "SELECT geom, avg(gamma_vv) gamma_vv_mean, stddev(gamma_vv) gamma_vv_stddev, avg( gamma_vh ) gamma_vh_mean, stddev( gamma_vh) gamma_vh_stddev, avg( snap_vv ) snap_vv_mean, stddev( snap_vv ) snap_vv_stddev, avg( snap_vh ) snap_vh_mean, stddev( snap_vh ) snap_vh_stddev, avg(vv_error) vv_error_mean, stddev( vv_error ) vv_error_stddev, avg( vh_error ) vh_error_mean, stddev( vh_error) vh_error_stddev  " \
                                    "FROM pts GROUP BY geom );"

        param_list = ( AsIs( plist[ 'out_schema' ] ), AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ), 
                            AsIs( plist[ 'in_schema' ] ), AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ), 
                                AsIs( plist[ 'in_schema' ] ), AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ) )

    try:

        # create sample schema if not exists
        cur.execute( "CREATE SCHEMA IF NOT EXISTS %s;", ( [ AsIs( plist[ 'out_schema' ] ) ] )  )
        conn.commit()

        # execute query and commit
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

    parser = argparse.ArgumentParser(description='generate error pgsql tables')

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

    parser.add_argument('-s', '--slope',  
                        help='slope option (flat, steep, none)',
                        default='' )

    return parser.parse_args(args)


# parse arguments
args = parseArguments( sys.argv[1:] )
plist = {   'orbit' : args.orbit.upper(), 
            'product' : args.product, 
            'db' : args.database, 
            'in_schema' : 'result' }

# get schema names
if len ( args.slope ) > 0:
    plist[ 'in_schema' ] += '_' + args.slope

plist[ 'out_schema' ] = plist[ 'in_schema' ].replace( 'result', 'error' )

# for each landcover class 
for ctype in args.landcover: 

    if ctype in landcover_types:

        # create and populate pgsql table
        plist[ 'landcover' ] = ctype
        populateTable( plist )

