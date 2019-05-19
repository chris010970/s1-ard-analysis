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

# insert values into previously created table - use multiple threads configured with different start / end dates
def populateTable( plist, task ):

    # get connection
    conn = psycopg2.connect("dbname='{}' user='sac' host='localhost' password='sac'".format( plist[ 'db' ] ) )
    cur = conn.cursor()

    # retrieve pixel values nearest to poi for scenes in temporal range
    query = "INSERT INTO %s.%s ( " \
                "WITH p AS (SELECT id FROM scene_%s.product WHERE name = '%s'), " \
                    "b1 AS ( SELECT idx FROM scene_%s.band b, p WHERE b.pid = p.id AND name = 'vv' ), " \
                        "b2 AS ( SELECT idx FROM scene_%s.band b, p WHERE b.pid = p.id AND name = 'vh' ), " \
                            "cat AS ( SELECT fid, fdate FROM scene_%s.cat, p WHERE fdate >= '%s' AND fdate <= '%s' AND pid = p.id ), " \
                                "lc AS ( SELECT geom FROM sample.%s ), " \
                                    "tile AS ( SELECT cat.fdate, geom, ST_NearestValue( rast, b1.idx, geom ) vv, ST_NearestValue( rast, b2.idx, geom ) vh FROM scene_%s.%s s, lc, cat, b1, b2 WHERE ST_Intersects( rast, geom ) AND s.fid = cat.fid ) " \
                                        "SELECT fdate, geom, vv, vh FROM tile " \
                                            "WHERE vv IS NOT NULL AND vh IS NOT NULL ORDER BY fdate )"

    param_list = ( AsIs( plist[ 'schema' ] ), AsIs( plist[ 'table' ] ),
                        AsIs( plist[ 'alg' ] ), AsIs( plist[ 'product' ] ), 
                            AsIs( plist[ 'alg' ] ), 
                                AsIs( plist[ 'alg' ] ), 
                                    AsIs( plist[ 'alg' ] ), AsIs( task[ 'start' ].strftime('%Y-%m-%d %H:%M:%S') ), AsIs( task[ 'end' ].strftime('%Y-%m-%d %H:%M:%S') ), 
                                        AsIs( plist[ 'landcover' ] ), 
                                            AsIs( plist[ 'alg' ] ), AsIs( plist[ 'product' ] ) )

    try:

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


# create table ready to populate
def createTable( plist ):

    # get connection
    conn = psycopg2.connect("dbname='{}' user='sac' host='localhost' password='sac'".format( plist[ 'db' ] ) )
    cur = conn.cursor()

    try:

        # create sample schema if not exists
        cur.execute( "CREATE SCHEMA IF NOT EXISTS %s;", ( [ AsIs( plist[ 'schema' ] ) ] )  )
        conn.commit()

        # delete sample table if not exists
        cur.execute( "DROP TABLE IF EXISTS %s.%s;" , ( AsIs( plist[ 'schema' ] ), AsIs( plist[ 'table' ] ) ) )
        conn.commit()

        # delete sample table if not exists
        cur.execute( "CREATE TABLE IF NOT EXISTS %s.%s ( fdate TIMESTAMP, geom GEOMETRY, vv double precision, vh double precision );" , 
                ( AsIs( plist[ 'schema' ] ), AsIs( plist[ 'table' ] ) ) )
        conn.commit()

    # handle exception
    except psycopg2.Error as e:
        print ( e.pgerror )    

    # close connection
    print ( cur.query )
    conn.close()

    return 


# get date time object
def getDateTime( arg ):

    obj = None
    if arg is not None: 
        obj = dt.datetime.strptime( arg, '%d/%m/%Y %H:%M:%S')

    return obj


# get scene temporal range
def getSceneRange( plist ):

    rows = None

    # get connection
    conn = psycopg2.connect("dbname='{}' user='sac' host='localhost' password='sac'".format( plist[ 'db' ] ) )
    cur = conn.cursor()

    # construct query
    query = "SELECT extract( epoch from MIN(fdate) ), extract( epoch from MAX(fdate) ) FROM scene_{}.cat".format( plist[ 'alg' ] ) 
    try:

        # execute query
        cur.execute( query )
        rows = cur.fetchall()
        
    # handle exception
    except psycopg2.Error as e:
        print ( e.pgerror )    

    # close connection
    print ( cur.query )
    conn.close()

    return rows[ 0 ]


# get scene temporal range
def getDateTimeRange( plist, args ):

    # get datetime arguments 
    start_dt = getDateTime( args.start )
    end_dt = getDateTime( args.end )

    # retrieve datetime arguments from cat table
    if start_dt is None or end_dt is None:

        record = getSceneRange( plist )
        if start_dt is None:
            start_dt = dt.datetime.fromtimestamp( record[ 0 ] )

        if end_dt is None:
            end_dt = dt.datetime.fromtimestamp( record[ 1 ] )

    return start_dt, end_dt


# get scene temporal range
def getTaskList( plist, args ):

    # get scene temporal range
    start_dt, end_dt = getDateTimeRange( plist, args )

    interval = ( end_dt.timestamp() - start_dt.timestamp() ) / args.threads
    next = start_dt.timestamp()

    tasklist = []; index = 0
    while next < end_dt.timestamp():

        tasklist.append ( { 'index' : index, 'start' : dt.datetime.fromtimestamp( next ), 'end' : dt.datetime.fromtimestamp( next + interval ) } )

        next = next + interval + 1
        index = index + 1

    return tasklist


# generate random selection of points coincident with landcover class and aoi
def getPoiSampleList( args, ctype ):

    # function return vars
    x = []; y = []

    # get connection
    conn = psycopg2.connect("dbname='{}' user='sac' host='localhost' password='sac'".format( plist[ 'db' ] ) )
    cur = conn.cursor()

    # construct bbox filter if aoi defined
    sub_filter = ''
    if args.aoi is not None:
        sub_filter = "WHERE ST_Intersects ( geom, ST_Transform( ST_MakeEnvelope( {}, {}, {}, {}, 4326 ), {} ) )".format( args.aoi[ 0 ], args.aoi[ 1 ], args.aoi[ 2 ], args.aoi[ 3 ], args.aoi[ 4 ] )

    # construct query
    query = "CREATE TABLE sample.%s AS WITH pts AS ( SELECT geom FROM landcover_poi.%s ORDER BY RANDOM() ) " \
                " SELECT geom FROM pts " \
                    + sub_filter + " ORDER BY RANDOM() LIMIT %s;"

    try:

        # create sample schema if not exists
        cur.execute( "CREATE SCHEMA IF NOT EXISTS sample;" )
        conn.commit()

        # delete sample table if not exists
        cur.execute( "DROP TABLE IF EXISTS sample.%s;" , ( [ AsIs( ctype ) ] ) )
        conn.commit()

        # execute query
        cur.execute( query, ( AsIs( ctype ), AsIs( ctype ), AsIs( args.samples ) ) )
        conn.commit()
        
    # handle exception
    except psycopg2.Error as e:
        print ( e.pgerror )    

    # close connection
    # print ( cur.query )
    conn.close()

    return


# parse command line arguments
def parseArguments(args=None):

    parser = argparse.ArgumentParser(description='generate-result-tables')

    # mandatory arguments
    parser.add_argument(    'product',
                            help='product',
                            action="store")

    # optional arguments
    parser.add_argument('-s', '--start',
                        help='start date filter (DD/MM/YYYY HH:MM:SS)')

    parser.add_argument('-e', '--end',
                        help='end date filter (DD/MM/YYYY HH:MM:SS)')

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

    parser.add_argument('-o', '--orbit',  
                        help='orbit direction (ascending, descending)',
                        default='both' )

    parser.add_argument('-c', '--landcover',  
                        nargs='+',                     
                        help='landcover options (forest, grassland)',
                        default=[ 'forest', 'grassland' ] )

    parser.add_argument('-t', '--threads',  
                        type=int,
                        help='number of threads',
                        default=1 )

    parser.add_argument('-g', '--alg',  
                        nargs='+',                     
                        help='algorithm options',
                        default=[ 'gamma', 'snap' ] )


    return parser.parse_args(args)


# parse arguments
args = parseArguments( sys.argv[1:] )
plist = { 'orbit' : args.orbit.upper(), 'product' : args.product, 'db' : args.database }

# generate sample points for landcover classes
for ctype in landcover_types:
    if ctype in args.landcover:
        getPoiSampleList( args, ctype )

# for each algorithm
for alg in args.alg:

    # initialise schema
    plist[ 'alg' ] = alg
    plist[ 'schema' ] = 'result_' + alg

    # for each landcover class 
    for ctype in landcover_types:

        if ctype in args.landcover:

            plist[ 'landcover' ] = ctype
            plist[ 'table' ] = plist[ 'product' ] + '_' + ctype

            # construct tasklist and result array
            createTable( plist )
            tasklist = getTaskList( plist, args )

            # create thread per task (sub-query)
            threads = []
            for task in tasklist:

                # We start one thread per url present.
                process = Thread(target=populateTable, args=[ plist, task ] )
                process.start()
                threads.append(process)

            # pause main thread until all child threads complete
            for process in threads:
                process.join()

