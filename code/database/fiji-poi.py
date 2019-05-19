#!/usr/bin/env python

import os
import sys
import math
import time

import psycopg2
from psycopg2.extensions import AsIs

import numpy as np

# globals
landcover_types = { 'forest' : [ 1, 15 ], 'grassland' : [ 2 ], 'cultivated' : [ 3, 16, 23, 25, 27 ], 'sugarcane' : [ 6 ], 'coconut' : [ 7, 12 ] }
schema = 'landcover_poi'

# define on class by class basis
max_samples             = 10000
default_sample_rate     = 250000
default_min_area        = 1000000

# translate landcover into sql filter based on numeric identifier
def getTypeFilter( ctype ):

    id_list = landcover_types[ ctype ]
    sub_query = ''

    # construct query with id list
    for land_id in id_list:

        if len ( sub_query ) > 0:
            sub_query += ' OR id = ' + str( land_id )
        else:
            sub_query += ' id = ' + str( land_id )

    return sub_query


# create evergreen table
def getEvergreenPoi():

    # get connection
    conn = psycopg2.connect("dbname='fiji' user='sac' host='localhost' password='sac'")
    cur = conn.cursor()

    # dump forest points coincident with moist rainfall zone (probably evergreen forest)
    query = "CREATE TABLE IF NOT EXISTS %s.evergreen AS " \
                "WITH moist AS ( SELECT geom FROM ancillary.rainfall_zone WHERE DN = 3 )  " \
                    "SELECT area, (ST_Dump(a.geom)).geom FROM landcover_poi.forest a, moist b WHERE ST_Intersects ( a.geom, b.geom ); "

    try:

        # execute query
        cur.execute( query, [ AsIs( schema ) ] ) 
        conn.commit()
        print ( cur.query )

        # execute query        
        cur.execute( "CREATE INDEX ON %s.evergreen USING GIST (geom);", [ AsIs( schema ) ] )
        conn.commit()

    # handle exception
    except psycopg2.Error as e:
        print ( e.pgerror )    

    # close connection
    conn.commit()

    conn.close()
    cur.close()

    return


# construct poi tables
def getLandcoverPoi( ctype ):

    # get connection
    conn = psycopg2.connect("dbname='fiji' user='sac' host='localhost' password='sac'")
    cur = conn.cursor()

    sample_rate = default_sample_rate
    min_area = default_min_area

    # oversample smaller areas
    if ctype == 'sugarcane' or ctype == 'coconut':
        sample_rate = 100
        min_area = 1000

    # generate random points inside landcover polygons 
    query = "CREATE TABLE IF NOT EXISTS %s.%s AS " \
                "WITH polys AS (  " \
                    "SELECT gid, ST_Area(geom) area, CAST( (ST_Area(geom) / %s ) AS INTEGER ) samples, geom FROM ancillary.fiji_32760 WHERE %s )" \
                        "SELECT area, (ST_Dump( ST_GeneratePoints( geom, LEAST( samples, %s ) ) ) ).geom geom FROM polys WHERE area > %s;"

    try:

        # execute query        
        cur.execute( "CREATE SCHEMA IF NOT EXISTS %s;", [ AsIs( schema ) ] ) 
        conn.commit()

        cur.execute( query, ( AsIs( schema ), AsIs( ctype ), sample_rate, AsIs( getTypeFilter( ctype ) ), max_samples, min_area ) ) 
        conn.commit()
        print ( cur.query )

        # execute query        
        cur.execute( "CREATE INDEX ON %s.%s USING GIST (geom);", ( AsIs( schema ), AsIs( ctype ) ) )
        conn.commit()

    # handle exception
    except psycopg2.Error as e:
        print ( e.pgerror )    

    # close connection
    conn.commit()

    conn.close()
    cur.close()

    return


# for each landcover class
for ctype in landcover_types:
    getLandcoverPoi( ctype )

# classify point samples into evergreen / dry tropical
getEvergreenPoi()

