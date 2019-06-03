#!/usr/bin/env python

import os
import sys
import math
import time

import numpy as np

import psycopg2
from psycopg2.extensions import AsIs

# globals
schema = 'landcover_poi'

# define on class by class basis
max_samples             = 10000
default_sample_rate     = 5

# create points of interest
def getForestPoi():

    # get connection
    conn = psycopg2.connect("dbname='alps' user='sac' host='localhost' password='sac'")
    cur = conn.cursor()

    # generate random points inside landcover polygons 
    query = "CREATE TABLE IF NOT EXISTS %s.forest AS ( " \
                "WITH polys AS ( SELECT objectid gid, code_18, area_ha, CAST( ( area_ha / %s ) AS INTEGER ) samples, shape FROM ancillary.clc2018_clc2018_v2018_20b " \
                    "WHERE code_18 = '311' OR code_18 = '312' OR code_18 = '313' AND area_ha > 15 ), " \
                        "pts AS ( SELECT gid, code_18, area_ha, (ST_Dump( ST_GeneratePoints( shape, LEAST( samples, %s ) ) ) ).geom geom FROM polys ) " \
                            "SELECT area_ha, geom, ST_NearestValue( rast, 1, geom ) slope FROM ancillary.dem_slope, pts WHERE ST_Intersects( rast, geom ) ); "

    try:

        # execute query        
        cur.execute( "CREATE SCHEMA IF NOT EXISTS %s;", [ AsIs( schema ) ] ) 
        conn.commit()

        # execute query
        cur.execute( query, ( AsIs( schema ), AsIs( default_sample_rate ), AsIs( max_samples )  ) )
        conn.commit()

        # execute query        
        cur.execute( "CREATE INDEX ON %s.forest USING GIST (geom);", [ AsIs( schema ) ] )
        conn.commit()

    # handle exception
    except psycopg2.Error as e:
        print ( e.pgerror )    

    # close connection
    conn.commit()
    print ( cur.query )

    conn.close()
    cur.close()

    return


# classify point samples into evergreen / dry tropical
getForestPoi()

