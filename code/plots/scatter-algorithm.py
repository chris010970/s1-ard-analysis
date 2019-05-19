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

sys.path.insert(0, '../.')
from S1_ARD import scatter

from matplotlib import pyplot as plt
import matplotlib

matplotlib.rcParams['figure.figsize'] = (18, 12)
matplotlib.rcParams['font.size'] = 8

# suported landcover classes
landcover_types = { 'forest', 'grassland', 'sugarcane', 'evergreen' }
colors= [ 'red', 'green', 'purple', 'lightblue', 'orange', 'teal', 'coral', 'lightblue', 'lime', 'lavender', 'turquoise', 'darkgreen', 'tan', 'salmon', 'gold' ]

# get title
def getTitle( args ):

    # switch on orbit argument
    title = '{} ARD Interoperability Analysis : GAMMA vs SNAP (All Scenes)'.format ( args.database.capitalize() ) 
    if args.orbit.upper() == 'ASCENDING' or args.orbit.upper() == 'DESCENDING':
        title = '{} ARD Interoperability Analysis : GAMMA vs SNAP ({} Scenes)'.format( args.database.capitalize(), args.orbit.capitalize() ) 

    return title

# convert records into numpy arrays for matplotlib
def getData( records ):

    x = []; y = []

    # for each record block
    for record in records:

        # parse results in list
        x.append( record[ 0 ] )
        y.append( record[ 1 ] )

    # convert results in dictionary with numpy arrays
    return np.array( x ), np.array( y )


# get records from data tables
def getRecords( plist ):

    records = None

    # get connection
    conn = psycopg2.connect("dbname='{}' user='sac' host='localhost' password='sac'".format( plist[ 'db' ] ) )
    cur = conn.cursor()

    # determine table of reference
    query = ''; param_list = ()
    if plist[ 'orbit' ] == 'ASCENDING' or plist[ 'orbit' ] == 'DESCENDING':   

        # statistics generated for specific orbit direction
        query = "SELECT gamma_%s_mean, snap_%s_mean FROM error.%s_%s_%s WHERE gamma_%s_stddev < %s AND snap_%s_stddev < %s ;" 
        param_list = ( AsIs( plist[ 'pol' ] ), AsIs( plist[ 'pol' ] ), 
                            AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ), AsIs( plist[ 'orbit' ] ),
                                AsIs( plist[ 'pol' ] ), AsIs( plist[ 'max_variance' ] ), 
                                    AsIs( plist[ 'pol' ] ), AsIs( plist[ 'max_variance' ] ) )

    else:

        # holistic statistics 
        query = "SELECT gamma_%s_mean, snap_%s_mean FROM error.%s_%s WHERE gamma_%s_stddev < %s AND snap_%s_stddev < %s ;" 
        param_list = ( AsIs( plist[ 'pol' ] ), AsIs( plist[ 'pol' ] ), 
                            AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ), 
                                AsIs( plist[ 'pol' ] ), AsIs( plist[ 'max_variance' ] ), 
                                    AsIs( plist[ 'pol' ] ), AsIs( plist[ 'max_variance' ] ) )

    try:

        # execute query
        cur.execute( query, param_list )
        records = cur.fetchall()

    # handle exception
    except psycopg2.Error as e:
        print ( e.pgerror )    

    # close connection
    print ( cur.query )
    conn.close()

    return records


# parse command line arguments
def parseArguments(args=None):

    parser = argparse.ArgumentParser(description='process-plots-error')

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
                        help='landcover options (evergreen, grassland)',
                        default=[ 'evergreen', 'grassland' ] )

    parser.add_argument('-p', '--polarization',  
                        nargs='+',                     
                        help='polarization options (VV, VH)',
                        default=[ 'VV', 'VH' ] )

    return parser.parse_args(args)


# parse arguments
args = parseArguments( sys.argv[1:] )
plist = { 'orbit' : args.orbit.upper(), 'product' : args.product, 'db' : args.database, 'max_variance' : 4.0 }

# define plot structure
rows = len( args.polarization ); cols = len ( args.landcover )
idx = 1

fig = plt.figure()
fig.suptitle( getTitle( args ), fontsize=14)

# for each polarization
for pol in args.polarization:

    plist[ 'pol' ] = pol.lower()

    # for each landcover class 
    for ctype in args.landcover:

        if ctype in landcover_types:

            plist[ 'landcover' ] = ctype
            records = getRecords( plist )
                       
            # move onto next subplot
            plt.subplot( rows, cols, idx )
            x, y = getData ( records )

            # sort out labelling 
            xlab = None; ylab = None; title = None
            if idx in [1, cols + 1 ]:
                ylab = r'mean $\gamma^0$ [dB] (gamma)'
                                                                                
            if idx > cols:
                xlab = r'mean $\gamma^0$ [dB] (snap)'

            # plot titles along top
            if idx <= cols:
                title = '{}'.format( ctype.capitalize() )

            # adjust limits according to polarization
            lim = ( -15, 0 )
            if pol == 'VH':
                lim = ( -20, -5 )

            # complete scatter subplot
            scatter( x, y, title=title,
                            xlab=xlab, ylab=ylab,
                            xlim=lim, ylim=lim,
                            denscol=True, measures=['cv_y', 'eq'], regline=True, grid=True)
            idx += 1

# show plot
plt.show()

