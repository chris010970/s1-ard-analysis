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

from util import saveFile

matplotlib.rcParams['figure.figsize'] = (18, 12)
matplotlib.rcParams['font.size'] = 8

# suported landcover classes
landcover_types = { 'forest', 'grassland', 'sugarcane', 'evergreen' }
colors= [ 'red', 'green', 'purple', 'lightblue', 'orange', 'teal', 'coral', 'lightblue', 'lime', 'lavender', 'turquoise', 'darkgreen', 'tan', 'salmon', 'gold' ]

# get title
def getTitle( args ):

    # switch on orbit filter
    subtitle = 'All Scenes' 
    if args.orbit.upper() == 'ASCENDING' or args.orbit.upper() == 'DESCENDING':
        subtitle = '{} Scenes'.format( args.orbit.capitalize() ) 

    # switch on slope
    if len( args.slope ) > 0:
        subtitle += '- {}'.format( args.slope.capitalize() )

    return '{} ARD Interoperability Analysis : GAMMA vs SNAP ({})'.format ( args.database.capitalize(), subtitle )


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
        query = "SELECT gamma_%s_mean, snap_%s_mean FROM %s.%s_%s_%s WHERE gamma_%s_stddev < %s AND snap_%s_stddev < %s ;" 
        param_list = ( AsIs( plist[ 'pol' ] ), AsIs( plist[ 'pol' ] ), 
                            AsIs( plist[ 'schema' ] ), AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ), AsIs( plist[ 'orbit' ] ),
                                AsIs( plist[ 'pol' ] ), AsIs( plist[ 'max_variance' ] ), 
                                    AsIs( plist[ 'pol' ] ), AsIs( plist[ 'max_variance' ] ) )

    else:

        # holistic statistics 
        query = "SELECT gamma_%s_mean, snap_%s_mean FROM %s.%s_%s WHERE gamma_%s_stddev < %s AND snap_%s_stddev < %s ;" 
        param_list = ( AsIs( plist[ 'pol' ] ), AsIs( plist[ 'pol' ] ), 
                            AsIs( plist[ 'schema' ] ), AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ), 
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

    parser.add_argument('-s', '--slope',  
                        help='slope option (flat, steep, none)',
                        default='' )

    return parser.parse_args(args)


# parse arguments
args = parseArguments( sys.argv[1:] )
plist = { 'orbit' : args.orbit.upper(), 'product' : args.product, 'db' : args.database, 'max_variance' : 4.0, 'schema' : 'error' }

# get schema names
if len ( args.slope ) > 0:
    plist[ 'schema' ] += '_' + args.slope

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
            ax = plt.subplot( rows, cols, idx )
            x, y = getData ( records )

            # sort out labelling 
            title = None
            ylab = r'mean {} $\gamma^0$ [dB] (gamma)'.format( pol )                                                                            
            xlab = r'mean {} $\gamma^0$ [dB] (snap)'.format( pol )

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
                            denscol=True, measures=[ 'mean_x', 'mean_y', 'rmse', 'eq' ], regline=True, o2o=True, grid=True)

            ax.legend(loc='upper left')
            idx += 1


# show plot
#plt.tight_layout()

# save file
saveFile( args, 'algorithm' )
#plt.show()

