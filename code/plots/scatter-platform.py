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
from util import saveFile

from matplotlib import pyplot as plt
import matplotlib

matplotlib.rcParams['figure.figsize'] = (18, 12)
matplotlib.rcParams['font.size'] = 14

# landcover types
landcover_types = { 'forest', 'grassland', 'sugarcane', 'evergreen' }
colors= [ 'red', 'green', 'purple', 'lightblue', 'orange', 'teal', 'coral', 'lightblue', 'lime', 'lavender', 'turquoise', 'darkgreen', 'tan', 'salmon', 'gold' ]

# get title
def getTitle( args ):

    # switch on slope
    subtitle = args.landcover.capitalize()

    if len( args.slope ) > 0:
        subtitle += '- {}'.format( args.slope.capitalize() )

    return '{} ARD Interoperability Analysis : Sentinel-1A vs Sentinel-1B ({})'.format ( args.database.capitalize(), subtitle )


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


# get records from database
def getRecords( plist ):

    records = None

    # get connection
    conn = psycopg2.connect("dbname='{}' user='sac' host='localhost' password='sac'".format( plist[ 'db' ] ) )
    cur = conn.cursor()

    # get error statistics filtered by orbit direction
    query = "WITH pts AS( SELECT a.%s_%s_mean mean_a, a.%s_%s_stddev stddev_a, b.%s_%s_mean mean_d, b.%s_%s_stddev stddev_d FROM error_s1a.%s_%s a " \
                "INNER JOIN error_s1b.%s_%s b ON a.geom = b.geom ) SELECT mean_a, mean_d FROM pts WHERE stddev_a < %s AND stddev_d < %s;"

    param_list = ( AsIs( plist[ 'alg' ] ), AsIs( plist[ 'pol' ] ), AsIs( plist[ 'alg' ] ), AsIs( plist[ 'pol' ] ), 
                        AsIs( plist[ 'alg' ] ), AsIs( plist[ 'pol' ] ), AsIs( plist[ 'alg' ] ), AsIs( plist[ 'pol' ] ), 
                            AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ), 
                                AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ),
                                    AsIs( plist[ 'max_variance' ] ), AsIs( plist[ 'max_variance' ] ) )

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

    parser = argparse.ArgumentParser(description='scatter plot comparison between orbits')

    # mandatory arguments
    parser.add_argument(    'product',
                            help='product',
                            action="store")

    parser.add_argument('-d', '--database',
                        help='database',
                        default='fiji')

    parser.add_argument('-c', '--landcover',  
                        help='landcover options (forest)',
                        default='forest' )

    parser.add_argument('-p', '--polarization',  
                        nargs='+',                     
                        help='polarization options (VV, VH)',
                        default=[ 'VV', 'VH' ] )

    parser.add_argument('-g', '--alg',  
                        nargs='+',                     
                        help='algorithm options',
                        default=[ 'gamma', 'snap' ] )

    parser.add_argument('-s', '--slope',  
                        help='slope option (flat, steep, none)',
                        default='' )

    return parser.parse_args(args)


# parse arguments
args = parseArguments( sys.argv[1:] )
plist = { 'product' : args.product, 'landcover' : args.landcover, 'db' : args.database, 'max_variance' : 2.0 }

# define plot structure
rows = len( args.alg ); cols = len ( args.polarization )
idx = 1

# create figure with title
fig = plt.figure()
fig.suptitle( getTitle( args ), fontsize=14)

# for each polarization
for pol in args.polarization:

    plist[ 'pol' ] = pol.lower()
    
    # for each polarization
    for alg in args.alg:

        plist[ 'alg' ] = alg.lower()
        records = getRecords( plist )
                   
        # move onto next subplot
        ax = plt.subplot( rows, cols, idx )
        x, y = getData ( records )

        # sort out labelling 
        title = None
        ylab = r'mean {} $\gamma^0$ [dB] (s1a)'.format( pol )                                                                            
        xlab = r'mean {} $\gamma^0$ [dB] (s1b)'.format ( pol )

        # plot titles along top
        if idx <= cols:
            title = '{}'.format( alg.upper() )

        # adjust limits according to polarization
        lim = ( -15, 0 )
        if pol == 'VH':
            lim = ( -20, -5 )

        # complete scatter subplot
        scatter( x, y, title=title,
                        xlab=xlab, ylab=ylab,
                        xlim=lim, ylim=lim,
                        denscol=True, measures=['mean_x', 'mean_y', 'rmse', 'eq' ], regline=True, o2o=True, grid=True)

        ax.legend(loc='upper left')
        idx += 1

# save file
saveFile( args, 'platform-' + args.landcover + '.png' )

# show plot
#plt.show()

