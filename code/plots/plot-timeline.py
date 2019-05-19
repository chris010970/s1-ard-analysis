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

from matplotlib import pyplot as plt
import matplotlib
import matplotlib.dates as mdates

matplotlib.rcParams['figure.figsize'] = (18, 12)
matplotlib.rcParams['font.size'] = 8

# landcover types
landcover_types = { 'forest', 'grassland', 'sugarcane', 'evergreen' }
colors= [ 'red', 'green', 'purple', 'lightblue', 'orange', 'teal', 'coral', 'lightblue', 'lime', 'lavender', 'turquoise', 'darkgreen', 'tan', 'salmon', 'gold' ]


# get nice title
def getTitle( args ):

    # switch on orbit filter
    title = '{} ARD Interoperability Analysis : Temporal Signatures (All Scenes)'.format ( args.database.capitalize() )
    if args.orbit.upper() == 'ASCENDING' or args.orbit.upper() == 'DESCENDING':
        title = '{} ARD Interoperability Analysis : Temporal Signatures ({} Scenes)'.format( args.database.capitalize(), args.orbit.capitalize() ) 

    return title


# convert db query results to lists
def getData( records ):

    dates = []; y = []; yerr = []

    # for each record block
    for record in records:

        dates.append( time.strftime('%Y-%m-%d', time.localtime ( record[ 0 ] ) )  )

        # parse results in list
        y.append( record[ 1 ] )
        yerr.append( record[ 2 ] )

    x = [dt.datetime.strptime(d,'%Y-%m-%d').date() for d in dates ]

    # convert results in dictionary with numpy arrays
    return np.array(x), np.array(y), np.array(yerr)


# get records from database
def getRecords( plist ):

    # get connection
    conn = psycopg2.connect("dbname='{}' user='sac' host='localhost' password='sac'".format ( plist[ 'db' ] ) )
    cur = conn.cursor()

    # visualise temporal stack statistics
    query = ''; param_list = ()
    if plist[ 'orbit' ] == 'ASCENDING' or plist[ 'orbit' ] == 'DESCENDING':   

        # switch on optional orbit direction filter
        query = "SELECT extract( epoch from fdate ), %s_%s_mean, %s_%s_stddev FROM timeline.%s_%s_%s";
        param_list = ( AsIs( plist[ 'alg' ] ), AsIs( plist[ 'pol' ] ), 
                            AsIs( plist[ 'alg' ] ), AsIs( plist[ 'pol' ] ),  
                                AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ),  AsIs( plist[ 'orbit' ] ) )

    else:

        query = "SELECT extract( epoch from fdate ), %s_%s_mean, %s_%s_stddev FROM timeline.%s_%s";
        param_list = ( AsIs( plist[ 'alg' ] ), AsIs( plist[ 'pol' ] ), 
                            AsIs( plist[ 'alg' ] ), AsIs( plist[ 'pol' ] ),  
                                AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ) )

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

    parser = argparse.ArgumentParser(description='plot timeline')

    # mandatory arguments
    parser.add_argument(    'product',
                            help='product',
                            action="store")

    # optional arguments
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

    parser.add_argument('-p', '--polarization',  
                        nargs='+',                     
                        help='polarization options (forest, grassland)',
                        default=[ 'VV', 'VH' ] )

    parser.add_argument('-g', '--alg',  
                        nargs='+',                     
                        help='algorithm options (gamma, snap)',
                        default=[ 'gamma', 'snap' ] )

    return parser.parse_args(args)


# parse arguments
args = parseArguments( sys.argv[1:] )
plist = { 'orbit' : args.orbit.upper(), 'product' : args.product, 'db' : args.database }

# define plot structure
rows = len( args.alg ); cols = len ( args.landcover )
idx = 1

# create figure with title
fig = plt.figure()
fig.suptitle( getTitle( args ), fontsize=14)

# for each dem type
for alg in args.alg:

    plist[ 'alg' ] = alg

    # for each landcover class 
    for ctype in args.landcover:

        if ctype in landcover_types:

            plist[ 'landcover' ] = ctype

            # move onto next subplot
            plt.subplot( rows, cols, idx )

            # sort out labelling 
            xlab = None; ylab = None; title = None
            if idx == 1:
                ylab = r'mean $\gamma^0$ [dB] (gamma)'
            elif idx == cols + 1:
                ylab = r'mean $\gamma^0$ [dB] (snap)'
                             
            # plot titles along top
            if idx <= cols:
                title = '{}'.format( ctype.capitalize() )

            plt.title(title)
            plt.xlabel(xlab); plt.ylabel(ylab)

            # for each polarization
            for pol in args.polarization:

                plist[ 'pol' ] = pol   
                records = getRecords( plist )

                # plot time signature
                x, y, yerr = getData ( records )
                plt.plot(x, y)

                # alpha fill stddev range
                ymin = y - yerr; ymax = y + yerr
                plt.fill_between(x, ymax, ymin, alpha=0.3 )

            # next subplot
            plt.grid()  
            idx += 1

# show plot
plt.gcf().autofmt_xdate()
plt.show()

