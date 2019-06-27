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

import numpy as np
from matplotlib import pyplot as plt
from matplotlib.offsetbox import AnchoredText

import matplotlib
from scipy import stats 
from util import saveFile

matplotlib.rcParams['figure.figsize'] = (18, 12)
matplotlib.rcParams['font.size'] = 12

# supported land cover classes 
landcover_types = { 'forest', 'grassland', 'sugarcane', 'evergreen' }
colors= [ 'red', 'green', 'purple', 'lightblue', 'orange', 'teal', 'coral', 'lightblue', 'lime', 'lavender', 'turquoise', 'darkgreen', 'tan', 'salmon', 'gold' ]

# get a nice title
def getTitle( args ):

    # switch on orbit filter
    subtitle = 'All Scenes' 
    if args.orbit.upper() == 'ASCENDING' or args.orbit.upper() == 'DESCENDING':
        subtitle = '{} Scenes'.format( args.orbit.capitalize() ) 

    # switch on slope
    if len( args.slope ) > 0:
        subtitle += '- {}'.format( args.slope.capitalize() )

    return '{} ARD Interoperability Analysis : GAMMA vs SNAP Error Distribution ({})'.format ( args.database.capitalize(), subtitle )


# convert records into numpy arrays for matplotlib
def getData( records ):

    mean = []; stddev = []

    # parse results in list
    for record in records:

        mean.append( record[ 0 ] )
        stddev.append( record[ 1 ] )

    # convertto numpy arrays
    return np.array( mean ), np.array( stddev )


# execute query
def getRecords( plist ):

    records = None

    # get connection
    conn = psycopg2.connect("dbname='{}' user='sac' host='localhost' password='sac'".format( plist[ 'db' ] ) )
    cur = conn.cursor()

    # determine table of reference
    query = ''; param_list = ()
    if plist[ 'orbit' ] == 'ASCENDING' or plist[ 'orbit' ] == 'DESCENDING':   

        # statistics generated for specific orbit direction
        query = "SELECT %s_error_mean, %s_error_stddev FROM %s.%s_%s_%s WHERE gamma_%s_stddev < %s AND snap_%s_stddev < %s ;" 
        param_list = ( AsIs( plist[ 'pol' ] ), AsIs( plist[ 'pol' ] ), 
                            AsIs( plist[ 'schema' ] ), AsIs( plist[ 'product' ] ), AsIs( plist[ 'landcover' ] ), AsIs( plist[ 'orbit' ] ),
                                AsIs( plist[ 'pol' ] ), AsIs( plist[ 'max_variance' ] ), 
                                    AsIs( plist[ 'pol' ] ), AsIs( plist[ 'max_variance' ] ) )

    else:

        # holistic statistics 
        query = "SELECT %s_error_mean, %s_error_stddev FROM %s.%s_%s WHERE gamma_%s_stddev < %s AND snap_%s_stddev < %s ;" 
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

    parser = argparse.ArgumentParser(description='generate error histograms')

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


# reject outliers
def reject_outliers(data, m=2):
    return data[abs(data - np.mean(data)) < m * np.std(data)]


# parse arguments
args = parseArguments( sys.argv[1:] )
plist = { 'orbit' : args.orbit.upper(), 'product' : args.product, 'db' : args.database, 'max_variance' : 4.0, 'schema' : 'error' }

# get schema names
if len ( args.slope ) > 0:
    plist[ 'schema' ] += '_' + args.slope

# create figure and initialise title
fig = plt.figure()
fig.suptitle( getTitle( args ), fontsize=14)

# define sub-plot structure
rows = len( args.polarization ); cols = len ( args.landcover )
idx = 1

# for each polarization
for pol in args.polarization:

    plist[ 'pol' ] = pol.lower()

    # for each landcover class 
    for ctype in args.landcover:

        if ctype in landcover_types:

            # get records
            plist[ 'landcover' ] = ctype
            records = getRecords( plist )
                       
            # move onto next subplot
            plt.subplot( rows, cols, idx )
            mean, stddev = getData ( records )

            # sort out labelling 
            if idx in [1, cols + 1 ]:
                plt.ylabel( 'frequency' )
                                                                                
            if idx > cols:
                plt.xlabel( r'mean signed deviation $\gamma^0$ [dB]' )

            # plot titles along top
            if idx <= cols:
                plt.title( '{}'.format( ctype.capitalize() ) )

            # draw histogram
            plt.hist( mean, density=True, bins=300, range=(-5, 5), alpha=0.8 )

            # compute and plot normal distribution first
            xt = plt.xticks()[0]  
            xmin, xmax = min(xt), max(xt)  
            lnspc = np.linspace(xmin, xmax, len(mean))

            # get normal distribution stats
            pdf_mean, pdf_sigma = stats.norm.fit( reject_outliers( mean ) )
            pdf_g = stats.norm.pdf(lnspc, pdf_mean, pdf_sigma)
            plt.plot(lnspc, pdf_g, label="Gaussian")

            # create text with pdf stats
            fields = []
            fields.append ( r'Gaussian PDF' )
            fields.append ( r'$\mu$ = {:.2f}'.format( pdf_mean ) )
            fields.append ( r'$\sigma$ = {:.2f}'.format( pdf_sigma ) )
            text = '\n'.join(fields)

            # create and draw text box
            text_box = AnchoredText(text, frameon=True, loc='upper right')
            plt.setp(text_box.patch, facecolor='white')  # , alpha=0.5
            plt.gca().add_artist(text_box)

            # enable grid
            plt.grid()
            idx += 1

# save file
saveFile( args, 'error-distribution' )

# show plot
plt.show()

