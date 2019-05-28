#!/usr/bin/env python

import os
import sys
import pdb
import math
import time
import affine
import graphviz

import pandas as pd
import numpy as np
import datetime as dt

from geomet import wkt
from osgeo import gdal

from matplotlib import pyplot as plt
from matplotlib.offsetbox import AnchoredText
from mpl_toolkits.mplot3d import Axes3D

from skimage.feature.texture import greycomatrix, greycoprops

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report,confusion_matrix, f1_score

from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier

from scipy import stats
from scipy import ndimage

import matplotlib
import matplotlib.dates as mdates

# import shared functions
sys.path.insert(0, '/sac/bin/util')
import fio

# landcover types
landcover = [ 'evergreen', 'grassland' ]

# plot defaults
matplotlib.rcParams['figure.figsize'] = (18, 12)
matplotlib.rcParams['font.size'] = 7


# visualise class-separability using different features
def plotFeatureSeparability( X, y, name_list ):

    # get subplot dimensions
    cols = math.ceil( np.shape( X )[ 1 ] / 2 )
    rows = 2

    # for each feature
    fid = 0; fig = plt.figure()
    for name in name_list:

        ax =plt.subplot(rows, cols, fid+1)

        cid = 0; fmean = []
        for ctype in landcover:

            # generate histogram of feature values
            plt.hist( X[ y == cid, fid ],
                      label=ctype,
                      bins=30,
                      alpha=0.3)

            # compute mean
            fmean.append( np.mean( X[ y == cid, fid ] ) )
            cid += 1

        # create and draw text box
        fields = []
        fields.append ( r'{}'.format( name ) )
        fields.append ( r'{0:.2f}'.format( abs( np.diff( fmean )[ 0 ] ) ) )
        text = '\n'.join(fields)

        text_box = AnchoredText(text, frameon=True, loc='upper right')
        plt.setp(text_box.patch, facecolor='white')  # , alpha=0.5
        plt.gca().add_artist(text_box)

        ax.set_xlim( -5, 5 )
        fid += 1

    # show it
    plt.gca().legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.show()

    return


# get scatter matrix
def getScatterMatrix( feature ):

    # construct scatter matrix
    X = None; y = np.asarray([]); cid = 0
    for ctype in landcover:

        # drops nans and retrieve feature vector list
        feature[ ctype ] = feature[ ctype ].dropna()
        data = feature[ ctype ].iloc[:, 3:].values

        X = data if X is None else np.concatenate( ( X, data ) )

        # create column vector of class id labels
        data = np.ones( np.shape( data )[ 0 ] ) * cid
        y = data if y is None else np.concatenate( ( y, data ) )

        # increment class id
        cid += 1

    # standardise feature matrix
    return StandardScaler().fit_transform( X ), y


# retrieve texture features with descriptive names
def getTextureMetrics( glcm, config ):

    values = []

    # compute glcm texture metrics
    for metric in config[ 'metrics' ]:
        values = np.concatenate( ( values, greycoprops( glcm,  metric ).flatten() ) )

    return values


# compute glcm
def getGLCM( img, config ):

    # scale image to 8bit or less
    img_scale = np.clip ( img, config[ 'min' ], config[ 'max' ] )
    img_scale = ( img_scale - config[ 'min' ] ) / ( config[ 'max' ] - config[ 'min' ] )

    img8 = ( img_scale * ( config[ 'levels' ] - 1 ) ).astype( np.uint8 )

    # return glcm 
    return greycomatrix( img8, config[ 'distance' ], config[ 'angle' ], levels=config[ 'levels' ])


# get sub-image
def getSubImage( pt, dataset, PATCH_SIZE ):

    # get image coordinates
    def getImageCoordinates( pt, rev ):

        pt_json = wkt.loads(pt)

        map_x = pt_json[ 'coordinates' ][ 0 ]
        map_y = pt_json[ 'coordinates' ][ 1 ]

        col, row = rev * ( map_x, map_y )

        return col, row

    # convert sample locations into image coordinates
    col, row = getImageCoordinates( pt, dataset[ 'transform' ][ 'rev' ] )
    img = None

    if col > 0 and row > 0 and col < dataset[ 'band' ].XSize and row < dataset[ 'band' ].YSize:

        x1 = int( max ( col - PATCH_SIZE, 0 ) )
        x2 = int( min ( col + PATCH_SIZE + 1, dataset[ 'band' ].XSize - 1 ) )

        y1 = int( max ( row - PATCH_SIZE, 0 ) )
        y2 = int( min ( row + PATCH_SIZE + 1, dataset[ 'band' ].YSize - 1 ) )

        # get sub-image 
        img = dataset[ 'band' ].ReadAsArray( x1, y1, x2-x1, y2-y1).astype(np.float)

    return img


# get dataset
def getDataset( scene_path, pol ):

    ds = None; transform = {}
    
    # generate file list
    filelist = fio.getFileList( 'S1*_*{}_tnr_bnr_Orb_Cal_ML_TF_TC_dB.tif'.format( pol ), scene_path )
    if len( filelist ) == 1:

        # open raster
        ds = gdal.Open( filelist[ 0 ] )
        if ds is not None:

            transform[ 'fwd' ] = affine.Affine.from_gdal( *ds.GetGeoTransform() )
            transform[ 'rev' ] = ~transform[ 'fwd' ]

            band = ds.GetRasterBand(1);

    return { 'ds' : ds, 'band' : band, 'transform' : transform }


# get feature frame
def getFeatureFrame( sample, scene_path, max_samples ):

    # get training dataset
    dataset = { 'VV' : getDataset( scene_path, 'VV' ), 'VH' : getDataset( scene_path, 'VH' ) }
    feature = sample.copy()

    # for each land cover type
    MAX_VARIANCE = 10.0
    for ctype in landcover:

        # initialise texture feature columns
        feature[ ctype ] = feature[ ctype ].reindex( columns=feature[ ctype ].columns.tolist() + glcm_vv_config[ 'names' ]  )
#        feature[ ctype ] = feature[ ctype ].reindex( columns=feature[ ctype ].columns.tolist() + glcm_vh_config[ 'names' ]  )
        feature[ ctype ] = feature[ ctype ].reindex( columns=feature[ ctype ].columns.tolist() + glcm_cr_config[ 'names' ]  )

        # for each sample point
        row = 0; samples = 0
        for pt in sample[ ctype ][ 'geom' ]:

            # get sub-images
            img_vv = getSubImage( pt, dataset[ 'VV' ], 9 )
            img_vh = getSubImage( pt, dataset[ 'VH' ], 9 )

            # valid sub-image
            if img_vv is not None and img_vh is not None:

                nodata_vv = np.sum( img_vv == dataset[ 'VV' ][ 'band' ].GetNoDataValue() )
                nodata_vh = np.sum( img_vh == dataset[ 'VH' ][ 'band' ].GetNoDataValue() )

                # ignore sub-images with no data
                if nodata_vv == 0 and nodata_vh == 0:

                    # high variance indicating class mixing
                    if np.var( img_vv ) < MAX_VARIANCE or np.var( img_vh ) < MAX_VARIANCE:

                        feature[ ctype ].at[ row, 'vv' ] = np.mean( img_vv )
                        feature[ ctype ].at[ row, 'vh' ] = np.mean( img_vh )

                        feature[ ctype ].at[ row, 'vv_95' ] = np.percentile( img_vv, 95.0 )
                        feature[ ctype ].at[ row, 'vh_95' ] = np.percentile( img_vh, 95.0 )

                        #img_cr = img_vv - img_vh
                        #feature[ ctype ].at[ row, 'cr' ] = np.mean( img_cr )

                        img_rfdi = ( img_vv - img_vh ) / ( img_vv + img_vh )
                        feature[ ctype ].at[ row, 'rfdi' ] = np.mean( img_rfdi )

                        # get sub-image concurrency matrices
                        glcm_vv = getGLCM( img_vv, glcm_vv_config )
 #                       glcm_vh = getGLCM( img_vh, glcm_vh_config ) 
                        glcm_cr = getGLCM( img_rfdi, glcm_cr_config ) 
                            
                        # add texture metrics to frame
                        feature[ ctype ].loc[ row, glcm_vv_config[ 'names' ] ] = getTextureMetrics( glcm_vv, glcm_vv_config )
#                        feature[ ctype ].loc[ row, glcm_vh_config[ 'names' ] ] = getTextureMetrics( glcm_vh, glcm_vh_config )
                        feature[ ctype ].loc[ row, glcm_cr_config[ 'names' ] ] = getTextureMetrics( glcm_cr, glcm_cr_config )

                        # break on max samples
                        samples += 1
                        if samples >= max_samples:
                            break;

            # move onto next row
            row += 1

    return feature, feature[ landcover[ 0 ] ].columns[ 3: ]


# generate texture metric name list
def getNameList( config, pol ):

    names = []
    for metric in config[ 'metrics' ]:
        
        cnt = 1; 
        
        for distance in range ( len( config[ 'distance' ] ) ):
            for angle in range ( len( config[ 'angle' ] ) ):

                names.append( '{}{}_{}'.format( metric, cnt, pol ) )
                cnt += 1;

    return names

# get sample data 
def getSampleFrame( url ):

    sample = {}
    for ctype in landcover:

        obj = '{}/{}.csv'.format( url, ctype )
        sample[ ctype ] = pd.read_csv( obj, names=['geom','annual_vv','annual_vh'], header=None )

    return sample;
    

# get point locations and annual variability
sample = getSampleFrame( 'https://raw.githubusercontent.com/chris010970/s1-ard-analysis/master/results/fiji/csv' )

# glcm configuration parameters
glcm_vv_config = {  'metrics' : [ 'energy' ],
                    #'metrics' : [ 'energy' ],
                    'distance' : [ 1 ], 
                    'angle' : [ 0.0, 3.0 * ( np.pi / 4.0 ) ],
                    'levels' : 32,
                    'min' : -12.0,
                    'max' : -5.0 }

glcm_vh_config = {  'metrics' : [ 'dissimilarity' ],
                    'distance' : [ 1 ], 
                    'angle' : [ 0.0, 3.0 * ( np.pi / 4.0 ) ],
                    'levels' : 64,
                    'min' : -18.0,
                    'max' : -8.0 }

glcm_cr_config = {  'metrics' : [ 'dissimilarity' ],
                    'distance' : [ 1 ], 
                    'angle' : [ 0.0, 3.0 * ( np.pi / 4.0 ) ],
                    'levels' : 32,
                    'min' : -1.0,
                    'max' : 0.0 }

# initialise feature name lists
glcm_vv_config[ 'names' ] = getNameList ( glcm_vv_config, 'vv' )
glcm_vh_config[ 'names' ] = getNameList ( glcm_vh_config, 'vh' )
glcm_cr_config[ 'names' ] = getNameList ( glcm_cr_config, 'cr' )

# get training set
feature, name_list = getFeatureFrame( sample, '/data/ard/fiji/39634/20180515_063955/snap/SRTM-1Sec-HGT-20m/', 1000 )

# get scatter matrix
X, y = getScatterMatrix( feature ) 
plotFeatureSeparability( X, y, name_list )

# create random forest with 1000 decision trees
model = RandomForestClassifier(1000)
model.fit( X, y)

plt_x = np.linspace(0,len(name_list)-1,len(name_list))
print("Features sorted by their score:")
print(sorted(zip(map(lambda x: float("{0:.2f}".format(round(x, 4))), model.feature_importances_), name_list ),
             reverse=True))

plt.figure(figsize=(25,20))

plt.bar(plt_x, model.feature_importances_, width=0.5, color="blue",align='center')
plt.gca().set_xticklabels(plt_x, rotation=60 )
plt.xticks(plt_x, name_list)
plt.ylabel("relative information")
plt.xlabel("features")
plt.show()

# compute svm fit
#model = SVC( kernel='rbf' )
model = MLPClassifier(hidden_layer_sizes=(30,30,30),max_iter=1000)
model.fit( X, y )

y_pred = model.predict(X)

print( confusion_matrix( y, y_pred ) )
print( classification_report( y, y_pred ) )


# generate file list
filelist = fio.getFileList( 'S1*_VV_tnr_bnr_Orb_Cal_ML_TF_TC_dB.tif', '/data/ard/fiji' )
for obj in filelist:

    path = os.path.dirname( obj )

    # get training set
    feature, name_list = getFeatureFrame( sample, path, 1000 )

    # get scatter matrix
    X, y = getScatterMatrix( feature ) 
    #plotFeatureSeparability( X, y, name_list )

    y_pred = model.predict(X)

    print( path )
    print( confusion_matrix( y, y_pred ) )
    print( classification_report( y, y_pred ) )



