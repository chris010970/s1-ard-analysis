#!/usr/bin/env python

import os
import pdb

from matplotlib import pyplot as plt

root = '/data/S1_ARD/results'

# save plot to file
def saveFile( args, img ):
    
    # get path
    path = root + '/' + args.database + '/all'
    if 'slope' in args and len( args.slope ) > 0:
        path = path.replace( 'all', args.slope )

    # get filename
    filename = img

    if '.png' not in filename:
        filename = img + '-all.png'
        if args.orbit.upper() == 'ASCENDING' or args.orbit.upper() == 'DESCENDING':
            filename = filename.replace( 'all', args.orbit.lower() )
    
    # create path
    if not os.path.exists( path ):
        os.makedirs( path, 0o755 );

    plt.savefig( path + '/' + filename ) #, transparent=True )
    return

