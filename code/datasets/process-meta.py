#!/usr/bin/env python

import os
import sys
import json
from collections import OrderedDict

import psycopg2
from psycopg2.extensions import AsIs

# import shared functions
sys.path.insert(0, '/sac/bin/util')
import fio
import parser

metadata = OrderedDict(  [  ( 'filename', 'VARCHAR(128)' ), \
                            ( 'ingestiondate', 'TIMESTAMP' ), \
                            ( 'beginposition', 'TIMESTAMP'), \
                            ( 'endposition', 'TIMESTAMP' ), \
                            ( 'missiondatatakeid', 'INTEGER' ), \
                            ( 'orbitnumber', 'INTEGER' ), \
                            ( 'lastorbitnumber', 'INTEGER' ), \
                            ( 'relativeorbitnumber', 'INTEGER' ), \
                            ( 'lastrelativeorbitnumber', 'INTEGER' ), \
                            ( 'footprint', 'VARCHAR(512)' ), \
                            ( 'orbitdirection', 'VARCHAR( 32 )' ) ] )


# locate unique key value from meta file
def getInsertRecordSql( record ):

    query = " ( '" +  record[ 0 ] + "', "
    idx = 1

    # for keys in metadata dict
    for key in metadata:

        # add quotes to string values
        if 'INTEGER' in metadata[ key ]:
            query += record[ idx ]
        else:
            query += "'" + record[ idx ] + "'"

        # last item ?
        if idx < len( record ) - 1 :   
            query += ', '

        idx = idx + 1

    query += ' ) '

    return query


# sql to create metadata db table
def getColumnListSql():

    # append column name and type to query string
    query = 'fdate'

    for key in metadata:

        if len( query ) > 0:
            query += ', '

        query += key 

    return query


# sql to create metadata db table
def getCreateTableSql():

    # append column name and type to query string
    query = 'CREATE TABLE meta ( id SERIAL PRIMARY KEY, fdate TIMESTAMP '
    for key in metadata:

        if len( query ) > 0:
            query += ', '

        query += key + ' ' + metadata[ key ]

    query += ' );'
    return query


# create and initialise meta data table
def initialiseTable( records, db ):

    # get connection
    conn = psycopg2.connect("dbname='{}' user='sac' host='localhost' password='sac'".format( db ) )
    cur = conn.cursor()

    try:

        # drop table if exists
        cur.execute( "DROP TABLE IF EXISTS meta;" )
        conn.commit()

        # create table with fields defined above
        cur.execute( getCreateTableSql() )
        conn.commit()

        # construct query to insert metadata records
        query = 'INSERT INTO meta  ( ' + getColumnListSql() + ' ) VALUES '
        for record in records:

            query += getInsertRecordSql ( record ) 

            if record != records[-1] :   
                query += ', '

        # execute insert query
        cur.execute( query )
        conn.commit()

    # handle exception
    except psycopg2.Error as e:
        print ( e.pgerror )    

    # close connection
    # print ( cur.query )
    conn.close()

# locate unique key value from meta file
def getValue( data, key ):

    value = None
    for i in data.keys():

        for j in data[ i ]:
            
            if type ( j ) is dict and 'name' in j and j[ 'name' ] == key:                
                value = j[ 'content' ]

    return value                


# get list of meta files from raw directory
def getRecords( path ):

    # get list of meta files from raw directory
    records = []

    filelist = fio.getFileList( '*.meta', path )
    for pathname in filelist:

        # parse date time from pathname
        dt = parser.getDateTime( pathname )
        if dt is not None:

            with open( pathname ) as json_file:  

                # create record    
                data = json.load(json_file)
                record = [ dt.strftime ( '%Y-%m-%d %H:%M:%S' ) ]

                # copy values into dict
                for key in metadata.keys():
                    record.append ( getValue( data, key ) )

                records.append ( record )

    return records


# entry point
path = '/data/raw/alps/'
db = 'alps'

initialiseTable ( getRecords( path ), db )

