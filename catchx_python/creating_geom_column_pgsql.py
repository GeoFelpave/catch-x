# -*- coding: utf-8 -*- 
"""==============================================================================

 Title			:creating_geom_column_pgsql.py
 Description		:Adding the geom column to the data as a result of nc_file_to_pgsql.py inside the postgreSQL database
 Author		:LF Velasquez - Earthwatch Europe
 Date			:Nov 07 2018
 Version		:1.0
 Usage			:python creating_geom_column_pgsql.py
 Notes			:This script can also be run for the tables created as a result of running nc_file_to_pgsql.py
 python version	:2.7.14

=============================================================================="""

# =============================================================================
# Modules - Libraries
# =============================================================================

import psycopg2
from os import walk
import datetime

# =============================================================================
# Functions
# =============================================================================
def pgsql(table):
    """
        param string table: The name of the table inside pgsql
    """
    conn = psycopg2.connect(host='<host>', database='<db_name>', user='<user>', password='<password>')
    cur = conn.cursor()
    cur.execute('ALTER TABLE %s ADD COLUMN geom_column geometry(Point,4326); UPDATE %s SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326);' % (table,table))
    conn.commit()
    conn.close()

# =============================================================================
# Setting Global Variables
# =============================================================================
months = ['01','02','03','04','05','06','07','08','09','10','11','12']
years = [1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014]
errorName = []

try:
    print ' I have started the process ...'
    startTime = datetime.datetime.now()
    for year in years:
        try:
            for month in months:
                tableName = 'temperature_ei_025_' + str(year) + month
                pgsql(tableName)
                print '--------------------------'  
                print '--------------------------' 
                print '--------------------------'
                print ' I have done %s ' %tableName
        except:
            errorName.append(tableName)
            pass
except:
    print '--------------------------'  
    print '--------------------------' 
    print '--------------------------'
    print 'wrong, read below'
    raise

print '--------------------------'  
print '--------------------------' 
print 'Read errors below'
print errorName # printing table's names when an error was found
print '--------------------------'  
print '--------------------------' 
print '--------------------------'
print 'check pgsql for the geom column'
print 'Time running the script' + ' ' + str(datetime.datetime.now() - startTime) 
