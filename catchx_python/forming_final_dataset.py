# -*- coding: utf-8 -*- 
"""==============================================================================

 Title			:forming_final_dataset.py
 Description		:Creating the final table holding the end result of the pre-processing task
 Author		:LF Velasquez - Earthwatch Europe
 Date			:Nov 07 2018
 Version		:1.0
 Usage			:python forming_final_dataset.py
 Notes			:The values inside the array_Months variable are the name of the tables created
                         as result of year_dataset_weighted_version.py
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

#creating final table
def pgsql(finalTableName,arrayMonths):
    """
        param string finalTableName: The name of the table to be created
        param array arrayMonthse : Array with table names inside pgsql
    """
    conn = psycopg2.connect(host='<host>', database='<db_name>', user='<user>', password='<password>')
    cur = conn.cursor()
    query = ('CREATE TABLE %s AS '
             'SELECT * FROM %s UNION SELECT * FROM %s UNION SELECT * FROM %s '
             'UNION SELECT * FROM %s UNION SELECT * FROM %s UNION SELECT * FROM %s '
             'UNION SELECT * FROM %s UNION SELECT * FROM %s UNION SELECT * FROM %s '
             'UNION SELECT * FROM %s UNION SELECT * FROM %s UNION SELECT * FROM %s '
             'UNION SELECT * FROM %s UNION SELECT * FROM %s UNION SELECT * FROM %s '
             'UNION SELECT * FROM %s UNION SELECT * FROM %s UNION SELECT * FROM %s '
             'UNION SELECT * FROM %s UNION SELECT * FROM %s UNION SELECT * FROM %s '
             'UNION SELECT * FROM %s UNION SELECT * FROM %s UNION SELECT * FROM %s UNION SELECT * FROM %s')%(finalTableName,arrayMonths[0],arrayMonths[1],arrayMonths[2],arrayMonths[3],arrayMonths[4],arrayMonths[5],
                                        arrayMonths[6],arrayMonths[7],arrayMonths[8],arrayMonths[9],arrayMonths[10],arrayMonths[11],
                                        arrayMonths[12],arrayMonths[13],arrayMonths[14],arrayMonths[15],arrayMonths[16],arrayMonths[17],
                                        arrayMonths[18],arrayMonths[19],arrayMonths[20],arrayMonths[21],arrayMonths[22],arrayMonths[23],arrayMonths[24])
    cur.execute(query)
    conn.commit()
    conn.close()


# =============================================================================
# Setting Global Variables & Preparing outputs
# =============================================================================

years = [1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014]
tableName = 'add_name_of_final_table'
yearsName = []

try:
    print ' I have started the process ...'
    startTime = datetime.datetime.now()
    
    # =============================================================================
    # Start of Main Process
    # =============================================================================
    
    for year in years:
        
        yearTableName = 'name_of_table_inside_pgsql' + str(year)
        yearsName.append(yearTableName)
        
    pgsql(tableName,yearsName)
    print '25y Data table created'  
    print '--------------------------'        

except:
    print '--------------------------'  
    print '--------------------------' 
    print '--------------------------'
    print 'wrong, read below'
    raise

print '--------------------------'  
print '--------------------------' 
print '--------------------------'
print 'check pgsql for 25 year table'
print 'Time running the script' + ' ' + str(datetime.datetime.now() - startTime) 
