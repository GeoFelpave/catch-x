# -*- coding: utf-8 -*- 
"""==============================================================================

 Title			:land_cover_csv_pgsql.py
 Description		:Adding the land cover csv files to the pgsql database
 Author		:LF Velasquez - Earthwatch Europe
 Date			:Nov 07 2018
 Version		:1.0
 Usage			:python land_cover_csv_pgsql.py
 Notes			:The csv files are the result of running land_cover_to_csv.py
 python version	:2.7.14

=============================================================================="""
# =============================================================================
# Modules - Libraries
# =============================================================================
import os, datetime, psycopg2, shutil, sys, csv
from os import walk

# =============================================================================
# Functions
# =============================================================================

# Adding csv to table in pgsql
def process_csv(conn, table_name, file_object):
    """
        param string conn: database connection
        param string table_name : target table in pgsql
        param string file_object : csv file name
    """
    cursor = conn.cursor()
    cursor.copy_expert(sql=SQL_STATEMENT % table_name, file=file_object)
    conn.commit()
    cursor.close()
# =============================================================================
# Start of Main Process
# =============================================================================
try: 
    print ' I have started the process'
    startTime = datetime.datetime.now()

    #reading files in folder
    path = raw_input('Enter path to land cover csv files: ')
    pgsqlTable = "target_table_in_pgsql"
    fileNames = []
    fileError = []
    csvfile = r"D:\LuisData\CatchX\Data\WorkFolder\ScriptErrors" + os.sep + "tempErrorFiles_LandCoverToPGSQL.csv"
    print ' ....starting process...'

    # PGSQL connection and query
    db_conn = "host='<host>' dbname='<db_name>' user='<user>' password='<password>'"
    connection = psycopg2.connect(db_conn)
     
    SQL_STATEMENT = """
        COPY %s FROM STDIN WITH
            CSV
            HEADER
            DELIMITER AS ','
    """

    # Create list of fileNames
    for (dirpath, dirnames, filenames) in walk(path):
        for filename in filenames:
            if filename.endswith('csv'):
                fileNames.append(filename)
            

    # Start process for each file
    for fileName in fileNames:
        try:
            
            myFile = path + os.sep + fileName
            csvFile = open(myFile)
            print myFile
            print '--------------------------'  
            print '--------------------------' 
            print '--------------------------'
            print 'Moving %s file to pgsql table' %('Land Cover_' + fileName.split("_")[3])
            process_csv(connection, pgsqlTable, csvFile)
            

        except:
##            print ( ' Error found in file %s ' %fileName
            fileError.append(fileName)
            pass

        
    print '--------------------------'  
    print '--------------------------' 
    print '--------------------------'
    print 'creating csv with error files'
    #Assuming res is a flat list
    with open(csvfile, "w") as output:
        writer = csv.writer(output, lineterminator='\n')
        for fileName in fileError:
            writer.writerow([fileName])
    print 'check for error in the csv with error files created as: %s' %csvfile
    connection.close()
except:
    print '--------------------------'  
    print '--------------------------' 
    print '--------------------------'
    print 'wrong, read below'
    raise

print '--------------------------'  
print '--------------------------' 
print '--------------------------'
print 'Check PGSQL'
print 'Time running the script' + ' ' + str(datetime.datetime.now() - startTime) 
