# -*- coding: utf-8 -*- 
"""==============================================================================

 Title			:nc_file_to_pgsql_ensemble.py
 Description		:adding .nc files to pgsql
 Author		:LF Velasquez - Earthwatch Europe
 Date			:Nov 07 2018
 Version		:1.0
 Usage			:python nc_file_to_pgsql_ensemble.py
 Notes			:This script is similar to nc_file_to_pgsql.py in here we deal with
                         null values and data provided in monthly temporal resolution
 python version	:2.7.14

=============================================================================="""
# =============================================================================
# Modules - Libraries
# =============================================================================
import datetime, os, csv
import xarray as xr
import pandas as pd
import geopandas
from sqlalchemy import create_engine
from os import walk

# =============================================================================
# FUNCTIONS
# =============================================================================

# Adding dataframe to pgsql
def pgsql(name,dataframe):
    """
        param string Name: The name of the table to be added to pgsql
        param dataframe dataframe : Pandas dataframe
    """
    engine = create_engine(r'postgresql://user:password@host/db_name')
    c = engine.connect()
    conn = c.connection
    dataframe.to_sql(name,engine)
    conn.close()

try: 
    print ' I have started the process'
    startTime = datetime.datetime.now()

    # =============================================================================
    # Setting Global Variables & Preparing outputs
    # =============================================================================

    #reading all netcdf files in folder
    path = raw_input('Enter path to year files: ')
    print ' ....starting process...'
   
    fileNames = []
    folderNames = []
    fileError = []
    csvfile = path + os.sep + "tempErrorFiles.csv"
##    csvfile = r"D:\LuisData\CatchX\Data\WorkFolder\ScriptErrors" + os.sep + "tempErrorFiles_" + datetime.datetime.now().strftime("%Y%m%d")+ ".csv"
    years = [1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014]

    # =============================================================================
    # Start of Main Process
    # =============================================================================
    
    ## creating list with the folder names
    for (dirpath, dirnames, filenames) in walk(path):
        folderNames.extend(dirnames)
        break

    ## Create list with file names
    for folderName in folderNames:
        newPath = path + os.sep + folderName
        fileNames = []
        for (dirpath, dirnames, filenames) in walk(newPath):
            fileNames.extend(filenames)
            break
        print 'I am doing %s'  %folderName
        print '--------------------------' 
        print '--------------------------'
        for fileName in fileNames:
            try: 
                #open the file and convert to pandas dataframe
                ds = xr.open_dataset(newPath + os.sep + fileName)
                df = ds.to_dataframe()

                df_no_missing = df.dropna() #Remove Nan values

                #create the four column dataframe
                df_no_missing = df_no_missing.reset_index()

                #define data range
                in_range_df = df_no_missing[df_no_missing["time"].isin(pd.date_range("1990-01-01", "2014-12-01"))]

                for year in years:
                    try:                        
                        # Creating the right names for the tables into pgsql
                        tableName = ("ecmwf_runoff" + "_" + fileName.split("_")[2] + "_" + fileName.split("_")[3] + "_" + str(year)).lower()
                        print tableName
                        print '--------------------------'  
                        print '--------------------------' 
                        print '--------------------------'
                        print ' I am doing %s dataframe ' %tableName
                        include = in_range_df[in_range_df['time'].dt.year == year]
                        
                        #change column names to be compatible with pgsql
                        include.columns = ['lat', 'lon', 'date','runoff']
                        print '--------------------------'  
                        print '--------------------------' 
                        print '--------------------------'
                        print ' I have created %s dataframe ' %tableName
                        
                        ## adding dataframe to pgsql
                        print ' I am adding %s dataframe to pgsql ' %tableName
                        pgsql(tableName,include)

                        print '--------------------------'  
                        print '--------------------------' 
                        print '--------------------------'

                    except:
                        fileError.append(fileName)
                        pass
            except:
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
            print 'csv with error files created at %s' %path

except:
    print '--------------------------'  
    print '--------------------------' 
    print '--------------------------'
    print 'wrong, read below'
    raise


print '--------------------------'  
print '--------------------------' 
print '--------------------------'
print 'Time running the script' + ' ' + str(datetime.datetime.now() - startTime) 
