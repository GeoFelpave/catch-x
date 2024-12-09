# -*- coding: utf-8 -*- 
"""==============================================================================

 Title			:nc_file_to_pgsql.py
 Description		:adding .nc files to pgsql
 Author	        :LF Velasquez - Earthwatch Europe
 Date			:Nov 07 2018
 Version		:1.0
 Usage			:python nc_file_to_pgsql.py
 Notes			:This script creates monthly tables for each dataset used. In order
                         to create the right table the user must know the variables within
                         the nc file. This can be found running the following in python:
                         
                                         import xarray as xr
                                         ds = xr.open.dataset(<filepath + filename>)
                                         print ds
 python version	:2.7.14

=============================================================================="""
# =============================================================================
# Modules - Libraries
# =============================================================================
import datetime, os, csv
import xarray as xr
import pandas as pd
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
            print fileName
            try:
                #Creating the right names for the tables into pgsql
                tableName = ("temperature" + "_" + fileName.split("_")[2] + "_" + fileName.split("_")[3] + "_" + fileName.split("_")[4].split(".")[0]).lower()
                
                #Open the file and converted to pandas dataframe
                ds = xr.open_dataset(newPath + os.sep + fileName)
                df = ds.to_dataframe()
                
                #Create the four column dataframe
                df = df.reset_index()
                
                #Add temperature values for each point over the month 
                dfMonth = df.Tair.groupby([df.lat,df.lon]).mean().reset_index()
                
                #Change column names to be compatible with pgsql
                dfMonth.columns = ['lat', 'lon', 'temp_avg']
                
                #Add a time column
                tempDate = fileName.split("_")[4].split(".")[0]
                MY =  '01' + '/' + tempDate[4:] + '/' + tempDate[:4]
                
                #Convert string date to date format in python - yyyy-mm-dd
                MY_date = datetime.datetime.strptime(MY, "%d/%m/%Y").date()
                
                #Add to the dataframe
                dfMonth['date'] = MY_date
                print '--------------------------'  
                print '--------------------------' 
                print '--------------------------'
                print ' I have created %s dataframe ' %tableName
                

                ##Adding dataframe to pgsql
                print ' I am adding %s dataframe to pgsql ' %tableName
                pgsql(tableName,dfMonth)

                print '--------------------------'  
                print '--------------------------' 
                print '--------------------------'

            except:
                fileError.append(fileName)
                pass

        
    print '--------------------------'  
    print '--------------------------' 
    print '--------------------------'
    print 'creating csv with error files'

    with open(csvfile, "w") as output:
        writer = csv.writer(output, lineterminator='\n')
        for fileName in fileError:
            writer.writerow([fileName])
    print 'csv with error files created at: %s' %path

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
