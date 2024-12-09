# -*- coding: utf-8 -*- 
"""==============================================================================

 Title			:land_cover_to_csv.py
 Description		:Create land cover distribution csv file per catchment
 Author		:LF Velasquez - Earthwatch Europe
 Date			:Nov 07 2018
 Version		:1.0
 Usage			:python land_cover_to_csv.py
 Notes			:This is scrip uses the land cover .tiff files to produce the csv files
                         needed for land_cover_csv_to_pgsql
 python version	:2.7.14

=============================================================================="""
# =============================================================================
# Modules - Libraries
# =============================================================================
import datetime, os, csv
from os import walk
import arcpy
from arcpy import env
from arcpy.sa import *

# =============================================================================
# Functions
# =============================================================================

#create csv file
def tableToCSV(input_tbl, csv_filepath):
    """
        param string input_tbl: transpose table with land cover data
        param string csv_filepath : Path and name of csv file
    """
    fld_list = arcpy.ListFields(input_tbl)
    fld_names = [fld.name for fld in fld_list]
    with open(csv_filepath, 'wb') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(fld_names)
        with arcpy.da.SearchCursor(input_tbl, fld_names) as cursor:
            for row in cursor:
                writer.writerow(row)
        print csv_filepath + " CREATED"
    csv_file.close()

# =============================================================================
# Start of Main Process
# =============================================================================

try: 
    print ' I have started the process'
    startTime = datetime.datetime.now()

    #reading files in folder
    path = raw_input('Enter path to land cover year files: ')
    dbPath = "path_to_database_with_land_cover_database"
    csvPath = "path_to_folder_to_create_csv_files"
    print ' ....starting process...'

    #set global variables
    fileNames = []
    fileError = []
    csvfile = "path_to_folder_for_file_with_errors" + os.sep + "tempErrorFiles_LandCoverToCSV.csv"

    # Check Spatial Extension
    if arcpy.CheckExtension("Spatial") == "Available":
        arcpy.AddMessage("Checking out Spatial")
        arcpy.CheckOutExtension("Spatial")
    else:
        arcpy.AddError("Unable to get spatial analyst extension")
        arcpy.AddMessage(arcpy.GetMessages(0))
        sys.exit(0)

    # Create list of fileNames
    for (dirpath, dirnames, filenames) in walk(path):
        for filename in filenames:
            if filename.endswith('tif'):
                fileNames.append(filename)
            

    # Start process for each file
    for fileName in fileNames:
        try:
            print '--------------------------'  
            print '--------------------------' 
            print '--------------------------'
            print 'Starting Tabulate Area and Transpose Table for Catchments for %s' %('Land Cover_' + fileName.split("-")[7])
            # Description: Calculates cross tabulated areas between two datasets.
            # Set local variables Tabulate
            inZoneData = path + os.sep + fileName
            zoneField = "Value"
            inClassData = "path_to_catchment_shp_file"
            classField = "hydro_id"
            outTableTab = dbPath + os.sep + "LC_Tabulate_" + fileName.split("-")[7]
            # Set local variables Transpose
            outTableTransp = dbPath + os.sep + "LC_Tabulate_" + fileName.split("-")[7] + "_transp"
            transposedFieldName = "hybas_id"
            valueFieldName = "area_lc"
            attrFields = "VALUE" # This is the land cover code

            #Check if table exist and delete before 
            if arcpy.Exists(outTableTab):
                arcpy.Delete_management(outTableTab)

            # Execute TabulateArea
            TabulateArea(inZoneData, zoneField, inClassData, classField, outTableTab,'#')
            print '--------------------------'  
            print '--------------------------' 
            print '--------------------------'
            print 'Tabulate Area calculated for %s' %fileName.split("-")[7]
            print '--------------------------'  
            print '--------------------------' 
            print '--------------------------'
            print 'Transpose process started'

            # Create list of fields
            field_names = [f.name for f in arcpy.ListFields(outTableTab)]
            fieldToDelete = [0,1] # This gets rid of OBJECTID and VALUE
            for i in sorted(fieldToDelete,reverse=True):
                del field_names[i]

            field_names = [str(i) for i in field_names] # remove u from string 
            new_fieldNames = ([s.strip('HYDRO_') for s in field_names]) # creating new list for field names

            trans_fieldNames = zip(field_names, new_fieldNames) # creating list with tuple (current field name, future field name)

            #Conver list to string
            fieldsToTranspose = ";".join(map(str,trans_fieldNames))
            fieldsToTranspose = fieldsToTranspose.replace("(","")
            fieldsToTranspose = fieldsToTranspose.replace(")","")
            fieldsToTranspose = fieldsToTranspose.replace(",","")
            fieldsToTranspose = fieldsToTranspose.encode("utf-8")

            #Check if table exist and delete
            if arcpy.Exists(outTableTransp):
                arcpy.Delete_management(outTableTransp)
                
            ### Execute TransposeTimeFields
            arcpy.TransposeFields_management(outTableTab, fieldsToTranspose, outTableTransp, transposedFieldName, valueFieldName, attrFields)

            ### Add field with year of data
            arcpy.AddField_management(outTableTransp,"lc_year", "TEXT", "", "", "25", "", "", "NON_REQUIRED", "")
            #Calculate Field  
            print("Adding year to table...")  
            Expression = fileName.split("-")[7]
            arcpy.CalculateField_management(outTableTransp, "lc_year", Expression, "PYTHON")
            print '--------------------------'  
            print '--------------------------' 
            print '--------------------------'
            print 'I have transposed table for %s' %fileName.split("-")[7]
            print '--------------------------'  
            print '--------------------------' 
            print '--------------------------'
            print 'Creating csv file'
            csvFileName = csvPath + os.sep + "lc_sa_lv07_" + fileName.split("-")[7] + ".csv"
            tableToCSV(outTableTransp, csvFileName)

        except:
##            print ( ' Error found in file %s ' %fileName
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
    print 'csv with error files created as %s' %path
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

