# -*- coding: utf-8 -*- 
"""==============================================================================

 Title			:osm_assigning_catchment_names.py
 Description		:Assign a river name to each individual catchment
 Author		:LF Velasquez - Earthwatch Europe
 Date			:Nov 07 2018
 Version		:1.0
 Usage			:python osm_assigning_catchment_names.py
 Notes			:This script uses data harvested from OSM. Where the word river is
                         used in a different language i.e. rio, the section named 'calculate new name'
                         needs to be commented out or edited to allow the code to run enterily.
 python version	:2.7.14

=============================================================================="""
# =============================================================================
# Modules - Libraries
# =============================================================================
import arcpy
import pandas as pd
import os
import datetime


try:
    print ' I have started the process'
    startTime = datetime.datetime.now()
    
    # =============================================================================
    # Setting Global Variables & Preparing outputs
    # =============================================================================
    root = "path_to_the_main_folder"
    riverData = root + os.sep "path_to_shp_or_featureclass_file_for_the_rivers"
    catchm = "path_to_the_shp_or_featureclass_file_for_the_catchments"
    outputGdb = "path_to_the_output_gdb"
    riverStreams_OSM = outputGdb + os.sep + "RiversStreams_OSM"
    riverDiss = outputGdb + os.sep + "riverDiss"
    streamDiss = outputGdb + os.sep + "streamDiss"
    waterDiss = outputGdb + os.sep + "waterDiss"
    waterCatch_intersect = outputGdb + os.sep + "water_catchm_int"
    final_river_catch = outputGdb + os.sep + "riverCatchm_final"

    ### Preparing the output gdb
    if arcpy.Exists(riverStreams_OSM):
        arcpy.Delete_management(riverStreams_OSM)
        print ' deleted riverStreams'
    if arcpy.Exists(waterDiss):
        arcpy.Delete_management(waterDiss)
        print ' deleted waterDiss'
    if arcpy.Exists(riverDiss):
        arcpy.Delete_management(riverDiss)
        print ' deleted riverDiss'
    if arcpy.Exists(streamDiss):
        arcpy.Delete_management(streamDiss)
        print ' deleted streamDiss'
    if arcpy.Exists(waterCatch_intersect):
        arcpy.Delete_management(waterCatch_intersect)
        print ' deleted waterCatch'
    if arcpy.Exists(final_river_catch):
        arcpy.Delete_management(final_river_catch)
        print ' deleted final_river_catch'
    ####
        
    # =============================================================================
    # Start of Main Process
    # =============================================================================
    
    ## Get records for stream and river with name assigned to them and create new dataset
    print ' I am selecting rivers and streams ' 
    arcpy.MakeFeatureLayer_management(riverData,"riverOSM")
    exp = "( waterway = 'river' OR waterway = 'stream') AND name <>" + "''"
    arcpy.SelectLayerByAttribute_management("riverOSM","NEW_SELECTION",exp)
    arcpy.CopyFeatures_management("riverOSM",riverStreams_OSM)
    
    ## Create new field for the new name - deleting the word river
    arcpy.MakeFeatureLayer_management(riverStreams_OSM,"riverStream_OSM")
    arcpy.AddField_management("riverStream_OSM", "name_edited", "TEXT","#","#",100)
    
    print ' I am creating the new name for the river'
    ## calculate new name
    newName = "!name!.replace('River','')"
    arcpy.CalculateField_management("riverStream_OSM","name_edited",newName,"PYTHON_9.3")
    newName = "!name_edited!.rstrip()"
    arcpy.CalculateField_management("riverStream_OSM","name_edited",newName,"PYTHON_9.3")
    
    ## Dissolving Rivers
    print ' I am dissolving by name and rivers'
    exp = "waterway = 'river'"
    arcpy.SelectLayerByAttribute_management("riverStream_OSM","NEW_SELECTION",exp)
    ## dissolve data by name and type of waterway
    arcpy.Dissolve_management("riverStream_OSM",riverDiss,["name","waterway"],"","#","#")
    
    ## Dissolving Streams
    print ' I am dissolving by name and stream'
    exp = "waterway = 'stream'"
    arcpy.SelectLayerByAttribute_management("riverStream_OSM","NEW_SELECTION",exp)
    ## dissolve data by name and type of waterway
    arcpy.Dissolve_management("riverStream_OSM",streamDiss,["name","waterway"],"","#","#")
    
    ## Merging datasets
    print ' I am merging rivers and streams '  
    arcpy.Merge_management([riverDiss,streamDiss],waterDiss)
    
    print ' Intersecting with watersheds'
    ## Intersect new river and streams data with catchments
    arcpy.Intersect_analysis([waterDiss,catchm],waterCatch_intersect,"ALL","","")   
    
    
    print ' Dissolving by watershed and river name'
    ## Dissolve by catchment id and edite name
    arcpy.Dissolve_management(waterCatch_intersect,final_river_catch,["HYBAS_ID","name"],"","#","#")
    
    print '--------------------------'  
    print '--------------------------' 
    print '--------------------------' 
    print ' I am starting the final process'
    ## Get list of unique catchment Ids'
    catchmList = []
    rows = arcpy.SearchCursor(final_river_catch, ["HYBAS_ID"])
    for row in rows:
        catchmList.append(row.getValue("HYBAS_ID"))
    del rows
                       
    # Conver feature class table to panda dataframe
    fieldList = ["name", "HYBAS_ID", "Shape_Length"]
    
    df = pd.DataFrame([row for row in arcpy.da.SearchCursor(final_river_catch,fieldList)])
    df.columns = ["river","catchment_id", "river_length"]
    print df
    
    # Look for the longest river in each catchment
    df_grouped = df.groupby(["catchment_id"]).agg({"river_length":"max"})
    df_grouped = df_grouped.reset_index()
    df_grouped = df_grouped.rename(columns={"river_length":"mainRiver_length"})
    df = pd.merge(df,df_grouped,how='left',on=["catchment_id"]) # merge the group dataframe with the main dataframe
    df = df[df["river_length"] == df["mainRiver_length"]] # select row where the river length matches the mainRiver length
    
    # Create csv from the panda data frame
    filename = root + os.sep + "Catchment_MainRiverName" + datetime.datetime.now().strftime("%d%m%Y") + ".csv"
    df.to_csv(filename, encoding='utf-8') 
        


except:
    print 'wrong, read below'
    raise

print ' Done - Check CSV file!'
print '--------------------------'  
print '--------------------------' 
print '--------------------------'  
print 'Time running the script' + ' ' + str(datetime.datetime.now() - startTime) 
