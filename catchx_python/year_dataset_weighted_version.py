# -*- coding: utf-8 -*- 
"""==============================================================================

 Title			:year_dataset_weighted_version.py
 Description		:Create a yearly table inside pgsql
 Author		:LF Velasquez - Earthwatch Europe
 Date			:Nov 07 2018
 Version		:1.0
 Usage			:python year_dataset_weighted_version.py
 Notes			:This version of the script uses the Snow variable for any other variables
                         the SQL query needs updating to reflect the right fields for the tables inside pgsql.
                         The values inside the array_Months variable are the names of the tables as result of
                         executing nc_file_to_pgsql.py or nc_file_to_pgsql_ensemble.py
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

def pgsql_weighted(finalTable, catchmentTable, thiessenTable, arrayMonths):
    """
        param string finalTable: The name of the table to be created inside pgsql
        param string catchmentTable: The name of the table inside pgsql containing the catchments data information
        param string thiessenTable: The name of the table inside pgsql containing the thiessen polygon data information
        param array arrayMonths: Array with the table names inside pgsql containing the raw data - snowfall in this case
    """
    conn = psycopg2.connect(host='<host>', database='<db_name>', user='<user>', password='<password>')
    cur = conn.cursor()
    query = ('SET max_parallel_workers_per_gather=4; CREATE TABLE %s AS WITH JAN AS (WITH GRID AS (select b.hybas_id as hybas,t.objectid as gridid, '
            '(st_area(st_intersection(b.geom, t.geom)))/(st_area(b.geom)) as weight '
            'from %s b, %s t where st_intersects(b.geom, t.geom) '
            '),LLUVIA AS (SELECT precip.snowf as snow, precip.date as dmy, grid.objectid as uniqueid '
            'FROM %s as precip, %s as grid WHERE ST_Intersects(grid.geom,precip.geom) '
            ') SELECT catch.hybas_id,catch.geom,LLUVIA.dmy, SUM(LLUVIA.snow * GRID.weight) as weighted_snow '
            ' FROM %s as catch LEFT JOIN GRID ON catch.hybas_id=GRID.hybas '
            'LEFT JOIN LLUVIA ON GRID.gridid=LLUVIA.uniqueid GROUP BY catch.hybas_id, catch.geom, LLUVIA.dmy), '
            'FEB AS (WITH GRID AS (select b.hybas_id as hybas,t.objectid as gridid, '
            '(st_area(st_intersection(b.geom, t.geom)))/(st_area(b.geom)) as weight '
            'from %s b, %s t where st_intersects(b.geom, t.geom) '
            '),LLUVIA AS (SELECT precip.snowf as snow, precip.date as dmy, grid.objectid as uniqueid '
            'FROM %s as precip, %s as grid WHERE ST_Intersects(grid.geom,precip.geom) '
            ') SELECT catch.hybas_id,catch.geom,LLUVIA.dmy, SUM(LLUVIA.snow * GRID.weight) as weighted_snow '
            ' FROM %s as catch LEFT JOIN GRID ON catch.hybas_id=GRID.hybas '
            'LEFT JOIN LLUVIA ON GRID.gridid=LLUVIA.uniqueid GROUP BY catch.hybas_id, catch.geom, LLUVIA.dmy), '
            'MAR AS (WITH GRID AS (select b.hybas_id as hybas,t.objectid as gridid, '
            '(st_area(st_intersection(b.geom, t.geom)))/(st_area(b.geom)) as weight '
            'from %s b, %s t where st_intersects(b.geom, t.geom) '
            '),LLUVIA AS (SELECT precip.snowf as snow, precip.date as dmy, grid.objectid as uniqueid '
            'FROM %s as precip, %s as grid WHERE ST_Intersects(grid.geom,precip.geom) '
            ') SELECT catch.hybas_id,catch.geom,LLUVIA.dmy, SUM(LLUVIA.snow * GRID.weight) as weighted_snow '
            ' FROM %s as catch LEFT JOIN GRID ON catch.hybas_id=GRID.hybas '
            'LEFT JOIN LLUVIA ON GRID.gridid=LLUVIA.uniqueid GROUP BY catch.hybas_id, catch.geom, LLUVIA.dmy), '
            'APR AS (WITH GRID AS (select b.hybas_id as hybas,t.objectid as gridid, '
            '(st_area(st_intersection(b.geom, t.geom)))/(st_area(b.geom)) as weight '
            'from %s b, %s t where st_intersects(b.geom, t.geom)'
            '),LLUVIA AS (SELECT precip.snowf as snow, precip.date as dmy, grid.objectid as uniqueid '
            'FROM %s as precip, %s as grid WHERE ST_Intersects(grid.geom,precip.geom) '
            ') SELECT catch.hybas_id,catch.geom,LLUVIA.dmy, SUM(LLUVIA.snow * GRID.weight) as weighted_snow '
            ' FROM %s as catch LEFT JOIN GRID ON catch.hybas_id=GRID.hybas '
            'LEFT JOIN LLUVIA ON GRID.gridid=LLUVIA.uniqueid GROUP BY catch.hybas_id, catch.geom, LLUVIA.dmy), '
            'MAY AS (WITH GRID AS (select b.hybas_id as hybas,t.objectid as gridid, '
            '(st_area(st_intersection(b.geom, t.geom)))/(st_area(b.geom)) as weight '
            'from %s b, %s t where st_intersects(b.geom, t.geom) '
            '),LLUVIA AS (SELECT precip.snowf as snow, precip.date as dmy, grid.objectid as uniqueid '
            'FROM %s as precip, %s as grid WHERE ST_Intersects(grid.geom,precip.geom) '
            ') SELECT catch.hybas_id,catch.geom,LLUVIA.dmy, SUM(LLUVIA.snow * GRID.weight) as weighted_snow '
            ' FROM %s as catch LEFT JOIN GRID ON catch.hybas_id=GRID.hybas '
            'LEFT JOIN LLUVIA ON GRID.gridid=LLUVIA.uniqueid GROUP BY catch.hybas_id, catch.geom, LLUVIA.dmy), '
            'JUN AS (WITH GRID AS (select b.hybas_id as hybas,t.objectid as gridid, '
            '(st_area(st_intersection(b.geom, t.geom)))/(st_area(b.geom)) as weight '
            'from %s b, %s t where st_intersects(b.geom, t.geom) '
            '),LLUVIA AS (SELECT precip.snowf as snow, precip.date as dmy, grid.objectid as uniqueid '
            'FROM %s as precip, %s as grid WHERE ST_Intersects(grid.geom,precip.geom) '
            ') SELECT catch.hybas_id,catch.geom,LLUVIA.dmy, SUM(LLUVIA.snow * GRID.weight) as weighted_snow '
            ' FROM %s as catch LEFT JOIN GRID ON catch.hybas_id=GRID.hybas '
            'LEFT JOIN LLUVIA ON GRID.gridid=LLUVIA.uniqueid GROUP BY catch.hybas_id, catch.geom, LLUVIA.dmy), '
            'JUL AS (WITH GRID AS (select b.hybas_id as hybas,t.objectid as gridid, '
            '(st_area(st_intersection(b.geom, t.geom)))/(st_area(b.geom)) as weight '
            'from %s b, %s t where st_intersects(b.geom, t.geom) '
            '),LLUVIA AS (SELECT precip.snowf as snow, precip.date as dmy, grid.objectid as uniqueid '
            'FROM %s as precip, %s as grid WHERE ST_Intersects(grid.geom,precip.geom) '
            ') SELECT catch.hybas_id,catch.geom,LLUVIA.dmy, SUM(LLUVIA.snow * GRID.weight) as weighted_snow '
            ' FROM %s as catch LEFT JOIN GRID ON catch.hybas_id=GRID.hybas '
            'LEFT JOIN LLUVIA ON GRID.gridid=LLUVIA.uniqueid GROUP BY catch.hybas_id, catch.geom, LLUVIA.dmy), '
            'AGS AS (WITH GRID AS (select b.hybas_id as hybas,t.objectid as gridid, '
            '(st_area(st_intersection(b.geom, t.geom)))/(st_area(b.geom)) as weight '
            'from %s b, %s t where st_intersects(b.geom, t.geom) '
            '),LLUVIA AS (SELECT precip.snowf as snow, precip.date as dmy, grid.objectid as uniqueid '
            'FROM %s as precip, %s as grid WHERE ST_Intersects(grid.geom,precip.geom) '
            ') SELECT catch.hybas_id,catch.geom,LLUVIA.dmy, SUM(LLUVIA.snow * GRID.weight) as weighted_snow '
            ' FROM %s as catch LEFT JOIN GRID ON catch.hybas_id=GRID.hybas '
            'LEFT JOIN LLUVIA ON GRID.gridid=LLUVIA.uniqueid GROUP BY catch.hybas_id, catch.geom, LLUVIA.dmy), '
            'SEP AS (WITH GRID AS (select b.hybas_id as hybas,t.objectid as gridid, '
            '(st_area(st_intersection(b.geom, t.geom)))/(st_area(b.geom)) as weight '
            'from %s b, %s t where st_intersects(b.geom, t.geom) '
            '),LLUVIA AS (SELECT precip.snowf as snow, precip.date as dmy, grid.objectid as uniqueid '
            'FROM %s as precip, %s as grid WHERE ST_Intersects(grid.geom,precip.geom) '
            ') SELECT catch.hybas_id,catch.geom,LLUVIA.dmy, SUM(LLUVIA.snow * GRID.weight) as weighted_snow '
            ' FROM %s as catch LEFT JOIN GRID ON catch.hybas_id=GRID.hybas '
            'LEFT JOIN LLUVIA ON GRID.gridid=LLUVIA.uniqueid GROUP BY catch.hybas_id, catch.geom, LLUVIA.dmy), '
            'OCT AS (WITH GRID AS (select b.hybas_id as hybas,t.objectid as gridid, '
            '(st_area(st_intersection(b.geom, t.geom)))/(st_area(b.geom)) as weight '
            'from %s b, %s t where st_intersects(b.geom, t.geom) '
            '),LLUVIA AS (SELECT precip.snowf as snow, precip.date as dmy, grid.objectid as uniqueid '
            'FROM %s as precip, %s as grid WHERE ST_Intersects(grid.geom,precip.geom) '
            ') SELECT catch.hybas_id,catch.geom,LLUVIA.dmy, SUM(LLUVIA.snow * GRID.weight) as weighted_snow '
            ' FROM %s as catch LEFT JOIN GRID ON catch.hybas_id=GRID.hybas '
            'LEFT JOIN LLUVIA ON GRID.gridid=LLUVIA.uniqueid GROUP BY catch.hybas_id, catch.geom, LLUVIA.dmy), '
            'NOV AS (WITH GRID AS (select b.hybas_id as hybas,t.objectid as gridid, '
            '(st_area(st_intersection(b.geom, t.geom)))/(st_area(b.geom)) as weight '
            'from %s b, %s t where st_intersects(b.geom, t.geom) '
            '),LLUVIA AS (SELECT precip.snowf as snow, precip.date as dmy, grid.objectid as uniqueid '
            'FROM %s as precip, %s as grid WHERE ST_Intersects(grid.geom,precip.geom) '
            ') SELECT catch.hybas_id,catch.geom,LLUVIA.dmy, SUM(LLUVIA.snow * GRID.weight) as weighted_snow '
            ' FROM %s as catch LEFT JOIN GRID ON catch.hybas_id=GRID.hybas '
            'LEFT JOIN LLUVIA ON GRID.gridid=LLUVIA.uniqueid GROUP BY catch.hybas_id, catch.geom, LLUVIA.dmy), '
            'DEC AS (WITH GRID AS (select b.hybas_id as hybas,t.objectid as gridid, '
            '(st_area(st_intersection(b.geom, t.geom)))/(st_area(b.geom)) as weight '
            'from %s b, %s t where st_intersects(b.geom, t.geom) '
            '),LLUVIA AS (SELECT precip.snowf as snow, precip.date as dmy, grid.objectid as uniqueid '
            'FROM %s as precip, %s as grid WHERE ST_Intersects(grid.geom,precip.geom) '
            ') SELECT catch.hybas_id,catch.geom,LLUVIA.dmy, SUM(LLUVIA.snow * GRID.weight) as weighted_snow '
            ' FROM %s as catch LEFT JOIN GRID ON catch.hybas_id=GRID.hybas '
            'LEFT JOIN LLUVIA ON GRID.gridid=LLUVIA.uniqueid GROUP BY catch.hybas_id, catch.geom, LLUVIA.dmy) '
            'SELECT * FROM JAN UNION SELECT * FROM FEB UNION SELECT * FROM MAR UNION SELECT * FROM APR UNION SELECT * FROM MAY UNION SELECT * FROM JUN '
            'UNION SELECT * FROM JUL UNION SELECT * FROM AGS UNION SELECT * FROM SEP UNION SELECT * FROM OCT UNION SELECT * FROM NOV UNION SELECT * FROM DEC')%(finalTable,str(catchmentTable),thiessenTable,arrayMonths[0],thiessenTable,catchmentTable,
                                                                                                                                                                catchmentTable,thiessenTable,arrayMonths[1],thiessenTable,catchmentTable,
                                                                                                                                                                catchmentTable,thiessenTable,arrayMonths[2],thiessenTable,catchmentTable,
                                                                                                                                                                catchmentTable,thiessenTable,arrayMonths[3],thiessenTable,catchmentTable,
                                                                                                                                                                catchmentTable,thiessenTable,arrayMonths[4],thiessenTable,catchmentTable,
                                                                                                                                                                catchmentTable,thiessenTable,arrayMonths[5],thiessenTable,catchmentTable,
                                                                                                                                                                catchmentTable,thiessenTable,arrayMonths[6],thiessenTable,catchmentTable,
                                                                                                                                                                catchmentTable,thiessenTable,arrayMonths[7],thiessenTable,catchmentTable,
                                                                                                                                                                catchmentTable,thiessenTable,arrayMonths[8],thiessenTable,catchmentTable,
                                                                                                                                                                catchmentTable,thiessenTable,arrayMonths[9],thiessenTable,catchmentTable,
                                                                                                                                                                catchmentTable,thiessenTable,arrayMonths[10],thiessenTable,catchmentTable,
                                                                                                                                                                catchmentTable,thiessenTable,arrayMonths[11],thiessenTable,catchmentTable)


    cur.execute(query)
    conn.commit()
    conn.close()

# =============================================================================
# Setting Global Variables & Preparing outputs
# =============================================================================  
months = ['01','02','03','04','05','06','07','08','09','10','11','12']
years = [1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014]

try:
    # =============================================================================
    # Start of Main Process
    # =============================================================================
    print '--------------------------'  
    print '--------------------------' 
    print '--------------------------'
    print ' creating weighted variable tables...'
    print '--------------------------'  
    print '--------------------------' 
    print '--------------------------'
    for year in years:
        yymm_snow = []
        catchmentTable = 'add_catchment_table_name'
        thiessenTable = 'add_table_with_thiessen_covering_cathcmentTable' 
        yearTableName_snow = 'mswep_snowfall_' + str(sample) + '_' + str(year)
        print '--------------------------'  
        print '--------------------------' 
        print '--------------------------'
        print ' I am doing %s' %(yearTableName_snow)
        for month in months:
            tableName_snow = 'snowfall_mswep_025_' + str(year) + month
            yymm_snow.append(tableName_snow)
            
        pgsql_weighted_snow(yearTableName_snow, catchmentTable, thiessenTable, yymm_snow)
except:
    print '--------------------------'  
    print '--------------------------' 
    print '--------------------------'
    print 'wrong, read below'
    raise

print '--------------------------'  
print '--------------------------' 
print '--------------------------'
print 'check pgsql for yearly tables'
print 'Time running the script' + ' ' + str(datetime.datetime.now() - startTime) 
