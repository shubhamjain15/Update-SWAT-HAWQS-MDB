# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
import os
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import argparse
from configparser import ConfigParser
import os
import sys
import pyodbc
from typing import AnyStr
from io import open
import re

#Function to read all mgt extension file names in folder and subfolders
def find_files_with_extension(root_folder, extension):
    file_list = []
    for foldername, subfolders, filenames in os.walk(root_folder):
        for filename in filenames:
            if filename.endswith(extension):
                file_list.append(os.path.join(foldername, filename))
    return file_list

#Function to read the mgt file information
def read_mgt_info(file_name):
    with open(file_name, 'r') as file:
        lines = file.readlines()    
    # Define regular expressions for extracting values
    hru_pattern = re.compile(r'HRU:(\d+)')
    subbasin_pattern = re.compile(r'Subbasin:(\d+)')
    luse_pattern = re.compile(r'Luse:(\w+)')
    soil_pattern = re.compile(r'Soil:(\d+)')
    slope_pattern = re.compile(r'Slope:(\S+)')
    matches = {}
    # Extract values using regular expressions
    matches['HRU'] = hru_pattern.findall(lines[0])[1]
    matches['SUBBASIN'] = subbasin_pattern.findall(lines[0])[0]
    matches['LANDUSE'] = luse_pattern.findall(lines[0])[0]
    matches['SOIL'] = soil_pattern.findall(lines[0])[0]
    matches['SLOPE_CD'] = slope_pattern.findall(lines[0])[0]
    return(matches)

#Function to read management variables
def read_management_variables(file_name):
    with open(file_name, 'r') as file:
        lines = file.readlines()
    var_lines = lines[2:29]
    pattern = re.compile(r'\s*([\d.]+)\s*\|\s*([A-Z_]+):(.*)')
    # Create a dictionary to store variables and values
    variables_dict = {}
    for line in var_lines:
        match = pattern.search(line)
        if match:
            value = float(match.group(1).strip())
            variable = match.group(2).strip()
            variables_dict[variable] = value
    return(variables_dict)

#Function to read the sheduled management operations
def read_scheduled_management_operations(file_name):
    with open(file_name, 'r') as file:
        var_lines = file.readlines()
    var_lines = var_lines[30:]
    mgt_data = []
    for i in range(len(var_lines)-1):
        vars = {}
        vars['MONTH'] = var_lines[i][1:3].strip()
        vars['DAY'] = var_lines[i][4:6].strip()
        vars['HUSC'] = var_lines[i][7:15].strip()
        vars['MGT_OP'] = int(var_lines[i][16:18].strip())
        if vars['MGT_OP'] == 1:
            #Planting operation line
            vars['PLANT_ID'] = var_lines[i][19:23].strip()
            vars['CURYR_MAT'] = var_lines[i][28:30].strip()
            vars['HEATUNITS'] = var_lines[i][31:43].strip()
            vars['LAI_INIT'] = var_lines[i][44:50].strip()
            vars['BIO_INIT'] = var_lines[i][51:62].strip()
            vars['HI_TARG']= var_lines[i][63:67].strip()
            vars['BIO_TARG'] = var_lines[i][68:74].strip()
            vars['CNOP'] = var_lines[i][75:80].strip()
        if vars['MGT_OP'] == 2:
            #Irrigation application line
            vars['IRR_SC'] = var_lines[i][24:27].strip()
            vars['IRR_NO'] = var_lines[i][28:30].strip()
            vars['IRR_AMT'] = var_lines[i][31:43].strip()
            vars['IRR_SALT'] = var_lines[i][44:50].strip()
            vars['IRR_EFM'] = var_lines[i][51:62].strip()
            vars['IRR_SQ'] = var_lines[i][63:67].strip()
        if vars['MGT_OP'] == 3:
            #3 Fertilizer application line 
            vars['FERT_ID'] = var_lines[i][19:23].strip()
            vars['FRT_KG'] = var_lines[i][31:43].strip()
            vars['FRT_SURFACE'] = var_lines[i][44:50].strip()
        if vars['MGT_OP'] == 4:
            #4 Pesticide application line
            vars['PEST_ID'] = var_lines[i][19:23].strip()
            vars['PST_KG'] = var_lines[i][31:43].strip()
            vars['PST_DEP'] = var_lines[i][44:50].strip()
        if vars['MGT_OP'] == 5:
            #5 Harvest and kill line
            vars['CNOP'] = var_lines[i][31:42].strip()
            vars['HI_OVR'] = var_lines[i][44:50].strip()
            vars['FRAC_HARVK'] = var_lines[i][51:62].strip()
        if vars['MGT_OP'] == 6:
            #6 Tillage operation line
            vars['MGT_OP'] = var_lines[i][16:18].strip()
            vars['TILL_ID'] = var_lines[i][19:23].strip()
            vars['CNOP'] = var_lines[i][31:43].strip()
        if vars['MGT_OP'] == 7:
            #7 Harvest operation line
            vars['IHV_GBM'] = var_lines[i][24:27].strip()
            vars['HARVEFF'] = var_lines[i][31:43].strip()
            vars['HI_OVR'] = var_lines[i][44:50].strip()
        if vars['MGT_OP'] == 8:
            #8 Kill line
            vars['MGT_OP'] = var_lines[i][16:18].strip()
        if vars['MGT_OP'] == 9:
            #9 Grazing operation line
            vars['GRZ_DAYS'] = var_lines[i][19:23].strip()
            vars['MANURE_ID'] = var_lines[i][24:27].strip()
            vars['BIO_EAT'] = var_lines[i][31:43].strip()
            vars['BIO_TRMP'] = var_lines[i][44:50].strip()
            vars['MANURE_KG'] = var_lines[i][51:62].strip()
        if vars['MGT_OP'] == 10:
            #10 Auto irrigation line
            vars['WSTRS_ID'] = var_lines[i][19:23].strip()
            vars['IRR_SCA'] = var_lines[i][24:27].strip()
            vars['IRR_NOA'] = var_lines[i][28:30].strip()
            vars['AUTO_WSTRS'] = var_lines[i][31:43].strip()
            vars['IRR_EFF'] = var_lines[i][44:50].strip()
            vars['IRR_MX'] = var_lines[i][51:62].strip()
            vars['IRR_ASQ'] = var_lines[i][63:67].strip()
        if vars['MGT_OP'] == 11:
            #11 Auto fertilization line
            vars['AFERT_ID'] = var_lines[i][19:23].strip()
            vars['AUTO_NSTRS'] = var_lines[i][31:43].strip()
            vars['AUTO_NAPP'] = var_lines[i][44:50].strip()
            vars['AUTO_NYR'] = var_lines[i][51:62].strip()
            vars['AUTO_EFF'] = var_lines[i][63:67].strip()
            vars['AFRT_SURFACE'] = var_lines[i][68:74].strip()
        if vars['MGT_OP'] == 12:
            #12 Street seeping line
            vars['MGT_OP'] = var_lines[i][16:18].strip()
            vars['SWEEPEFF'] = var_lines[i][31:43].strip()
            vars['FR_CURB'] = var_lines[i][44:50].strip()
        if vars['MGT_OP'] == 13:
            #13 Release/impound line
            vars['MGT_OP'] = var_lines[i][16:18].strip()
            vars['IMP_TRIG'] = var_lines[i][19:23].strip()
        if vars['MGT_OP'] == 14:
            #14 Continuous fertilization line
            vars['MGT_OP'] = var_lines[i][16:18].strip()
            vars['FERT_DAYS'] = var_lines[i][19:23].strip()
            vars['CFRT_ID'] = var_lines[i][24:27].strip()
            vars['IFRT_FREQ'] = var_lines[i][28:30].strip()
            vars['CFRT_KG'] = var_lines[i][31:43].strip()
        if vars['MGT_OP'] == 15:
            #15 Continuous persticide line
            vars['MGT_OP'] = var_lines[i][16:18].strip()
            vars['CPST_ID'] = var_lines[i][19:23].strip()
            vars['PEST_DAYS'] = var_lines[i][24:27].strip()
            vars['IPEST_FREQ'] = var_lines[i][28:30].strip()
            vars['CPST_KG'] = var_lines[i][31:43].strip()
        if vars['MGT_OP'] == 16:
            #16 Burn operation line
            vars['MGT_OP'] = var_lines[i][16:18].strip()
            vars['BURN_FRLB'] = var_lines[i][31:43].strip()
        mgt_data.append(vars)
        for key, value in vars.items():
            if value == '':
                vars[key] = 0
        for key, value in vars.items():
            try:
                vars[key] = float(value)
            except ValueError:
                # If the conversion fails, keep the original value
                pass
    return(mgt_data)


#Function to generate SQL strings for parameters update
def generate_sql_updates_parameters(mgt_info, parameter_dict):
    sql_updates = []
    SQLStrings = []
    for parameter, value in parameter_dict.items():
        sql_update = "UPDATE mgt1 SET %s = %f WHERE ( SUBBASIN = %d AND HRU = %d AND LANDUSE = '%s' AND SOIL = '%s' AND SLOPE_CD = '%s' )" % (parameter, value,int(mgt_info['SUBBASIN']),int(mgt_info['HRU']),mgt_info['LANDUSE'], mgt_info['SOIL'],mgt_info['SLOPE_CD'])
        sql_updates.append(sql_update)
    for sql in sql_updates:
        sql += ";"
        SQLStrings.append(sql)
    return  SQLStrings

#Function to generate SQL strings to delete existing rows that were updated
def generate_sql_delete_rows(mgt_info):
    sql_updates = []
    SQLStrings = []
    sql_update = "DELETE FROM mgt2 WHERE ( SUBBASIN = %d AND HRU = %d AND LANDUSE = '%s' AND SOIL = '%s' AND SLOPE_CD = '%s' )" % (int(mgt_info['SUBBASIN']),int(mgt_info['HRU']),mgt_info['LANDUSE'], mgt_info['SOIL'],mgt_info['SLOPE_CD'])
    sql_update += ";"
    SQLStrings.append(sql_update)
    return  SQLStrings

#Function to generate SQL strings to update the management operations
def generate_sql_updates_operations(mgt_info,mgt_op,crops):
    SQL_strings = []
    for mgt_op_i in mgt_op:
        mgt2_cols_edit = {
        'SUBBASIN': 0, 'HRU': 0, 'LANDUSE': '', 'SOIL': '', 'SLOPE_CD': '', 'CROP': '',
        'YEAR': 0, 'MONTH': 0, 'DAY': 0,
        'HUSC': 0, 'MGT_OP': 0, 'HEATUNITS': 0, 'PLANT_ID': 0,
        'CURYR_MAT': 0, 'LAI_INIT': 0, 'BIO_INIT': 0, 'HI_TARG': 0, 'BIO_TARG': 0, 'CNOP': 0,
        'IRR_AMT': 0, 'FERT_ID': 0, 'FRT_KG': 0, 'FRT_SURFACE': 0, 'PEST_ID': 0, 'PST_KG': 0,
        'TILLAGE_ID': 0, 'HARVEFF': 0, 'HI_OVR': 0, 'GRZ_DAYS': 0, 'MANURE_ID': 0, 'BIO_EAT': 0,
        'BIO_TRMP': 0, 'MANURE_KG': 0, 'WSTRS_ID': 0, 'AUTO_WSTRS': 0, 'AFERT_ID': 0,
        'AUTO_NSTRS': 0, 'AUTO_NAPP': 0, 'AUTO_NYR': 0, 'AUTO_EFF': 0, 'AFRT_SURFACE': 0,
        'SWEEPEFF': 0, 'FR_CURB': 0, 'IMP_TRIG': 0, 'FERT_DAYS': 0, 'CFRT_ID': 0,
        'IFRT_FREQ': 0, 'CFRT_KG': 0, 'PST_DEP': 0, 'IHV_GBM': 0, 'IRR_SALT': 0, 'IRR_EFM': 0,
        'IRR_SQ': 0, 'IRR_EFF': 0, 'IRR_MX': 0, 'IRR_ASQ': 0, 'CPST_ID': 0, 'PEST_DAYS': 0,
        'IPEST_FREQ': 0, 'CPST_KG': 0, 'BURN_FRLB': 0, 'OP_NUM': 0, 'IRR_SC': 0, 'IRR_NO': 0,
        'IRR_SCA': 0, 'IRR_NOA': 0
        }
        for key in mgt_op_i:
            if key in mgt2_cols_edit:
                mgt2_cols_edit[key] = mgt_op_i[key]
        for key in mgt_info:
            if key in mgt2_cols_edit:
                mgt2_cols_edit[key] = mgt_info[key]
        
        mgt2_cols_edit['CROP'] = crops[mgt2_cols_edit['PLANT_ID']]
        columns = ', '.join(mgt2_cols_edit.keys())
        values = ', '.join(str(value) if isinstance(value, (int, float)) else f"'{value}'" for value in mgt2_cols_edit.values())
        sql_string = f"INSERT INTO mgt2 ( {columns} ) VALUES ( {values} );"
        SQL_strings.append(sql_string)
    return(SQL_strings)

#Function to update SQL strings in MDB
#Function to update SWAT database from SQL strings
def UpdateSWATDatabase(SWAT_MDB, SQLs):
    odbc_conn_str = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;UID=;PWD=;' % SWAT_MDB
    # print(odbc_conn_str)
    conn = pyodbc.connect(odbc_conn_str)
    cursor = conn.cursor()
    for sql in SQLs:
        print("--Execute SQL: %s..." % sql)
        cursor.execute(sql)
        print("---- {} rows updated!".format(cursor.rowcount))
    cursor.commit()
    conn.close()


#Function to read CROP data from MDB 
def readCROPdata(SWAT_MDB):
    odbc_conn_str = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;UID=;PWD=;' % SWAT_MDB
    conn = pyodbc.connect(odbc_conn_str)
    cursor = conn.cursor()
    sql_query = "SELECT ICNUM, CPNM FROM crop"
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    crop_dict = {row.ICNUM: row.CPNM for row in rows}
    return(crop_dict)

def main():
    #Read all mgt files in the folders and subfolders
    mgt_files = find_files_with_extension("Management", ".mgt")
    PROJ_PATH = os.getcwd()
    SWAT_PROJ_NAME = 'default.mdb'
    SWAT_MDB = PROJ_PATH + os.sep + SWAT_PROJ_NAME

    crops = readCROPdata(SWAT_MDB) # read crop data
    crops[0] = "None"

    for file in mgt_files:
        mgt_info = read_mgt_info(file)   # Read first line of .mgt file 
        mgt_parameters = read_management_variables(file) # Read parameters of .mgt file
        mgt_op = read_scheduled_management_operations(file) # Read Management operations from .mgt file

        del_strings = generate_sql_delete_rows(mgt_info) # Create SQL strings to delete the existing rows that are already in mgt2 sheet that are to be updated
        param_strings = generate_sql_updates_parameters(mgt_info, mgt_parameters) # Create SQL strings to update the parameters in mgt1
        op_strings = generate_sql_updates_operations(mgt_info,mgt_op,crops)  # Create SQL strings to update management operations

        UpdateSWATDatabase(SWAT_MDB, del_strings)
        UpdateSWATDatabase(SWAT_MDB, param_strings)
        UpdateSWATDatabase(SWAT_MDB, op_strings)

    #Reorder rows and reset OID
    odbc_conn_str = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;UID=;PWD=;' % SWAT_MDB
    conn = pyodbc.connect(odbc_conn_str)
    cursor = conn.cursor()
    # Create a new table with the desired order
    order_query = f"SELECT * INTO mgt2_ordered FROM mgt2 ORDER BY SUBBASIN, HRU, LANDUSE, SOIL, SLOPE_CD, YEAR, MONTH, DAY, HUSC, MGT_OP,AFERT_ID;"
    cursor.execute(order_query)
    # Drop the original table and rename the ordered table
    cursor.execute(f"DROP TABLE mgt2;")
    cursor.execute(f"SELECT * INTO mgt2 FROM mgt2_ordered;")
    cursor.execute(f"DROP TABLE mgt2_ordered;")
    cursor.execute(f"SELECT {' ,'.join([col.column_name for col in cursor.columns(table= 'mgt2').fetchall() if col.column_name != 'OID'])} INTO mgt2_updated2 FROM mgt2")
    cursor.execute(f"ALTER TABLE mgt2_updated2 ADD COLUMN OID AUTOINCREMENT")
    columns_without_oid = [col.column_name for col in cursor.columns(table='mgt2_updated2').fetchall() if col.column_name != 'OID']
    cursor.execute(f"DROP TABLE mgt2;")
    select_statement = f"SELECT OID, {', '.join(columns_without_oid)}"
    cursor.execute(f"{select_statement} INTO mgt2 FROM mgt2_updated2;")
    cursor.execute(f"DROP TABLE mgt2_updated2;")
    cursor.commit()
    conn.close()

if __name__ == '__main__':
    main()


