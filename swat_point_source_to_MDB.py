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

#Function to get the subbasin number from the p.dat file
def extract_p_subbasin_number(file_name):
    with open(file_name, 'r') as file:
            lines = file.readlines()
    for line in lines:
        if 'Subbasin:' in line:
            subbasin_info = line.split('Subbasin:')[1]
            subbasin_number = int(subbasin_info.split()[0])
            return subbasin_number
    raise ValueError("Subbasin number not found")

#Function to get the updated parameter values from p.dat file
def extract_p_parValues(file_name):
    with open(file_name, 'r') as file:
            lines = file.readlines()
    if len(lines) > 6:
        header_row = lines[5].split()
        values_row = list(map(float, lines[-1].split()))
        par_dict = dict(zip(header_row, values_row))
        return par_dict
    else:
        raise ValueError("Point source data in some different format")

# Get existing numbers in the sheet in MDB
def get_subbasins_mdb(SWAT_MDB, table_name, column_name):
    odbc_conn_str = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;UID=;PWD=;' % SWAT_MDB
    conn = pyodbc.connect(odbc_conn_str)
    cursor = conn.cursor()
    query = f'SELECT {column_name} FROM {table_name}'
    cursor.execute(query)
    values = [row.__getattribute__(column_name) for row in cursor.fetchall()]
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    return values

#Function to generate SQL strings
#If a subbasin does not exist in the mdb, it creates a new line with OID, Subbasin number and TYPE (TYPE is set to 11 but TBD change from fig.fig)
def generate_sql_updates(subbasin,subbasins_mdb, parameter_dict):
    sql_updates = []
    SQLStrings = []
    if subbasin not in subbasins_mdb:
        sql_update = "INSERT INTO pp (OID) SELECT MAX(OID) + 1 FROM pp"
        sql_updates.append(sql_update)
        sql_update =  "UPDATE pp SET SUBBASIN = %f WHERE (OID = (SELECT MAX(OID) FROM pp))" % (int(subbasin))
        sql_updates.append(sql_update)
        sql_update =  "UPDATE pp SET TYPE = %f WHERE (OID = (SELECT MAX(OID) FROM pp))" % (int(11))
        sql_updates.append(sql_update)
    for parameter, value in parameter_dict.items():
        sql_update = "UPDATE pp SET %s = %f WHERE ( SUBBASIN = %a )" % (parameter, value,subbasin)
        sql_updates.append(sql_update)
    for sql in sql_updates:
        sql += ";"
        SQLStrings.append(sql)
    return  SQLStrings

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

def main():
    #Execute code - get SQL strings for all p.dat files
    PROJ_PATH = os.getcwd()
    SWAT_PROJ_NAME = 'default.mdb'
    SWAT_MDB = PROJ_PATH + os.sep + SWAT_PROJ_NAME 
    pattern = re.compile(r'\d+p\.dat')
    point_source_files = [file for file in os.listdir() if pattern.match(file)]
    SQL_strings = []
    subbasins_mdb = get_subbasins_mdb(SWAT_MDB,'pp','SUBBASIN')
    for file in point_source_files:
        subbasin = extract_p_subbasin_number(file)
        parameters_subbasin = extract_p_parValues(file)
        sql_subbasin = generate_sql_updates(subbasin,subbasins_mdb, parameters_subbasin)
        SQL_strings.append(sql_subbasin)
    SQL_strings = pd.Series(SQL_strings).explode().tolist()
    #Execute code - update swat database using SQL strings
    UpdateSWATDatabase(SWAT_MDB, SQL_strings)


if __name__ == '__main__':
    main()
