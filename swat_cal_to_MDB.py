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

def current_path(local_function):
    from inspect import getsourcefile
    fpath = getsourcefile(local_function)
    if fpath is None:
        return None
    return os.path.dirname(os.path.abspath(fpath))

def get_swat_project_name(desc='Specify SWAT project name to run this script.'):
    # define input arguments
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-name', type=str, help='Name or full file path of SWAT project')
    # parse arguments
    args = parser.parse_args()
    if args.name is None:
        raise ValueError('You MUST specify the name of SWAT project!')
    if os.path.isfile(args.name):
        ws = os.path.dirname(args.name)
        name = os.path.basename(args.name)
    else:
        ws = current_path(lambda: 0)
        name = args.name
    if '.mdb' not in name:
        name = '%s.mdb' % name
    return ws, name

def is_string(in_str):
    return isinstance(in_str, (str, u''.__class__))

def split_string(str_src, spliters=None, elim_empty=False):
    if is_string(spliters):
        spliters = [spliters]
    if spliters is None or not spliters:
        spliters = [' ', '\t']
    dest_strs = list()
    src_strs = [str_src]
    while True:
        old_dest_strs = src_strs[:]
        for s in spliters:
            for src_s in src_strs:
                temp_strs = src_s.split(s)
                for temp_s in temp_strs:
                    temp_s = temp_s.strip()
                    if temp_s == '' and elim_empty:
                        continue
                    if is_string(temp_s):
                        try:
                            temp_s = str(temp_s)
                        except:
                            pass
                    dest_strs.append(temp_s)
            src_strs = dest_strs[:]
            dest_strs = list()
        if old_dest_strs == src_strs:
            dest_strs = src_strs[:]
            break
    return dest_strs


ext2Table = {'mgt': 'mgt1'}

class paraIdentifier:
    ### the parameter identifier format proposed by SWAT-CUP
    ### x__<parname>.<ext>__<hydrogrp>__<soltext>__<landuse>__<subbsn>__<slope>
    def __init__(self):
        self.name = ''  ### SWAT parameter name as it appears in the Absolute_SWAT_Values.txt
        self.ext = ''  ### SWAT file extention code or table name in SWAT database, e.g., sol, hru, rte, etc.
        self.hydrogrp = []  ### (Optional) soil hydrological group ('A', 'B', 'C', or 'D')
        self.soltext = []  ### (Optional) soil texture
        self.landuse = []  ### (Optional) landuse
        self.subbsn = []  ### (Optional) subbasin number(s)
        self.slope = []  ### (Optional) slope, e.g., '0-20'
        self.indent = ''  ### identifier code, v means replaced, a means added, r means multiplied.
        self.value = 9999  ###
        self.lyr = []  ### (Optional) soil layer number(s), e.g., 1,2,4-6 means 1,2,4,5,6

    def printStr(self):
        print(
                "indentifer: %s, name: %s, table: %s, layers: %s, hydroGRP: %s, soil texture: %s, landuse: %s, subbasin: %s, slope: %s" % \
                (self.indent, self.name, self.ext, self.lyr, self.hydrogrp, self.soltext,
                 self.landuse,
                 self.subbsn, self.slope))


def ExtractMultiNums(str):
    ### the foundamental format of str is 1,2,4-6.
    orgNums = split_string(str, ',')
    destNums = []
    if len(orgNums) == 1:
        tempNumStrs = split_string(orgNums[0], '-')
        if len(tempNumStrs) == 1:
            return [int(tempNumStrs[0])]
        elif len(tempNumStrs) == 2:
            iStart = int(tempNumStrs[0])
            iEnd = int(tempNumStrs[1])
            for i in range(iStart, iEnd + 1):
                destNums.append(i)
        else:
            print("Error occurred in %s in model.in!" % str)
            sys.exit(1)
    else:
        for numStr in orgNums:
            try:
                i = int(numStr)
                destNums.append(i)
            except ValueError:
                tempNumStrs = split_string(numStr, '-')
                if len(tempNumStrs) == 2:
                    iStart = int(tempNumStrs[0])
                    iEnd = int(tempNumStrs[1])
                    for i in range(iStart, iEnd + 1):
                        destNums.append(i)
                else:
                    print("Please check the format of %s!" % str)
                    sys.exit(1)
    return destNums


def ReadModelIn(infile):
    f = open(infile)
    paraObjs = []
    for line in f:
        line = line.split('\n')[0]
        line.strip()
        if line != '' and line.find('//') < 0:
            tempParaStrs = split_string(line, elim_empty=True)
            # print tempParaStrs
            if len(tempParaStrs) != 2:
                print("Please check the item %s in model.in!" % tempParaStrs)
                sys.exit(1)
            else:
                tempParaObj = paraIdentifier()
                tempParaObj.indent = tempParaStrs[0][0]
                tempParaObj.value = float(tempParaStrs[1])
                tempPrefixs = tempParaStrs[0].split('__')
                # print tempPrefixs
                if len(tempPrefixs) > 1:
                    # print  tempPrefixs[1]
                    tempName, tempExt = tempPrefixs[1].split('.')
                    if tempExt in ext2Table:
                        tempParaObj.ext = ext2Table.get(tempExt)
                    else:
                        tempParaObj.ext = tempExt
                    tempNameStrs = tempName.split('(')
                    if len(tempNameStrs) == 1:
                        tempParaObj.name = tempName
                    else:
                        tempParaObj.name = tempNameStrs[0]
                        tempLyrStrs = tempNameStrs[1].split(')')
                        if tempLyrStrs[0] != '':
                            tempParaObj.lyr = ExtractMultiNums(tempLyrStrs[0])
                        else:
                            ### all layers, -1 is indicator in the following process
                            tempParaObj.lyr = [-1]
                    if len(tempPrefixs) > 2 and tempPrefixs[2] != '':
                        tempParaObj.hydrogrp = split_string(tempPrefixs[2], ',')
                    if len(tempPrefixs) > 3 and tempPrefixs[3] != '':
                        tempParaObj.soltext = split_string(tempPrefixs[3], ',')
                    if len(tempPrefixs) > 4 and tempPrefixs[4] != '':
                        tempParaObj.landuse = split_string(tempPrefixs[4], ',')
                    if len(tempPrefixs) > 5 and tempPrefixs[5] != '':
                        tempParaObj.subbsn = ExtractMultiNums(tempPrefixs[5])
                    if len(tempPrefixs) > 6 and tempPrefixs[6] != '':
                        tempParaObj.slope = split_string(tempPrefixs[6], ',')
                else:
                    print("Please check the item %s in model.in!" % tempParaStrs[0])
                    sys.exit(1)
                paraObjs.append(tempParaObj)
    f.close()
    # for para in paraObjs:
    #     para.printStr()
    return paraObjs


def findFields(MDBPath, tableName, paraName):
    odbc_conn_str = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};Dbq=%s;UID=;PWD=;' % MDBPath
    # print odbc_conn_str
    conn = pyodbc.connect(odbc_conn_str)
    cursor = conn.cursor()
    field_sel = []
    for row in cursor.columns(table=tableName):
        tempName = row.column_name
        if tempName.find(paraName) == 0:
            field_sel.append(tempName)
    return field_sel

def ConstructSQLs(paraObjs, MDBPath):
    SQLStrings = []
    for para in paraObjs:
        filterHydgrp = ""
        if para.hydrogrp != []:
            for hyd in para.hydrogrp:
                filterHydgrp += " HYDGRP='%s' or " % hyd
            filterHydgrp = filterHydgrp[:(len(filterHydgrp) - 3)]
        filterSoltext = ""
        if para.soltext != []:
            for text in para.soltext:
                filterSoltext += " TEXTURE='%s' or " % text
            filterSoltext = filterSoltext[:(len(filterSoltext) - 3)]
        filterLanduse = ""
        if para.landuse != []:
            for lu in para.landuse:
                filterLanduse += " LANDUSE='%s' or " % lu
            filterLanduse = filterLanduse[:(len(filterLanduse) - 3)]
        filterSlope = ""
        if para.slope != []:
            for slp in para.slope:
                filterSlope += " SLOPE_CD='%s' or " % slp
            filterSlope = filterSlope[:(len(filterSlope) - 3)]
        filterSubbsn = ""
        if para.subbsn != []:
            for bsn in para.subbsn:
                filterSubbsn += " SUBBASIN=%d or " % bsn
            filterSubbsn = filterSubbsn[:(len(filterSubbsn) - 3)]
        filterStr = "WHERE "
        if filterHydgrp != "":
            filterStr += "(%s) and " % filterHydgrp
        if filterLanduse != "":
            filterStr += "(%s) and " % filterLanduse
        if filterSlope != "":
            filterStr += "(%s) and " % filterSlope
        if filterSoltext != "":
            filterStr += "(%s) and " % filterSoltext
        if filterSubbsn != "" and para.ext != 'bsn':  # table 'bsn' only has one record!
            filterStr += "(%s) and " % filterSubbsn
        paraNames = []
        if para.lyr != []:
            if para.lyr[0] == -1:
                paraNames = findFields(MDBPath, para.ext, para.name)
            else:
                for l in para.lyr:
                    paraNames.append(para.name + str(l))
        else:
            paraNames.append(para.name)
        # print paraNames
        curSQLStrs = []
        absValueSQLStrs = []
        for paraname in paraNames:
            if para.indent == 'v':
                baseSQLStr = "UPDATE %s SET %s = %f " % (para.ext, paraname, para.value)
            elif para.indent == 'a':
                baseSQLStr = "UPDATE %s SET %s = %s + %f " % (
                    para.ext, paraname, paraname, para.value)
            elif para.indent == 'r':
                baseSQLStr = "UPDATE %s SET %s = %s * (1 + %f) " % (
                    para.ext, paraname, paraname, para.value)
            else:
                print("Error occurred in Constructing SQL, please check %s in model.in" % paraname)
                sys.exit(1)
            curSQLStrs.append(baseSQLStr)
        # print(curSQLStrs)
        if filterStr != "WHERE ":
            filterStr = filterStr[:(len(filterStr) - 4)]
        else:
            filterStr = ""
        for sql in curSQLStrs:
            sql += filterStr
            sql += ";"
            SQLStrings.append(sql)
        #Add PRF for PRF_BSN. This is a special case, remove for general purposes.
        for sql_string in SQLStrings:
            if "PRF_BSN" in sql_string:
                SQLStrings.append(sql_string.replace("PRF_BSN", "PRF"))
        #SQLStrings.extend(absValueSQLStrs)
    for sql in SQLStrings:
        print(sql)
    return SQLStrings


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

def main():
    PROJ_PATH = r'C:\Users\shubham.jain\OneDrive - Texas A&M AgriLife\BRC_Temple\transfer_to_access\Python'
    SWAT_PROJ_NAME = 'default.mdb'
    SWAT_MDB = PROJ_PATH + os.sep + SWAT_PROJ_NAME
    MODEL_IN = 'model.in'
    PARA_OBJs = ReadModelIn(MODEL_IN)

    SQLs = ConstructSQLs(PARA_OBJs, SWAT_MDB)
    UpdateSWATDatabase(SWAT_MDB, SQLs)


if __name__ == '__main__':
    main()