#!/usr/bin/env python
# -*- coding: latin-1 -*-

from datetime import datetime # for datetime.now()
timStart = datetime.now()

import arcpy#, sys
import getpass
import string
#from arcpy import env
import arcEC

strName = "KMS - Walk D.R."
strVer = "1.0.3"
strBuild = "'130524"

### History ###
# Ver. 1.0.0
# Ver. 1.0.1
#    Try to make it work on NIS - Change to pick feature layer from TOC, not directly by Connection
# Ver. 1.0.2
#    Cleaning and testing before sending to Esri
# Ver. 1.0.3
#    Kevin Ingram have introduced "Writing status back to the DR table" '130524/KJI

### To do ###
# Introduce .log file

def strip_u(strU,lstBad):
    while strU[0] in lstBad:
        strU = strU[1:]
    while strU[-1:] in lstBad:
        strU = strU[:-1]
    return strU

def GetConnection(strP):
    numC = string.find(strP,"Connection")
    strP = strP[numC:]
    numC = string.find(strP,": ")
    strP = strP[numC+1:]
    numC = string.find(strP,",")
    strP = strP[:numC]
    strP = strip_u(strP,[u' '])
    strP = strP.replace(')','')
    strP = strP.replace('.gdb (','.gdb/')
    return strP

# Script arguments

Reviewer_Workspace = arcpy.GetParameterAsText(0)
Sessions = arcpy.GetParameterAsText(1)
MarkSolved = arcpy.GetParameterAsText(2)
Log = arcpy.GetParameterAsText(3)

# Local variables:
lstOfBadNulls = (0,-32767,"0","-32767","<Null>")

#Paths to tables in Input Reviewer workspace
tblSessionsTable = Reviewer_Workspace + "\\GDB_REVSESSIONTABLE"
tblRevTableMain = Reviewer_Workspace + "\\REVTABLEMAIN"
tblRevCheckRun = Reviewer_Workspace + "\\REVCHECKRUNTABLE"

lstSessionIDs = []
WhereClause = ""

arcEC.SetMsg(" ::DR1::     WS : "+Reviewer_Workspace,0)
arcEC.SetMsg(" ::DR1:: SesTab.: "+tblSessionsTable,0)
arcEC.SetMsg(" ::DR1:: RevTab.: "+tblRevTableMain,0)
arcEC.SetMsg(" ::DR1:: ChkRun.: "+tblRevCheckRun,0)

#------------------------------------------------------------------------------
# Build Where Clause for selecting records
#------------------------------------------------------------------------------

# --- Walk tblSessionsTable - whatever it is ...
#lstF = tblSessionsTable.fields
#arcEC.SetMsg("\n ::DR2:: SesTbl.: "+str(type(tblSessionsTable))+" "+tblSessionsTable,0)
desIn = arcpy.Describe(tblSessionsTable)
#arcEC.SetMsg(" ::DR2::  Des.: "+arcEC.Describe2String(desIn),0)
arcEC.SetMsg(" ::DR2::  tblSessionsTable : "+arcEC.Table2Ascii(tblSessionsTable),0)

# --- Get the IDs for the input session(s) ---

rows = arcpy.SearchCursor(tblSessionsTable)
rowcount = int(arcpy.GetCount_management(tblSessionsTable).getOutput(0))
for row in rows:
    if row.SESSIONNAME in Sessions:
        lstSessionIDs.append(str(row.SESSIONID))
del row,rows
numSessioncount = len(lstSessionIDs)

arcEC.SetMsg("\n ::DR3:: SesIDs.: "+str(lstSessionIDs)+" ("+str(numSessioncount)+")",0)
# expect e.g.: ['1', '2']

# --- if you did not select all the session, make a whereclause to select only features from the desired sessions ---
if numSessioncount <> rowcount:
    WhereClause = "("
    SessionFieldName = arcpy.AddFieldDelimiters(Reviewer_Workspace, "SessionID")
    for sessionID in lstSessionIDs:
        WhereClause = WhereClause + SessionFieldName  + " = " + str(sessionID) + " OR "
    WhereClause = WhereClause[:-4] + ")"
wherecount = len(WhereClause)

# expression parameter of make layer and create table view only allows 247 characters
# If the number of sessions selected is more than 13 this limit will be reached.
if wherecount > 247:
    arcpy.AddError("The expression resulting from adding the selected sessions and Expression value is too long.")
    arcpy.AddError(WhereClause)
    arcpy.AddError("Whereclause length " + str(wherecount))
else:
    pass

arcEC.SetMsg("\n ::DR4:: WreClu.: "+WhereClause,0)

#------------------------------------------------------------------------------
# Read the \\REVTABLEMAIN table
#------------------------------------------------------------------------------

arcEC.SetMsg("\n ::DR5:: WALKING THE TABLE...:\n",0)

#arcEC.SetMsg("\n ::DR6::  tblRevTableMain : "+arcEC.Table2Ascii(tblRevTableMain),0)  # <- Take very long time ...
#arcEC.SetMsg("\n ::DR6::  tblRevTableMain : "+arcEC.Table2Ascii_byFields(tblRevTableMain),0)

#KJI
inrows = arcpy.SearchCursor(tblRevTableMain, WhereClause, "", "", "ORIGINCHECK; REVIEWSTATUS; PARAMETERS")
                #UpdateCursor (dataset, {where_clause}, {spatial_reference}, {fields}, {sort_fields})
inrows = arcpy.UpdateCursor(tblRevTableMain, WhereClause, "", "CORRECTIONTECHNICIAN; CORRECTIONDATE; CORRECTIONSTATUS", "ORIGINCHECK; REVIEWSTATUS; PARAMETERS")
#KJI

#    ORIGINCHECK  : What type of check. Must be "Domain Check"
#    REVIEWSTATUS : What's wrong, e.g. "CATSLC: Invalid domain value"
#    PARAMETERS   : Where is it wrong. Isolate Connection1 from e.g. [Connection1: C:\Martin\Work\HomeGDB.gdb (samp1), Param Use Full Database: true, Check Nulls:false]
#    ORIGINTABLE  : Consider for double check of Where...
#    Add later    : Fields to set CorrectionStatus, etc. i.e. e.g. CORRECTIONTECHNICIAN
# ** Collect cells to be corrected, i.e. must be DomainViolation and (<number> 0 or <string> "")
dic2BC = dict() # To Be Corrected
for inrow in inrows:
    strOrgChk = inrow.ORIGINCHECK
    if strOrgChk == "Domain Check":
        strRevSta = inrow.REVIEWSTATUS
        if "Invalid domain value" in strRevSta:
            strPrmtrs = inrow.PARAMETERS
            if "Connection" in strPrmtrs:
                # * Collect the cell ------------------------------------------
                arcEC.SetMsg(" ::DR7::  Collecting RECORDID = "+str(inrow.RECORDID),0)
                # Connection
                strConnection = GetConnection(strPrmtrs)
                arcEC.SetMsg(" ::DR7::  Con.: "+strConnection,0)
                # Record where clause
                numGeoRecordID = inrow.OBJECTID
                strGeoWhereClause = "(OBJECTID = "+str(numGeoRecordID)+")"
                arcEC.SetMsg(" ::DR7::  Whr.: "+strGeoWhereClause,0)
                # Field(s)
                lstTokens = strRevSta.split(':')
                strField = lstTokens[0]
                arcEC.SetMsg(" ::DR7::  Fld.: "+strField,0)
                try:
                    #desCon = arcpy.Describe(strConnection)
                    #arcEC.SetMsg(" ::DR7::  DesCon.: "+arcEC.Describe2String(desCon),0)
                    strConnection = "DangersP" # <--- XXX
                    #UpdateCursor (dataset, {where_clause}, {spatial_reference}, {fields}, {sort_fields})
                    corrows = arcpy.UpdateCursor(strConnection,strGeoWhereClause,"",strField,"")
                    for rowC in corrows:
                        desC = arcpy.Describe(strConnection)
                        #arcEC.SetMsg(" ::DR8::  Desc.geo.: "+arcEC.Describe2String(desC),0)
                        # - This is where the actual value correction happens ---------------------
                        oldVal = rowC.getValue(strField)
                        arcEC.SetMsg(" ::DR8::    Old Val.: "+str(oldVal)+" "+str(type(oldVal)),0)
                        if oldVal in lstOfBadNulls:
                            rowC.setNull(strField)
                        # Consider also to set Editor fields, alternatively use a geocalculator call ... XXX
                        corrows.updateRow(rowC)

#KJI
                        #Update correction fields of the Reviewer Table row
                        if MarkSolved == 'true':
                            inrow.CORRECTIONTECHNICIAN = getpass.getuser()
                            now = datetime.datetime.now()
                            inrow.CORRECTIONDATE = now.strftime("%A, %B %d, %Y %I:%M:%S %p")
                            inrow.CORRECTIONSTATUS = "Resolved - Programmatically replaced bogus Null with real Null"
                            inrows.updateRow(inrow)
#KJI
                        # -------------------------------------------------------------------------
                except:
                    arcEC.ecError("Can't open Connection table:"+strConnection+". In RECORDID = "+str(inrow.RECORDID),110,110)
            else:
                arcEC.ecWarning("'Connection' not in inrow.PARAMETERS. In RECORDID = "+str(inrow.RECORDID),103)
        else:
            arcEC.ecWarning("'Invalid domain value' not in inrow.REVIEWSTATUS. In RECORDID = "+str(inrow.RECORDID),102)
    else:
        arcEC.ecWarning("inrow.ORIGINCHECK != 'Domain Check'. In RECORDID = "+str(inrow.RECORDID),101)

del inrow, inrows