import arcpy
import sys

## Version 1.8 (8 functions) '130213/MaHvi

def SetMsg(msg, severity=0): # 0:Message, 1:Warning, 2:Error
    print msg
    try:
        for string in msg.split('\n'):
            string = ":) "+string
            if severity == 0:
                arcpy.AddMessage(string)
            elif severity == 1:
                arcpy.AddWarning(string)
            elif severity == 2:
                arcpy.AddError(string)
    except:
        pass
    
def ecMessage(strI,numI=0,severity=0):
    """ Neither message number nor severity is mandatory """
    if numI == 0:
        SetMsg("   Message: "+strI,0)
    else:
        SetMsg("   Message: "+str(numI)+" : "+strI,0)
    
def ecWarning(strI,numI,severity=0):
    """ Severity is not mandatory """
    SetMsg(" ! Warning: "+str(numI)+" : "+strI,1)
    
def ecError(strI,numI,severity):
    """ Severity > 0 causes program termination """
    SetMsg("!!!Error: "+str(numI)+" : "+strI,2)
    if severity > 0:
        sys.exit(numI)
    
def Describe2String(desIn):
    strReport = ""
    if hasattr(desIn, "Name"):
        strReport +="\n Name: "+desIn.Name
    if hasattr(desIn, "baseName"):
        strReport +="\n baseName: "+desIn.baseName
    if hasattr(desIn, "dataType"):
        strReport +="\n dataType: "+desIn.dataType 
    #if hasattr(desIn, "dataElementType"):
    #    strReport +="\n dataElementType: "+desIn.dataElementType
    if hasattr(desIn, "catalogPath"):
        strReport +="\n catalogPath: "+desIn.catalogPath
    if hasattr(desIn, "children"):
        strReport +="\n children: "+str(len(desIn.children))
    if hasattr(desIn, "fields"):
        strReport +="\n fields: "+str(len(desIn.fields))
        if len(desIn.fields) > 0:
            for fldX in desIn.fields:
                strReport +="\n  field: "+fldX.name
    if hasattr(desIn, "pludder"):
        strReport +="\n pludder: "+desIn.pludder
    return strReport

def Table2Ascii(tblIn):
    strReport = ""
    desIn = arcpy.Describe(tblIn)
    if hasattr(desIn, "dataType"):
        if desIn.dataType == "Table":
            strReport +="Table2Ascii"
            if hasattr(desIn, "fields"):
                strReport +="\n  fields: "+str(len(desIn.fields))
                if len(desIn.fields) > 0:
                    for fldX in desIn.fields:
                        strReport +="\n  field: "+fldX.name+" <"+fldX.type+">"
                    rows = arcpy.SearchCursor(tblIn)
                    numRows = 0
                    for rowX in rows:
                        strReport += "\n  "
                        for fldX in desIn.fields:
                            strReport += "|"+str(rowX.getValue(fldX.name))
                        numRows += 1
                    strReport += "\n  Row count: "+str(numRows)
    return strReport

def Table2Ascii_byFields(tblIn):
    strReport = ""
    desIn = arcpy.Describe(tblIn)
    if hasattr(desIn, "dataType"):
        if desIn.dataType == "Table":
            strReport +="Table2Ascii_ByFields"
            if hasattr(desIn, "fields"):
                strReport +="\n  fields: "+str(len(desIn.fields))
                if len(desIn.fields) > 0:
                    for fldX in desIn.fields:
                        rows = arcpy.SearchCursor(tblIn)
                        strReport +="\n  field: "+fldX.name+" <"+fldX.type+">"
                        strReport += "\n  "
                        for rowX in rows:
                            strReport += "|"+str(rowX.getValue(fldX.name))
                        rows.reset()
    return strReport

def Dict2String(dicIn):
    strReport = ""
    lstK = dicIn.keys()
    lstK.sort()
    for K in lstK:
        strReport += str(K)+" : "+str(dicIn[K])+"\n"
    return strReport


# Music that accompanied the coding of this script:
#   Deep Forest - Savana Dance