import arcpy
import cx_Oracle
import time
import datetime
import os

fileYear = raw_input('Enter current year: ')

start_time = time.time()
today = datetime.datetime.now()

print "Begin " + time.strftime("%m_%d_%Y_%H:%M:%S")

print "Verifying Directories..."

path = "D:\\Projects_2022\\RoadbedWidth_PavementRequest\\File_Build\\" + fileYear
if not os.path.exists(path):
    os.makedirs(path)

gdb = path + "\\Pavement_File_Build_" + fileYear + ".gdb"
grid_connection = 'app_texas_grid_rpt/semperfi@oracle-amazon-gridprod:1521/GRIDDB'

# lastYear = str(int(fileYear) - 1)
# lastYearHeader = path + "\\For_" + lastYear + "\\OnSystem_PVMT" + lastYear + ".shp"


arcpy.env.overwriteOutput = True
arcpy.CreateFileGDB_management(path, "OnSystem_PVMT_" + fileYear)


#NHS asset and gather GID's
NHS_tbl = gdb + "\\NHS_tbl"

print "Creating NHS table..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CreateTable_management(gdb, "NHS_tbl")
arcpy.MakeTableView_management(NHS_tbl, "NHS_tbl_vw")
arcpy.AddField_management("NHS_tbl_vw", "GID", "LONG", 10)
arcpy.AddField_management("NHS_tbl_vw", "RIA_RTE_ID", "TEXT", "", "", 10)
arcpy.AddField_management("NHS_tbl_vw", "FRM_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("NHS_tbl_vw", "TO_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("NHS_tbl_vw", "NHS", "SHORT", 1)

conn = cx_Oracle.connect(grid_connection)
cursor = conn.cursor()

query = """SELECT
    gridop.rte_defn_ln.rdbd_gmtry_ln_id AS GID,
    gridop.rte_defn_ln.rte_defn_ln_nm AS RIA_RTE_ID,
    gridop.asset_ln.asset_ln_begin_dfo_ms AS FRM_DFO,
    gridop.asset_ln.asset_ln_end_dfo_ms AS TO_DFO,
    gridop.nhs_type.nhs_type_cd AS NHS
FROM
    gridop.asset
    INNER JOIN gridop.asset_ln ON gridop.asset.asset_id = gridop.asset_ln.asset_id
    INNER JOIN gridop.rte_defn_ln ON gridop.asset.rdbd_gmtry_ln_id = gridop.rte_defn_ln.rdbd_gmtry_ln_id
    INNER JOIN gridop.rte ON gridop.rte.rte_id = gridop.rte_defn_ln.rte_id
    INNER JOIN gridop.nhs ON gridop.asset_ln.asset_id = gridop.nhs.asset_id
    INNER JOIN gridop.nhs_type ON gridop.nhs_type.nhs_type_id = gridop.nhs.nhs_type_id
WHERE
    gridop.rte_defn_ln.rte_defn_ln_prmry_flag = 1
AND
    gridop.nhs_type.nhs_type_cd > 0
AND
    gridop.rte.rte_prfx_type_id IN (1,7,27)
AND
    gridop.rte_defn_ln.rdbd_type_id = 5
ORDER BY GID, FRM_DFO"""

cur = cursor.execute(query)

GIDS = []

for row in cur:
    GID = row[0]
    RIA_RTE_ID = row[1]
    FRM_DFO = row[2]
    TO_DFO = row[3]
    NHS = row[4]

    insertRow = arcpy.da.InsertCursor("NHS_tbl_vw", ["GID", "RIA_RTE_ID", "FRM_DFO", "TO_DFO", "NHS"])
    insertRow.insertRow([GID, RIA_RTE_ID, FRM_DFO, TO_DFO, NHS])
    GIDS.append(GID)
    del insertRow
del cur

result = arcpy.GetCount_management("NHS_tbl_vw")
count = int(result.getOutput(0))
print count

arcpy.Delete_management("NHS_tbl_vw")
arcpy.Delete_management("in_memory")

#FC asset and gether GID's
FC_tbl = gdb + "\\FC_tbl"

print "Creating FC table..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CreateTable_management(gdb, "FC_tbl")
arcpy.MakeTableView_management(FC_tbl, "FC_tbl_vw")
arcpy.AddField_management("FC_tbl_vw", "GID", "LONG", 10)
arcpy.AddField_management("FC_tbl_vw", "RIA_RTE_ID", "TEXT", "", "", 10)
arcpy.AddField_management("FC_tbl_vw", "FRM_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("FC_tbl_vw", "TO_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("FC_tbl_vw", "FC", "SHORT", 1)

conn = cx_Oracle.connect(grid_connection)
cursor = conn.cursor()

query2 = """SELECT
    gridop.rte_defn_ln.rdbd_gmtry_ln_id AS GID,
    gridop.rte_defn_ln.rte_defn_ln_nm AS RIA_RTE_ID,
    gridop.asset_ln.asset_ln_begin_dfo_ms AS FRM_DFO,
    gridop.asset_ln.asset_ln_end_dfo_ms AS TO_DFO,
    gridop.funcl_sys_type.funcl_sys_type_cd AS FC
FROM
    gridop.asset
    INNER JOIN gridop.asset_ln ON gridop.asset.asset_id = gridop.asset_ln.asset_id
    INNER JOIN gridop.rte_defn_ln ON gridop.asset.rdbd_gmtry_ln_id = gridop.rte_defn_ln.rdbd_gmtry_ln_id
    INNER JOIN gridop.rte ON gridop.rte.rte_id = gridop.rte_defn_ln.rte_id
    INNER JOIN gridop.funcl_sys ON gridop.asset_ln.asset_id = gridop.funcl_sys.asset_id
    INNER JOIN gridop.funcl_sys_type ON gridop.funcl_sys_type.funcl_sys_type_id = gridop.funcl_sys.funcl_sys_type_id
WHERE
    gridop.rte_defn_ln.rte_defn_ln_prmry_flag = 1
AND
    gridop.funcl_sys_type.funcl_sys_type_cd <= 3
AND
    gridop.rte.rte_prfx_type_id IN (1,7,27)
AND
    gridop.rte_defn_ln.rdbd_type_id = 5
ORDER BY GID, FRM_DFO"""


cur = cursor.execute(query2)

for row in cur:
    GID = row[0]
    RIA_RTE_ID = row[1]
    FRM_DFO = row[2]
    TO_DFO = row[3]
    FC = row[4]

    insertRow = arcpy.da.InsertCursor("FC_tbl_vw", ["GID", "RIA_RTE_ID", "FRM_DFO", "TO_DFO", "FC"])
    insertRow.insertRow([GID, RIA_RTE_ID, FRM_DFO, TO_DFO, FC])
    GIDS.append(GID)
    del insertRow
del cur

result = arcpy.GetCount_management("FC_tbl_vw")
count = int(result.getOutput(0))
print count

arcpy.Delete_management("FC_tbl_vw")
arcpy.Delete_management("in_memory")


#HPMS Sample asset and gather GID's
HPMS_SAMP_tbl = gdb + "\\HPMS_SAMP_tbl"

print "Creating HPMS Sample table..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CreateTable_management(gdb, "HPMS_SAMP_tbl")
arcpy.MakeTableView_management(HPMS_SAMP_tbl, "HPMS_SAMP_tbl_vw")
arcpy.AddField_management("HPMS_SAMP_tbl_vw", "GID", "LONG", 10)
arcpy.AddField_management("HPMS_SAMP_tbl_vw", "RIA_RTE_ID", "TEXT", "", "", 10)
arcpy.AddField_management("HPMS_SAMP_tbl_vw", "FRM_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("HPMS_SAMP_tbl_vw", "TO_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("HPMS_SAMP_tbl_vw", "HPMS_ID", "LONG", 10)

conn = cx_Oracle.connect(grid_connection)
cursor = conn.cursor()

query3 = """SELECT
    gridop.rte_defn_ln.rdbd_gmtry_ln_id AS GID,
    gridop.rte_defn_ln.rte_defn_ln_nm AS RIA_RTE_ID,
    gridop.asset_ln.asset_ln_begin_dfo_ms AS FRM_DFO,
    gridop.asset_ln.asset_ln_end_dfo_ms AS TO_DFO,
    gridop.hpms_samp.hpms_samp_nbr AS HPMS_ID
FROM
    gridop.asset
    INNER JOIN gridop.asset_ln ON gridop.asset.asset_id = gridop.asset_ln.asset_id
    INNER JOIN gridop.rte_defn_ln ON gridop.asset.rdbd_gmtry_ln_id = gridop.rte_defn_ln.rdbd_gmtry_ln_id
    INNER JOIN gridop.rte ON gridop.rte.rte_id = gridop.rte_defn_ln.rte_id
    INNER JOIN gridop.hpms_samp ON gridop.asset_ln.asset_id = gridop.hpms_samp.asset_id
WHERE
    gridop.rte_defn_ln.rte_defn_ln_prmry_flag = 1
AND
    gridop.rte.rte_prfx_type_id IN (1,7,27)
AND
    gridop.rte_defn_ln.rdbd_type_id = 5
ORDER BY GID, FRM_DFO"""


cur = cursor.execute(query3)

for row in cur:
    GID = row[0]
    RIA_RTE_ID = row[1]
    FRM_DFO = row[2]
    TO_DFO = row[3]
    HPMS_ID = row[4]

    insertRow = arcpy.da.InsertCursor("HPMS_SAMP_tbl_vw", ["GID", "RIA_RTE_ID", "FRM_DFO", "TO_DFO", "HPMS_ID"])
    insertRow.insertRow([GID, RIA_RTE_ID, FRM_DFO, TO_DFO, HPMS_ID])
    GIDS.append(GID)
    del insertRow
del cur

result = arcpy.GetCount_management("HPMS_SAMP_tbl_vw")
count = int(result.getOutput(0))
print count

arcpy.Delete_management("HPMS_SAMP_tbl_vw")
arcpy.Delete_management("in_memory")

#SETUP Chunkify for list of GID's over 1000 records
roundNum = int(round((len(GIDS) / 1000))) + 1


def chunkify(lst, n):
    global chunks
    chunks = [lst[i::n] for i in xrange(n)]


chunkify(GIDS, roundNum)

GIDstring = ""
cnt = 1
for chunk in chunks:
    if cnt == 1 and len(chunks) == 1:
        GIDstring = """ AND GRIDOP.RTE_DEFN_LN.RDBD_GMTRY_LN_ID IN {0}""".format(
            str(str(chunk).replace("[", "(")).replace("]", ")"))

    elif cnt == 1 and len(chunks) > 1:
        GIDstring = """ AND (GRIDOP.RTE_DEFN_LN.RDBD_GMTRY_LN_ID IN {0}""".format(
            str(str(chunk).replace("[", "(")).replace("]", ")"))

    elif cnt > 1 and cnt < len(chunks):
        GIDstring += """   OR GRIDOP.RTE_DEFN_LN.RDBD_GMTRY_LN_ID IN {0}""".format(
            str(str(chunk).replace("[", "(")).replace("]", ")"))

    elif cnt > 1 and cnt == len(chunks):
        GIDstring += """   OR GRIDOP.RTE_DEFN_LN.RDBD_GMTRY_LN_ID IN {0} ) """.format(
            str(str(chunk).replace("[", "(")).replace("]", ")"))
    cnt += 1



#Roadway Status asset for proposed roads
RDWY_STAT_tbl = gdb + "\\RDWY_STAT_tbl"

print "Creating Roadway Status table..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CreateTable_management(gdb, "RDWY_STAT_tbl")
arcpy.MakeTableView_management(RDWY_STAT_tbl, "RDWY_STAT_tbl_vw")
arcpy.AddField_management("RDWY_STAT_tbl_vw", "GID", "LONG", 10)
arcpy.AddField_management("RDWY_STAT_tbl_vw", "FRM_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("RDWY_STAT_tbl_vw", "TO_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("RDWY_STAT_tbl_vw", "STAT_ID", "SHORT", 1)

conn = cx_Oracle.connect(grid_connection)
cursor = conn.cursor()

query4 = """SELECT
    gridop.rte_defn_ln.rdbd_gmtry_ln_id AS GID,
    gridop.asset_ln.asset_ln_begin_dfo_ms AS FRM_DFO,
    gridop.asset_ln.asset_ln_end_dfo_ms AS TO_DFO,
    gridop.rdway_stat.rdway_stat_type_id AS STAT_ID
FROM
    gridop.asset
    INNER JOIN gridop.asset_ln ON gridop.asset.asset_id = gridop.asset_ln.asset_id
    INNER JOIN gridop.rte_defn_ln ON gridop.asset.rdbd_gmtry_ln_id = gridop.rte_defn_ln.rdbd_gmtry_ln_id
    INNER JOIN gridop.rdway_stat ON gridop.asset_ln.asset_id = gridop.rdway_stat.asset_id
WHERE
    gridop.rte_defn_ln.rte_defn_ln_prmry_flag = 1
AND
    gridop.rdway_stat.rdway_stat_type_id = 1
"""

query4 = query4 + GIDstring
cur = cursor.execute(query4)

for row in cur:
    GID = row[0]
    FRM_DFO = row[1]
    TO_DFO = row[2]
    STAT_ID = row[3]

    insertRow = arcpy.da.InsertCursor("RDWY_STAT_tbl_vw", ["GID", "FRM_DFO", "TO_DFO", "STAT_ID"])
    insertRow.insertRow([GID, FRM_DFO, TO_DFO, STAT_ID])
    del insertRow
del cur

result = arcpy.GetCount_management("RDWY_STAT_tbl_vw")
count = int(result.getOutput(0))
print count

arcpy.Delete_management("RDWY_STAT_tbl_vw")
arcpy.Delete_management("in_memory")


# County Number asset
CNTY_NBR_tbl = gdb + "\\CNTY_NBR_tbl"

print "Creating County Number table..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CreateTable_management(gdb, "CNTY_NBR_tbl")
arcpy.MakeTableView_management(CNTY_NBR_tbl, "CNTY_NBR_tbl_vw")
arcpy.AddField_management("CNTY_NBR_tbl_vw", "GID", "LONG", 10)
arcpy.AddField_management("CNTY_NBR_tbl_vw", "FRM_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("CNTY_NBR_tbl_vw", "TO_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("CNTY_NBR_tbl_vw", "COUNTY", "SHORT", 3)

conn = cx_Oracle.connect(grid_connection)
cursor = conn.cursor()

query5 = """SELECT
    gridop.rte_defn_ln.rdbd_gmtry_ln_id AS GID,
    gridop.asset_ln.asset_ln_begin_dfo_ms AS FRM_DFO,
    gridop.asset_ln.asset_ln_end_dfo_ms AS TO_DFO,
    gridop.rte_cnty.cnty_type_nbr AS COUNTY
FROM
    gridop.asset
    INNER JOIN gridop.asset_ln ON gridop.asset.asset_id = gridop.asset_ln.asset_id
    INNER JOIN gridop.rte_defn_ln ON gridop.asset.rdbd_gmtry_ln_id = gridop.rte_defn_ln.rdbd_gmtry_ln_id
    INNER JOIN gridop.rte_cnty ON gridop.asset_ln.asset_id = gridop.rte_cnty.asset_id
WHERE
    gridop.rte_defn_ln.rte_defn_ln_prmry_flag = 1
"""

query5 = query5 + GIDstring

cur = cursor.execute(query5)

for row in cur:
    GID = row[0]
    FRM_DFO = row[1]
    TO_DFO = row[2]
    COUNTY = row[3]

    insertRow = arcpy.da.InsertCursor("CNTY_NBR_tbl_vw", ["GID", "FRM_DFO", "TO_DFO", "COUNTY"])
    insertRow.insertRow([GID, FRM_DFO, TO_DFO, COUNTY])
    del insertRow
del cur

result = arcpy.GetCount_management("CNTY_NBR_tbl_vw")
count = int(result.getOutput(0))
print count

arcpy.Delete_management("CNTY_NBR_tbl_vw")
arcpy.Delete_management("in_memory")


#District asset
DIST_NBR_tbl = gdb + "\\DIST_NBR_tbl"

print "Creating District Number table..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CreateTable_management(gdb, "DIST_NBR_tbl")
arcpy.MakeTableView_management(DIST_NBR_tbl, "DIST_NBR_tbl_vw")
arcpy.AddField_management("DIST_NBR_tbl_vw", "GID", "LONG", 10)
arcpy.AddField_management("DIST_NBR_tbl_vw", "FRM_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("DIST_NBR_tbl_vw", "TO_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("DIST_NBR_tbl_vw", "DISTRICT", "SHORT", 2)

conn = cx_Oracle.connect(grid_connection)
cursor = conn.cursor()

query6 = """SELECT
    gridop.rte_defn_ln.rdbd_gmtry_ln_id AS GID,
    gridop.asset_ln.asset_ln_begin_dfo_ms AS FRM_DFO,
    gridop.asset_ln.asset_ln_end_dfo_ms AS TO_DFO,
    gridop.rte_dist.dist_type_nbr AS DISTRICT
FROM
    gridop.asset
    INNER JOIN gridop.asset_ln ON gridop.asset.asset_id = gridop.asset_ln.asset_id
    INNER JOIN gridop.rte_defn_ln ON gridop.asset.rdbd_gmtry_ln_id = gridop.rte_defn_ln.rdbd_gmtry_ln_id
    INNER JOIN gridop.rte_dist ON gridop.asset_ln.asset_id = gridop.rte_dist.asset_id
WHERE
    gridop.rte_defn_ln.rte_defn_ln_prmry_flag = 1
"""

query6 = query6 + GIDstring

cur = cursor.execute(query6)

for row in cur:
    GID = row[0]
    FRM_DFO = row[1]
    TO_DFO = row[2]
    DISTRICT = row[3]

    insertRow = arcpy.da.InsertCursor("DIST_NBR_tbl_vw", ["GID", "FRM_DFO", "TO_DFO", "DISTRICT"])
    insertRow.insertRow([GID, FRM_DFO, TO_DFO, DISTRICT])
    del insertRow
del cur

result = arcpy.GetCount_management("DIST_NBR_tbl_vw")
count = int(result.getOutput(0))
print count

arcpy.Delete_management("DIST_NBR_tbl_vw")
arcpy.Delete_management("in_memory")


#Number of Lanes asset
NUM_LANES_tbl = gdb + "\\NUM_LANES_tbl"

print "Creating Number of Lanes table..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CreateTable_management(gdb, "NUM_LANES_tbl")
arcpy.MakeTableView_management(NUM_LANES_tbl, "NUM_LANES_tbl_vw")
arcpy.AddField_management("NUM_LANES_tbl_vw", "GID", "LONG", 10)
arcpy.AddField_management("NUM_LANES_tbl_vw", "FRM_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("NUM_LANES_tbl_vw", "TO_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("NUM_LANES_tbl_vw", "NUM_LANES", "SHORT", 2)

conn = cx_Oracle.connect(grid_connection)
cursor = conn.cursor()

query7 = """SELECT
    gridop.rte_defn_ln.rdbd_gmtry_ln_id AS GID,
    gridop.asset_ln.asset_ln_begin_dfo_ms AS FRM_DFO,
    gridop.asset_ln.asset_ln_end_dfo_ms AS TO_DFO,
    gridop.nbr_thru_lane.nbr_thru_lane_cnt AS NUM_LANES
FROM
    gridop.asset
    INNER JOIN gridop.asset_ln ON gridop.asset.asset_id = gridop.asset_ln.asset_id
    INNER JOIN gridop.rte_defn_ln ON gridop.asset.rdbd_gmtry_ln_id = gridop.rte_defn_ln.rdbd_gmtry_ln_id
    INNER JOIN gridop.nbr_thru_lane ON gridop.asset_ln.asset_id = gridop.nbr_thru_lane.asset_id
WHERE
    gridop.rte_defn_ln.rte_defn_ln_prmry_flag = 1
"""

query7 = query7 + GIDstring

cur = cursor.execute(query7)

for row in cur:
    GID = row[0]
    FRM_DFO = row[1]
    TO_DFO = row[2]
    NUM_LANES = row[3]

    insertRow = arcpy.da.InsertCursor("NUM_LANES_tbl_vw", ["GID", "FRM_DFO", "TO_DFO", "NUM_LANES"])
    insertRow.insertRow([GID, FRM_DFO, TO_DFO, NUM_LANES])
    del insertRow
del cur

result = arcpy.GetCount_management("NUM_LANES_tbl_vw")
count = int(result.getOutput(0))
print count

arcpy.Delete_management("NUM_LANES_tbl_vw")
arcpy.Delete_management("in_memory")


#Max Speed Limit asset
SPD_MAX_tbl = gdb + "\\SPD_MAX_tbl"

print "Creating Max Speed table..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CreateTable_management(gdb, "SPD_MAX_tbl")
arcpy.MakeTableView_management(SPD_MAX_tbl, "SPD_MAX_tbl_vw")
arcpy.AddField_management("SPD_MAX_tbl_vw", "GID", "LONG", 10)
arcpy.AddField_management("SPD_MAX_tbl_vw", "FRM_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("SPD_MAX_tbl_vw", "TO_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("SPD_MAX_tbl_vw", "SPD_MAX", "SHORT", 2)

conn = cx_Oracle.connect(grid_connection)
cursor = conn.cursor()

query8 = """SELECT
    gridop.rte_defn_ln.rdbd_gmtry_ln_id AS GID,
    gridop.asset_ln.asset_ln_begin_dfo_ms AS FRM_DFO,
    gridop.asset_ln.asset_ln_end_dfo_ms AS TO_DFO,
    gridop.max_spd_lmt.max_spd_lmt_ms AS SPD_MAX
FROM
    gridop.asset
    INNER JOIN gridop.asset_ln ON gridop.asset.asset_id = gridop.asset_ln.asset_id
    INNER JOIN gridop.rte_defn_ln ON gridop.asset.rdbd_gmtry_ln_id = gridop.rte_defn_ln.rdbd_gmtry_ln_id
    INNER JOIN gridop.max_spd_lmt ON gridop.asset_ln.asset_id = gridop.max_spd_lmt.asset_id
WHERE
    gridop.rte_defn_ln.rte_defn_ln_prmry_flag = 1
"""

query8 = query8 + GIDstring

cur = cursor.execute(query8)

for row in cur:
    GID = row[0]
    FRM_DFO = row[1]
    TO_DFO = row[2]
    SPD_MAX = row[3]

    insertRow = arcpy.da.InsertCursor("SPD_MAX_tbl_vw", ["GID", "FRM_DFO", "TO_DFO", "SPD_MAX"])
    insertRow.insertRow([GID, FRM_DFO, TO_DFO, SPD_MAX])
    del insertRow
del cur

result = arcpy.GetCount_management("SPD_MAX_tbl_vw")
count = int(result.getOutput(0))
print count

arcpy.Delete_management("SPD_MAX_tbl_vw")
arcpy.Delete_management("in_memory")


#Surface Type asset
SRF_TYPE_tbl = gdb + "\\SRF_TYPE_tbl"

print "Creating Surface Type table..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CreateTable_management(gdb, "SRF_TYPE_tbl")
arcpy.MakeTableView_management(SRF_TYPE_tbl, "SRF_TYPE_tbl_vw")
arcpy.AddField_management("SRF_TYPE_tbl_vw", "GID", "LONG", 10)
arcpy.AddField_management("SRF_TYPE_tbl_vw", "FRM_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("SRF_TYPE_tbl_vw", "TO_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("SRF_TYPE_tbl_vw", "SRF_TYPE", "SHORT", 2)
arcpy.AddField_management("SRF_TYPE_tbl_vw", "BROAD_CODE", "TEXT", "", "", 1)
arcpy.AddField_management("SRF_TYPE_tbl_vw", "FHWA_SRF", "SHORT", 2)

conn = cx_Oracle.connect(grid_connection)
cursor = conn.cursor()

query9 = """SELECT
    gridop.rte_defn_ln.rdbd_gmtry_ln_id AS GID,
    gridop.asset_ln.asset_ln_begin_dfo_ms AS FRM_DFO,
    gridop.asset_ln.asset_ln_end_dfo_ms AS TO_DFO,
    gridop.srfc_type.srfc_type_cd AS SRF_TYPE,
    DECODE(gridop.srfc_type.srfc_type_cd,
        1,'C', 2,'J',  3,'J',  4,'A',  5,'A',  6,'A',  7,'A',
        8,'J', 9,'A', 10,'A', 11,'X', 12,'X', 13,'X', 99,'X', 'J') AS BROAD_CODE,
    DECODE(gridop.srfc_type.srfc_type_cd,
        1,5,  2,4,   3,3,   4,6,    5,6,    6,6,   7,8,
        8,9,  9,7,  10,2,  11,11,  12,11,  13,1,  99,11,  10) AS FHWA_SURF
FROM
    gridop.asset
    INNER JOIN gridop.asset_ln ON gridop.asset.asset_id = gridop.asset_ln.asset_id
    INNER JOIN gridop.rte_defn_ln ON gridop.asset.rdbd_gmtry_ln_id = gridop.rte_defn_ln.rdbd_gmtry_ln_id
    INNER JOIN gridop.rdbd_srfc ON gridop.asset_ln.asset_id = gridop.rdbd_srfc.asset_id
    INNER JOIN gridop.srfc_type ON gridop.srfc_type.srfc_type_id = gridop.rdbd_srfc.srfc_type_id
WHERE
    gridop.rte_defn_ln.rte_defn_ln_prmry_flag = 1
"""

query9 = query9 + GIDstring

cur = cursor.execute(query9)

for row in cur:
    GID = row[0]
    FRM_DFO = row[1]
    TO_DFO = row[2]
    SRF_TYPE = row[3]
    BROAD_CODE = row[4]
    FHWA_SRF = row[5]

    insertRow = arcpy.da.InsertCursor("SRF_TYPE_tbl_vw", ["GID", "FRM_DFO", "TO_DFO", "SRF_TYPE", "BROAD_CODE", "FHWA_SRF"])
    insertRow.insertRow([GID, FRM_DFO, TO_DFO, SRF_TYPE, BROAD_CODE, FHWA_SRF])
    del insertRow
del cur

result = arcpy.GetCount_management("SRF_TYPE_tbl_vw")
count = int(result.getOutput(0))
print count

arcpy.Delete_management("SRF_TYPE_tbl_vw")
arcpy.Delete_management("in_memory")

#Owner asset
OWNER_tbl = gdb + "\\OWNER_tbl"

print "Creating Owner table..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CreateTable_management(gdb, "OWNER_tbl")
arcpy.MakeTableView_management(OWNER_tbl, "OWNER_tbl_vw")
arcpy.AddField_management("OWNER_tbl_vw", "GID", "LONG", 10)
arcpy.AddField_management("OWNER_tbl_vw", "FRM_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("OWNER_tbl_vw", "TO_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("OWNER_tbl_vw", "OWNER", "SHORT", 2)
arcpy.AddField_management("OWNER_tbl_vw", "OWN_SPECIF", "TEXT", "", "", 30)

conn = cx_Oracle.connect(grid_connection)
cursor = conn.cursor()

query10 = """SELECT
    gridop.rte_defn_ln.rdbd_gmtry_ln_id AS GID,
    gridop.asset_ln.asset_ln_begin_dfo_ms AS FRM_DFO,
    gridop.asset_ln.asset_ln_end_dfo_ms AS TO_DFO,
    gridop.ownr.ownr_type_id AS OWNER,
    DECODE(gridop.ownr.ownr_type_id,
        1,'State Highway Agency', 2,'County', 4,'City', 5,'Private Toll', 6,'Local Toll Authority', 7,'Other Federal Agency',
        8,'Bureau of Indian Affairs', 9,'Bureau of Fish and Wildlife', 10,'U.S. Forest Service', 11,'National Park Service',
        12,'Bureau of Reclamation', 13,'Corp of Engineers', 14,'Navy/Marines', 15,'Army', 16,'Regional Mobility Authority',
        17,'Other', 18,'Unknown') AS OWN_SPECIF
FROM
    gridop.asset
    INNER JOIN gridop.asset_ln ON gridop.asset.asset_id = gridop.asset_ln.asset_id
    INNER JOIN gridop.rte_defn_ln ON gridop.asset.rdbd_gmtry_ln_id = gridop.rte_defn_ln.rdbd_gmtry_ln_id
    INNER JOIN gridop.ownr ON gridop.asset_ln.asset_id = gridop.ownr.asset_id
WHERE
    gridop.rte_defn_ln.rte_defn_ln_prmry_flag = 1
"""

query10 = query10 + GIDstring

cur = cursor.execute(query10)

for row in cur:
    GID = row[0]
    FRM_DFO = row[1]
    TO_DFO = row[2]
    OWNER = row[3]
    OWN_SPECIF = row[4]

    insertRow = arcpy.da.InsertCursor("OWNER_tbl_vw", ["GID", "FRM_DFO", "TO_DFO", "OWNER", "OWN_SPECIF"])
    insertRow.insertRow([GID, FRM_DFO, TO_DFO, OWNER, OWN_SPECIF])
    del insertRow
del cur

result = arcpy.GetCount_management("OWNER_tbl_vw")
count = int(result.getOutput(0))
print count

arcpy.Delete_management("OWNER_tbl_vw")
arcpy.Delete_management("in_memory")


#Roadway Maintenance asset
RDWAY_MAIN_tbl = gdb + "\\RDWAY_MAIN_tbl"

print "Creating Maintenance table..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CreateTable_management(gdb, "RDWAY_MAIN_tbl")
arcpy.MakeTableView_management(RDWAY_MAIN_tbl, "RDWAY_MAIN_tbl_vw")
arcpy.AddField_management("RDWAY_MAIN_tbl_vw", "GID", "LONG", 10)
arcpy.AddField_management("RDWAY_MAIN_tbl_vw", "FRM_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("RDWAY_MAIN_tbl_vw", "TO_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("RDWAY_MAIN_tbl_vw", "RDWAY_MAIN", "SHORT", 2)
arcpy.AddField_management("RDWAY_MAIN_tbl_vw", "MAINT_SPEC", "TEXT", "", "", 30)

conn = cx_Oracle.connect(grid_connection)
cursor = conn.cursor()

query11 = """SELECT
    gridop.rte_defn_ln.rdbd_gmtry_ln_id AS GID,
    gridop.asset_ln.asset_ln_begin_dfo_ms AS FRM_DFO,
    gridop.asset_ln.asset_ln_end_dfo_ms AS TO_DFO,
    gridop.rdway_maint_agcy.maint_agcy_type_id AS RDWAY_MAIN,
    DECODE(gridop.rdway_maint_agcy.maint_agcy_type_id,
        1,'State Highway Agency', 2,'County', 4,'City', 5,'Private Toll', 6,'Local Toll Authority', 7,'Other Federal Agency',
        8,'Bureau of Indian Affairs', 9,'Bureau of Fish and Wildlife', 10,'U.S. Forest Service', 11,'National Park Service',
        12,'Bureau of Reclamation', 13,'Corp of Engineers', 14,'Navy/Marines', 15,'Army', 16,'Regional Mobility Authority',
        17,'Other', 18,'Unknown') AS MAINT_SPEC
FROM
    gridop.asset
    INNER JOIN gridop.asset_ln ON gridop.asset.asset_id = gridop.asset_ln.asset_id
    INNER JOIN gridop.rte_defn_ln ON gridop.asset.rdbd_gmtry_ln_id = gridop.rte_defn_ln.rdbd_gmtry_ln_id
    INNER JOIN gridop.rdway_maint_agcy ON gridop.asset_ln.asset_id = gridop.rdway_maint_agcy.asset_id
WHERE
    gridop.rte_defn_ln.rte_defn_ln_prmry_flag = 1
"""

query11 = query11 + GIDstring

cur = cursor.execute(query11)

for row in cur:
    GID = row[0]
    FRM_DFO = row[1]
    TO_DFO = row[2]
    RDWAY_MAIN = row[3]
    MAINT_SPEC = row[4]

    insertRow = arcpy.da.InsertCursor("RDWAY_MAIN_tbl_vw", ["GID", "FRM_DFO", "TO_DFO", "RDWAY_MAIN", "MAINT_SPEC"])
    insertRow.insertRow([GID, FRM_DFO, TO_DFO, RDWAY_MAIN, MAINT_SPEC])
    del insertRow
del cur

result = arcpy.GetCount_management("RDWAY_MAIN_tbl_vw")
count = int(result.getOutput(0))
print count

arcpy.Delete_management("RDWAY_MAIN_tbl_vw")
arcpy.Delete_management("in_memory")


#Traffic assets
TRAFFIC_tbl = gdb + "\\TRAFFIC_tbl"

print "Creating Traffic table..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CreateTable_management(gdb, "TRAFFIC_tbl")
arcpy.MakeTableView_management(TRAFFIC_tbl, "TRAFFIC_tbl_vw")
arcpy.AddField_management("TRAFFIC_tbl_vw", "GID", "LONG", 10)
arcpy.AddField_management("TRAFFIC_tbl_vw", "FRM_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("TRAFFIC_tbl_vw", "TO_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("TRAFFIC_tbl_vw", "ADT_YEAR", "SHORT", 4)
arcpy.AddField_management("TRAFFIC_tbl_vw", "ADT_CUR", "LONG", 6)
arcpy.AddField_management("TRAFFIC_tbl_vw", "AADT_TRUCK", "SHORT", 5)
arcpy.AddField_management("TRAFFIC_tbl_vw", "AADT_SINGL", "SHORT", 5)
arcpy.AddField_management("TRAFFIC_tbl_vw", "AADT_COMBI", "SHORT", 5)

conn = cx_Oracle.connect(grid_connection)
cursor = conn.cursor()

query12 = """SELECT
    gridop.rte_defn_ln.rdbd_gmtry_ln_id AS GID,
    gridop.asset_ln.asset_ln_begin_dfo_ms AS FRM_DFO,
    gridop.asset_ln.asset_ln_end_dfo_ms AS TO_DFO,
    gridop.aadt.aadt_curr_yr_dt AS ADT_YEAR,
    gridop.aadt.aadt_curr_ms AS ADT_CUR,
    gridop.aadt.aadt_trk_qty AS AADT_TRUCK,
    gridop.aadt.aadt_sngl_trk_qty AADT_SINGL,
    gridop.aadt.aadt_combn_trk_qty AS AADT_COMBI
FROM
    gridop.asset
    INNER JOIN gridop.asset_ln ON gridop.asset.asset_id = gridop.asset_ln.asset_id
    INNER JOIN gridop.rte_defn_ln ON gridop.asset.rdbd_gmtry_ln_id = gridop.rte_defn_ln.rdbd_gmtry_ln_id
    INNER JOIN gridop.aadt ON gridop.asset_ln.asset_id = gridop.aadt.asset_id
WHERE
    gridop.rte_defn_ln.rte_defn_ln_prmry_flag = 1
"""

query12 = query12 + GIDstring

cur = cursor.execute(query12)

for row in cur:
    GID = row[0]
    FRM_DFO = row[1]
    TO_DFO = row[2]
    ADT_YEAR = row[3]
    ADT_CUR = row[4]
    AADT_TRUCK = row[5]
    AADT_SINGL = row[6]
    AADT_COMBI = row[7]

    insertRow = arcpy.da.InsertCursor("TRAFFIC_tbl_vw", ["GID", "FRM_DFO", "TO_DFO", "ADT_YEAR", "ADT_CUR", "AADT_TRUCK", "AADT_SINGL", "AADT_COMBI"])
    insertRow.insertRow([GID, FRM_DFO, TO_DFO, ADT_YEAR, ADT_CUR, AADT_TRUCK, AADT_SINGL, AADT_COMBI])
    del insertRow
del cur

result = arcpy.GetCount_management("TRAFFIC_tbl_vw")
count = int(result.getOutput(0))
print count

arcpy.Delete_management("TRAFFIC_tbl_vw")
arcpy.Delete_management("in_memory")


#RTE URB asset
RTE_URB_tbl = gdb + "\\RTE_URB_tbl"

print "Creating RTE URB table..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CreateTable_management(gdb, "RTE_URB_tbl")
arcpy.MakeTableView_management(RTE_URB_tbl, "RTE_URB_tbl_vw")
arcpy.AddField_management("RTE_URB_tbl_vw", "GID", "LONG", 10)
arcpy.AddField_management("RTE_URB_tbl_vw", "FRM_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("RTE_URB_tbl_vw", "TO_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("RTE_URB_tbl_vw", "RU", "SHORT", 1)
arcpy.AddField_management("RTE_URB_tbl_vw", "UAN_HPMS", "TEXT","","", 5)
arcpy.AddField_management("RTE_URB_tbl_vw", "URBAN_NAME", "TEXT", "", "", 30)

conn = cx_Oracle.connect(grid_connection)
cursor = conn.cursor()

query13 = """SELECT
    gridop.rte_defn_ln.rdbd_gmtry_ln_id AS GID,
    gridop.asset_ln.asset_ln_begin_dfo_ms AS FRM_DFO,
    gridop.asset_ln.asset_ln_end_dfo_ms AS TO_DFO,
    gridop.rte_urb.urb_pop_type_id AS RU,
    gridop.rte_urb.urb_type_nbr AS UAN_HPMS,
    gridop.urb_type.urb_type_nm AS URBAN_NAME
FROM
    gridop.asset
    INNER JOIN gridop.asset_ln ON gridop.asset.asset_id = gridop.asset_ln.asset_id
    INNER JOIN gridop.rte_defn_ln ON gridop.asset.rdbd_gmtry_ln_id = gridop.rte_defn_ln.rdbd_gmtry_ln_id
    INNER JOIN gridop.rte_urb ON gridop.asset_ln.asset_id = gridop.rte_urb.asset_id
    INNER JOIN gridop.urb_type ON gridop.urb_type.urb_type_nbr = gridop.rte_urb.urb_type_nbr
WHERE
    gridop.rte_defn_ln.rte_defn_ln_prmry_flag = 1
"""

query13 = query13 + GIDstring

cur = cursor.execute(query13)

for row in cur:
    GID = row[0]
    FRM_DFO = row[1]
    TO_DFO = row[2]
    RU = row[3]
    UAN_HPMS = row[4]
    URBAN_NAME = row[5]

    insertRow = arcpy.da.InsertCursor("RTE_URB_tbl_vw", ["GID", "FRM_DFO", "TO_DFO", "RU", "UAN_HPMS", "URBAN_NAME"])
    insertRow.insertRow([GID, FRM_DFO, TO_DFO, RU, UAN_HPMS, URBAN_NAME])
    del insertRow
del cur

result = arcpy.GetCount_management("RTE_URB_tbl_vw")
count = int(result.getOutput(0))
print count

arcpy.Delete_management("RTE_URB_tbl_vw")
arcpy.Delete_management("in_memory")


#RTE CITY asset
RTE_CITY_tbl = gdb + "\\RTE_CITY_tbl"

print "Creating RTE CITY table..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CreateTable_management(gdb, "RTE_CITY_tbl")
arcpy.MakeTableView_management(RTE_CITY_tbl, "RTE_CITY_tbl_vw")
arcpy.AddField_management("RTE_CITY_tbl_vw", "GID", "LONG", 10)
arcpy.AddField_management("RTE_CITY_tbl_vw", "FRM_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("RTE_CITY_tbl_vw", "TO_DFO", "DOUBLE", 8, 3)
arcpy.AddField_management("RTE_CITY_tbl_vw", "CITY", "LONG", 5)
arcpy.AddField_management("RTE_CITY_tbl_vw", "CITY_NAME", "TEXT", "", "", 25)

conn = cx_Oracle.connect(grid_connection)
cursor = conn.cursor()

query14 = """SELECT
    gridop.rte_defn_ln.rdbd_gmtry_ln_id AS GID,
    gridop.asset_ln.asset_ln_begin_dfo_ms AS FRM_DFO,
    gridop.asset_ln.asset_ln_end_dfo_ms AS TO_DFO,
    gridop.rte_city.city_type_nbr AS CITY,
    gridop.city_type.city_type_nm AS CITY_NAME
FROM
    gridop.asset
    INNER JOIN gridop.asset_ln ON gridop.asset.asset_id = gridop.asset_ln.asset_id
    INNER JOIN gridop.rte_defn_ln ON gridop.asset.rdbd_gmtry_ln_id = gridop.rte_defn_ln.rdbd_gmtry_ln_id
    INNER JOIN gridop.rte_city ON gridop.asset_ln.asset_id = gridop.rte_city.asset_id
    INNER JOIN gridop.city_type ON gridop.city_type.city_type_nbr = gridop.rte_city.city_type_nbr
WHERE
    gridop.rte_defn_ln.rte_defn_ln_prmry_flag = 1
"""

query14 = query14 + GIDstring

cur = cursor.execute(query14)

for row in cur:
    GID = row[0]
    FRM_DFO = row[1]
    TO_DFO = row[2]
    CITY = row[3]
    CITY_NAME = row[4]

    insertRow = arcpy.da.InsertCursor("RTE_CITY_tbl_vw", ["GID", "FRM_DFO", "TO_DFO", "CITY", "CITY_NAME"])
    insertRow.insertRow([GID, FRM_DFO, TO_DFO, CITY, CITY_NAME])
    del insertRow
del cur

result = arcpy.GetCount_management("RTE_CITY_tbl_vw")
count = int(result.getOutput(0))
print count

arcpy.Delete_management("RTE_CITY_tbl_vw")
arcpy.Delete_management("in_memory")



#Create geometry
GRID = "Database Connections\gridprod.sde"
sr = arcpy.SpatialReference(4269)
RDBD_GMTRY = gdb + "\\RDBD_GMTRY"


query15 = """SELECT
GRIDOP.RDBD_GMTRY_LN.RDBD_GMTRY_LN_ID,
GRIDOP.RDBD_GMTRY_LN.RDBD_GMTRY_LN_ID AS GID,
GRIDOP.RDBD_GMTRY_LN.RDBD_GMTRY_LN_MS AS SEG_LEN,
GRIDOP.RDBD_GMTRY_LN.SHAPE
FROM GRIDOP.RDBD_GMTRY_LN"""

GIDstring2 = ""
cnt = 1
for chunk in chunks:
    if cnt == 1:
        GIDstring2 = """ WHERE GRIDOP.RDBD_GMTRY_LN.RDBD_GMTRY_LN_ID IN {0}""".format(
            str(str(chunk).replace("[","(")).replace("]",")"))
    else:
        GIDstring2 += """   OR GRIDOP.RDBD_GMTRY_LN.RDBD_GMTRY_LN_ID IN {0}""".format(
            str(str(chunk).replace("[", "(")).replace("]", ")"))

    cnt += 1

query15 = query15 + GIDstring2

print "Copying Roadbed Geometry data..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.MakeQueryLayer_management(GRID, "RDBD_GMTRY", query15,"","","",sr)

result = arcpy.GetCount_management("RDBD_GMTRY")
count = int(result.getOutput(0))
print count


print "Creating Roadbed Geometry feature class..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CopyFeatures_management("RDBD_GMTRY", gdb + "\\RDBD_GMTRY")


# Overlay of all assets

Overlay_NHS_FC = gdb + "\\Overlay_NHS_FC"
Overlay_NHS_FC_HPMS = gdb + "\\Overlay_NHS_FC_HPMS"
Overlay_NHS_FC_HPMS_STAT = gdb + "\\Overlay_NHS_FC_HPMS_STAT"
Overlay_NHS_FC_HPMS_STAT_CNTY = gdb + "\\Overlay_NHS_FC_HPMS_STAT_CNTY"
Overlay_NHS_FC_HPMS_STAT_CNTY_DIST = gdb + "\\Overlay_NHS_FC_HPMS_STAT_CNTY_DIST"
Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES = gdb + "\\Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES"
Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD = gdb + "\\Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD"
Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF = gdb + "\\Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF"
Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN = gdb + "\\Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN"
Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT = gdb + "\\Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT"
Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF = gdb + "\\Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF"
Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB = gdb + "\\Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB"
Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_CITY = gdb + "\\Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_CITY"

# Overlay_NHS_FC_CNTY = gdb + "\\Overlay_NHS_FC_CNTY"
# Overlay_NHS_FC_CNTY_DIST = gdb + "\\Overlay_NHS_FC_CNTY_DIST"
# Overlay_NHS_FC_CNTY_DIST_RDWAY_STAT = gdb + "\\Overlay_NHS_FC_CNTY_DIST_RDWAY_STAT"
# Overlay_NHS_FC_CNTY_DIST_RDWAY_STAT_GIDs = gdb + "\\Overlay_NHS_FC_CNTY_DIST_RDWAY_STAT_GIDs"

# Overlay_NHS_FC_RDWY_STAT_GIDs = gdb + "\\Overlay_NHS_FC_RDWY_STAT_GIDs"
# lastYearHeader_dis = gdb + "\\lastYearHeader_dis"
# lastYearHeader_tbl = gdb + "\\lastYearHeader_tbl"
# Matched_GIDs = gdb + "\\Matched_GIDs"
Overlay_dissolve = gdb + "\\Overlay_dissolve"
OffSystem_HEADER = gdb + "\\OffSystem_HEADER_" + str(fileYear)


arcpy.MakeTableView_management(NHS_tbl, "NHS_tbl_vw")
arcpy.MakeTableView_management(FC_tbl, "FC_tbl_vw")

print "Overlaying NHS and FC tables..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.OverlayRouteEvents_lr("NHS_tbl_vw", "GID LINE FRM_DFO TO_DFO", "FC_tbl_vw",
                            "GID LINE FRM_DFO TO_DFO", "UNION", Overlay_NHS_FC,
                            "GID LINE FRM_DFO TO_DFO", "NO_ZERO", "FIELDS", "INDEX")


arcpy.Delete_management("FC_tbl_vw")
arcpy.Delete_management("NHS_tbl_vw")
arcpy.Delete_management("in_memory")


arcpy.MakeTableView_management(HPMS_SAMP_tbl, "HPMS_SAMP_tbl_vw")
arcpy.MakeTableView_management(Overlay_NHS_FC, "Overlay_NHS_FC_tbl_vw")

print "Overlaying NHS/FC and HPMS tables..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.OverlayRouteEvents_lr("Overlay_NHS_FC_tbl_vw", "GID LINE FRM_DFO TO_DFO", "HPMS_SAMP_tbl_vw",
                            "GID LINE FRM_DFO TO_DFO", "UNION", Overlay_NHS_FC_HPMS,
                            "GID LINE FRM_DFO TO_DFO", "NO_ZERO", "FIELDS", "INDEX")

arcpy.Delete_management("Overlay_NHS_FC_tbl_vw")
arcpy.Delete_management("HPMS_SAMP_tbl_vw")
arcpy.Delete_management("in_memory")


arcpy.MakeTableView_management(RDWY_STAT_tbl, "RDWY_STAT_tbl_vw")
arcpy.MakeTableView_management(Overlay_NHS_FC_HPMS, "Overlay_NHS_FC_HPMS_tbl_vw")

print "Overlaying NHS/FC/HPMS and Roadway Status tables..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.OverlayRouteEvents_lr("Overlay_NHS_FC_HPMS_tbl_vw", "GID LINE FRM_DFO TO_DFO", "RDWY_STAT_tbl_vw",
                            "GID LINE FRM_DFO TO_DFO", "UNION", Overlay_NHS_FC_HPMS_STAT,
                            "GID LINE FRM_DFO TO_DFO", "NO_ZERO", "FIELDS", "INDEX")

arcpy.Delete_management("Overlay_NHS_FC_HPMS_tbl_vw")
arcpy.Delete_management("RDWY_STAT_tbl_vw")
arcpy.Delete_management("in_memory")


arcpy.MakeTableView_management(CNTY_NBR_tbl, "CNTY_NBR_tbl_vw")
arcpy.MakeTableView_management(Overlay_NHS_FC_HPMS_STAT, "Overlay_NHS_FC_HPMS_STAT_tbl_vw")

print "Overlaying NHS/FC/HPMS/STAT and County tables..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.OverlayRouteEvents_lr("Overlay_NHS_FC_HPMS_STAT_tbl_vw", "GID LINE FRM_DFO TO_DFO", "CNTY_NBR_tbl_vw",
                            "GID LINE FRM_DFO TO_DFO", "UNION", Overlay_NHS_FC_HPMS_STAT_CNTY,
                            "GID LINE FRM_DFO TO_DFO", "NO_ZERO", "FIELDS", "INDEX")


arcpy.Delete_management("Overlay_NHS_FC_HPMS_STAT_tbl_vw")
arcpy.Delete_management("CNTY_NBR_tbl_vw")
arcpy.Delete_management("in_memory")


arcpy.MakeTableView_management(DIST_NBR_tbl, "DIST_NBR_tbl_vw")
arcpy.MakeTableView_management(Overlay_NHS_FC_HPMS_STAT_CNTY, "Overlay_NHS_FC_HPMS_STAT_CNTY_tbl_vw")

print "Overlaying NHS/FC/HPMS/STAT/CNTY and District tables..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.OverlayRouteEvents_lr("Overlay_NHS_FC_HPMS_STAT_CNTY_tbl_vw", "GID LINE FRM_DFO TO_DFO", "DIST_NBR_tbl_vw",
                            "GID LINE FRM_DFO TO_DFO", "UNION", Overlay_NHS_FC_HPMS_STAT_CNTY_DIST,
                            "GID LINE FRM_DFO TO_DFO", "NO_ZERO", "FIELDS", "INDEX")


arcpy.Delete_management("Overlay_NHS_FC_HPMS_STAT_CNTY_tbl_vw")
arcpy.Delete_management("DIST_NBR_tbl_vw")
arcpy.Delete_management("in_memory")


arcpy.MakeTableView_management(NUM_LANES_tbl, "NUM_LANES_tbl_vw")
arcpy.MakeTableView_management(Overlay_NHS_FC_HPMS_STAT_CNTY_DIST, "Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_tbl_vw")

print "Overlaying NHS/FC/HPMS/STAT/CNTY/DIST and Number of Lanes tables..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.OverlayRouteEvents_lr("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_tbl_vw", "GID LINE FRM_DFO TO_DFO", "NUM_LANES_tbl_vw",
                            "GID LINE FRM_DFO TO_DFO", "UNION", Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES,
                            "GID LINE FRM_DFO TO_DFO", "NO_ZERO", "FIELDS", "INDEX")


arcpy.Delete_management("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_tbl_vw")
arcpy.Delete_management("NUM_LANES_tbl_vw")
arcpy.Delete_management("in_memory")


arcpy.MakeTableView_management(SPD_MAX_tbl, "SPD_MAX_tbl_vw")
arcpy.MakeTableView_management(Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES, "Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_tbl_vw")

print "Overlaying NHS/FC/HPMS/STAT/CNTY/DIST/LANES and Maximum Speed Limit tables..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.OverlayRouteEvents_lr("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_tbl_vw", "GID LINE FRM_DFO TO_DFO", "SPD_MAX_tbl_vw",
                            "GID LINE FRM_DFO TO_DFO", "UNION", Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD,
                            "GID LINE FRM_DFO TO_DFO", "NO_ZERO", "FIELDS", "INDEX")


arcpy.Delete_management("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_tbl_vw")
arcpy.Delete_management("SPD_MAX_tbl_vw")
arcpy.Delete_management("in_memory")


arcpy.MakeTableView_management(SRF_TYPE_tbl, "SRF_TYPE_tbl_vw")
arcpy.MakeTableView_management(Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD, "Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_tbl_vw")

print "Overlaying NHS/FC/HPMS/STAT/CNTY/DIST/LANES/SPD and Surface Type tables..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.OverlayRouteEvents_lr("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_tbl_vw", "GID LINE FRM_DFO TO_DFO", "SRF_TYPE_tbl_vw",
                            "GID LINE FRM_DFO TO_DFO", "UNION", Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF,
                            "GID LINE FRM_DFO TO_DFO", "NO_ZERO", "FIELDS", "INDEX")


arcpy.Delete_management("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_tbl_vw")
arcpy.Delete_management("SRF_TYPE_tbl_vw")
arcpy.Delete_management("in_memory")


arcpy.MakeTableView_management(OWNER_tbl, "OWNER_tbl_vw")
arcpy.MakeTableView_management(Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF, "Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_tbl_vw")

print "Overlaying NHS/FC/HPMS/STAT/CNTY/DIST/LANES/SPD/SRF and Ownership tables..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.OverlayRouteEvents_lr("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_tbl_vw", "GID LINE FRM_DFO TO_DFO", "OWNER_tbl_vw",
                            "GID LINE FRM_DFO TO_DFO", "UNION", Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN,
                            "GID LINE FRM_DFO TO_DFO", "NO_ZERO", "FIELDS", "INDEX")


arcpy.Delete_management("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_tbl_vw")
arcpy.Delete_management("OWNER_tbl_vw")
arcpy.Delete_management("in_memory")


arcpy.MakeTableView_management(RDWAY_MAIN_tbl, "RDWAY_MAIN_tbl_vw")
arcpy.MakeTableView_management(Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN, "Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_tbl_vw")

print "Overlaying NHS/FC/HPMS/STAT/CNTY/DIST/LANES/SPD/SRF/OWN and Maintenance tables..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.OverlayRouteEvents_lr("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_tbl_vw", "GID LINE FRM_DFO TO_DFO", "RDWAY_MAIN_tbl_vw",
                            "GID LINE FRM_DFO TO_DFO", "UNION", Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT,
                            "GID LINE FRM_DFO TO_DFO", "NO_ZERO", "FIELDS", "INDEX")


arcpy.Delete_management("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_tbl_vw")
arcpy.Delete_management("RDWAY_MAIN_tbl_vw")
arcpy.Delete_management("in_memory")


arcpy.MakeTableView_management(TRAFFIC_tbl, "TRAFFIC_tbl_vw")
arcpy.MakeTableView_management(Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT, "Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_tbl_vw")

print "Overlaying NHS/FC/HPMS/STAT/CNTY/DIST/LANES/SPD/SRF/OWN/MAINT and Traffic tables..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.OverlayRouteEvents_lr("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_tbl_vw", "GID LINE FRM_DFO TO_DFO", "TRAFFIC_tbl_vw",
                            "GID LINE FRM_DFO TO_DFO", "UNION", Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF,
                            "GID LINE FRM_DFO TO_DFO", "NO_ZERO", "FIELDS", "INDEX")


arcpy.Delete_management("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_tbl_vw")
arcpy.Delete_management("TRAFFIC_tbl_vw")
arcpy.Delete_management("in_memory")


arcpy.MakeTableView_management(RTE_URB_tbl, "RTE_URB_tbl_vw")
arcpy.MakeTableView_management(Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF, "Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_tbl_vw")

print "Overlaying NHS/FC/HPMS/STAT/CNTY/DIST/LANES/SPD/SRF/OWN/MAINT/TRAF and Urban tables..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.OverlayRouteEvents_lr("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_tbl_vw", "GID LINE FRM_DFO TO_DFO", "RTE_URB_tbl_vw",
                            "GID LINE FRM_DFO TO_DFO", "UNION", Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB,
                            "GID LINE FRM_DFO TO_DFO", "NO_ZERO", "FIELDS", "INDEX")


arcpy.Delete_management("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_tbl_vw")
arcpy.Delete_management("RTE_URB_tbl_vw")
arcpy.Delete_management("in_memory")


arcpy.MakeTableView_management(RTE_CITY_tbl, "RTE_CITY_tbl_vw")
arcpy.MakeTableView_management(Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB, "Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_tbl_vw")

print "Overlaying NHS/FC/HPMS/STAT/CNTY/DIST/LANES/SPD/SRF/OWN/MAINT/TRAF/URB and City tables..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.OverlayRouteEvents_lr("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_tbl_vw", "GID LINE FRM_DFO TO_DFO", "RTE_CITY_tbl_vw",
                            "GID LINE FRM_DFO TO_DFO", "UNION", Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_CITY,
                            "GID LINE FRM_DFO TO_DFO", "NO_ZERO", "FIELDS", "INDEX")


arcpy.Delete_management("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_tbl_vw")
arcpy.Delete_management("RTE_CITY_tbl_vw")
arcpy.Delete_management("in_memory")


arcpy.MakeTableView_management(Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_CITY, "Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_CITY")

print "Deleting extra rows..." + time.strftime("%m_%d_%Y_%H:%M:%S")
with arcpy.da.UpdateCursor("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_CITY", ["NHS", "FC", "HPMS_ID"]) as cursor:
    for row in cursor:
        if row[0] == 0 and row[1] == 0 and row[2] == 0:
            cursor.deleteRow()

print "Deleting 0 length records..."
with arcpy.da.UpdateCursor("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_CITY", ["FRM_DFO", "TO_DFO"]) as cursor:
    for row in cursor:
        if round(row[0], 3) == round(row[1], 3):
            cursor.deleteRow()


print "Deleting proposed roads..." + time.strftime("%m_%d_%Y_%H:%M:%S")
with arcpy.da.UpdateCursor("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_CITY", ["STAT_ID"]) as cursor:
    for row in cursor:
        if row[0] == 1:
            cursor.deleteRow()

print "Updating Route field..." + time.strftime("%m_%d_%Y_%H:%M:%S")
with arcpy.da.UpdateCursor("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_CITY", ["RIA_RTE_ID", "RIA_RTE_ID2"]) as cursor:
    for row in cursor:
        if row[0] is None or row[0] == "":
            row[0] = row[1]
            cursor.updateRow(row)
del cursor

with arcpy.da.UpdateCursor("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_CITY", ["RIA_RTE_ID", "RIA_RTE_ID3"]) as cursor:
    for row in cursor:
        if row[0] is None or row[0] == "":
            row[0] = row[1]
            cursor.updateRow(row)
del cursor

print "Deleting fields..." + time.strftime("%m_%d_%Y_%H:%M:%S")
fields = arcpy.ListFields("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_CITY")
for field in fields:
    if field.name in ('RIA_RTE_ID2', 'RIA_RTE_ID3', 'STAT_ID'):
        arcpy.DeleteField_management("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_CITY", field.name)

print "Adding PMIS ID field..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.AddField_management("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_CITY", "PMIS_ID", "TEXT", "", "", 10)


# GIDstring3 = ""
# cnt = 1
# for chunk in chunks:
#     if cnt == 1:
#         GIDstring3 = """ "GID" IN {0}""".format(
#             str(str(chunk).replace("[","(")).replace("]",")"))
#     else:
#         GIDstring3 += """   OR "GID" IN {0}""".format(
#             str(str(chunk).replace("[", "(")).replace("]", ")"))
#
#     cnt += 1

# arcpy.MakeFeatureLayer_management(lastYearHeader,"lastYearHeader_lay")
# arcpy.SelectLayerByAttribute_management("lastYearHeader_lay", "NEW_SELECTION", GIDstring3)
# arcpy.TableToTable_conversion("lastYearHeader_lay", gdb, "Matched_GIDs")
# arcpy.MakeTableView_management(Matched_GIDs, "Matched_GIDs_tbl_vw")

# print "Overlaying NHS/FC/County/District/Roadway Stat and " + lastYear + " matched GID tables..." + time.strftime("%m_%d_%Y_%H:%M:%S")
# arcpy.OverlayRouteEvents_lr("Overlay_NHS_FC_CNTY_DIST_RDWAY_STAT_tbl_vw", "GID LINE FRM_DFO TO_DFO", "Matched_GIDs_tbl_vw",
#                             "GID LINE FRM_DFO TO_DFO", "UNION", Overlay_NHS_FC_CNTY_DIST_RDWAY_STAT_GIDs,
#                             "GID LINE FRM_DFO TO_DFO", "NO_ZERO", "FIELDS", "INDEX")
#
# arcpy.Delete_management("Overlay_NHS_FC_CNTY_DIST_RDWAY_STAT_tbl_vw")
# arcpy.Delete_management("lastYearHeader_lay")
# arcpy.Delete_management("Matched_GIDs_tbl_vw")
# arcpy.Delete_management("in_memory")


# arcpy.MakeTableView_management(Overlay_NHS_FC_CNTY_DIST_RDWAY_STAT_GIDs, "Overlay_NHS_FC_CNTY_DIST_RDWAY_STAT_GIDs_tbl_vw")


# print "Deleting fields..." + time.strftime("%m_%d_%Y_%H:%M:%S")
# fields = arcpy.ListFields("Overlay_NHS_FC_CNTY_DIST_RDWAY_STAT_GIDs_tbl_vw")
# for field in fields:
#     if field.name not in ('OBJECTID', 'GID', 'RIA_RTE_ID', 'FRM_DFO', 'TO_DFO', 'PMISID', 'NHS', 'CNTY_NBR', 'DIST_NBR'):
#         arcpy.DeleteField_management("Overlay_NHS_FC_CNTY_DIST_RDWAY_STAT_GIDs_tbl_vw", field.name)

# arcpy.DissolveRouteEvents_lr("Overlay_NHS_FC_CNTY_DIST_RDWAY_STAT_GIDs_tbl_vw", "GID LINE FRM_DFO TO_DFO", "RIA_RTE_ID;PMISID;NHS;CNTY_NBR;DIST_NBR", Overlay_dissolve,"GID LINE FRM_DFO TO_DFO" , "DISSOLVE", "INDEX")

# arcpy.MakeTableView_management(Overlay_dissolve, "Overlay_dissolve_tbl_vw")
#
# arcpy.MakeFeatureLayer_management(lastYearHeader,"lastYearHeader_lay")
# arcpy.Dissolve_management("lastYearHeader_lay", lastYearHeader_dis, "PMISID;RIA_RTE_ID", "", "MULTI_PART", "DISSOLVE_LINES")
# arcpy.MakeFeatureLayer_management(lastYearHeader_dis,"lastYearHeader_dis_lay")

# ly_rte_pmisid_dict = {}
# with arcpy.da.SearchCursor("lastYearHeader_dis_lay", ["RIA_RTE_ID", "PMISID"]) as cursor:
#     for row in cursor:
#         ly_rte_pmisid_dict[row[0]] = row[1]
# del cursor
#
#
# with arcpy.da.UpdateCursor("Overlay_dissolve_tbl_vw", ["RIA_RTE_ID", "PMISID"]) as cursor:
#     for row in cursor:
#         for k,v in ly_rte_pmisid_dict.items():
#             if row[1] == "" and row[0] == k:
#                 row[1] = v
#                 cursor.updateRow(row)
# del cursor

arcpy.MakeFeatureLayer_management(RDBD_GMTRY, "RDBD_GMTRY_lay")

print "Making route event layer..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.MakeRouteEventLayer_lr("RDBD_GMTRY_lay", "GID", "Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_CITY", "GID LINE FRM_DFO TO_DFO", "OffSystem_HEADER", "",
                             "", "NO_ANGLE_FIELD", "NORMAL", "ANGLE", "LEFT", "POINT")

print "Copying features..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.CopyFeatures_management("OffSystem_HEADER", OffSystem_HEADER)

arcpy.Delete_management("OffSystem_HEADER")
arcpy.Delete_management("Overlay_NHS_FC_HPMS_STAT_CNTY_DIST_LANES_SPD_SRF_OWN_MAINT_TRAF_URB_CITY")
arcpy.Delete_management("in_memory")

arcpy.MakeFeatureLayer_management(OffSystem_HEADER, "OffSystem_HEADER_"+str(fileYear))

print "Creating Off-System Header .shp..." + time.strftime("%m_%d_%Y_%H:%M:%S")
arcpy.FeatureClassToShapefile_conversion("OffSystem_HEADER_" +str(fileYear), path)

end_time = time.time()
print "Elapsed time: {0}".format(time.strftime('%H:%M:%S', time.gmtime(end_time - start_time)))
