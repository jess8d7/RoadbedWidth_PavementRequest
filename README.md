# RoadbedWidth_PavementRequest

Pavement section has requested a tabular file of all the pavement widths broken down by

1 Fiscal year
2 Signed Highway and Roadbed ID 
3 Beginning DFO
4 Ending DFO
5 Total surface roadway width
6 Right shoulder width
7 Left shoulder width
8 Roadbed width

5 + 6 + 7 = 8

They will take this tabular data and put in into PA.  They want an overwrite from GRID, maybe quarterly, definitely each fiscal year.

Want this data for On Sys, primary roadbeds (K,A,X,L,R)

Use 'Chunkify' script because SQL will only process/pull 1000 records at a time - line 224-213 in script
Line 869-892 - crucial
276 &277 - passing in GIDs

"query 1 + query 1 = GIDlists" is basically adding the list of GIDs to the SQL clause, in other words, it's like adding a 'where' clause to the last line of SQL

Overlays happen in line 895 - can only happen two at a time

Will need to field calculate totals (5+6+7 = 8)

Field calculate fiscal year

Probably don't need shape information
