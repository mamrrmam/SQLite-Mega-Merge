#################################################################################
# SQLite Mega Merge Script                                                      #
#                                                                               #
# @author         Charles Duso "SQLite Merge Script"                            #
# @description    Merges databases that have the same tables and schema.        #
# @date           August 7th, 2016                                              #
# @modified by    Mack Robinson                                                 #
# @modified       April 22, 2021                                                #
# @mods           Now includes block processing to                              #
#                 handle large numbers of databases                             #
#################################################################################

#################################################################################
############################## Import Libraries #################################

import sqlite3
import time
import sys
import traceback
import os
from time import gmtime, strftime

#################################################################################
############################## Global Variables #################################

dbCount = 0  # Variable to count the number of databases
listDB = []  # Variable to store the names of the databases
listTable = []  # Variable to store table names

#################################################################################
############################## Define Functions #################################

# 1. Attach a database to the currently connected database
#
# @param db_name the name of the database file (i.e. "example.db")
# @return none

def attach_database(db_name, u, n):
    global dbCount
    global listDB
    db_add = f"db_{u}_{n}"
    print(f"Attaching database: '{db_name}' at block: {u}, position: {n} as '{db_add}'.")
    try:
        curs.execute(f"ATTACH DATABASE '{db_name}' as '{db_add}'")
        listDB[u].append(db_add) 
        ##appends the most recently attached database to the current block in listDB
    except Exception():
        traceback.exc()

# 2. Close the current database connection
#
# @return none

def close_connection():
    curs.close()
    conn.close()

# 3. Get the table names of a database
#
# @param db_name the name of the database file (i.e. "example.db")
# @return a string array of the table names

def get_table_names():
    temp = []
    tables = []
    curs.execute("SELECT name FROM sqlite_master WHERE type='table';")
    temp = curs.fetchall()
    for i in range(0, len(temp)):
        if ("Example_Table1" in temp[i][0]):
            continue
        if ("Example_Table2" in temp[i][0]):
            continue
        if ("Example_Table3" in temp[i][0]):
            continue
        if ("Example_Table4" in temp[i][0]):
            continue
        else:
            tables.append(temp[i][0])
    return tables

# 4. Get the column names of a table
#
# @param table_name the name of the database file (i.e. "example.db")
# @return a string array of the column names - strips primary ids column

def get_column_names(table_name):
    curs.execute(f"PRAGMA table_info({table_name});")
    temp = curs.fetchall()
    columns = []
    for i in range(0, len(temp)):
        if ("id" in temp[i][1]) | ("ID" in temp[i][1]):
            continue
        else:
            columns.append(temp[i][1])
    return columns

# 5. Compare two lists to see if they have identical data
#
# @param list1 the first list parameter for comparison
# @param list2 the second list parameter for comparison
# @return will return a boolean (0 lists !=, 1 lists ==)

def compare_lists(list1, list2):
    if len(list1) != len(list2):
        return 0
    else:
        for i in range(0, len(list1)):
            if list1[i] != list2[i]:
                return 0
    return 1

# 6. Convert a list of string objects to a string of comma separated items.
#
# @param listObj the list to convert
# @return a string containing the list items - separated by commas.

def list_to_string(list_obj):
    list_string = ""
    for i in range(0, len(list_obj)):
        if i == (len(list_obj) - 1):
            list_string = list_string + list_obj[i]
        else:
            list_string = list_string + list_obj[i] + ", "
    return list_string


# 7. Merge a table from an attached database to the source table
#
# @param table_name the name of the table to merge
# @param column_names the names of the columns to include in the merge
# @param db_name_table_name the name of the attached database and the table i.e. "db_name.table_name"
# @return none

def merge_table(table_name, column_names, db_name):
    db_name_table_name = db_name + "." + table_name
    try:
        curs.execute(f"INSERT INTO {table_name}({column_names}) SELECT {column_names} FROM {db_name_table_name};")
        conn.commit()
    except Exception:
        traceback.print_exc()


# 8. Divide otherDBs into blocks of ten or less because sqlite can't attach more than ten at a time
#
# @param list (ie. otherDBs)
# @param n (the size of the block, in our case 10)

def divide_list(list, n):
    for i in range(0, len(list), n):
        yield list[i:i +n]

# 9. Get the column types of a table
#
# @param table_name the name of the table (i.e. "example.db")
# unlike get_column_names returns a formatted string 
# format: "colname1 coltype1, colname2 coltype2, ...) 
# still strips primary ids column

def get_column_names_types(table_name):
    curs.execute(f"PRAGMA table_info({table_name});")
    temp = curs.fetchall()
    column_names_types = ""
    for i in range(0, len(temp)):
        if ("id" in temp[i][1]) | ("ID" in temp[i][1]):
            continue
        else:
            colname = f"{temp[i][1]}" 
            coltype = f"{temp[i][2]}"
            if i == (len(temp) -1):
                column_names_types = column_names_types + colname + " " + coltype
            else:
                column_names_types = column_names_types + colname + " " + coltype + ", "
    return column_names_types


#################################################################################
############################## Input Parameters #################################

# 1. DEFINE A LIST OF DBs TO MERGE
################################## 
# define a list of DBs using filenames from the folder containing databases by running ls /home/ubuntu/databases/*.db > filenames.txt
# ...needs to be full path for sqlite3 to interpret correctly
# otherDBs can also be defined by a list e.g. otherDBs = [] for example,
# otherDBs = ['/home/ubuntu/databases/5-9856-4928.db', '/home/ubuntu/databases/5-9856-5376.db']

with open('filenames.txt', 'r') as fileNames:
    otherDBs = [line.strip() for line in fileNames]

# 2. DEFINE A MAIN DB AS A TEMPLATE FOR THE MERGE
################################## 
# This is where the main database is ie. the first if a list of identical DBs but can be defined otherwise

mainDB = otherDBs[0]

# 3. QUALITY CONTROL
################################## 

if len(otherDBs) == 0:
    print("ERROR: No databases have been added for merging.")
    sys.exit()

#################################################################################
############################# Quality Control ###################################

# 1. Initialize Connection and get main list of tables
################################## 

conn = sqlite3.connect(mainDB)  # Connect to the main database
curs = conn.cursor()  # Connect a cursor
listTable = get_table_names()  # Get the table names
listTable.sort()
print(listTable)
close_connection()

# 2. Compare databases for quality control
##################################

startTime = time.time()
print("Comparing databases. Started at: " + strftime("%H:%M", gmtime()))
exc_DBs = [] # create a array of DBs with tables that do not match mainDB

i=0 #iterator
while i < len(otherDBs):
    conn = sqlite3.connect(otherDBs[i])
    curs = conn.cursor()
    temp = get_table_names()  # Get the current list of tables
    temp.sort()
    if len(listTable) > len(temp):
        print(f"Table is missing from non-primary database: {otherDBs[i]}")
        print("Database will NOT BE MERGED with the main database.")
        exc_DBs.append([otherDBs[i], "Reason: Missing Table(s), database excluded from merge."])
        otherDBs.remove(otherDBs[i])  # Remove the table to avoid errors
        continue

    if len(listTable) < len(temp):
        print(f"Extra table(s) in non-primary database: {otherDBs[i]}")
        print("TABLES that are NOT in main database will NOT be added.")
        exc_DBs.append([otherDBs[i], "Reason: Extra Table(s) can not be merged, database included in merge."])
        continue

    if listTable != temp:
        print(f"Tables do not match in non-primary database: {otherDBs[i]}")
        print("The database will NOT BE MERGED with the main database.")
        exc_DBs.append([otherDBs[i], "Reason: Table(s) did not match, database excluded from merge."])
        otherDBs.remove(otherDBs[i])  # Remove the table to avoid errors
        continue

    if listTable == temp:
        i += 1
        print(f"The tables in file no.{i} are a match with the main database.")
        continue

    close_connection()
    num = len(otherDBs)
    print(f"There are {num} databases whose tables matched the main database.")

# 3. Log exceptions that were removed or otherwise not a good fit for merge
################################## 

for y in range(0, len(exc_DBs)):
   print(f"{exc_DBs[y][0]} was logged as an exception. {exc_DBs[y][1]}")

# 4. Log errors when no otherDBs were found
################################## 

if len(otherDBs) == 0:
    print("ERROR: No databases to merge. Databases were either removed due to \
          inconsistencies, or databases were not added properly.")
    sys.exit()

# 5. Print notification that quality control is complete
##################################

print("Finished comparing databases. Time elapsed: %.3f" % (time.time() -
                                                            startTime))

#################################################################################
############################ Merging Databases ##################################

# 1. Initialization Parameters
##################################

startTime = time.time()
print("Initializing merging of databases. Started at: " + strftime("%H:%M", gmtime()))

# 2. Pre-processing module produces a donor table based on the template of the original
##################################
## The donor table will not have any constraints on columns

print("Pre-processing databases initiated at: " + strftime("%H:%M", gmtime()))

# Pre-processing loop

for h in range(0, len(otherDBs)):
    conn = sqlite3.connect(otherDBs[h], timeout = 10)
    curs = conn.cursor() 
    lngt = len(otherDBs)
    print(f"Now pre-processing {h} of {lngt}")
    for g in range(0, len(listTable)):
        colnamtyp = get_column_names_types(listTable[g])
        colnam = list_to_string(get_column_names(listTable[g]))
        curs.execute("PRAGMA legacy_alter_table = TRUE;")
        curs.execute(f"CREATE TABLE _{listTable[g]}({colnamtyp});")
        curs.execute(f"INSERT INTO _{listTable[g]}({colnam}) SELECT {colnam} FROM {listTable[g]};")
        curs.execute(f"DROP TABLE {listTable[g]};")
        curs.execute(f"ALTER TABLE _{listTable[g]} RENAME TO {listTable[g]};")
        conn.commit()
    close_connection()
    print(f"Pre-processing of {otherDBs[h]} complete). Time elapsed: %.3f" % (time.time() -
                                                                              startTime))

# 3. Database Merge Module 
##################################
## A nested for loop iterates through the blocks in this version
## to prevent an error from trying to attach more than ten DBs at a time

print("Merging databases initiated at: " + strftime("%H:%M", gmtime()))
otherDBs.pop(0) # removes the first database (mainDB) from filenames, turn this off if MainDB is not in filenames.txt
DBs_attacher = list(divide_list(otherDBs, 10))
nBlocks = len(DBs_attacher)
print("Total: "+str(Total_DBs_attacher)+" Blocks: "+str(nBlocks))

# Merge Loop

for u in range(0, len(DBs_attacher)):                                         # Block level iterator
    conn = sqlite3.connect(mainDB, timeout = 15)                              # Attach main database
    curs = conn.cursor()                                                      # Attach cursor
    listDB.append([])                                                         # Add a new block to listDB
    print("Now processing: "+str(u)+" of "+str(nBlocks))
    for n in range(0, len(DBs_attacher[u])):                                  # Sub-Block level iterator, n<=10 
        attach_database(DBs_attacher[u][n], u, n)                             # Attach databases within block
        for j in range(0, len(listTable)):                                    # for each table in each database
            columns = list_to_string(get_column_names(listTable[j]))          # get each column for each table
            merge_table(listTable[j], columns, listDB[u][n])                  # and insert rows from these columns, in this database, in the equivalent table in main
            conn.commit()                                                     # Commit changes 
        os.remove(f"{DBs_attacher[u][n]}")                                    # Removes merged db after the merge to conserve space on disk
    close_connection()                                                        # Close connection at end of each block of ten databases

print("Databases finished merging. Time elapsed: %.3f" % (time.time() -
                                                          startTime))
