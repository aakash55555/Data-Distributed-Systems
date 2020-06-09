#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

################### DO NOT CHANGE ANYTHING BELOW THIS #############################

def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    con = openconnection
    cur = con.cursor()
    # Function to Parallel sort the InputTable on SortingColumnName and store results in OutputTable
    RANGE_TABLE_PREFIX = "sort_part"
    col_maxValue = maximumColumnValue(SortingColumnName, InputTable, openconnection)
    #print("----------------------- ", col_maxValue)
    col_minValue = minimumColumnValue(SortingColumnName, InputTable, openconnection)
    #print("----------------------- ", col_minValue)
    total_threads = 5
    range_Interval = (float)(col_maxValue - col_minValue)/total_threads
    #print(range_Interval)
    #metadata = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'".format(InputTable)
    #cur.execute(metadata)
    fetch_colname = table_column_creation(openconnection, InputTable)
    #print("--------------------!!!!!!!", fetch_colname)

    for i in range(total_threads):
        RangeTname = RANGE_TABLE_PREFIX + str(i)
        cur.execute("DROP TABLE IF EXISTS {}".format(RangeTname))
        create_table = "CREATE TABLE {} ({} {})".format(RangeTname, fetch_colname[0][0], fetch_colname[0][1])
        cur.execute(create_table)

        for column in range(1, len(fetch_colname)):
            add_columnname_schema(openconnection, column, RangeTname, fetch_colname)

    threads = [0,0,0,0,0]
    for i in range(total_threads):
        if  i == 0:
            minValue = col_minValue
            maxValue = col_minValue + range_Interval
            threads[i] = threading.Thread(target=sorting, args=(InputTable, RANGE_TABLE_PREFIX, SortingColumnName, minValue, maxValue, i, openconnection))
            threads[i].start()
        else:
            minValue = maxValue
            maxValue = maxValue + range_Interval
            threads[i] = threading.Thread(target=sorting, args=(InputTable, RANGE_TABLE_PREFIX, SortingColumnName, minValue, maxValue, i, openconnection))
            threads[i].start()
    #createSchema(openconnection, inputTable2_Name, InputTable2, Table1JoinColumn)
    #print("Done upto this point")

    output = "DROP TABLE IF EXISTS {}".format(OutputTable)
    cur.execute(output)
    outputTable = "CREATE TABLE {} ({} {})".format(OutputTable, fetch_colname[0][0], fetch_colname[0][1])
    cur.execute(outputTable)
    for column in range(1,len(fetch_colname)):
        add_columnname_schema(openconnection, column, OutputTable, fetch_colname)

    for thread in range(total_threads):
        threads[thread].join()

    for thread in range(total_threads):
        insert_output_table(openconnection, RANGE_TABLE_PREFIX, OutputTable, thread)

    for thread in range(total_threads):
        drop_table = "DROP TABLE IF EXISTS {}{};".format(RANGE_TABLE_PREFIX, str(thread))
        cur.execute(drop_table)

    cur.close()
    con.commit()

def insert_output_table(openconnection, RANGE_TABLE_PREFIX, OutputTable, thread):
    con = openconnection
    cur = con.cursor()
    insert_output = "INSERT INTO {} SELECT * FROM {}{};".format(OutputTable, RANGE_TABLE_PREFIX, str(thread))
    cur.execute(insert_output)

def add_columnname_schema(openconnection, column, TableName, fetch_colname):
    con = openconnection
    cur = con.cursor()
    create_schema = "ALTER TABLE {} ADD COLUMN {} {}".format(TableName, fetch_colname[column][0], fetch_colname[column][1])
    cur.execute(create_schema)
    #print(create_schema)

def maximumColumnValue(SortingColumnName, InputTable, openconnection):
    con = openconnection
    cur = con.cursor()
    get_max = "SELECT MAX({}) FROM {}".format(SortingColumnName, InputTable)
    cur.execute(get_max)
    maximum_val = float(cur.fetchone()[0])
    return maximum_val

def minimumColumnValue(SortingColumnName, InputTable, openconnection):
    con = openconnection
    cur = con.cursor()
    get_min = "SELECT MIN({}) FROM {}".format(SortingColumnName, InputTable)
    cur.execute(get_min)
    minimum_val = float(cur.fetchone()[0])
    return minimum_val

def table_column_creation(openconnection, InputTable):
    con = openconnection
    cur = con.cursor()
    column_name = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}';".format(InputTable)
    cur.execute(column_name)
    fetch_colname = cur.fetchall()
    return fetch_colname

# def createSchema(openconnection, createdtableName, InputTable, TableJoinColumn, index, col_minValue, total_range):
#     con = openconnection
#     cur = con.cursor()
#     #col_min_value = col_minValue
#     #col_max_value = col_min_value + total_range
#     if index == 0:
#         col_min_value = col_minValue
#         col_max_value = col_min_value + total_range
#         query = "CREATE TABLE " + createdtableName + " AS SELECT * FROM " + InputTable + " WHERE " + TableJoinColumn + " >= " + str(
#             col_min_value) + " AND " + TableJoinColumn + " <= " + str(col_max_value) + ";"
#     else:
#         col_min_value = col_max_value
#         col_max_value = col_min_value + total_range
#         query = "CREATE TABLE " + createdtableName + " AS SELECT * FROM " + InputTable + " WHERE " + TableJoinColumn + " > " + str(
#             col_min_value) + " AND " + TableJoinColumn + " <= " + str(col_max_value) + ";"
#     cur.execute(query)

def sorting(InputTable, RANGE_TABLE_PREFIX, SortingColumnName, min_range_value, max_range_value, i, openconnection):
 # Function to sort a single partition based on SortingColumnName using a thread
    con = openconnection
    cur = con.cursor()
    RangeTname = RANGE_TABLE_PREFIX + str(i)
    if i == 0:
        query = "INSERT INTO {} SELECT * FROM {}  WHERE {} >= {} AND {} <= {} ORDER BY {} ASC"\
         .format(RangeTname, InputTable, SortingColumnName, str( min_range_value), SortingColumnName,str( max_range_value), SortingColumnName)
    else:
        query = "INSERT INTO {} SELECT * FROM {}  WHERE {} > {} AND {} <= {} ORDER BY {} ASC" .format(RangeTname, InputTable, SortingColumnName, str(min_range_value), SortingColumnName, str(max_range_value),SortingColumnName)

    cur.execute(query)


def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    # Implement ParallelJoin Here.
    con = openconnection
    cur = con.cursor()

    col_maxValue_table1 = maximumColumnValue(Table1JoinColumn, InputTable1, openconnection)
    col_minValue_table1 = minimumColumnValue(Table1JoinColumn, InputTable1, openconnection)
    col_maxValue_table2 = maximumColumnValue(Table2JoinColumn, InputTable2, openconnection)
    col_minValue_table2 = minimumColumnValue(Table2JoinColumn, InputTable2, openconnection)
    total_threads = 5

    col_maxValue = max(col_maxValue_table1, col_maxValue_table2)
    col_minValue = min(col_minValue_table1, col_minValue_table2)
    total_range = (col_maxValue - col_minValue) / total_threads

    fetch_col_table1 = table_column_creation(openconnection, InputTable1)
    fetch_col_table2 = table_column_creation(openconnection, InputTable2)

    #
    # outputTable2 = "CREATE TABLE {} ({} {})".format(OutputTable, fetch_colname_table2[0][0], fetch_colname_table2[0][1])
    # cur.execute(outputTable2)

    for index in range(total_threads):
        inputTable2_Prefix = "input_table2"
        inputTable2_Name = inputTable2_Prefix + str(index)
        if index == 0:
            col_min_value = col_minValue
            col_max_value = col_min_value + total_range
            table_creation = "CREATE TABLE " + inputTable2_Name + " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " >= " + str(
                col_min_value) + " AND " + Table2JoinColumn + " <= " + str(col_max_value) + ";"
        else:
            col_min_value = col_max_value
            col_max_value = col_min_value + total_range
            table_creation = "CREATE TABLE " + inputTable2_Name + " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " > " + str(
                col_min_value) + " AND " + Table2JoinColumn + " <= " + str(col_max_value) + ";"
        cur.execute(table_creation)
        #createSchema(openconnection, inputTable2_Name, InputTable2, Table2JoinColumn, index, col_minValue, total_range)

    for index in range(total_threads):
        inputTable1_Prefix = "input_table1"
        inputTable1_Name = inputTable1_Prefix + str(index)
        if index == 0:
            col_min_value = col_minValue
            col_max_value = col_min_value + total_range
            table_creation = "CREATE TABLE " + inputTable1_Name + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " >= " + str(
                col_min_value) + " AND " + Table1JoinColumn + " <= " + str(col_max_value) + ";"
        else:
            col_min_value = col_max_value
            col_max_value = col_min_value + total_range
            table_creation = "CREATE TABLE " + inputTable1_Name + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " > " + str(
                col_min_value) + " AND " + Table1JoinColumn + " <= " + str(col_max_value) + ";"
        cur.execute(table_creation)
        #createSchema(openconnection, inputTable1_Name, InputTable1, Table1JoinColumn, index, col_minValue, total_range)


    #cur.execute("DROP TABLE IF EXISTS {}".format(OutputTable))
    outputTable1 = "CREATE TABLE {} ({} {})".format(OutputTable, fetch_col_table1[0][0], fetch_col_table1[0][1])
    cur.execute(outputTable1)

    for column in range(1, len(fetch_col_table1)):
        add_columnname_schema(openconnection, column, OutputTable, fetch_col_table1)

    for column in range(len(fetch_col_table2)):
        add_columnname_schema(openconnection, column, OutputTable, fetch_col_table2)

    for i in range(total_threads):
        OutputRangeT_PREFIX = "output_table"
        OutputRangeTname = OutputRangeT_PREFIX + str(i)
        create_table = "CREATE TABLE {} ({} {})".format(OutputRangeTname, fetch_col_table1[0][0], fetch_col_table1[0][1])
        cur.execute(create_table)

        for column in range(1, len(fetch_col_table1)):
            add_columnname_schema(openconnection, column, OutputRangeTname, fetch_col_table1)
        for column in range(len(fetch_col_table2)):
            add_columnname_schema(openconnection, column, OutputRangeTname, fetch_col_table2)

    threads = [0, 0, 0, 0, 0]
    for index in range(total_threads):
        threads[index] = threading.Thread(target=Joining_tables, args=(Table1JoinColumn, Table2JoinColumn, openconnection, index, OutputRangeT_PREFIX, inputTable1_Prefix, inputTable2_Prefix))
        threads[index].start()

    for index in range(total_threads):
        threads[index].join()

    for index in range(total_threads):
        OutputRangeT_PREFIX = "output_table"
        insert_output_table(openconnection, OutputRangeT_PREFIX, OutputTable, index)

    for index in range(total_threads):
        cur.execute("DROP TABLE IF EXISTS {}{}".format(inputTable1_Prefix, str(index)))
        cur.execute("DROP TABLE IF EXISTS {}{}".format(inputTable2_Prefix, str(index)))
        cur.execute("DROP TABLE IF EXISTS {}{}".format(OutputRangeT_PREFIX, str(index)))

    cur.close()


def Joining_tables(Table1JoinColumn, Table2JoinColumn, openconnection, index, OutputRangeT_PREFIX, inputTable1_Prefix, inputTable2_Prefix):
    con = openconnection
    cur = con.cursor()
    join_tables = "INSERT INTO {}{} SELECT * FROM {}{} INNER JOIN {}{} ON {}{}.{} = {}{}.{};".format(OutputRangeT_PREFIX, str(index), inputTable1_Prefix, str(index),
                inputTable2_Prefix, str(index), inputTable1_Prefix, str(index), Table1JoinColumn, inputTable2_Prefix, str(index), Table2JoinColumn)
    cur.execute(join_tables)
    return

# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
