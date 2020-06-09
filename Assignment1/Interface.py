# connect to Postgres
import psycopg2
import math

DATABASE_NAME = 'dds_assgn1'

def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    tableName = ratingstablename
    filepath = ratingsfilepath
    create_db(dbname=DATABASE_NAME)
    con = openconnection
    cur = con.cursor()
    cur.execute("drop table if exists " + tableName)
    cur.execute("create table " + tableName + "(UserID integer, MovieID integer, Rating float, timeStamp bigint)")
    """
    print("created table")
    """
    with open(filepath, 'r') as datFile:
        fileToCopy = datFile.read();
    filetoCopy = fileToCopy.replace('::', ':').strip(" ")
    with open(filepath, 'w') as datFile:
        datFile.write(filetoCopy)
    Data_file = open(filepath, 'r')
    cur.copy_from(Data_file, tableName, sep=':', columns=('UserID', 'MovieID', 'Rating', 'timeStamp'))
    cur.execute("alter table " + tableName + " drop column timeStamp")
    """
    print("copied to table")
    cur.execute("select * from "+ tableName)
    res=cur.fetchall()
    print(res)
    """
    cur.close()
    con.commit()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    max_rating = 5.0
    con = openconnection
    cur = con.cursor()
    partition_length = max_rating / numberofpartitions
    min_range = 0
    RANGE_TABLE_PREFIX = 'range_part'
    max_range = min_range + partition_length

    if (numberofpartitions <= 0 or math.floor(numberofpartitions) != math.ceil(numberofpartitions)):
        print("You are only allowed to enter integral number of partitions more than 0")
        return

    for i in range(0, numberofpartitions):
        RangeTableName = RANGE_TABLE_PREFIX + str(i)
        cur.execute("DROP TABLE IF EXISTS " + RangeTableName)
        cur.execute("CREATE TABLE " + RangeTableName + " (UserID INT, MovieID INT,Rating REAL)")
        """
        print("after creating table using range partition")
        """
        if i == 0:
            cur.execute(
                "INSERT INTO " + RangeTableName + " SELECT * from " + ratingstablename + " where Rating >= " + str(min_range) + " AND Rating <= " + str(max_range))

        else:
            cur.execute(
                "INSERT INTO " + RangeTableName + " SELECT * from " + ratingstablename + " where Rating > " + str(min_range) + " AND Rating <= " + str(max_range))

        min_range = max_range
        max_range = max_range + partition_length

    """
    for printing the tables

    print("Printing table 0")
    cur.execute("select * from {0}0".format(RANGE_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 1")
    cur.execute("select * from {0}1".format(RANGE_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 2")
    cur.execute("select * from {0}2".format(RANGE_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 3")
    cur.execute("select * from {0}3".format(RANGE_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 4")
    cur.execute("select * from {0}4".format(RANGE_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)
    """
    cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    con = openconnection
    cur = con.cursor()

    if (numberofpartitions <= 0 or math.floor(numberofpartitions) != math.ceil(numberofpartitions)):
        print("You are only allowed to enter integral number of partitions more than 0")
        return

    for i in range(0, numberofpartitions):
        RoundRobinTable = RROBIN_TABLE_PREFIX + str(i)
        cur.execute("DROP TABLE IF EXISTS " + RoundRobinTable)

    for i in range(0, numberofpartitions):
        RoundRobinTable = "rrobin_part" + str(i)
        sql = "CREATE TABLE " + RoundRobinTable + " AS SELECT USERID, MOVIEID, RATING FROM (SELECT *, Row_Number() over()rowNumber FROM " + ratingstablename + ")n WHERE (rowNumber-1)%" + str(numberofpartitions) + "=" + str(i) + ";"
        cur.execute(sql)

    """
    for printing the tables
    
    print("Printing table 0")
    cur.execute("select * from {0}0".format(RROBIN_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 1")
    cur.execute("select * from {0}1".format(RROBIN_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 2")
    cur.execute("select * from {0}2".format(RROBIN_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 3")
    cur.execute("select * from {0}3".format(RROBIN_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 4")
    cur.execute("select * from {0}4".format(RROBIN_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)
    """
    cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    con = openconnection
    cur = con.cursor()

    if (rating < 0.0 or rating > 5.0):
        print("Rating should be in between the range 0 to 5")
        return

    cur.execute("INSERT INTO " + ratingstablename + "(UserID, MovieID, Rating) values(" + str(userid) + "," + str(itemid) + "," + str(rating) + ");");

    """
    print("Printing table after insertion---------------")
    cur.execute("select * from {0}".format(ratingstablename))
    res = cur.fetchall()
    print(res)
    """

    partition = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '{0}%';".format(RROBIN_TABLE_PREFIX)

    cur.execute(partition)
    openconnection.commit()
    total_partitions = cur.fetchone();
    """
    print("total partitions are {0}".format(total_partitions))
    """
    totalRows = "SELECT COUNT(*) from {0}".format(ratingstablename)
    cur.execute(totalRows)
    openconnection.commit()
    rowCount = cur.fetchone()
    """
    print("total number of rows are {0}".format(rowCount))
    """
    partitionnumber = rowCount[0] % total_partitions[0]

    cur.execute(
        "INSERT INTO " + RROBIN_TABLE_PREFIX + str(partitionnumber - 1) + "(UserID, MovieID, Rating) values(" + str(userid) + "," + str(itemid) + "," + str(rating) + ");");

    """
    for printing the tables
    
    print("Insertion using Round Robin Partition")
    print("Printing table 0")
    cur.execute("select * from {0}0".format(RROBIN_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 1")
    cur.execute("select * from {0}1".format(RROBIN_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 2")
    cur.execute("select * from {0}2".format(RROBIN_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 3")
    cur.execute("select * from {0}3".format(RROBIN_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 4")
    cur.execute("select * from {0}4".format(RROBIN_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)
    """
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    RANGE_TABLE_PREFIX = 'range_part'
    con = openconnection
    cur = con.cursor()

    if (rating < 0.0 or rating > 5.0):
        print("Rating should be in between the range 0 to 5")
        return

    cur.execute("INSERT INTO " + ratingstablename + "(UserID, MovieID, Rating) values(" + str(userid) + "," + str(itemid) + "," + str(rating) + ");");
    """
    print("Printing table after insertion---------------")
    cur.execute("select * from rahulians")
    res = cur.fetchall()
    print(res)
    """
    partition = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '{0}%';".format(RANGE_TABLE_PREFIX)

    """
    partition = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'range_part%';"
    """
    cur.execute(partition)
    openconnection.commit()
    total_partitions = cur.fetchone();
    """
    print("total partitions are {0}".format(total_partitions))
    """
    totalRows = "SELECT COUNT(*) from {0}".format(ratingstablename)
    cur.execute(totalRows)
    openconnection.commit()
    """
    print("total number of rows are are {0}".format(rowCount))
    """
    interval = 5.0 / total_partitions[0]

    partitionnumber = 0
    upperBound = interval
    lowerBound = 0
    while lowerBound < 5.0:
        if lowerBound == 0:
            if rating >= 0.0 and rating <= upperBound:
                break
            partitionnumber = partitionnumber + 1
            lowerBound = lowerBound + interval
            upperBound = upperBound + interval
        else:
            if rating > lowerBound and rating <= upperBound:
                break
            partitionnumber = partitionnumber + 1
            lowerBound = lowerBound + interval
            upperBound = upperBound + interval

    cur.execute("INSERT INTO " + RANGE_TABLE_PREFIX + str(partitionnumber) + "(UserID, MovieID, Rating) values(" + str(
        userid) + "," + str(
        itemid) + "," + str(rating) + ");");

    """
    for printing the tables
  
    print("Insertion using Range Partition")
    print("Printing table 0")
    cur.execute("select * from {0}0".format(RANGE_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 1")
    cur.execute("select * from {0}1".format(RANGE_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 2")
    cur.execute("select * from {0}2".format(RANGE_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 3")
    cur.execute("select * from {0}3".format(RANGE_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)

    print("Printing table 4")
    cur.execute("select * from {0}4".format(RANGE_TABLE_PREFIX))
    res = cur.fetchall()
    print(res)
    """
    cur.close()


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print ('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()
