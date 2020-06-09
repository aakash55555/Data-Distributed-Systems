#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()

# Method for Range Query
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):

    cur = openconnection.cursor()
    rangePartitionTable = "RangeRatingsPart"
    robinPartitionTable = "RoundRobinRatingsPart"
    rangeMetadata = "RangeRatingsMetadata"
    rrobinMetadata = "RoundRobinRatingsMetadata" 	

# Range Query for the Range Partitions
    cur.execute("Select PartitionNum from {} where {} <= minrating or {} >= maxrating".format(rangeMetadata, str(ratingMinValue), str(ratingMaxValue)))
    rangeRatingPartitions = cur.fetchall()

    for rangePartition in rangeRatingPartitions:
        RatingstableName = rangePartitionTable + str(rangePartition[0])
        cur.execute("Select * from {} where Rating >= {} AND Rating <= {}".format(RatingstableName, str(ratingMinValue), str(ratingMaxValue)))
        values = cur.fetchall()

        with open(outputPath, "a") as rangeFile:
            for value in values:
                rangeFile.write(RatingstableName + "," + str(value[0]) + "," + str(value[1]) + "," + str(value[2]) + "\n")

# Range Query for Round Robin Partitions
    cur.execute("select partitionnum from {};".format(rrobinMetadata))
    rrobinRatingPartitions = cur.fetchall()

    for rating in range(0, rrobinRatingPartitions[0][0]):
        robinTableName = robinPartitionTable + str(rating)
        cur.execute("select * from {} where rating >= {} and rating <= {};".format(robinTableName, str(ratingMinValue), str(ratingMaxValue)))
        values = cur.fetchall()
        with open(outputPath, "a") as rrobinFile:
            for value in values:
                rrobinFile.write(str(robinTableName) + "," + str(value[0]) + "," + str(value[1]) + "," + str(value[2]) + "\n")

# Method for Point Query
def PointQuery(ratingValue, openconnection, outputPath):

    cur = openconnection.cursor()
    rangePartitionTable = "RangeRatingsPart"
    robinPartitionTable = "RoundRobinRatingsPart"
    rangeMetadata = "RangeRatingsMetadata"
    rrobinMetadata = "RoundRobinRatingsMetadata" 

# Point Query for the Range Partitions
    cur.execute("Select PartitionNum from {} where {} >= minrating and {} <= maxrating".format(rangeMetadata, str(ratingValue), str(ratingValue)))
    rangeRatingPartitions = cur.fetchall()

    for rangePartition in rangeRatingPartitions:
        RatingstableName = rangePartitionTable + str(rangePartition[0])
        cur.execute("Select * from {} where Rating = {}".format(RatingstableName, str(ratingValue)))
        values = cur.fetchall()

        with open(outputPath, "a") as rangeFile:
            for value in values:
                rangeFile.write(RatingstableName + "," + str(value[0]) + "," + str(value[1]) + "," + str(value[2]) + "\n")

# Point Query for the Round Robin Partitions
    cur.execute("select partitionnum from {};".format(rrobinMetadata))
    rrobinRatingPartitions = cur.fetchall()

    for rating in range(0, rrobinRatingPartitions[0][0]):
        robinTableName = robinPartitionTable + str(rating)
        cur.execute("select * from {} where rating = {};".format(robinTableName, str(ratingValue)))
        values = cur.fetchall()
        with open(outputPath, "a") as rrobinFile:
            for value in values:
                rrobinFile.write(str(robinTableName) + "," + str(value[0]) + "," + str(value[1]) + "," + str(value[2]) + "\n")
