#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys

def WriteFile(file_name, dict_rows):
    with open(file_name,"w") as fl:
        for (k,v) in dict_rows.items():
            table_name = k
            for row in v:
                fl.write(table_name + "," + str(row[0]) + "," + str(row[1]) + "," + str(row[2]))
                fl.write("\n")


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):

    lessopr = ""
    greateropr = ""
    str_rating_min = str(ratingMinValue)
    str_rating_max = str(ratingMaxValue)
    cur = openconnection.cursor()
    totalrows = dict()

    select_stmt_range_partition_metadata = "select * from RangeRatingsMetadata"
    cur.execute(select_stmt_range_partition_metadata)
    range_partitions = cur.fetchall()
    for partition in range_partitions:
        str_part = str(partition[0])
        if not partition[2] < ratingMinValue or partition[1] >= ratingMaxValue:
            greateropr = " >= "
            lessopr = " <= "

            select_stmt_range = "select * from RangeRatingsPart" + str_part + " where rating" + greateropr \
                                + str_rating_min + " and rating" + lessopr + str_rating_max
            cur.execute(select_stmt_range)
            rows1 = cur.fetchall()
            totalrows["RangeRatingsPart" + str_part] = rows1
            # WriteFile(outputPath,"RangeRatingsPart" + str_part,rows)

    select_stmt_rrobin_partition_metadata = "select * from RoundRobinRatingsMetadata"
    cur.execute(select_stmt_rrobin_partition_metadata)
    rrobin_partitions = cur.fetchall()
    for partition in range(0,rrobin_partitions[0][0]):
        str_part = str(partition)
        select_stmt_rrobin = "select * from RoundRobinRatingsPart" + str_part + " where rating" + greateropr \
                            + str_rating_min + " and rating" + lessopr + str_rating_max
        cur.execute(select_stmt_rrobin)
        rows2 = cur.fetchall()
        totalrows["RoundRobinRatingsPart" + str_part] = rows2
        # WriteFile(outputPath, "RoundRobinRatingsPart" + str_part, rows)

    WriteFile(outputPath,totalrows)


def PointQuery(ratingValue, openconnection, outputPath):

    str_rating_val = str(ratingValue)
    cur = openconnection.cursor()
    totalrows = dict()

    select_stmt_range_partition_metadata = "select * from RangeRatingsMetadata"
    cur.execute(select_stmt_range_partition_metadata)
    range_partitions = cur.fetchall()
    for partition in range_partitions:
        str_part = str(partition[0])
        if partition[1] <= ratingValue <= partition[2]:
            select_stmt_range = "select * from RangeRatingsPart" + str_part + " where rating = " + str_rating_val
            cur.execute(select_stmt_range)
            rows1 = cur.fetchall()
            totalrows["RangeRatingsPart" + str_part] = rows1
            #WriteFile(outputPath, "RangeRatingsPart" + str_part, rows)

    select_stmt_rrobin_partition_metadata = "select * from RoundRobinRatingsMetadata"
    cur.execute(select_stmt_rrobin_partition_metadata)
    rrobin_partitions = cur.fetchall()
    for partition in range(0, rrobin_partitions[0][0]):
        str_part = str(partition)
        select_stmt_rrobin = "select * from RoundRobinRatingsPart" + str_part + " where rating = " + str_rating_val
        cur.execute(select_stmt_rrobin)
        rows2 = cur.fetchall()
        totalrows["RoundRobinRatingsPart" + str_part] = rows2
        #WriteFile(outputPath, "RoundRobinRatingsPart" + str_part, rows)

    WriteFile(outputPath, totalrows)