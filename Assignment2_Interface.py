#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import Assignment1 as a
# Donot close the connection inside this file i.e. do not perform openconnection.close()
#range__metadata = RangeRatingsMetadata
#roundR_metadata = RoundRobinRatingsMetadata
#rangetablepartition = rangeratingspart
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
	try:#Implement RangeQuery Here.
		cur = openconnection.cursor()
		ratings_Min = ratingMinValue
		ratings_Max = ratingMaxValue
		if ((0.0<=ratings_Min <= 5.0) and (0.0<=ratings_Max<= 5.0) and (ratings_Max >=ratings_Min)):
			cur.execute("SELECT maxrating from rangeratingsmetadata") ### Lines to make him look at
			upperbound_range = cur.fetchall() #print the last column of the select function execute above
			i=0
			#print upperbound_range
			while(1):			
				#print upperbound_range[i][0]
				if (ratings_Min > upperbound_range[i][0]):
					i = i+1
				else:
					lower_bound = i
					#print "the lower table index is", lower_bound
					break
			i = 0
			while(1):
				if (ratings_Max > upperbound_range[i][0]):
					i = i+1
				else:
					upper_bound = i
					#print "the upper table index is", upper_bound
					break
			
			range_list_table_lookup = range(lower_bound,upper_bound+1)
			#print range_list_table_lookup
			file = open("RangeQueryOut.txt","w")
			for l in range_list_table_lookup:
				rows = []
				cur.execute('SELECT * from rangeratingspart' + str(l)) ### Lines to make him look at
				rows = cur.fetchall()
				#print rows
				 ### Lines to make him look at
				for row in rows:
					rat = row[2]
					if (ratings_Min <= rat <= ratings_Max):
						file.write("{},{},{},{} \n".format("rangeratingspart" + str(l),row[0],row[1],row[2])) ### Lines to make him look at
			#file.close()
			cur.execute('SELECT * from RoundRobinRatingsMetadata')
			numberofpartitionslist = cur.fetchall()
			numberofpartitions = numberofpartitionslist[0][0]
			for l in range(numberofpartitions):
				cur.execute('SELECT * from RoundRobinRatingsPart' + str(l)) ### Lines to make him look at
				rows = []
				rows = cur.fetchall()
				
				for row in rows:
					rat = row[2]
					if (ratings_Min <= rat <= ratings_Max):
						file.write("{},{},{},{} \n".format("roundrobinratingspart" + str(l),row[0],row[1],row[2])) ### Lines to make him look at
			file.close()
		else:
			print ("Please enter the valid values")
			
		cur.close()
	except Exception as E:
		print E



def PointQuery(ratingsTableName, ratingValue, openconnection):
	#Implement PointQuery Here.
	# Remove this once you are done with implementation
	cur = openconnection.cursor()
	pointvalue = ratingValue
	if ((0.0<=pointvalue<= 5.0)):
		cur.execute('SELECT maxrating from RangeRatingsMetadata')
		Range_upper = cur.fetchall()
		i=0
		while(1):
			if (pointvalue > Range_upper[i][0]):
				i = i+1
			else:
				table_suffxi = i
				#print "the table suffix to look is", table_suffix
				break
		rows = []
		cur.execute('SELECT * from rangeratingspart'+str(table_suffix))
		rows = cur.fetchall()
		file1 = open("PointQueryOut.txt","w")
		for row in rows:
			rat = row[2]
			if (rat == pointvalue):
				file1.write("{},{},{},{} \n".format("rangeratingspart"+str(table_suffix),row[0],row[1],row[2]))
		#file1.close()
		cur.execute('SELECT * from RoundRobinRatingsMetadata')
		numberofpartitionslist = cur.fetchall()
		numberofpartitions = numberofpartitionslist[0][0]
		for l in range(numberofpartitions):
			cur.execute('SELECT * from RoundRobinRatingsPart'+str(l))
			rows = []
			rows = cur.fetchall()
			#file1 = open("PointQueryOut.txt","w")
			for row in rows:
				rat = row[2]
				if (rat == pointvalue):
					file1.write ("{},{},{},{} \n".format("roundrobinratingspart" + str(l),row[0],row[1],row[2]))
		file1.close()


	else:
		print("please enter a valid rating value")
		
	cur.close()

