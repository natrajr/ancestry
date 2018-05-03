''' PURPOSE: 
(1) DOWNLOAD SPOTLOG FILE FROM AWS S3 
(2) CREATE NEW OMD_ID FIELD OF TYPE 'INT' TO REPLACE ALPHANUMERIC OMD_ID FIELD. CHECK FOR DUPLICATES IN OMD_ID FIELD
(3) CHECK MAX(DATE) AND FORMATTING OF FIELDS IN SPOTLOG FILE
(4) PUSH SPOTLOG FILE TO \\CLBIT03SQLD101A\FileImporterExternal FOR BIT ETL PROCESS TO UPDATE S.OMDSPOTLOG TABLE
(5) UPDATE A.SPOTLOG_MIGRATION_OMD_AND_OCEAN TABLE
(6) WRITE MSL EXCEL FILE
'''
import numpy as np
import pandas as pd
import subprocess
import datetime as dt
import time
import re
from sqlalchemy import *
import psycopg2
from setup import * # Sets up DB connections. Setup.py must be in working directory
import spotlog_lib as sl


# Get the desired Spotlog file from S3
print "Accesing S3 bucket containing Spot Logs...Here are the Spot Logs on S3: "
subprocess.call('aws s3 ls s3://brandscience-analytic/ancestry/outbound/us/spots/', shell=True)
spotlogFilename=raw_input("From the list above, Copy/Paste the name of the Spot Log file you'd like to download:")


# Set the destination for the Spot Log file download. Then download Spot Log from S3
destination=raw_input("Provide a filepath for downloading the Spot Log (Don't need a '/' at the end of the path). If left empty, it will go to your downloads folder: ")
if destination == "":
	destination = '~/Downloads/'+spotlogFilename
else:
	destination=destination+'/'+spotlogFilename
print "Downloading %s From S3 to %s ...\n" % (spotlogFilename, destination)
subprocess.call('aws s3 cp s3://brandscience-analytic/ancestry/outbound/us/spots/%s %s' % (spotlogFilename, destination), shell= True)

# Read Spot Log data from the destination
spotlog_data=pd.read_csv(destination, sep = ',', low_memory=False)


# Check original omd_id field for duplicates before creating new omd_id. OMD should send uniques
sl.checkDuplicates(spotlog_data, 'omd_id')


# Check the formatting of fields
sl.checkColumnFormatting(spotlog_data, 'property_starttime' ,'^(\d|\d\d):\d\d:\d\d\s(AM|PM)$')
sl.checkColumnFormatting(spotlog_data, 'property_endtime' ,'(\d|\d\d):\d\d:\d\d\s(AM|PM)')
sl.checkColumnFormatting(spotlog_data, 'spot_starttime' ,'(\d|\d\d):\d\d:\d\d')
sl.checkColumnFormatting(spotlog_data, 'spotlength' ,'(:30|:15|:20)')
sl.checkColumnFormatting(spotlog_data, 'spot_date' ,'(\d|\d\d)\/(\d|\d\d)\/\d\d\d\d')
sl.checkColumnFormatting(spotlog_data, 'week_of' ,'(\d|\d\d)\/(\d|\d\d)\/\d\d\d\d')
sl.checkColumnFormatting(spotlog_data, 'buy_type' ,'(U|S|PE)')
sl.checkColumnFormatting(spotlog_data, 'dp' ,'(C|S|X|B|D|F|L|M|N|O|P|A|W)')
sl.checkColumnFormatting(spotlog_data, 'c_b_s' ,'(National Cable|National Broadcast|Syndication)')
sl.checkColumnFormatting(spotlog_data, 'clearance' ,'(Cleared|cleared)')
sl.checkColumnFormatting(spotlog_data, 'product' ,'(DUAL|DNA|ACOM)')
sl.checkColumnFormatting(spotlog_data, 'message' ,'(DUAL|DNA|ACOM)')


# New list to hold incrementally increasing integer vals which will replace the alphanumeric IDs in omd_id field. omdIdCounter used in FOR LOOP
new_omdIDs=[]
omdIdCounter=0

# Create list of new OMD IDs
for ID in spotlog_data.omd_id:
	omdIdCounter+=1
	new_omdIDs=np.append(new_omdIDs, omdIdCounter)

# Add new_omdID to spotlog_data. Cast new_omdID as an integer (it's float otherwise)
print "Replacing alphanumeric omd_id column with incrementally increasing integers...\n"
spotlog_data['new_omd_id']=new_omdIDs.astype(int)

# Delete original alphanumeric omd_id field. Rename new_omd_id column to omd_id. Reorder columns to align with table structure
del spotlog_data['omd_id']
spotlog_data = spotlog_data.rename(columns={'new_omd_id': 'omd_id'})
spotlogColumns = spotlog_data.columns.tolist()
spotlogColumns = spotlogColumns[-1:] + spotlogColumns[:-1]
spotlog_data = spotlog_data[spotlogColumns]


# Check max date in Spot Log. Change spot_date from string to datetime
print "Checking the max date in the Spot Log File..."
maxSpotdate=max(pd.to_datetime(spotlog_data['spot_date'], format='%m/%d/%Y'))
confirmSpotlogDate=raw_input("The max date in the Spot Log is %s. Confirm (y/n)?: " % (maxSpotdate))

# Check number of spots, total impressions, & spend
print "Checking the daily total spots, C3 impressions, & spend for the last week..."
spotlog_data['calendar_spotdate']=pd.to_datetime(spotlog_data['calendar_spotdate'])
check_spotlog_group=spotlog_data.groupby(['calendar_spotdate'], sort=True).agg({'omd_id':'count', 'imps_000': 'sum','spot_rate': 'sum'})
check_spotlog_group=check_spotlog_group.rename(columns={'omd_id': 'number_spots', 'imps_000': 'total_imps000', 'spot_rate': 'total_spot_rate'})


print check_spotlog_group.tail(n=10)
confirmSpotChecks=raw_input("Do the values for the metrics above make sense (y/n)?:")


# If date is confirmed, the number of spots checks out & formatting of cols is good 
## Output spotlog_data to csv using BIT naming convention. Then copy the file to BIT file importer

if (confirmSpotlogDate == 'Y' or confirmSpotlogDate == 'y') and (confirmSpotChecks == 'Y' or confirmSpotChecks == 'y'):
	
	spotlogFilenameDate=re.search('\d\d\d\d-\d\d-\d\d', spotlogFilename).group(0).replace('-','_')
	#BitOutputFilename=spotlogFilenameDate+'_omd_spotlog_file.csv'
	BitOutputFilename='2017_05_12_omd_spotlog_file.csv'
	localBITfileDest=raw_input("Provide a filepath for a copy of the file that's uploaded to BIT (no '/' at the end of the path). If empty, it goes to your downloads folder:")
	if localBITfileDest == "":
		localBITfileDest="~/Downloads/"+BitOutputFilename
	else:
		localBITfileDest=localBITfileDest+'/'+BitOutputFilename
	print "Local copy of BIT Spot Log file is being output to %s" % (localBITfileDest)
	spotlog_data.to_csv(localBITfileDest, sep= ',', index=False, float_format='%f')	
	print "The Spot Log file is being copied to BIT's file importer"
	subprocess.call("cp %s /Volumes/fileimporterexternal/%s" % (localBITfileDest,BitOutputFilename), shell= True)
	timestamp=dt.datetime.now()
	print 'DONE! Loaded to BIT at %s.\n' % (timestamp)
	print "DO NOT CLOSE YOUR TERMINAL OR EXIT THIS SCRIPT. Starting in 10mins, s.omdspotlog will be periodically checked\n"
	time.sleep(600)

	print "The s.omdspotlog table will be checked about every 5 minutes for the update. DO NOT CLOSE YOUR TERMINAL OR EXIT THIS SCRIPT"
	# Create db connection
	print "Connecting to Ancestry DB to check s.omdspotlog table"
	db=db_connection()
	
	# Run an initial query to check max(spot_date) in s.omdspotlog
#todayStringTimestamp=dt.datetime.now()
	maxSpotdate=str(maxSpotdate.year)+'/'+str(maxSpotdate.month)+'/'+str(maxSpotdate.day)
#todayString=str(todayStringTimestamp.year)+'/'+str(todayStringTimestamp.month)+'/'+str(todayStringTimestamp.day)
	omdspotlogTableDate=sl.checkOmdSpotLogTable(db)

	while omdspotlogTableDate != maxSpotdate:
		print "The s.omdspotlog table has not been updated"
		omdspotlogTableDate=sl.checkOmdSpotLogTable(db)
		time.sleep(300)
	print "The s.omdspotlog table has been updated. Running the Ocean/OMD merge script"


	# Update the Master Spot Log Table & Merge Ocean & OMD data
	sl.updateMasterSpotLogTable()


	# Prepare Excel MSL file
	db=db_connection()
	print "Running Query to get data for Excel MSL File..."
	ExcelMSLData=sl.getFinalMSLData(db)
	db.dispose()
	print "Writing MSL Excel File..."
	writer = pd.ExcelWriter('/Users/rnatraj/Documents/omd/spotlog_files/final_msl_files/excel_files/%s_SPOTLOG_MIGRATION_FILE.xlsx' % (spotlogFilenameDate))
	ExcelMSLData.to_excel(writer, 'Sheet1', index=False)
	writer.save()
	print "Done writing MSL Excel File!"
