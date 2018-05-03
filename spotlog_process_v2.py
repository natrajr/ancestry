''' 
PURPOSE OF THIS SCRIPT: 
(1) DOWNLOAD SPOTLOG FILE FROM AWS S3 
(2) SPOT CHECK DATA & FORMATTING OF FIELDS IN SPOTLOG FILE
(3) MERGE LIFT FROM ATTRIBUTION FILE WITH SPOT LOG FILE
(4) UPDATE A.OMDSPOTLOG TABLE
(5) UPDATE A.SPOTLOG_MIGRATION_OMD_AND_OCEAN TABLE
(6) WRITE FINAL MSL EXCEL FILE
'''

# Modules required for running this script. Runs on python 2.7
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


# Get the desired Spotlog file from S3 bucket
print "Accesing S3 bucket containing Spot Logs...Here are the Spot Logs on S3: "
subprocess.call('aws s3 ls s3://brandscience-analytic/ancestry/outbound/us/spots/', shell=True)
spotlogFilename=raw_input("From the list above, Copy/Paste the name of the Spot Log file you'd like to download:")


# Get the latest attribution file from S3
print "Accesing S3 bucket containing Attribution files...Attribution files on S3: "
subprocess.call('aws s3 ls s3://brandscience-analytic/ancestry/outbound/us/attribution/', shell=True)
attributionFilename=raw_input("From the list above, Copy/Paste the name of the Attribution file you'd like to download:")
attributionFilenameClean=attributionFilename.replace(' ', '_')


'''
# Get user input to set the destination for the Spot Log and attribution file downloads. Then download Spot Log from S3
spotLogdestination=raw_input("Provide a destination for the Spot Log file (No '/' at the end of the path). If left empty, it will go to your downloads folder: ")
attributionDestination=raw_input("Provide a destination for the Attribution file (No '/' at the end of the path). If left empty, it will go to your downloads folder: ")


if destination == "":
	destination = '~/Downloads/'+spotlogFilename
else:
	destination=destination+'/'+spotlogFilename
print "Downloading %s From S3 to %s ...\n" % (spotlogFilename, destination)
'''


# SET A STATIC VALUE FOR DESTINATIONS IF YOU DONT WANT TO INPUT THE VARIABLE EACH TIME THE SCRIPT RUNS
spotLogdestination='/Users/rnatraj/Documents/omd/spotlog_files/new_s3_raw_files/%s' % (spotlogFilename)
attributionDestination='/Users/rnatraj/Documents/omd/immediate_attribution_output/%s' % (attributionFilenameClean)

print 'Downloading Spot Log from S3...'
subprocess.call('aws s3 cp s3://brandscience-analytic/ancestry/outbound/us/spots/%s %s' % (spotlogFilename, spotLogdestination), shell= True)


print 'Downloading Attribution file from S3...'
subprocess.call("aws s3 cp s3://brandscience-analytic/ancestry/outbound/us/attribution/'%s' %s" % (attributionFilename, attributionDestination), shell= True)

# Read Spot Log data from the spot log destination
spotlog_data=pd.read_csv(spotLogdestination, sep = ',', low_memory=False, thousands =',')

# Read attribution data from the attributionb file destination. Only get the omd_id and lift columns. Rest of the file is the same as the spot log
attribution_data=pd.read_csv(attributionDestination, sep = ',', low_memory=False, thousands =',', usecols= ['omd_id','lift_total', 'lift_acom', 'lift_dna', 'Outlier', 'Outlier_Category'])


# Check omd_id field in both files for duplicate values. omd_id must be unique
print 'Checking Spot Log for duplicate omd_id values...'
sl.checkDuplicates(spotlog_data, 'omd_id')
print 'Checking Attribution file for duplicate omd_id values...'
sl.checkDuplicates(attribution_data, 'omd_id')


# Check the formatting of fields in the spot log using regular expressions. First argument is a dataframe, second is column/vector in the dataframe, third is the regular expression to check for
sl.checkColumnFormatting(spotlog_data, 'program_starttime' ,'^(\d|\d\d):\d\d:\d\d\s(AM|PM)$')
sl.checkColumnFormatting(spotlog_data, 'program_endtime' ,'(\d|\d\d):\d\d:\d\d\s(AM|PM)')
sl.checkColumnFormatting(spotlog_data, 'spot_starttime' ,'(\d|\d\d):\d\d:\d\d')
sl.checkColumnFormatting(spotlog_data, 'spotlength' ,'(:30|:15|:20|:60)')
sl.checkColumnFormatting(spotlog_data, 'spot_date' ,'(\d|\d\d)\/(\d|\d\d)\/\d\d')
sl.checkColumnFormatting(spotlog_data, 'calendar_spotdate' ,'(\d|\d\d)\/(\d|\d\d)\/\d\d')
sl.checkColumnFormatting(spotlog_data, 'week_of' ,'(\d|\d\d)\/(\d|\d\d)\/\d\d')
sl.checkColumnFormatting(spotlog_data, 'buy_type' ,'(U|S|PE)')
sl.checkColumnFormatting(spotlog_data, 'dp' ,'(C|S|X|B|D|F|L|M|N|O|P|A|W)')
sl.checkColumnFormatting(spotlog_data, 'c_b_s' ,'(National Cable|National Broadcast|Syndication)')
sl.checkColumnFormatting(spotlog_data, 'airing_type' ,'(Cable|Broadcast|Syndication)')
sl.checkColumnFormatting(spotlog_data, 'product' ,'(DUAL|DNA|ACOM)')
sl.checkColumnFormatting(spotlog_data, 'message' ,'(DUAL|DNA|ACOM)')
sl.checkColumnFormatting(spotlog_data, 'network_tier' ,'(small|medium|large|Syndication)')
sl.checkColumnFormatting(spotlog_data, 'grps' ,'\d+\.*\d+')
sl.checkColumnFormatting(spotlog_data, 'grps_actual_c3' ,'\d+\.*\d+')
sl.checkColumnFormatting(spotlog_data, 'grps_live_sd' ,'\d+\.*\d+')
sl.checkColumnFormatting(spotlog_data, 'grps_best_available' ,'\d+\.*\d+')
sl.checkColumnFormatting(spotlog_data, 'adj_impressions' ,'\d+\.*\d+')
sl.checkColumnFormatting(spotlog_data, 'imps_000' ,'\d+\.*\d+') 
sl.checkColumnFormatting(spotlog_data, 'imps_000_live_sd' ,'\d+\.*\d+')
sl.checkColumnFormatting(spotlog_data, 'imps_000_actual_c3' ,'\d+\.*\d+')
sl.checkColumnFormatting(spotlog_data, 'imps_000_best_available' ,'\d+\.*\d+')
sl.checkColumnFormatting(spotlog_data, 'spot_rate' ,'\d+\.*\d+')


# Check max date in Spot Log. Change spot_date from string to datetime
print "Checking the max date in the Spot Log File..."
maxSpotdate=max(pd.to_datetime(spotlog_data['spot_date']))
confirmSpotlogDate=raw_input("The max date in the Spot Log is %s. Confirm (y/n)?: " % (maxSpotdate))


# Check number of spots, total impressions, & spend for the prior week
print "Checking the daily total spots, C3 impressions, & spend for the last week..."
check_spotlog_group=spotlog_data[[ 'omd_id','calendar_spotdate', 'imps_000', 'spot_rate']]
check_spotlog_group['calendar_spotdate']=pd.to_datetime(check_spotlog_group['calendar_spotdate'])
check_spotlog_group=check_spotlog_group.groupby(['calendar_spotdate'], sort=True).agg({'omd_id':'count', 'imps_000': 'sum','spot_rate': 'sum'})
check_spotlog_group=check_spotlog_group.rename(columns={'omd_id': 'number_spots', 'imps_000': 'total_imps_000', 'spot_rate': 'total_spot_rate'})
print check_spotlog_group.tail(n=10)
confirmSpotChecks=raw_input("Do the values for the metrics above make sense (y/n)?:")


# Remove apostrophes from creative_title & creative_title_uniform. Apostrophe's can cause issues when running COPY statement to upload data to Redshift
spotlog_data['creative_title']=spotlog_data['creative_title'].str.replace("'","")
spotlog_data['creative_title_uniform']=spotlog_data['creative_title_uniform'].str.replace("'","")


# Join columns from attribution file to spot log file using omd_id field
spotlog_attributed_data=pd.merge(spotlog_data, attribution_data, how='left', on='omd_id', sort=True, copy=True, indicator=False)


# Write spot log data with lift attribution to a csv. This csv will uploaded to the a.omdspotlog table 
spotlog_attributed_data.to_csv('/Users/rnatraj/Documents/omd/spotlog_files/spotlog_with_attribution/attributed_%s' % (spotlogFilename), index=False, float_format='%f', date_format='%m/%d/%Y')


# If date is confirmed, the number of spots checks out & formatting of cols is good, upload the attributed spot log data to the S3 upload folder. Then run the COPY statement.
if (confirmSpotlogDate.lower() == 'y' ) and (confirmSpotChecks.lower() == 'y'):
	timestamp=dt.datetime.now()
	confirm_s3upload_conn=raw_input("Connect to the BIT S3upload folder before proceeding!! Are you connected? (y/n)?:")
	
	# WHILE LOOP HERE TO WAIT TILL USER IS CONNECTED TO S3 UPLOAD FOLDER PERHAPS?
	if confirm_s3upload_conn.lower() == 'y':
		print 'Uploading Spot Log file to BIT S3 upload folder...'
		subprocess.call("cp /Users/rnatraj/Documents/omd/spotlog_files/spotlog_with_attribution/attributed_%s //Volumes/s3upload/attributed_%s" % (spotlogFilename,spotlogFilename), shell= True)
		print 'Upload Complete at %s. Wait ~5 mins for file to be swept to s3://acom-redshift-analytics/S3upload/. DO NOT CLOSE YOUR TERMINAL' % (timestamp)
		
		# Wait a few minutes after uploading to s3Upload folder. Then check if the attributed spot log file is in the s3Upload folder
		time.sleep(300)

		# Run checks3UploadFolder function to check whether spot log has been swept to S3 bucket
		checkVal=sl.checks3UploadFolder(spotlogFilename)

		if checkVal == 1:
			
			# Before uploading the new data to Redshift, clear/truncate the a.omdspotlog table
			sl.truncateOmdSpotlogTable()
			# Run the COPY statement to upload data from S3 bucket to a.omdspotlog table
			sl.copyToRedshift(spotlogFilename)
			# Merge Ocean data with new spot log data and update the a.spotlog_migration_omd_and_ocean table.
			sl.mergeMSLData()

			# Open a connection to the Ancestry DB
			db=db_connection()
			
			# Get data from a.spotlog_migration_omd_and_ocean to write the MSL excel file
			FinalMSLData=sl.getFinalMSLData(db)

			# Get weekly data for weekly performance grid
			print 'Getting weekly data for performance grid...'
			weekly_grid_data=sl.getPerformanceGridData(db,sl.weekly_DRTV_NTV_GridQuery)
			print 'Getting pct changes for weekly grid...'
			weekly_data_with_change=sl.getPercentChanges(weekly_grid_data, ['ntv_spend', 'drtv_spend', 'ntv_imps', 'drtv_imps'])

			# Get monthly data for weekly performance grid
			print 'Getting monthly data for performance grid...'
			monthly_grid_data=sl.getPerformanceGridData(db,sl.monthlyPerformanceGridQuery)
			print 'Getting pct changes for monthly grid...'
			monthly_data_with_change=sl.getPercentChanges(monthly_grid_data, ['tv_spend', 'total_imps', 'tv_kits_sold', 'tv_signups', 'cpk', 'cps', 'nmuv_tv_visits'])

			# Close the DB connection
			db.dispose()

			# Set the filename for the final MSL file
			spotlogFilenameDate=re.search('\d\d\d\d-\d\d-\d\d', spotlogFilename).group(0).replace('-','_')
			print "Writing MSL File..."

			# Write the MSL excel file 
			writer = pd.ExcelWriter('/Users/rnatraj/Documents/omd/spotlog_files/final_msl_files/excel_files/%s_SPOTLOG_MIGRATION_FILE.xlsx' % (spotlogFilenameDate))
			print 'Writing Spot log data...'
			FinalMSLData.to_excel(writer, 'Sheet1', index=False)
			print 'Writing Weekly Grid data...'
			weekly_data_with_change.to_excel(writer, 'Sheet2', index=False)
			print 'Writing Monthly Grid data...'
			monthly_data_with_change.to_excel(writer, 'Sheet3', index=False)
			writer.save()
			print "Done writing MSL File!"
