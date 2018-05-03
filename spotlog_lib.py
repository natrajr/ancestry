# coding: utf-8
# Spotlog Library containing functions & SQL queries to process spotlog file from OMD
import os, sys
import numpy as np
import pandas as pd
import re
import datetime as dt
import time
from setup import * # Sets up DB connections. Setup.py must be in working directory
from sqlalchemy import *
import psycopg2


# Function checks a column in a dataframe for duplicates. Outputs number of dupes.
def checkDuplicates(dataframe, df_column):
	print "Checking for dupes in the %s column" % (df_column)
	duplicate_counter = 0
	duplicate_values= []
	duplicated_results = dataframe.duplicated(subset= df_column)
	for index,val in enumerate(duplicated_results):
		if val == True:
			duplicate_counter+=1
			duplicate_values=np.append(duplicate_values, dataframe.omd_id[index])
		else:
			duplicate_counter=duplicate_counter
	if duplicate_counter > 0:
		print "There are %s duplicates in the %s column" % (duplicate_counter, df_column)		
		print duplicate_values
	else:
		print "There are no duplicates in the %s column \n" % (df_column)

# Checks if the formatting in a column matches format that is expected to be in the column (based on regex parameter). Check for blanks in the column as well
def checkColumnFormatting(dataframe, column, regex):
	errorArray=[]
	errorCount=0
	blanksCount=0
	checkColumn=dataframe[column]
	checkColumn = checkColumn.fillna('')
	for index,val in enumerate(checkColumn):
		val=str(val)
		result = bool(re.match(regex, val))
		if val == '':
			blanksCount+=1
		if result:
			errorCount=errorCount
		elif result == False and val != '':
			errorCount+=1
			stringIndex=str(index)
			errorArray=np.append(errorArray, stringIndex)
	if errorCount == 0:
		print "The %s column is formatted correctly. There are %s blanks in this column" % (column, blanksCount)
	else:
		print "There are %s INCORRECTLY FORMATTED values in the %s column. Here are the indexes for them" % (errorCount, column)
		print errorArray
		print "There are %s blanks in the %s column" % (blanksCount, column)

# Function runs sql query to truncate a.omdspotlog table
def truncateOmdSpotlogTable():
	print "Truncating a.omdspotlog..."
	executeQuery(truncateQuery)
	print "Done truncating a.omdspotlog"


# Function that checks the BIT-managed s3upload folder for a file specific by the `filename` argument
def checks3UploadFolder(filename):
	print 'Checking s3Upload folder for %s file...' % (filename)
	checkpath='//Volumes/s3upload/attributed_%s' % (filename)
	while os.path.exists(checkpath) == True:
		print '%s: File still in s3Upload folder. Waiting for another 4mins...' % (str(dt.datetime.now()))
		time.sleep(240)
		checks3UploadFolder(filename)
	print 'File has been swept into S3 Analyst bucket!'
	return 1


# Function that runs a COPY statement to upload data from a file specified by the `filename` argument in the S3 bucket to a.omdspotlog table
def copyToRedshift(filename):
	copyQuery="copy a.omdspotlog from 's3://acom-redshift-analytics/s3upload/attributed_%s' credentials 'aws_iam_role=arn:aws:iam::691122639733:role/Redshift-analysts' CSV COMPUPDATE OFF STATUPDATE OFF IGNOREHEADER 1 DATEFORMAT AS 'MM/DD/YYYY' ACCEPTINVCHARS '^';" % (filename)
	print '%s: Running COPY statement to upload Spot Log to a.omdspotlog table...' % (str(dt.datetime.now()))
	executeQuery(copyQuery)

# Function runs sql query to merge Ocean & OMD spot log data and update a.spotlog_migration_omd_and_ocean table
def mergeMSLData():
	print "%s: Merging Ocean & OMD data, then inserting new data into a.spotlog_migration_omd_and_ocean..." % (str(dt.datetime.now()))
	executeQuery(mergeMSLDataQuery)
	print "Done merge/update of a.spotlog_migration_omd_and_ocean!"
	
# Function runs sql query to get data from a.spotlog_migration_omd_and_ocean table for final Master Spot log excel file
def getFinalMSLData(db):
	print "Getting MSL Data..."
	finalMSLData=pd.read_sql(getMSLDataquery, db)
	return finalMSLData

# Function that runs SQL query to get data from a.spotlog_migration_omd_and_ocean table for Performance Tracking Grid
def getPerformanceGridData(db,sql_query):
	grid_data=pd.read_sql(sql_query,db)
	return grid_data


# Function accepts a dataframe and array of columns. For each column in the df, percent change is calculated. The full df is returned 
def getPercentChanges(dataframe, columns):
	for column in columns:
		dataframe['%s_pct_change' % (column)]=dataframe[column].pct_change(periods=1)
	return dataframe




############################ SQL QUERIES BELOW ########################################################################################


# Query used to truncate omd spot log tables before inserting new data
truncateQuery='''truncate table a.omdspotlog;'''

# Query used to truncate/insert data into the a.spotlog_migration_omd_and_ocean table
mergeMSLDataQuery='''BEGIN;
truncate table a.spotlog_migration_omd_and_ocean;
insert into a.spotlog_migration_omd_and_ocean
select
a.agency_dsc,
a.omd_id,
a.network,
a.property,
a.program_starttime,
a.program_endtime,
a.program_aired,
a.buy_type,
a.dp,
a.spotlength,
a.spot_date,
a.spot_starttime,
a.c_b_s,
a.isci,
a.daypart,
a.broadcast_week,
a.broadcast_month,
a.quarter,
a.uniform_network,
a.airing_type,
a.isci_adjusted,
a.creative_title,
a.spot,
a.buy_type_category,
a.celebrity,
a.wdytya_premiere,
a.adu,
a.paid_unit,
a.broadcast_year,
a.broadcast_dow,
a.adj_impressions,
a.product,
a.message,
a.media_schedule,
a.repeat,
a.calendar_spotdate,
a.calendar_month,
a.calendar_dow,
a.imps_000,
a.position,
a.pod_position,
a.funded_by,
a.grps,
a.net_rate,
a.full_rate,
a.rate,
a.creative_campaign,
a.grps_live_sd,
a.imps_000_live_sd,
a.grps_actual_c3,
a.imps_000_actual_c3,
a.feed,
a.live_telecast,
a.genre_program,
a.network_tier,
a.genre_network,
a.creative_title_uniform,
a.grps_best_available,
a.imps_000_best_available,
a.lift_total,
a.lift_acom,
a.lift_dna,
a.outlier,
a.outlier_category,
row_number() over (order by spot_date asc) as row_number
from
(
select
cast('Ocean' as varchar(100)) as agency_dsc,
cast(id as varchar(250)) as omd_id,
cast(network as  varchar(250)) as network,
cast(property as varchar(250)) as property,
cast(property_starttime as varchar(50)) as program_starttime,
cast(property_endtime as varchar(50)) as program_endtime,
cast(null as varchar(200)) as program_aired,
cast(buy_type as varchar(50)) as buy_type,
cast(dp as varchar(15)) as dp,
cast(spotlength as varchar(15)) as spotlength,
cast(spot_date as date) as spot_date,
cast(spot_starttime as varchar(50)) as spot_starttime,
cast(c_b_s as varchar(50)) as c_b_s,
cast(isci as varchar(50)) as isci,
cast(daypart as varchar(50)) as daypart,
cast(broadcast_week as date) as broadcast_week,
cast(broadcast_month as date) as broadcast_month,
cast(broadcast_quarter as varchar(15)) as quarter,
cast(uniform_network as varchar(250)) as uniform_network,
cast(airing_type as varchar(50)) as airing_type,
cast(isci_adjusted as varchar(50)) as isci_adjusted,
cast(creative_title as varchar(200)) as creative_title,
cast(spot as varchar(5)) as spot,
cast(buy_type_category as varchar(150)) as buy_type_category,
cast(celebrity as varchar(200)) as celebrity,
cast(wdytya_premiere as varchar(100)) as wdytya_premiere,
cast(adu as varchar(10)) as adu,
cast(null as varchar(10)) as paid_unit,
cast(broadcast_year as varchar(10)) as broadcast_year,
cast(dow as varchar(50)) as broadcast_dow,
cast(adj_impressions as numeric(15,5)) as adj_impressions,
cast(product as varchar(50)) as product,
cast(message as varchar(50)) as message,
cast(media_schedule as varchar(50)) as media_schedule,
cast(broadcast_rerun as varchar(50)) as repeat,
cast(calendar_spotdate as date) as calendar_spotdate,
cast(calendar_month as date) as calendar_month,
cast(calendar_dow as varchar(100)) as calendar_dow,
cast(imps_000 as numeric(15,5)) as imps_000,
cast(position as numeric(15,5)) as position,
cast(null as numeric(15,5)) as pod_position,
cast(funded_by as varchar(50)) as funded_by,
cast(grps as numeric(15,5)) as grps,
cast(net_spend as numeric(15,5)) as net_rate,
creative_campaign,
cast(null as numeric(15,5)) as grps_live_sd,
cast(null as numeric(15,5)) as imps_000_live_sd,
cast(null as numeric(15,5)) as grps_actual_c3,
cast(null as numeric(15,5)) as imps_000_actual_c3,
cast(null as varchar(50)) as feed,
cast(null as varchar(150)) as live_telecast,
cast(null as varchar(250)) as genre_program,
cast(network_tier as varchar(150)) as network_tier,
cast(null as varchar(250)) as genre_network,
cast(null as varchar(250)) as creative_title_uniform,
cast(null as numeric(15,5)) as grps_best_available,
cast(null as numeric(15,5)) as imps_000_best_available,
cast(null as numeric(15,5)) as lift_total,
cast(null as numeric(15,5)) as lift_acom,
cast(null as numeric(15,5)) as lift_dna,
cast(spend as numeric(15,5)) as rate,
cast(full_rate as numeric(15,5)) as full_rate,
cast(null as varchar(50)) as outlier,
cast(null as varchar(200)) as outlier_category
from a.ocean_spotlog
union all
select 
cast('OMD' as varchar(100)) as agency_dsc,
omd_id,
network,
property,
program_starttime,
program_endtime,
program_aired,
buy_type,
dp,
spotlength,
spot_date,
spot_starttime,
c_b_s,
isci,
daypart,
week_of as broadcast_week,
month as broadcast_month,
quarter,
uniform_network,
airing_type,
isci_adjusted,
creative_title,
spot,
buy_type_category,
celebrity,
wdytya_premiere,
adu,
paid_unit,
broadcast_year,
broadcast_dow,
adj_impressions,
product,
message,
media_schedule,
repeat,
calendar_spotdate,
calendar_month,
calendar_dow,
imps_000,
position,
pod_position,
funded_by,
grps,
cast(spot_rate as numeric(15,5)) as net_rate,
creative_campaign,
grps_live_sd,
imps_000_live_sd,
grps_actual_c3,
imps_000_actual_c3,
feed,
live_telecast,
genre_program,
network_tier,
genre_network,
creative_title_uniform,
grps_best_available,
imps_000_best_available,
lift_total,
lift_acom,
lift_dna,
cast(null as numeric(15,5)) as rate,
cast(null as numeric(15,5)) as full_rate,
outlier,
outlier_category
from a.omdspotlog) a
order by a.spot_date asc;
update a.spotlog_migration_omd_and_ocean set imps_000_best_available = case when agency_dsc= 'Ocean' then imps_000 else imps_000_best_available end;
update a.spotlog_migration_omd_and_ocean set creative_title = replace(creative_title, '^','');
update a.spotlog_migration_omd_and_ocean set creative_title_uniform = replace(creative_title_uniform, '^','');
COMMIT;
'''

#grantQuery='''GRANT SELECT ON a.spotlog_migration_omd_and_ocean TO tbowe,bpurcell,ochen,lmcmurchie;'''

# Query used to get data from the a.spotlog_migration_omd_and_ocean table for the Master Spot Log excel file
getMSLDataquery="""
select 
row_number,
agency_dsc,
network,
uniform_network,
genre_network,
c_b_s,
property,
program_starttime,
program_endtime,
program_aired,
genre_program,
creative_campaign,
creative_title,
creative_title_uniform,
buy_type,
buy_type_category,
funded_by,
product,
message,
spot_date,
calendar_spotdate,
calendar_month,
calendar_dow,
spot_starttime,
spotlength,
broadcast_week,
quarter,
broadcast_month,
broadcast_dow,
dp,
daypart,
position,
pod_position,
isci,
isci_adjusted,
airing_type,
spot,
celebrity,
wdytya_premiere,
adu,
paid_unit,
media_schedule,
repeat,
net_rate,
rate,
full_rate,
imps_000_best_available,
imps_000,
imps_000_live_sd,
imps_000_actual_c3,
adj_impressions,
grps_best_available,
grps,
grps_live_sd,
grps_actual_c3,
lift_total,
lift_acom,
lift_dna,
outlier,
outlier_category
from a.spotlog_migration_omd_and_ocean
order by row_number asc"""


weekly_DRTV_NTV_GridQuery="""
select date_trunc('week',calendar_spotdate) as calendar_week,
sum(net_rate) as total_tv_spend,
sum(imps_000) as total_imps,
sum(case when (lower(buy_type_category) = 'fixed' and c_b_s ='National Cable') or (lower(buy_type_category) in ('fixed', 'dr') and c_b_s in ('National Broadcast', 'Syndication')) then net_rate else null end) as ntv_spend,
sum(case when buy_type_category = 'DR' and c_b_s = 'National Cable' then net_rate else null end) as drtv_spend,
sum(case when (lower(buy_type_category) = 'fixed' and c_b_s ='National Cable') or (lower(buy_type_category) in ('fixed', 'dr') and c_b_s in ('National Broadcast', 'Syndication')) then imps_000 else null end) as ntv_imps,
sum(case when buy_type_category = 'DR' and c_b_s = 'National Cable' then imps_000 else null end) as drtv_imps
from a.spotlog_migration_omd_and_ocean
where calendar_spotdate >= '2017-09-01'
group by 1
order by 1 asc
"""


monthlyPerformanceGridQuery="""
select a.month_of,
sl.tv_spend,
sl.total_imps,
dna.tv_kits_sold,
fs.tv_signups,
fs.tv_signups+dna.tv_kits_sold as total_orders,
sl.tv_spend/(fs.tv_signups+dna.tv_kits_sold) as cpo,
sl.tv_spend/dna.tv_kits_sold as cpk,
sl.tv_spend/(fs.tv_signups) as cps,
a.nmuv_tv_visits,
a.nmuv_tv_acom_visits,
a.nmuv_tv_dna_visits, 
sl.acom_spend,
sl.acom_imps,
sl.dna_spend,
sl.dna_imps,
sl.dual_spend,
sl.dual_imps,
r.tv_registrations,
fs.tv_subscriptions
from
(select date_trunc('month', fv.servertimemst) as month_of,
count(case when bouncedvisit = 0 and fv.entryusertypeid in (0,5,6,7,8,10,11) and visittraffictype in ('core brand', 'dna brand') then fv.visitorid else null end) as nmuv_tv_visits,
count(case when bouncedvisit = 0 and fv.entryusertypeid in (0,5,6,7,8,10,11) and visittraffictype in ('core brand') then fv.visitorid else null end) as nmuv_tv_acom_visits,
count(case when bouncedvisit = 0 and fv.entryusertypeid in (0,5,6,7,8,10,11) and visittraffictype in ('dna brand') then visitorid else null end) as nmuv_tv_dna_visits
from  p.Fact_Visits fv
inner join p.dim_site ds on ds.siteid = fv.siteid and ds.siteid = 3713
JOIN p.dim_useragent     AS ua ON fv.useragentstring = ua.useragentstring
JOIN p.dim_entrypagename AS ep ON fv.entrypagenameid = ep.entrypagenameid
JOIN p.dim_promotion     AS pr ON fv.firstpromotionid = pr.promotionid
join p.dim_usertype as ut on fv.entryusertypeid = ut.usertypeid
where fv.servertimemst >= '2016-01-01'
and visitnumber != 0
and spidervisit = 0
Group by date_trunc('month', fv.servertimemst)
) a
left join
(select date_trunc('month', calendar_spotdate) as month_of,
sum(net_rate) as tv_spend,
sum(imps_000) as total_imps,
sum(case when message = 'ACOM' then net_rate  else null end) as acom_spend,
sum(case when message = 'ACOM' then imps_000 else null end) as acom_imps,
sum(case when message = 'DNA' then net_rate  else null end) as dna_spend,
sum(case when message = 'DNA' then imps_000 else null end) as dna_imps,
sum(case when message = 'DUAL' then net_rate  else null end) as dual_spend,
sum(case when message = 'DUAL' then imps_000 else null end) as dual_imps,
sum(case when lower(buy_type_category) = 'fixed' then net_rate else null end) as ntv_spend,
sum(case when buy_type_category = 'DR' then net_rate else null end) as drtv_spend,
sum(case when lower(buy_type_category) = 'fixed' then imps_000 else null end) as ntv_imps,
sum(case when buy_type_category = 'DR' then imps_000 else null end) as drtv_imps
from a.spotlog_migration_omd_and_ocean
group by 1) sl on sl.month_of = a.month_of
left join
(select date_trunc('month',fr.registrationdate) as month_of,
count(case when dp.subchannel in ('Unknown','Paid Search – Brand','Direct', 'Paid Search Brand') then prospectid else null end) as tv_registrations
from p.fact_registrant fr
inner join p.dim_site ds on ds.siteid = fr.registrationsiteid and ds.siteid = 3713
inner join p.dim_promotion dp on fr.externalregistrantpromotionid = dp.promotionid
group by 1) r on r.month_of = a.month_of
left join
(select date_trunc('month',fs.signupcreatedate) as month_of,
sum(case when dp.subchannel in ('Unknown','Paid Search – Brand','Direct','Web Property', 'Paid Search Brand') then fs.signupquantity else null end) as tv_signups,
sum(case when dp.subchannel in ('Unknown','Paid Search – Brand','Direct','Web Property', 'Paid Search Brand') and netbillthroughquantity > 0 then fs.signupquantity else null end) as tv_subscriptions
from p.fact_subscription fs
inner join p.dim_programin dpi on fs.programinid = dpi.programinid and dpi.programinparentdescription = 'New'
inner join p.dim_promotion dp on fs.promotionid = dp.promotionid
inner join p.dim_subscriptiontype dst on fs.subscriptiontypeid = dst.subscriptiontypeid
inner join p.dim_country dc on dc.countryid = fs.countryid and countrysubregiondescription = 'US'
group by 1) fs on fs.month_of = a.month_of
left outer join
(select date_trunc('month',fns.ordercreatedate) as month_of,
sum(case when dp.subchannel in ('Unknown','Paid Search – Brand','Direct', 'Paid Search Brand') then fns.netsalesquantity else null end) as tv_kits_sold
from p.fact_netsales fns
inner join p.dim_promotion dp on fns.promotionid = dp.promotionid
inner join p.dim_product dpr on fns.productid = dpr.productid
inner join p.dim_productdnareporting dna on dpr.sku = dna.sku
inner join p.dim_site ds on ds.siteid = fns.siteid and ds.siteid = 3713
group by 1) dna on dna.month_of = a.month_of
order by a.month_of asc
"""

