"""
Created on Thu Jul  7 16:15:37 2022

@author: DE Team
"""

'''  ##Notes from Mitch O. on previous procedures used
I just truncated RawDb.OptumRxQuarterlyRebatesRawArchive, moved the data from RawDb.OptumRxQuarterlyRebatesRaw to that Archive, 
truncated RawDb.OptumRxQuarterlyRebatesRaw and then did an insert of the new spreadsheet (it is a full replace file).  
The two wrinkles, as I remember, are:
----	For some insane reason, they put a long text note in cell A1 on the spreadsheet, so before you use Pandas or anything to move the data, 
you’ll have to set up an instance of Excel within your Python script and delete that row.
----	They used a dash for any NULL numeric cells, which would throw off your Pandas object.  You’ll have to either replace them beforehand 
(and don’t replace *all*, since there are genuine negative amounts on the sheet, just ones where it’s the entire cell), or use a filter on the 
Pandas object somewhere. 
----    You will need to replace ',' with a space in PICO to upload csv properly in SS import tool 
'''


###basic load
#sourcing
import datetime
from dateutil.relativedelta import relativedelta
#gen lib
import pandas as pd
import pyodbc
import numpy as np
import sqlalchemy
import sys
 

#Set Date domains to make files look neat & pretty
today = datetime.date.today()#+relativedelta(years=1)
dateset = today
mon = str(dateset.month).rjust(2,'0')
day = str(dateset.day).rjust(2,'0')
#year has an added 1 to it to match quarter year format optum sends us
yr = str(dateset.year)[-4:]+'1'
current_year_date = str(dateset.year)[-4:]

#I did this to get only the date to compare it to Quarterly year 
current_date = datetime.datetime.now() #+relativedelta(years=1)
year = int(current_date.strftime("%Y"))



def CallStoredProc(conn1, procName, *args):
    sql = """DECLARE @ret int
             EXEC @ret = %s %s
             SELECT @ret""" % (procName, ','.join(['?'] * len(args)))
    return int(conn1.execute(sql, args).fetchone()[0])



conn1 = pyodbc.connect('Driver={SQL Server};'   
                      'Server=32BJHFSQL1A;'   ##SERVER NAME
                      'Database=Health_Fund_Working;'  ##DATABASE
                      'Schema=OptumRx'   ##SCHEMA
                      'Trusted_Connection=yes;')
cursor1 = conn1.cursor()
conn1.autocommit = True

###################################Connection to DB############################################################

#Connect to DB via SQL Server
conn = pyodbc.connect('Driver={SQL Server};'   
                      'Server=32BJHFSQL1A;'   ##SERVER NAME
                      'Database=DbRaw;'  ##DATABASE
                      'Schema=OptumRx'   ##SCHEMA
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
conn.autocommit = True

#testing to see if it needs to be the full server name
print('SQL1A')

con = sqlalchemy.create_engine('mssql://32bjHFSQL1A/DbRaw?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server',fast_executemany=True)   

#Check for database connection
if (conn == False):
    print("Connection Error")
else: 
    print("Connection Success")

#Eventually, there should be no more trunacte/archive ... a JobId will inform the rest of this program what records to work on

# query_data_change = pd.read_sql_query('''
                          
# TRUNCATE TABLE [DbRaw].[OptumRx].[QuarterlyRebatesRawArchive];


# INSERT INTO [DbRaw].[OptumRx].[QuarterlyRebatesRawArchive]
# SELECT *
# from [DbRaw].[OptumRx].[QuarterlyRebatesRaw];

# TRUNCATE TABLE [DbRaw].[OptumRx].[QuarterlyRebatesRaw];
        
		# ''' ,conn)
        
print("[DbRaw].[OptumRx].[QuarterlyRebatesRaw] has been truncated. Upload new file")
        
 ################  File transformation for uploading  ######################       
        
#read in file   (only need this one atm)
#df = pd.read_csv(r'optumrebates.txt', sep='|')       
df_history = pd.read_excel(r'//32bjfs3/Users_Folder/ksever/work/Optum/RebatesQuarterly.xlsx', dtype = str)        
df = pd.read_excel(r'M:/Optum RX/Adhoc/max1539g_365834_BUILDING_SERVICE_32BJ_HEALTH_FUND_REB0231879.xlsx', dtype = str)  
df_test = pd.read_excel(r'M:/Optum RX/Adhoc/max1539g_365834_BUILDING_SERVICE_32BJ_HEALTH_FUND_REB0231879.xlsx')  


sys.path
sys.path.insert(0,'C:\\Users\\ksever\\.anaconda') 

from fileExtract2 import file_extract
path =  file_extract('M:\Optum RX\Adhoc')# you need to write where to pull from 


from fileExtract2 import file_filter
#bring path(df with files) and write filters for it. Start date/end date, start size end size
var = file_filter(path, '2022', '2300', 500, 10000)
df = file_filter(path, '2022', '2300', 500, 10000)

#just call the df back into the code and with the filters it should merge them :) 
from fileExtract2 import file_extraction
df = file_extraction(df) 

#Pretty nice that my old code shortened the work for me :)

#----------------------------------------------------------------------------------------------------------
#Change first row to columns 
header_row = 0
df.columns = df.iloc[header_row]     
        
# #drop first row (of column names) 
df= df.drop([0])


df.drop(df[(df['Quarter'] == 'Quarter') & (df['Quarter'].index >1 )].index, inplace=True)



# #remove all commas from 'PICO' column
df['PICO']=df['PICO'].str.replace(',','') 
        

# ##Remove '-' from money rates and replace with NULL
df['Guarantee Rx Rate $']=df['Guarantee Rx Rate $'].replace('-',np.nan) 
df['Guarantee Rx']=df['Guarantee Rx'].replace('-',np.nan)  
df['Guarantee $']=df['Guarantee $'].replace('-',np.nan)  
df['Rebateable Rx']=df['Rebateable Rx'].replace('-',np.nan)  
df['Billed $']=df['Billed $'].replace('-',np.nan)  
df['Factored Billed Client Share %']=df['Factored Billed Client Share %'].replace('-',np.nan)  
df['Factored Billed Client Share $']=df['Factored Billed Client Share $'].replace('-',np.nan)  
df['Collected $']=df['Collected $'].replace('-',np.nan)  
df['Client Share %']=df['Client Share %'].replace('-',np.nan)  
df['Client Share $']=df['Client Share $'].replace('-',np.nan)  


# #adjust column names to correct naming convention 
df = df.rename({'Submit Year Month':'Submit_Year_Month', 'Carrier ID':'Carrier_ID', 'Carrier Name':'Carrier_Name', 'Account ID':'Account_ID', 'Account Name':'Account_Name', 'Group ID':'Group_ID', 'Group Name':'Group_Name', 'Disb Custom Category':'Disb_Custom_Category', 'Claim Type':'Claim_Type', 'Disb Brand Class':'Disb_Brand_Class', 'Guarantee Rx':'Guarantee_Rx', 'Guarantee Rx Rate $':'Guarantee_Rx_Rate_Amt', 'Guarantee $':'Guarantee_Amt', 'Rebateable Rx':'Rebateable_Rx', 'Billed $':'Billed_Amt', 'Factored Billed Client Share %':'Factored_Billed_Client_Share_Pct', 'Factored Billed Client Share $':'Factored_Billed_Client_Share_Amt', 'Collected $':'Collected_Amt', 'Client Share %':'Client_Share_Pct', 'Client Share $':'Client_Share_Amt', 'Total Due $':'Total_Due_Amt', 'Paid Previously $':'Paid_Previously_Amt', 'Current Paid $':'Current_Paid_Amt'}, axis=1)




#----------------------------------------------------------------------------------------------------------

#best code in the world; by Casey 
test = df1.convert_dtypes().dtypes  
#make columns from carrier_ID to Specialty str the rest int
# iloc [2:13] or 12 idk if it counts the last one or one before



#change to make life easier in sql 
df.rename(columns = {'Quarter':'RXQuarter', "Account_ID":"PlanName"}, inplace = True)



#convert to str to make manipulation and to add jobid for main.jobs
#over here I could prob make it into its proper astype int or str 
df1 = df
df1.insert(loc = 0, column = "JobId", value = 0)

#this is for plan name conversion
df_dict = {'BASIC':'BAS',"METROPOLITAN": 'METRO', "SUBURBAN":"SUB", "TRISTATE_NORTH": "NORTH", "TRISTATE": "TRI"}
df1["PlanName"]=df1["PlanName"].replace(df_dict)



d_test =df1[df1.columns[14:]] 


df1[df1.columns[0:3]] = df1[df1.columns[:3]].astype(float)
df1[df1.columns[0:3]] = df1[df1.columns[:3]].astype(int)
df1[df1.columns[0:3]] = df1[df1.columns[:3]].astype(str)
df1[df1.columns[14:]] = df1[df1.columns[14:]].astype(str)


data_yr = df1["RXQuarter"]

#isolate RXQuarter to filter through and seperate data into 3 buckets 

#this is to put data into 3 buckets and counters to make sure the numbers add up 
prev_years_list = []
Prev_years_counter = 0
Current_year_list = []
Current_year_counter = 0
Additional_years_list = []
Additional_years_counter = 0

#go through year+q to separate previous years(facts) and this year(estimate)
#the else was to account for extra dates and etc but does not end up being used in the end
for (columnName, columnData) in data_yr.iteritems():
    #checks for anything before current year
    if columnData < yr:
        Prev_years_counter+=1
        prev_years_list.append(columnData)
    #checks for everything in current year
    elif  columnData[0:4] == current_year_date:#old was 0:5 and yr
        Current_year_counter+=1
        Current_year_list.append(columnData)
    #checks for anything after this year
    else:
        Additional_years_counter+=1
        Additional_years_list.append(columnData)

 

#this takes the 3 buckets and matches them with df. If item in bucket then add the rest of the data to it
#EX: 2022 data is in seperate df then 2021 data and has all the other columns added onto it
prev_years = df1.query('RXQuarter in @prev_years_list')
Current_year = df1.query('RXQuarter in @Current_year_list')
Additional_years = df1.query('RXQuarter in @Additional_years_list')


#inserts column and based on the 3 df it adds final or estimate, third df has an issue
prev_years.insert(2, "CurrentStatus", "TotalAmt")
Current_year.insert(2, "CurrentStatus", "GuaranteeAmt")
Additional_years.insert(2, "CurrentStatus", "Future Estimate")

#adds another column with the values. If data year previous then current its total. 
#current and above have guarantee_amt 
prev_years.insert(loc = 3, column = "RebatesEarned", value = df1['Total_Due_Amt'])
Current_year.insert(loc = 3, column = "RebatesEarned", value = df1['Guarantee_Amt'])
Additional_years.insert(loc = 3, column = "RebatesEarned", value = df1['Guarantee_Amt'])


#merges dataframes together and concats them all together. NOW WE WRAP IT UP haha
frames = [prev_years, Current_year] #when I add additional_years it duplicates rxvalue and everything around it nan
current_frame = [prev_years, Current_year]
result = pd.concat(frames)
clean_result = pd.concat(current_frame)
#this puts the index in order so we can chop off first amounts
clean_result = clean_result.sort_index()


clean_result1 = clean_result.drop_duplicates(subset='RXQuarter', keep='last')
#looking to cut anything before 7306

for i in clean_result1['CurrentStatus']:
    if i == 'TotalAmt':
        print(clean_result1.index.tolist())
    else: 
        continue

cut_off = clean_result1.index[clean_result1['CurrentStatus'] == 'TotalAmt'].tolist()

cutting_point = cut_off[3]
cutting_point = cutting_point-1

test = clean_result.drop(clean_result.index[cutting_point:], inplace=True)




print(clean_result.shape)

#this is a life saver!!!!!!!!!!!!!
#sends data to sql in dbraw(dbraw because of con, could change that if needed)
#result.to_sql('QuarterlyRebatesRawArchive', con, schema='OptumRx', index=False, chunksize=100000, if_exists='replace')
clean_result.to_sql('QuarterlyRebatesArchive', con, schema='OptumRx', index=False, chunksize=100000, if_exists='replace')
#df1.to_sql('QuarterlyRebatesRaw', con, schema='OptumRx', index=False, chunksize=100000, if_exists='replace')



#this runs sp for me so I dont have to manually call it
runSP = CallStoredProc(conn1, 'optumrx.QuarterlyRebatesCleanSP')
    
if runSP == 1:
    print('yay')
else:
    print('nay'.format()) 

#result.convert_dtypes().dtypes  
#abc = result.columns




#min of left 4 for year 
#truncate this year 
#if min year is not this year (using datetime)
    #truncate last year (min year)
    
#(maybe paste statement goes in between)

#if min year is not this year (using datetime)
    # change flag column from G to T
    # change $ value to new $ value 

#paste new data + defualt(G) + defalt($ value) df.insert null to a new value




## But if this is a q1 file, then all the records from the previous year are NOT estimated, they are real ("Total Amt"), so update the two fields
#Now that we have those flags, before we write to RebatesClean, let's get rid of records that were "Estimate" (Guarantee), because they're getting replaced
#But never delete the Final ("Total"), because they're set in stone.  This way we keep all final history

# DELETE FROM OptumRx.RebatesClean WHERE the RebateSource field = Guarantee (BEFORE WE SEND THIS DATAFRAME TO RebatesClean

###Load data to SQL Server
#print("Writing data to QuarterlyRebatesRaw")

#conn.commit()
#df.to_sql('QuarterlyRebatesRaw', con, schema='OptumRx', chunksize=100000, if_exists='append', index=False)

#print("New data loaded to QuarterlyRebatesRaw")

cursor.close()

