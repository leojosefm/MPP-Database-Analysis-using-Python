###########################################################################################
##### Script name: netezza_analysis.py#####################################################
##### Developed by : Jeffrey Mattam#######################################################
##### This script analyses all tables, their data distributions and prepares an Excel file#######
###########################################
########### Importing required modules#############
import os
import pyodbc  ## not an inbuilt package -- have to be installed separately ; Used to connect to Netezza Database
import xlsxwriter ## not an inbuit package
import time
import logging
import pandas as pd ### not an inbuilt module
#################################################
cwd = os.getcwd()   ### Current working directory########
timestr=time.strftime("%Y%m%d%H%M%S") ##### Current timestamp####
### Logging enabled#######################
logfile=cwd+'\\'+timestr+'_netezza_analysis'
logging.basicConfig(filename=logfile+'.log',format='%(asctime)s %(message)s',level=logging.INFO) ### Log file
logging.info('Connecting to Netezza database')
###############################
###### Connect to Netezza Database##########
server=input('please type hostname:')
userid=input('User id:')
password=input('password: ')
database = input("Database to connect:")
conn = pyodbc.connect("DRIVER={NetezzaSQL};SERVER="+server+";PORT=5480;DATABASE="+database+";UID="+userid+";PWD="+password+";")
cur = conn.cursor()
logging.info('Connected to Netezza database')
###############################################
#### List of databases to analyse###########
input_string = input("Enter a list of databases you want to search separated by space ")
list_database  = input_string.split()

##### Dictionary declarations to be used in the Process
dict_tables={}
columns=[]
### Create dictionary of all tables with table name as key and database name as items####
logging.info('Creating Dictionary of all tables as table name as key and database name as items')
for database_name in list_database:
    tables_query="select distinct tablename from "+database_name+".._v_table where database='"+database_name+"'"
    cur.execute(tables_query)
    tables=cur.fetchall()
    for row in tables:
        dict_tables[row[0]]=database_name
logging.info('Table-Database dictionary created')

##########Writing report to a csv file################################################################
logging.info('Declaring output excel files headers')
fieldnames_distribution=['table','datasliceid','records'] ### Main column header for sheets with charts
fieldnames = ['Database', 'Tablename','no_of_dataslices_used','no_of_dataslices_with_less_records','Min_record_count_dataslice','maximum_record_count_dataslice','Total_record_count','distribute_key','unique_values_to_totalcount_distributekey','column_with_highestcardinality','unique_values_to_totalcount_highcardinalitycolumn'] ## column header of CSV file
data_list=[]
# Create a Pandas Excel writer using XlsxWriter as the engine.
# Access the XlsxWriter workbook and worksheet objects from the dataframe
writer = pd.ExcelWriter('netezza_tables_'+timestr+'.xlsx', engine='xlsxwriter') ###
workbook = writer.book
for table,database in dict_tables.items():
    total_count_query="select reltuples from "+database+".._v_table where database=\'"+database+"\' and tablename=\'"+table+"\'"
    #total_count_query="select count(*) from "+database+"..\""+table+"\""
    cur.execute(total_count_query)
    total_record_count=cur.fetchone()[0]
    logging.info('Record count of table :' + table + " is "+str(total_record_count))
    #logging.info('Calculating other parameters like cardinality , data distribution if the record count is less than 1M')
    if total_record_count>10000:
        dataslice_query="select count(*) from (select datasliceid, count(datasliceid) from "+database+"..\""+table+"\" group by datasliceid) TMP"
        cur.execute(dataslice_query)
        no_of_dataslices=cur.fetchone()[0]
        logging.info('Dataslices used by table :' + table + " is "+str(no_of_dataslices))
        logging.info('Calculating other parameters like cardinality , data distribution if the dataslices used is less than 480')
        #if no_of_dataslices <480:
        if 1==1:
            logging.info('Database being processed: '+database)
            minimum_dataslice="select case when min(RECORDS) is NULL then 0 else min(RECORDS) end from (select datasliceid, count(datasliceid) as RECORDS from "+database+"..\""+table+"\" group by datasliceid) TMP"
            maximum_dataslice="select case when max(RECORDS) is null then 0 else max(RECORDS) end  from (select datasliceid, count(datasliceid) as RECORDS from "+database+"..\""+table+"\" group by datasliceid) TMP"
            cur.execute(minimum_dataslice)
            minimum_record_count=cur.fetchone()[0]
            cur.execute(maximum_dataslice)
            maximum_record_count=cur.fetchone()[0]
            storage_skew=maximum_record_count-minimum_record_count
            logging.info('Writing to Pandas Dataframe: '+database+'..'+table)
            query_distribution="SELECT count(*) from (select datasliceid ,round(count(*)/(sum(count(*)) over()/480) * 100) as percentage_filled from "+database+"..\""+table+"\"  group by datasliceid) TMP where percentage_filled <50"
            cur.execute(query_distribution)
            skewed_dataslices=cur.fetchone()[0]
            query_distkey="SELECT attname from "+database+".._v_table_dist_map where tablename='"+table+"' and database='"+database+"'" ## query to find distribute key
            cur.execute(query_distkey)
            try:
                distkey=cur.fetchone()[0]
                distinct_count_query="SELECT count(distinct "+distkey+") FROM "+database+"..\""+table+"\""
                cur.execute(distinct_count_query)
                distkey_unique_count=cur.fetchone()[0]
                dist_key_cardinality=float(distkey_unique_count/total_record_count)
            except TypeError:
                distkey=' '
                dist_key_cardinality=' '
            column_list_query="select column_name from "+database+".._v_sys_columns where table_name='"+table+"'"
            cur.execute(column_list_query)
            columns=cur.fetchall()
            logging.info('creating dictionary of columns and their cardinality for table :'+table)
            dict_cardinality_columns={} ### Dictionary with column cardinality
            for row in columns: ## loop to create a dictionary with cardinality of all columns of a table
                cardinality_query="SELECT count(distinct \""+row[0]+"\") from "+database+"..\""+table+"\""
                cur.execute(cardinality_query)
                unique_count=cur.fetchone()[0]
                cardinality=float(unique_count/total_record_count)
                dict_cardinality_columns[database+"~"+table+"~"+row[0]]=cardinality
            logging.info('Cardinality dictionary for table: '+table+' created')
            maximum_value_column=max(dict_cardinality_columns,key=dict_cardinality_columns.get) ## finding column with maximum cardinality
            High_cardinality_column=maximum_value_column.split('~')[2]
            high_cardinality=round(dict_cardinality_columns.get(maximum_value_column),4)
            logging.info('Creating data distribution sheet for '+database+".."+table)
            query_dataslice="SELECT "+"'"+database+".."+table+"'"+" as TBL_NAME,datasliceid, count(*) as count_in_dataslice FROM "+database+"..\""+table+"\"  group by datasliceid"
            cur.execute(query_dataslice)
            data=cur.fetchall()
            df_distribution = pd.DataFrame.from_records(data, columns=fieldnames_distribution)
            sheetname=(database+"_"+table)[:25]
            df_distribution.to_excel(writer, sheet_name=sheetname,index=False,columns=fieldnames_distribution)
            logging.info('Data distibution of table :'+table+" written to excel sheet")
            logging.info('Adding Data distribution chart of table '+table+' to excel sheet')
            worksheet = writer.sheets[sheetname]
            chart = workbook.add_chart({'type': 'column'})  # create a chart object
            chart.add_series({ 'values':     '='+sheetname+'!$C$2:$C$481'        }) ###480 dataslices
            chart.set_title ({'name': table+' Distribution'})
            chart.set_x_axis({'name': 'Datasliceid'})
            chart.set_y_axis({'name': 'Record_count'})
            worksheet.insert_chart('E2', chart)
            logging.info('Data distribution sheet created for '+database+".."+table)
            data={'Database': database, 'Tablename': table,'no_of_dataslices_used':no_of_dataslices,\
            'no_of_dataslices_with_less_records':skewed_dataslices,'Min_record_count_dataslice':minimum_record_count,'maximum_record_count_dataslice':maximum_record_count,\
            'Total_record_count':total_record_count,'distribute_key':distkey,'unique_values_to_totalcount_distributekey':dist_key_cardinality,\
            'column_with_highestcardinality':High_cardinality_column,'unique_values_to_totalcount_highcardinalitycolumn':high_cardinality}
            data_list.append(data)

####################################################
##### Writing to Excel file#########################
if len(data_list)==0:
    logging.info('No tables in database')
else:
    df_tables = pd.DataFrame(data_list)
    logging.info('Pandas dataframe created : Now writing to Excel sheet')
    df_tables.to_excel(writer, sheet_name='Distribution',index=False,columns=fieldnames)
    worksheet = writer.sheets['Distribution']
    writer.save()
    logging.info('Excel file created')
######################################################
