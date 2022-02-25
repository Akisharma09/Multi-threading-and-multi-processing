"""
Created on Tue Dec  15, 2020

@author: Akshay.Sharma1

version: 1.0 (Initial version)

Purpose: To create parquet file from oracle table

ScriptName: multiThreadOracleToParquet.py

Arguments: #9

Argument Desc:

string_db_host = host name
int_db_port = database port eg. 1521
string_db_user = user name
string_db_pass = user pass
string_db_service = oracle database service name eg. RSDP
string_parquet_name = filename.parquet
string_output_path =  path where output file will be generated
sql_table = name of the oracle table from which data will be pulled 
max_workers= number of threads **Do not use number greater than 6 as each thread creates a seperate connection to oracle

Name                    Version             Date                Change desc
Akshay.sharma           1.0                 14/12/2020          Initial draft
Akshay.sharma           2.0                 16/12/2020          Added a new function putToQueue
Akshay.sharma           2.1                 21/12/2020          Naming parquet files under partition folders.

"""

"""
cmd C:/Users/akshay.sharma1/Anaconda3/python.exe c:/Users/akshay.sharma1/"OneDrive - IHS Markit"/python_files/multiThreadOracleToParquet.py <dbhostname> <dbportname> <dbusername> <dbpassword> <dbservicename> <parquetfilename> <parquetfiledestinationpath> <table_name> <numnber of threads>

The above is command line used on local system to run the below script
"""

#Importing python libraries 
import cx_Oracle # oracle specific library
import threading # used for multi threading
import pandas as pd # used to convert oracle data into data frame, which in turn will be converted into parquet file
import pyarrow as pa # used for parquet conversion engine
import pyarrow.parquet as pq # used for parquet conversion
import time # used to clock time
import queue # used to implement multi threading  
import logging # used to log information, debug and warning messages
import sys # used to pull command line arguments

#Get input variables for Oracle source and Output directory and name
string_db_host = sys.argv[1] # db host name from command line 
int_db_port = int(sys.argv[2]) # db port from command line
string_db_user = sys.argv[3] # db user name from command line
string_db_pass = sys.argv[4] # db password from command line
string_db_service = sys.argv[5] # db service name from command line
string_parquet_name = sys.argv[6] # parquet file name from command line
string_output_path = sys.argv[7] # parquet file dest. path from command line
sql_table = sys.argv[8] # db table name from command line
max_workers = int(sys.argv[9]) # number of threads to be used in parallel from command line

qu=queue.Queue() #Instantiating a queue object from Queue class

def fetchColumnName():
    """
    Purpose: Used to fetch column names and datatype from the table used in arguments

    Arguments: #0

    Return: The Function returns a tuple of lists.

    This function will return one list for column names and another list for their corresponding datatypes.

    Usage: column_names, data_type = fetchColumnName()

    """
    try:
        dsn = cx_Oracle.makedsn(string_db_host, int_db_port, service_name=string_db_service)
        connection = cx_Oracle.connect(user=string_db_user, password=string_db_pass, dsn=dsn)
        sql_query = "SELECT column_name,CASE WHEN data_type like '%CHAR%' THEN 'str' WHEN data_type = 'DATE' THEN 'datetime' WHEN data_type = 'NUMBER' AND data_scale <> 0 THEN 'float64' ELSE 'Int64' END data_type FROM USER_TAB_COLUMNS WHERE table_name = '"+sql_table+"' order by column_id asc"
        cursor = connection.cursor()
        cursor.execute(sql_query)
        column_data = cursor.fetchall()
        column_names = []
        d_types = []
        for row in column_data:
            column_names.append(row[0])
            d_types.append(row[1])
        return column_names, d_types
    except Exception as e:
            logging.info("SQL command failed. %s", str(e))
            sys.exit(-1)

def runQuery(connection, column_names, d_types):
    """
    Purpose: Used to fetch data from the database table and writing it to parquet file

    Arguments: #3 

    Arguments descrition:
    Connection: Oracle Connection
    column_names: Column name of oracle table
    d_types: datatypes of columns of oracle table

    Return: None.

    This function will take in 3 parameters and will use them to write data to parquet file, here this
    function is used via multi threads where each thread will run seperately until the queue is not 
    emptied. Queue is thread safe so we don't have to worry about thread iter leaving.

    Usage: 
    t=threading.Thread(target = runQuery, args=(connection, column_names, d_types))
    t.start() this command will start thread
    t.join() this will make sure the program will wait till the thread is done
    """
    while not qu.empty():
        try:
            parquet_file_path = string_output_path+"\\"+string_parquet_name
            cursor = connection.cursor()
            cursor.arraysize=25000
            cursor.prefetchrows = 25000
            report_year = qu.get()
            sql = "select * from "+sql_table+" WHERE REPORT_YEAR_MONTH = :rep_year"
            cursor.execute(sql,rep_year=report_year)
            logging.info('Starting execution of thread number: {} for report_year_month: {}'.format(threading.currentThread().getName(), report_year))
            tab_lst = []
            while True:
                rows = cursor.fetchmany()
                if not rows:
                    break
                df = pd.DataFrame(rows)
                df.columns = column_names

                for i, _ in enumerate(df.columns):
                    if d_types[i] != "datetime":
                        df[column_names[i]] = df[column_names[i]].astype(d_types[i])
                
                pa_table = pa.Table.from_pandas(df, preserve_index=False)

                for i, _ in enumerate(pa_table.columns):
                    if d_types[i] == "datetime":
                        pa_table[i].cast(pa.timestamp('s', tz='America/New_York'))
                
                tab_lst.append(pa_table)
                
            pa_table = pa.concat_tables(tab_lst)    
            pq.write_to_dataset(pa_table
                , parquet_file_path
                , partition_cols=['P_REPORT_YEAR_MONTH']
                , compression='snappy'
                , partition_filename_cb=lambda partition_key:string_parquet_name[:-9]+'_'+str(partition_key[0])+'.parquet'
                )
            # rows = cursor.fetchall()
            # df = pd.DataFrame(rows)
            # df.columns = column_names
                    
            # for i, _ in enumerate(df.columns):
            #     if d_types[i] != "datetime":
            #         df[column_names[i]] = df[column_names[i]].astype(d_types[i])
                
            
            # pa_table = pa.Table.from_pandas(df, preserve_index=False)

            # for i, _ in enumerate(pa_table.columns):
            #     if d_types[i] == "datetime":
            #         pa_table[i].cast(pa.timestamp('s', tz='America/New_York'))
                
            # pq.write_to_dataset(pa_table
            #     , parquet_file_path
            #     , partition_cols=['P_REPORT_YEAR_MONTH']
            #     , compression='snappy'
            #     ,partition_filename_cb=lambda partition_key:string_parquet_name[:-9]+'_'+str(partition_key[0])+'.parquet' #
            #     )

            logging.info('Ending execution of thread number: {} for report_year_month: {}'.format(threading.currentThread().getName(), report_year))

        except Exception as e:
            print("SQL command failed. "+str(e))
            sys.exit(-1)

def putToQueue():
    """
    Purpose: Used to insert values for report year month

    Arguments: #0

    Return: None

    This function will add distinct report year months in the queue for it to rocess using multiple threads.

    Usage: putToQueue()    
    """
    logging.info('Starting to put values in queue.')
    dsn = cx_Oracle.makedsn(string_db_host, int_db_port, service_name=string_db_service)
    connection = cx_Oracle.connect(user=string_db_user, password=string_db_pass, dsn=dsn)
    sql_query = "SELECT DISTINCT REPORT_YEAR_MONTH FROM "+sql_table
    cursor = connection.cursor()
    cursor.arraysize = 500
    cursor.prefetchrows = 501
    cursor.execute(sql_query)
    column_data = cursor.fetchall()
    for rep_month in column_data:
        qu.put(rep_month[0])
    logging.info('All values added to queue. Size of queue is: {}'.format(qu.qsize()))


def main():
    """
    Purpose: Used to to trigger the main program

    Arguments: #0

    Return: None.

    Usage: main()
    """
    start_time = time.time()
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")
                    
    logging.info("Starting to load data from oracle to parquet")

    putToQueue() # Add all report year months to queue

    dsn = cx_Oracle.makedsn(string_db_host, int_db_port, service_name=string_db_service)
    pool = cx_Oracle.SessionPool(string_db_user, string_db_pass, dsn, min = max_workers, max = max_workers, increment = 0, threaded = True)
    
    logging.info("fetching column name and datatypes for table: %s", sql_table)
    column_names, d_types = fetchColumnName()

    thread_list=[] # initializing thread list
    for i in range(max_workers):
        connection = pool.acquire()
        t=threading.Thread(name = '#' + str(i), target = runQuery, args=(connection, column_names, d_types))
        thread_list.append(t)
    for thread in thread_list:
        thread.start()
    for thread in thread_list:
        thread.join()
    logging.info("Done with writing data to parquet:")
    logging.info("Total time taken in seconds is: {}".format(time.time() - start_time))

if __name__ == '__main__':
    main()
