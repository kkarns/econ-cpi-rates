#######################################################################################################################
## 
## name:        
##      econ-cpi-rates.py
##
##      python script to access data from the Bureau of Labor Statistics' (BLS) Public Data 
##      Application Programming Interface (API). 
##      The same data is also available interactively online at https://www.bls.gov/cpi/data.htm
##      look for Consumer Price Index (CPI) - All Urban Consumers (CPI-U), Not Seasonally Adjusted - Series Id: CUUR0000SA0 
##
##      The script takes a year and a month argument, retrieves CPI value from the API and then 
##      populates this table BaseData.dbo.ECON_CPI_RATES.
##      The API retrieves data for a ranges of years, this script filters the result to just one month.
##
##      discussion here:  https://www.kkarnsdba.com/<tba>
##
## syntax:
##      econ-spi-rates.py                           defaults to previous month if no args (batch mode)
##      econ-cpi-rates.py 2019 12                   use year and month args as an override (interactive mode)
## 
## updated:
##      -- Tuesday, May 14, 2019 1:01 PM - initial work with API from https://github.com/OliverSherouse/bls/
##      -- Friday, December 6, 2019 4:58 PM - integrated keyring library instead of securestring
##      -- Monday, December 16, 2019 9:05 AM - added logging
##
## todo:

import bls
## bls source https://github.com/OliverSherouse/bls
## bls package - pip installed from https://pypi.org/project/bls/
import os
import pandas 
import sys
import pyodbc
import datetime
import time
import timestring
import keyring
import getpass
import logging 
                     
## set local code path
myDir = os.path.dirname(__file__)

## setup the logfile
logDir = myDir + '/logs' 
if not os.path.exists(logDir):
   os.makedirs(logDir)
logFile = logDir + '/econ-cpi-rates-' + str(time.strftime('%Y-%m-%d-%H%M%S')) + '.log'
logHandler = logging.FileHandler(os.environ.get("LOGFILE", logFile))
logHandler.setFormatter(logging.Formatter('%(levelname)s:%(asctime)s:  %(message)s'))
infoLogger = logging.getLogger()
infoLogger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
infoLogger.addHandler(logHandler)

print ('results are logged to ' + logFile)
infoLogger.info('results are logged to ' + logFile)

## using the current time - default to previous month
now = datetime.datetime.now()
lastMonth = now.month - 1 if now.month > 1 else 12
lastMonthYear = now.year if now.month > 1 else now.year - 1 
blsYear = lastMonthYear
blsMonth = lastMonth

## check for 2 arguments: Year and Month and override if available
if len(sys.argv) > 2:
   blsYear = sys.argv[1]
   blsMonth = sys.argv[2]

   if timestring.Date(str(blsMonth) + '/1/' + str(blsYear)) > timestring.Date(str(lastMonth) + '/1/' + str(lastMonthYear)):
      msg = 'Error.  Date cannot be in future.  Please set the override date to less than the current month'
      print (msg)
      infoLogger.info(msg)
      sys.exit(1)  
   msg = 'Using override date, which is:  ' + str(blsMonth) + '/1/' + str(blsYear) 
   print (msg)
   infoLogger.info(msg)
else:          
   msg = 'No arguments provided, so using default date, which is the previous month:  ' + str(blsMonth) + '/1/' + str(blsYear) 
   print (msg)
   infoLogger.info(msg)

## check acount running script while troubleshooting ADS issues
msg = 'using this account:  ' + getpass.getuser()
print (msg)
infoLogger.info(msg)

## retrieve our Bureau of Labor Statistics Public Data API key from OS environment value  
## key comes from https://data.bls.gov/registrationEngine/
## api may or may not work without our API key
## print(os.environ)    ## debug - check if BRS_API_KEY is in environ
try:  
   os.environ["BLS_API_KEY"]
except KeyError: 
   msg = "Please set the environment variable BLS_API_KEY with our key from the DOL"
   print(msg)
   infoLogger.info(msg)
   sys.exit(1)
BLS_API_KEY  = os.environ.get('BLS_API_KEY')


## call the API from the BLS python package for the desired year - a year of data values
cpi_information = bls.get_series('CUUR0000SA0', blsYear, blsYear, BLS_API_KEY)  
cpi_information.head()
inf=pandas.DataFrame(cpi_information)
msg = 'The raw data from the DOL API for the year parameters is:'
print (msg)
infoLogger.info(msg)
print (inf)
infoLogger.info(inf)


## filter only the CPI for the desired month 
try:
   var = inf.loc[str(blsYear)+'-'+str(blsMonth),:].item()
except KeyError:
   msg = 'The month requested ' + str(blsMonth) + '-' + str(blsYear) + ' is not available yet, it may take a few more days, often after the 10th day.'
   print(msg)
   infoLogger.info(msg)
   sys.exit(1)
msg = 'The value for the month requested ' + str(blsMonth) + '-' + str(blsYear) + ' is: ' + str(var)
print (msg)
infoLogger.info(msg)

## get credentials from Windows.Security.Credentials.PasswordVault, gui editor: rundll32 keymgr.dll, KRShowKeyMgr
service_id = 'econ-cpi-rates'
server   = keyring.get_password(service_id, 'econ-service-server') 
database = keyring.get_password(service_id, 'econ-service-db') 
username = keyring.get_password(service_id, 'econ-service-acct') 
password = keyring.get_password(service_id, 'econ-service-pass') 

## check the database and insert the new data
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = conn.cursor()
## SELECT CPI_YR, CPI_MTH, CPI_RATE FROM BaseData.gis.ECON_CPI_RATES WHERE CPI_YR = 2019 AND CPI_MTH = 1 ORDER BY CPI_YR DESC, CPI_MTH DESC;
tsql = "SELECT CPI_YR, CPI_MTH, CPI_RATE FROM BaseData.gis.ECON_CPI_RATES WHERE CPI_YR = " + str(blsYear) + " AND " + "CPI_MTH = " + str(blsMonth) + " ORDER BY CPI_YR DESC, CPI_MTH DESC;"
infoLogger.info(tsql)

with cursor.execute(tsql):
   row = cursor.fetchall()
   msg = str(len(row)) + ' existing match(es)'
   print (msg)
   infoLogger.info(msg)
   if len(row) > 0 :
      msg = 'This CPI value is already in the database:  CPI_YR = ' + str(blsYear) + ' AND ' + 'CPI_MTH = ' + str(blsMonth) 
      print (msg)
      infoLogger.info(msg)
   else:    
      tsql = "INSERT INTO BaseData.gis.ECON_CPI_RATES (CPI_YR,CPI_MTH,CPI_RATE) VALUES (?,?,?);"
      with cursor.execute(tsql,blsYear,blsMonth,var):         
         conn.commit()
         msg = 'Successfully Inserted! ' + tsql
         print (msg)
         infoLogger.info(msg)

msg = 'The latest 12 consumer price index entries in the database are:'
print (msg)
infoLogger.info(msg)
tsql = "SELECT TOP(12) CPI_YR, CPI_MTH, CPI_RATE FROM BaseData.gis.ECON_CPI_RATES ORDER BY CPI_YR DESC, CPI_MTH DESC;"
with cursor.execute(tsql):
   row = cursor.fetchone()
   while row:
      msg = str(row[0]) + " " + str(row[1]) + " " + str(row[2])
      print (msg)
      infoLogger.info(msg)
      row = cursor.fetchone()

sys.exit(1) ## this is the end.  Who knew?
