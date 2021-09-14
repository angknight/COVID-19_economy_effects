###webscraping stock data
import matplotlib.pyplot as plt
import matplotlib.ticker as plticker
import datetime as dt
import pandas as pd
import sqlite3
#https://www.w3schools.com/python/python_ml_getting_started.asp
import numpy as np
#link: https://pandas-datareader.readthedocs.io/en/latest/remote_data.html
import pandas_datareader.data as web
import os
import csv


dbname = "db.sqlite"

###Gets the data
def get_stock_data():
   #link: https://www.w3schools.com/python/python_datetime.asp
   #this gets todays time and time
   now = dt.datetime.now()

   ###start time as 1st day of 2020 and end time is current time
   # start = dt.datetime(2020,1,1)
   # end = now
   start = dt.datetime(2020,1,1)
   end = now
   #gets data off yahoo finance
   df = web.DataReader("^DJI",'yahoo',start,end)

   #displays the data that I am using
   #print(df)

   ###makes lists of Close Prices + volume + High Price+ Low Price, Values are FLOATS
   Close_Prices = (df["Close"].to_list())
   Volume = (df["Volume"].to_list())
   High = (df["High"].to_list())
   Low = (df["Low"].to_list())
   ###use df.index since Dates is the index of the dataframe df
   dates = (df.index.to_list())

   Dates = []

   for i in range(len(dates)):
      x = dates[i].strftime("%Y-%m-%d")
      Dates.append(x)

   Dates.reverse()
   Volume.reverse()
   High.reverse()
   Low.reverse()
   Close_Prices.reverse()

   return (Dates, Volume, High, Low, Close_Prices)

### Put into Sqlite database
#-------------------------------------------------------------------------#
def start_DB(dbname):

   directory = os.path.dirname(os.path.abspath(__file__)) + os.sep
   conn = sqlite3.connect(directory + dbname)
   cur = conn.cursor()

   #create table
   cur.execute("CREATE TABLE IF NOT EXISTS DJI_Data (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,Date TEXT,Volume INTEGER, High_Price NUMERIC, Low_Price NUMERIC, Close_Prices NUMERIC)")

def check_ID(dbname):

   directory = os.path.dirname(os.path.abspath(__file__)) + os.sep
   conn = sqlite3.connect(directory + dbname)
   cur = conn.cursor()

   id_list = []

   cur.execute('SELECT id FROM DJI_DATA')
   for row in cur:
      id_list.append(row)

   try:
      x = id_list[-1]
      id = x[0]
   except:
      id = 0

   return id

def insert_Data(dbname):
   dir = os.path.dirname(__file__) + os.sep
   conn = sqlite3.connect(dir + dbname)
   cur = conn.cursor()

   id = check_ID(dbname)

   data = get_stock_data() #tuple LISTS: Dates, Volume, High, Low, Close_Prices
   Dates = data[0]
   Volume = data[1]
   High = data[2]
   Low = data[3]
   Close_Prices = data[4]

   limit = 8

   threshhold = 39

   if id > threshhold:
      for i in range(id,len(Dates)):
         cur.execute("INSERT INTO DJI_Data (Date, Volume, High_Price, Low_Price, Close_Prices) VALUES(?, ?, ?, ?, ?)", (Dates[i], Volume[i], High[i], Low[i], Close_Prices[i]))

   else:

      for i in range(id, id + limit):
         #insert info from data into database table
         cur.execute("INSERT INTO DJI_Data (Date, Volume, High_Price, Low_Price, Close_Prices) VALUES(?, ?, ?, ?, ?)", (Dates[i], Volume[i], High[i], Low[i], Close_Prices[i]))
      #commit the changes to the database
   conn.commit()
      #close the cursor and the connection
   cur.close()


####calculations
#-------------------------------------------------------------------------#
def calculate(dbname):
   directory = os.path.dirname(os.path.abspath(__file__)) + os.sep
   conn = sqlite3.connect(directory + dbname)
   cur = conn.cursor()

   ###volume data
   volume_list = []
   cur.execute('Select Volume from DJI_DATA')
   for row in cur:
      volume_list.append(row)


   volume_list = np.array(volume_list)
   volume_mean = volume_list.mean() #0
   volume_std = volume_list.std() #1


   ###close price data
   Close_Price_List = []
   cur.execute('Select Close_Prices from DJI_DATA')
   for row in cur:
      Close_Price_List.append(row)



   Close_Price_List = np.array(Close_Price_List)
   Close_Price_Mean = Close_Price_List.mean() #2
   Close_Price_std = Close_Price_List.std()   #3

   percent_change_close = [0]
   for i in range(1,len(Close_Price_List)):
      x = (Close_Price_List[i] - Close_Price_List[i-1]) / Close_Price_List[i-1]
      percent_change_close.append(x)


   percent_change_close = np.array(percent_change_close) #4
   average_percent_change_close = percent_change_close.mean() #5

   list_average_percent_change_close = [] #6
   for i in range(len(percent_change_close)):
      list_average_percent_change_close.append(average_percent_change_close)


   ###High Low Price Data

   High_Price_List = []
   cur.execute('Select High_Price from DJI_DATA')
   for row in cur:
      High_Price_List.append(row)


   Low_Price_List = []
   cur.execute('Select Low_Price from DJI_DATA')
   for row in cur:
      Low_Price_List.append(row)


   High_Price_List = np.array(High_Price_List)
   Low_Price_List = np.array(Low_Price_List)

   diff_High_Low = High_Price_List - Low_Price_List #7
   mean_High_Low = diff_High_Low.mean() #8


   list_average_High_Low = [] #9
   for i in range(len(diff_High_Low)):
      list_average_High_Low.append(mean_High_Low)

   cur.close()

   return (volume_mean, volume_std, Close_Price_Mean, Close_Price_std, percent_change_close, average_percent_change_close, list_average_percent_change_close, diff_High_Low, mean_High_Low, list_average_High_Low)

def write_to_CSV(outFileName,dbname):
   directory = os.path.dirname(os.path.abspath(__file__)) + os.sep
   conn = sqlite3.connect(directory + dbname)
   cur = conn.cursor()
   id = check_ID(dbname);
   db_dates = cur.execute("SELECT Date FROM DJI_Data WHERE id BETWEEN 0 AND '" + str(id) + "' ORDER BY id ASC").fetchall()
   dates = []
   for date in db_dates:
      dates.append(date[0])

   calculations = calculate(dbname)

   dir = os.path.dirname(os.path.abspath(__file__)) + os.sep
   with open(dir + outFileName, "w") as outFile:
      csv_writer = csv.writer(outFile)
      csv_writer.writerow(["Date", "Percent Change in DJI Close Prices", "DJI: High Price - Low Price (Range)"])
      for i in range(len(dates)):
            col1 = dates[i]
            col2 = calculations[4][i]
            col3 = calculations[7][i]
            csv_writer.writerow([col1, col2, col3])

def visualize(dbname):
   directory = os.path.dirname(os.path.abspath(__file__)) + os.sep
   conn = sqlite3.connect(directory + dbname)
   cur = conn.cursor()
   id = check_ID(dbname);
   db_dates = cur.execute("SELECT Date FROM DJI_Data WHERE id BETWEEN 0 AND '" + str(id) + "' ORDER BY id ASC").fetchall()
   dates = []
   for date in db_dates:
      dates.append(date[0])

   calculations = calculate(dbname)

   fig = plt.figure(figsize = (15,5), facecolor ="lightgrey" )

   ax1 = fig.add_subplot(121)
   ax1.invert_xaxis()
   ax2 = fig.add_subplot(122)
   ax2.invert_xaxis()

   ax1.plot(dates,calculations[4], 'r')
   ax1.plot(dates,calculations[6], 'b', label = "Average % Change")

   ax1.legend()

   ax1.set(xlabel = 'Date', ylabel = "% Change in DJI Value", title = "% Change in Dow Jones Industrial Index Value [Close Price]")
   loc = plticker.MultipleLocator(base=50) # this locator puts ticks at regular intervals
   ax1.xaxis.set_major_locator(loc)
   ax1.grid()

   ax2.plot(dates, calculations[7] , 'r')
   ax2.plot(dates, calculations[9] , 'b', label = "Average Range")

   ax2.legend()
   ax2.set(xlabel = 'Date', ylabel = "Range of Dow Jones Index", title = "Range (High Price - Low Price) of Dow Jones Industrial Index Value Each Day")
   ax2.xaxis.set_major_locator(loc)
   ax2.grid()

   fig.savefig(directory + "dji-chart.png")
   # plt.show()


def Sean_Main():

   start_DB(dbname)
   check_ID(dbname)
   insert_Data(dbname)
   write_to_CSV("dji.csv",dbname)
   visualize(dbname)


Sean_Main()