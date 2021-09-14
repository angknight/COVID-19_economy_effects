import requests
import json
import sqlite3
import datetime #gets the date and time for a certain day
import pandas as pd
import os
import csv
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as plticker
from matplotlib.ticker import StrMethodFormatter

params = "/v1/us/daily.json"
url = "https://api.covidtracking.com" + params
dbname = "db.sqlite"

#---------------------------------- Retrieve data from Covid-19 API ------------------------------------
#define a function to gather data from API (input is API url)
def getData(url):
    response = requests.get(url)
    dataLst = response.json()
    dates = []
    positives = []
    #doesn't include last 37 days because those are days in Jan and Feb when there were
    #no positive cases. This way it will only get days starting Feb 28th, the last day USA had zero positive cases
    for day in dataLst[:-37]:
        date = day["date"]
        positive = day["positive"]
        dates.append(date)
        positives.append(positive)

    #putting list of dates and positive cases is chronological order
    # dates.reverse()
    # positives.reverse()
    newDates = []
    #putting dates into correct format as strings
    for date in dates:
        strDate = str(date)
        year = int(strDate[:4])
        month = int(strDate[4:6])
        day = int(strDate[6:])
        d = datetime.datetime(year, month, day)
        newDates.append(d.strftime("%Y-%m-%d"))

    return (newDates, positives)


#---------------------------------- Put data from API into a database -----------------------------------
def createDbTable(dbname):
    #create connection and cursor
    dir = os.path.dirname(os.path.abspath(__file__)) + os.sep
    conn = sqlite3.connect(dir + dbname)
    cur = conn.cursor()

    #create table
    cur.execute("CREATE TABLE IF NOT EXISTS positive_cases (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, date TEXT, positives INTEGER)")

def getID(dbname):
   dir = os.path.dirname(os.path.abspath(__file__)) + os.sep
   conn = sqlite3.connect(dir + dbname)
   cur = conn.cursor()

   idsLst = []

   cur.execute('SELECT id FROM positive_cases')
   for row in cur:
      idsLst.append(row)

   try:
      x = idsLst[-1]
      iD = x[0]
   except:
      iD = 0

   return iD


def dataIntoDB(dbname, url):
    dir = os.path.dirname(os.path.abspath(__file__)) + os.sep
    conn = sqlite3.connect(dir + dbname)
    cur = conn.cursor()

    iD = getID(dbname)

    data = getData(url) #this is a tuple with the two lists: list of dates then list of positives
    datesLst = data[0]
    positivesLst = data[1]

    limit = 8

    threshhold = 39

    if iD > threshhold:
        for i in range(iD, len(datesLst)):
            cur.execute("INSERT INTO positive_cases (date, positives) VALUES(?, ?)", (datesLst[i], positivesLst[i]))
    else:
        for i in range(iD, iD + limit):
            cur.execute("INSERT INTO positive_cases (date, positives) VALUES(?, ?)", (datesLst[i], positivesLst[i]))

    #commit the changes to the database
    conn.commit()
    #close the cursor and the connection
    cur.close()



#-------------------------------------- Calculations -----------------------------------------------
#define a function to select data from a file and calculate something
def calcPercChange(dbname):
    dir = os.path.dirname(os.path.abspath(__file__)) + os.sep
    conn = sqlite3.connect(dir + dbname)
    cur = conn.cursor()
    data = cur.execute("SELECT date, positives FROM positive_cases").fetchall()
    data = data[:-1] #excludes the first day because there were zero cases and you can't divide by zero
    output = []
    datesLst =[]
    percChangeLst = []

    for i in range(len(data) - 1):
        percent_change = round(((data[i][1] - data[i + 1][1]) / data[i + 1][1]) * 100, 2)
        datesLst.append(data[i][0])
        percChangeLst.append(percent_change)
    output.append(datesLst)
    output.append(percChangeLst)
    return output


 #---------------------------------- Write calculated data into a CSV file -----------------------------------
def writeToCsv(outFileName, url):
    data = getData(url)
    calcData = calcPercChange(dbname)
    dir = os.path.dirname(os.path.abspath(__file__)) + os.sep
    with open(dir + outFileName, "w") as outFile:
        csv_writer = csv.writer(outFile)
        csv_writer.writerow(["Date", "Percent Change in Positive Covid-19 Cases"])
        for i in range(len(calcData[0])):
            col1 = calcData[0][i]
            col2 = calcData[1][i]
            csv_writer.writerow([col1, col2])


#------------------------------------ Create a visualization from CSV file -------------------------------------
#define a function to select data from the database and make a visualization
def makeVis(dbname):
    #data for plotting
    output = calcPercChange(dbname)
    x = output[0]
    y = output[1]

    #create line graph
    fig, ax = plt.subplots()
    fig.set_facecolor("lavender")
    ax.plot(x, y, "m.-")
    ax.set_xlabel(xlabel = "Dates", fontsize = 11)
    ax.set_ylabel(ylabel = "Percent Change in Positive Covid-19 Cases", fontsize=11)
    ax.set_title(label="Percent Change in Positive Covid-19 Cases in the United States", fontsize=14)
    ax.set_facecolor('honeydew')
    plt.xticks(fontsize=7)
    plt.yticks(fontsize=9)
    loc = plticker.MultipleLocator(base=50) # this locator puts ticks at regular intervals
    ax.xaxis.set_major_locator(loc)
    plt.gca().invert_xaxis()
    plt.gca().yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
    ax.grid()
    fig.savefig("covid-cases-chart.png")
    # plt.show()



def angMain():
    getData(url)
    createDbTable(dbname)
    getID(dbname)
    dataIntoDB(dbname, url)
    writeToCsv("covid-cases.csv", url)
    makeVis(dbname)


angMain()

