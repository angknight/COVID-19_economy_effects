from datetime import datetime
from matplotlib.ticker import StrMethodFormatter
import csv
import json
import matplotlib.pyplot as pyplot
import matplotlib.ticker as plticker
import os
import requests
import sqlite3
import time

# FOR BETTER UNDERSTANDING, READ THIS THING FROM TOP TO
# BOTTON AND FOLLOW THE ORDER OF THE FUNCTIONS.

# Do not publish your API key anywhere public.
API_KEY = "a964732354e30d669470642ff6b45f4c"
# Default: db.sqlite
DB_NAME = "db.sqlite"

# Helper function.
# Returns the row count (integer) of a specific claim type.
# Takes an alphanumerical Federal Reserve "series_id."
def _uecGetOffset(conn, cur, series_id):
  directory = os.path.dirname(os.path.abspath(__file__)) + os.sep
  offset = cur.execute("SELECT count(claim_claims.claim_id) FROM claim_claims JOIN claim_types ON claim_claims.type_id = claim_types.type_id WHERE claim_types.type_name = '" + series_id + "'").fetchone()[0]
  return offset

# Helper function.
# Returns count of all rows regardless of claim type.
# Input: takes a table name and column name as a primary key.
# Output: if no rows, return 0.
# Otherwise return the current max id (integer).
def _uecGetMaxId(conn, cur, table, primary_key):
  max_id = cur.execute("SELECT max(" + primary_key + ") FROM " + table).fetchone() # How big is the table?
  if (max_id[0] is None):
    top_id = 0 # First row.
  else:
    top_id = max_id[0] # Subsequent rows.
  return top_id

# APP TASK: 9th function
def uecGraphClaims(series_id, datum, title):
  fig = pyplot.figure(figsize = (15, 5), facecolor ="lightgrey" )
  fig.suptitle(title, fontsize=12)
  for i in range(len(datum)):
    x = datum[i][0]
    y = datum[i][1]
    if i == 0:
      subplot = fig.add_subplot(121)
      subplot.plot(x, y, 'r')
      subplot.set_title(label = "Total Claims", fontsize=10)
      subplot.set_xlabel(xlabel = "Date", fontsize=10)
      subplot.set_ylabel(ylabel = "Number of Claims", fontsize=10)
      pyplot.xticks(fontsize=6)
      pyplot.yticks(fontsize=8)
      loc = plticker.MultipleLocator(base=12.5) # this locator puts ticks at regular intervals
      subplot.xaxis.set_major_locator(loc)
      pyplot.gca().invert_xaxis()
      pyplot.gca().yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
      pyplot.grid()
    elif i == 1:
      subplot = fig.add_subplot(122)
      subplot.plot(x, y, 'b')
      subplot.set_title(label = "Percent Change", fontsize=10)
      subplot.set_xlabel(xlabel = "Date", fontsize=10)
      subplot.set_ylabel(ylabel = "Change by Percent", fontsize=10)
      pyplot.xticks(fontsize=6)
      pyplot.yticks(fontsize=8)
      loc = plticker.MultipleLocator(base=12.5) # this locator puts ticks at regular intervals
      subplot.xaxis.set_major_locator(loc)
      pyplot.gca().invert_xaxis()
      pyplot.grid()
  pyplot.xticks(fontsize=6)
  pyplot.draw()
  # pyplot.show()
  fig.savefig("unemployment-" + series_id + "-chart.png")

# APP TASK: 6th function
def uecGetPercentChange(conn, cur, series_id):
  datum = cur.execute("SELECT claim_claims.date, claim_claims.claims FROM claim_claims JOIN claim_types ON claim_claims.type_id = claim_types.type_id WHERE claim_types.type_name = '" + series_id + "' ORDER BY claim_id ASC").fetchall()
  output = []
  dates = []
  percentages = []
  for i in range(len(datum) - 1):
    percent_change = round(((datum[i][1] - datum[i + 1][1]) / datum[i + 1][1]) * 100, 2)
    dates.append(datum[i][0][0:10])
    percentages.append(percent_change)
  output.append(dates)
  output.append(percentages)
  return output

# APP TASK: 5th function
# APP TASK: 8th function
def uecReadCsv(inFileName):
  directory = os.path.dirname(os.path.abspath(__file__)) + os.sep
  with open(directory + inFileName, "r") as inFile:
    csv_reader = csv.reader(inFile)
    output = []
    dates = []
    values = []
    skip_first_row = True
    for cols in csv_reader:
      if not skip_first_row:
        dates.append(cols[0])
        values.append(float(cols[1]))
      else:
        skip_first_row = False
    output.append(dates)
    output.append(values)
    return output

# APP TASK: 4th function
# APP TASK: 7th function
def uecWriteCsv(outFileName, datum):
  directory = os.path.dirname(os.path.abspath(__file__)) + os.sep
  with open(directory + outFileName, "w") as outFile:
    csv_writer = csv.writer(outFile)
    csv_writer.writerow(["Date", "Value"])
    for i in range(len(datum[0])):
      field1 = datum[0][i]
      field2 = datum[1][i]
      csv_writer.writerow([field1, field2])

# APP TASK: 3rd function
def uecGetTotalChange(conn, cur, series_id):
  datum = cur.execute("SELECT claim_claims.date, claim_claims.claims FROM claim_claims JOIN claim_types ON claim_claims.type_id = claim_types.type_id WHERE claim_types.type_name = '" + series_id + "' ORDER BY claim_id ASC").fetchall()
  output = []
  dates = []
  claims = []
  for data in datum:
    dates.append(data[0][:10])
    claims.append(data[1])
  output.append(dates)
  output.append(claims)
  return output

# APP TASK: 2nd function
# Populate the claim_claims table.
# Takes a Fed series_id and a list and inserts into the db.
def uecInsertClaimClaims(conn, cur, series_id, datum):
  claim_id = _uecGetMaxId(conn, cur, 'claim_claims', 'claim_id')
  series_id_num = cur.execute("SELECT type_id FROM claim_types WHERE type_name = '" + str(series_id) + "'").fetchone()[0]
  for data in datum:
    claim_id = claim_id + 1
    cur.execute("INSERT INTO claim_claims (claim_id, type_id, date, claims) VALUES(?, ?, ?, ?)", (claim_id, series_id_num, data['date'], data['value']))
    conn.commit()

# APP TASK: 1st function
# Get data from both Fed api URLs.
# Users _uecGetOffset() to skip n number of api records already stored in the database.
# Returns a list.
def uecGetData(conn, cur, settings, series_id):
  offset = _uecGetOffset(conn, cur, series_id)
  headers = "?series_id=" + series_id + "&api_key=" + API_KEY + "&file_type=" + settings['file_type'] + "&sort_order=" + settings['sort_order'] + "&observation_start=" + settings['observation_start'] + "&observation_end=" + settings['observation_end'] + "&limit=" + str(settings['limit']) + "&offset=" + str(offset)
  url = "https://api.stlouisfed.org/fred/series/observations" + headers
  response = requests.get(url)
  datum = response.json()['observations']
  return datum

# APP: 4nd function
# Inserts claim type rows, if absent, into clam_types.
# CONSTRAINT: Only these two series can execute; their hard-coded below.
# Additional series will need to be added below.
def uecInsertClaimTypes(conn, cur):
  icsa = cur.execute("SELECT type_id FROM claim_types WHERE type_name = 'ICSA'").fetchone()
  ccsa = cur.execute("SELECT type_id FROM claim_types WHERE type_name = 'CCSA'").fetchone()
  if icsa is None:
    cur.execute("INSERT INTO claim_types (type_id, type_name) VALUES(?, ?)", (1, 'ICSA'))
    conn.commit()
  if ccsa is None:
    cur.execute("INSERT INTO claim_types (type_id, type_name) VALUES(?, ?)", (2, 'CCSA'))
    conn.commit()

# APP: 3st function
# Create two tables which reference each other.
def uecCreateTables(conn, cur):
  cur.execute("CREATE TABLE IF NOT EXISTS claim_types (type_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, type_name TEXT NOT NULL UNIQUE)")
  cur.execute("CREATE TABLE IF NOT EXISTS claim_claims (claim_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, type_id INTEGER NOT NULL, date TEXT NOT NULL, claims INTEGER NOT NULL)")
  conn.commit()

# APP: 2st function
def uecAppTask(conn, cur, settings, series_id, title):
  data = uecGetData(conn, cur, settings, series_id)
  uecInsertClaimClaims(conn, cur, series_id, data)

  total_change = uecGetTotalChange(conn, cur, series_id)
  uecWriteCsv('unemployment-' + series_id + '-total.csv', total_change)
  total_change_total_csv = uecReadCsv('unemployment-' + series_id + '-total.csv')

  total_change_percentages = uecGetPercentChange(conn, cur, series_id)
  uecWriteCsv('unemployment-' + series_id + '-percentages.csv', total_change_percentages)
  total_change_percentages_csv = uecReadCsv('unemployment-' + series_id + '-percentages.csv')

  datum = []
  datum.append(total_change_total_csv)
  datum.append(total_change_percentages_csv)

  uecGraphClaims(series_id, datum, title)
  time.sleep(3) # Wait a few secs before hitting the Fed again.

# APP: 1st function
def uecApp(conn, cur):
  uecCreateTables(conn, cur)

  threshold = 29
  max_id = _uecGetMaxId(conn, cur, 'claim_claims', 'claim_id')
  if max_id > threshold:
    limit = 35
  else:
    limit = 3 # instead of 8 because we're storing two series

  settings = {
    'file_type': 'json',
    'sort_order': 'desc',
    'observation_start': "2000-01-01",
    'observation_end': datetime.now().strftime("%Y-%m-%d"),
    'limit': limit
  }

  uecInsertClaimTypes(conn, cur)
  uecAppTask(conn, cur, settings, "ICSA", "Initial Unemployment Claims")
  uecAppTask(conn, cur, settings, "CCSA", "Continued Unemployment Claims")

def uecMain():
  conn = sqlite3.connect(DB_NAME)
  cur = conn.cursor()
  uecApp(conn, cur)
  cur.close()

uecMain()