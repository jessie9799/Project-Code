
import pymysql
import csv
import datetime
from urllib.request import urlopen
from sqlalchemy import create_engine, types
import pandas as pd
import pymysql.cursors
import pymysql
from bs4 import BeautifulSoup
pymysql.install_as_MySQLdb()

Connection=pymysql.connect(host="127.0.0.1", user='root', password='12345', db='pj', cursorclass=pymysql.cursors.DictCursor)

engine=create_engine('mysql://root:12345@127.0.0.1/pj')

year=2020
url ="https://www.basketball-reference.com/leagues/NBA_2020_totals.html"
html=urlopen(url)
soup = BeautifulSoup(html)

#get the column header
soup.findAll('tr',limit=2)
headers=[th.getText() for th in soup.findAll('tr',limit=2)[0].findAll('th')]

headers = headers[1:]
headers

# avoid the first header row
rows = soup.findAll('tr')[1:]
player_stats = [[td.getText() for td in rows[i].findAll('td')] for i in range(len(rows))]

stats = pd.DataFrame(player_stats, columns = headers)
stats[['Player', 'Pos', 'Age']]
sql = "create table if not exists PlayerInfo (player varchar(20), pos varchar(5), age int(2));"
cursor=Connection.cursor()
#cursor.execute(sql)
#Connection.commit()

stats.to_sql('PlayerInfo', con=engine,index=False,if_exists='append')


print("done")
sql2="show tables;"
cursor.execute(sql2)

with open("NBA_Games_all_Decade.csv" , "r") as csvFile:
    csvReader = csv.reader(csvFile)
    colName = next(csvReader)
    targetCol = ["","Date","Tm","Opp","Result","MP","FG","FGA","FG%","2P","2PA","2P%","3P","3PA","3P%","FT","FTA","FT%","PTS","OPP_FG","OPP_FGA","OPP_FG%","OPP_2P","OPP_2PA","OPP_2P%","OPP_3P","OPP_3PA","OPP_3P%","OPP_FT","OPP_FTA","OPP_FT%","OPP_PTS"]
##    print(targetCol)
    targetIndex = {}
    for target in targetCol:
        targetIndex[target] = colName.index(target)
    
    colName = {}
    colName[""] = "matchId"
    colName["Date"] = "date"
    colName["Tm"] = "team"
    colName["Opp"] = "opponentTeam"
    colName["Result"] = "result"
    colName["MP"] = "minutesPlayed"
    colName["FG"] = "fieldGoalsMade"
    colName["FGA"] = "fieldGoalsAttempted"
    colName["FG%"] = "fieldGoalPercentage"
    colName["2P"] = "2pointersMade"
    colName["2PA"] = "2pointersAttempted"
    colName["2P%"] = "2pointersPercentage"
    colName["3P"] = "3pointersMade"
    colName["3PA"] = "3pointersAttempted"
    colName["3P%"] = "3pointersPercentage"
    colName["FT"] = "freeThrowsMade"
    colName["FTA"] = "freeThrowsAttempted"
    colName["FT%"] = "freeThrowsPercentage"
    colName["PTS"] = "PTS"
    colName["OPP_FG"] = "opp_FieldGoalMade"
    colName["OPP_FGA"] = "opp_FieldGoalAttempted"
    colName["OPP_FG%"] = "opp_FieldGoalPercentage"
    colName["OPP_2P"] = "opp_2pointersMade"
    colName["OPP_2PA"] = "opp_2pointersAttempted"
    colName["OPP_2P%"] = "opp_2pointersPercentage"
    colName["OPP_3P"] = "opp_3pointersMade"
    colName["OPP_3PA"] = "opp_3pointersAttempted"
    colName["OPP_3P%"] = "opp_3pointersPercentage"
    colName["OPP_FT"] = "opp_freeThrowsMade"
    colName["OPP_FTA"] = "opp_freeThrowsAttempted"
    colName["OPP_FT%"] = "opp_freeThrowsPercentage"
    colName["OPP_PTS"] = "opp_points"
            
    colType = {}
    colType[""] = "int"
    colType["Date"] = "date"
    colType["Tm"] = "text"
    colType["Opp"] = "text"
    colType["Result"] = "text"
    colType["MP"] = "int"
    colType["FG"] = "int"
    colType["FGA"] = "int"
    colType["FG%"] = "numeric"
    colType["2P"] = "int"
    colType["2PA"] = "int"
    colType["2P%"] = "numeric"
    colType["3P"] = "int"
    colType["3PA"] = "int"
    colType["3P%"] = "numeric"
    colType["FT"] = "int"
    colType["FTA"] = "int"
    colType["FT%"] = "numeric"
    colType["PTS"] = "int"
    colType["OPP_FG"] = "int"
    colType["OPP_FGA"] = "int"
    colType["OPP_FG%"] = "numeric"
    colType["OPP_2P"] = "int"
    colType["OPP_2PA"] = "int"
    colType["OPP_2P%"] = "numeric"
    colType["OPP_3P"] = "int"
    colType["OPP_3PA"] = "int"
    colType["OPP_3P%"] = "numeric"
    colType["OPP_FT"] = "int"
    colType["OPP_FTA"] = "int"
    colType["OPP_FT%"] = "numeric"
    colType["OPP_PTS"] = "int"


    #create table if it doesn't exist
    sql = "create table if not exists Matches ("
    for target in targetCol:
        sql += colName[target] + " " + colType[target] + "," 
    sql = sql[0:-1] + ", primary key (matchId));"
##    print( sql )
    cursor.execute(sql)

    sql = "create table if not exists Date (date date, month int, day int, year int, Day_ofthe_Week text, primary key (date));"
##    print( sql )
    cursor.execute(sql)
    
    dates = []
    #insert 100 rows
    count = 0
    for row in csvReader:
        count += 1
        sql = "insert into Matches values ("
        for target in targetCol:
            if colType[target] == "text" or colType[target] == "date":
                sql += '"' + row[targetIndex[target]] + '",'
            else:
                sql += row[targetIndex[target]] + ","
        sql = sql[0:-1] + ");"
##        print(sql)

        date = row[targetIndex["Date"]]
        if date not in dates:
            dates.append(date)
        cursor.execute(sql)
    print("Inserted", count, "rows in Matches table.")
    Connection.commit()

    count = 0
    for date in dates:
        count += 1
        month = date[5:7]
        day = date[8:]
        year = date[0:4]
        
        dayOfWeek = datetime.date(int(year), int(month), int(day)).weekday()
        if dayOfWeek == 0:
            dayOfWeek = "Mon"
        elif dayOfWeek == 1:
            dayOfWeek = "Tue"
        elif dayOfWeek == 2:
            dayOfWeek = "Wed"
        elif dayOfWeek == 3:
            dayOfWeek = "Thu"
        elif dayOfWeek == 4:
            dayOfWeek = "Fri"
        elif dayOfWeek == 5:
            dayOfWeek = "Sat"
        elif dayOfWeek == 6:
            dayOfWeek = "Sun"
        sql = 'insert into Date values ("' + date + '",' + month + "," + day + "," + year + ',"' + dayOfWeek + '");'
##        print(sql)
        cursor.execute(sql)
    print("Inserted", count, "rows in Date table.")
    Connection.commit()

    csvFile.close()
Connection.close
