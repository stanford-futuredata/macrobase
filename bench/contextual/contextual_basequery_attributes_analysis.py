import random
import psycopg2
import sys
from matplotlib.cbook import Null


port = None
if(len(sys.argv) > 1):
    port = sys.argv[1]

port = None
user = None
password = None
if(len(sys.argv) > 2):
    port = sys.argv[1]
    user = sys.argv[2]
    password = sys.argv[3]

conn = psycopg2.connect("dbname='postgres' host='localhost'" +
                         (" port="+port if port else "") +
                         (" user="+user if user else "") +
                         (" password="+password if password else ""))

cur = conn.cursor()


def getColumns(baseQuery):
    baseQuery = baseQuery.replace(";", " ")
    sql = baseQuery 
    cur.execute(sql)
    colNames = [desc[0] for desc in cur.description]
    return colNames;





def getColumnCountStats(baseQuery, colName):
    baseQuery = baseQuery.replace(";", " ")
    sql = "SELECT count(*), count(distinct %s), count(%s) FROM (%s) baseQuery" % (colName, colName, baseQuery)
    cur.execute(sql)
    record = cur.fetchone()
    return (record[0],record[1],record[2])

def getColumnMinMaxAverage(baseQuery, colName):
    try:
        baseQuery = baseQuery.replace(";", " ")
        sql = "SELECT min(%s), max(%s), avg(%s) FROM (%s) baseQuery" % (colName, colName, colName, baseQuery)
        cur.execute(sql)
        record = cur.fetchone()
        return (record[0],record[1],record[2])
    except psycopg2.Error as e:
        pass
        return (-1,-1,-1)
    

def getNumericColumns(baseQuery):
    # get all columnNames
    colNames = getColumns(baseQuery)
    
    baseQuery = baseQuery.replace(";", " ")
    sql = baseQuery 
    cur.execute(sql)
    row = cur.fetchone()
    
    result = list();
    for index, val in enumerate(row):
        colName = colNames[index]
        print "column {0} name {1} of type {2}".format(index, colName, type(val))
        if(type(val) == type(1) or  type(val) == type(1.0)):
            result.append(colName)
            
    return result;
   


def getPotentialDiscreteColumns(baseQuery):
    # get all columnNames
    colNames = getColumns(baseQuery)
    result = list()
    for colName in colNames:
        counts = getColumnCountStats(baseQuery,colName)
        numRows = counts[0]
        numDistinctValues = counts[1]
        numNonNulls = counts[2]
        if(numDistinctValues / numRows < 0.1):
            result.append(colName)
    return result

if __name__ == '__main__':
    
    #baseQuery = "SELECT * FROM mapmatch_history H, sf_datasets D WHERE H.dataset_id = D.id LIMIT 100000;"
    baseQuery = "SELECT * FROM marketing"
    
    print "Analyzing baseQuery: " + baseQuery
    
    numericColumns = getNumericColumns(baseQuery)
    print "Potential Metric Columns are : " + ', '.join(['\"' + x + '\"' for x in numericColumns])
    print "Potential Double Contextual Columns are : " + ', '.join(['\"' + x + '\"' for x in numericColumns])
    
    discreteColumns = getPotentialDiscreteColumns(baseQuery)
    print "Potential Discrete Contextual Columns are: " + ', '.join(['\"' + x + '\"' for x in discreteColumns])

