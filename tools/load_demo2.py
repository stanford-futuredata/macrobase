import random
import psycopg2
import sys
import numpy

if len(sys.argv) < 2:
  print "Usage: python load_data2.py <percentage_inliers> <port (optional)>"
  exit(-1)
x = float(sys.argv[1])

port = None
if(len(sys.argv) > 2):
    port = sys.argv[2]

user = "postgres"
password = "postgres"
if(len(sys.argv) > 3):
    user = sys.argv[3]
    password = sys.argv[4]

host = "localhost"
if (len(sys.argv) > 4):
    host = sys.argv[5]


# TODO: These can be made configurable later
NUM_ATTRIBUTES = 4
NUM_METRICS = 5
NUM_READINGS = 100000

attributes = [
  ["Corolla", "Camry", "Prius", "RAV4", "Sienna", "Yaris", "Avalon", "Land Cruiser"],
  ["2010", "2011", "2013", "2006", "2001", "1995", "2015"],
  ["Blue", "Gray", "Black", "Yellow", "Orange", "Green", "Brown", "White"],
  ["CA", "MA", "NY", "WY", "AR", "NV", "PA", "WA", "WI"]
]

conn = psycopg2.connect("dbname='postgres'" +
                         (" host='" + host + "'") +
                         (" port="+port if port else "") +
                         (" user="+user if user else "") +
                         (" password="+password if password else ""))

cur = conn.cursor()

print "Creating table..."

cur.execute("DROP TABLE IF EXISTS car_data_demo; CREATE TABLE car_data_demo ( model varchar(40), year_of_make varchar(4), color varchar(7), state varchar(2), metric1 numeric, metric2 numeric, metric3 numeric, metric4 numeric, metric5 numeric );")

print "...created!"

print "Loading readings..."

means1 = list()
means2 = list()
stddevs = list()
for i in xrange(NUM_METRICS):
  means1.append(random.random())
  means2.append(500*random.random() + 100.0)
  stddevs.append(random.random() * 10.0)

for i in xrange(NUM_READINGS):
    picked_attributes = list()
    picked_means = list()
    if (random.random() * 100 < x):
      for j in xrange(NUM_ATTRIBUTES):
        picked_attributes.append(random.choice(attributes[j][:4]))
        picked_means = means1
    else:
      for j in xrange(NUM_ATTRIBUTES):
        picked_attributes.append(random.choice(attributes[j][4:]))
        picked_means = means2

    metrics = list()
    for j in xrange(NUM_METRICS):
      metrics.append(numpy.random.normal(picked_means[j], stddevs[j]))

    sql = "INSERT INTO car_data_demo VALUES ('%s', '%s', '%s', '%s', %f, %f, %f, %f, %f);" % (picked_attributes[0], picked_attributes[1], picked_attributes[2], picked_attributes[3], metrics[0], metrics[1], metrics[2], metrics[3], metrics[4])
    cur.execute(sql)

print "...loaded!"

conn.commit()
print "Done! Look at car_data_demo."
