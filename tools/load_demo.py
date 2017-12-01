import random
import psycopg2
import sys


port = None
if(len(sys.argv) > 1):
    port = sys.argv[1]

user = None
password = None
if(len(sys.argv) > 2):
    user = sys.argv[2]
    password = sys.argv[3]

host = "localhost"
if (len(sys.argv) > 3):
    host = sys.argv[4]

conn = psycopg2.connect("dbname='postgres'" +
                         (" host='" + host + "'") +
                         (" port="+port if port else "") +
                         (" user="+user if user else "") +
                         (" password="+password if password else ""))

cur = conn.cursor()

readings = 100000

firmwares = ["0.2.4", "0.3.1", "0.3.2", "0.4"]
models = ["M101", "M104", "M204", "M205", "M404", "M606"]
devices = [random.randint(100, 10000) for i in range(0, 100)]
if 2040 in devices:
    devices.remove(2040)

states = ["CA", "MA", "NY", "WY", "AR", "NV"]

print "Creating table..."

cur.execute("DROP TABLE IF EXISTS sensor_data_demo; CREATE TABLE sensor_data_demo ( reading_id bigint NOT NULL, device_id bigint NOT NULL, state varchar(2), model varchar(40), firmware_version varchar(40), temperature numeric, power_drain numeric );")

print "...created!"

print "Loading inlier readings..."

for r in range(123912, 123912+readings-242):
    d_id = random.choice(devices)
    state = random.choice(states)
    model = random.choice(models)
    firmware_version = random.choice(firmwares)

    power_drain = .2+.2*random.random()

    if (state == "CA" and model == "M101" and firmware_version == "0.4"):
        temperature = 2 + random.random()*10
    else:
        temperature = 70+random.random()*10        

            

    sql = "INSERT INTO sensor_data_demo VALUES ('%s', '%s', '%s', '%s', '%s', %f, %f);" % (r, d_id, state, model, firmware_version, temperature, power_drain)
    cur.execute(sql)

print "...loaded!"
print "Loading outlier readings..."
    
d_id = 2040
state = random.choice(states)
model = random.choice(models)
firmware_version = random.choice(firmwares)
for i in range(0, int(readings*.01)+random.randint(1, 2042)):
    r += 1
    power_drain = .8 + random.random()*.2
    if (state == "CA" and model == "M101" and firmware_version == "0.4"):
        temperature = 2 + random.random()*10
    else:
        temperature = 70+random.random()*10        

    sql = "INSERT INTO sensor_data_demo VALUES ('%s', '%s', '%s', '%s', '%s', %f, %f);" % (r, d_id, state, model, firmware_version, temperature, power_drain)
    cur.execute(sql)

print "...loaded!"
    
conn.commit()

print "Done! Look at sensor_data_demo."
