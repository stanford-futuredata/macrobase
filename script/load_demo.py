import pandas as pd
import numpy as np
from sklearn import linear_model
from collections import defaultdict, Iterable
from itertools import chain, combinations
import operator
import random
import psycopg2

conn = psycopg2.connect("dbname='postgres' user='pbailis' host='localhost'")

cur = conn.cursor()

readings = 100000

firmwares = ["0.2.4", "0.3.1", "0.3.2", "0.4"]
models = ["M101", "M104", "M204", "M205", "M404", "M606"]
devices = [random.randint(100, 10000) for i in range(0, 100)]
if 2040 in devices:
    devices.remove(2040)

states = ["CA", "MA", "NY", "WY", "AR", "NV"]

cur.execute("DROP TABLE sensor_data; CREATE TABLE sensor_data ( reading_id bigint NOT NULL, device_id bigint NOT NULL, state varchar(2), model varchar(40), firmware_version varchar(40), temperature numeric, power_drain numeric );")

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

            

    sql = "INSERT INTO sensor_data VALUES ('%s', '%s', '%s', '%s', '%s', %f, %f);" % (r, d_id, state, model, firmware_version, temperature, power_drain)
    cur.execute(sql)


d_id = 2040
state = random.choice(states)
model = random.choice(models)
firmware_version = random.choice(firmwares)
for i in range(0, int(readings*.01)+random.randint(1, 2042)):
    power_drain = .8 + random.random()*.2
    if (state == "CA" and model == "M101" and firmware_version == "0.4"):
        temperature = 2 + random.random()*10
    else:
        temperature = 70+random.random()*10        

    sql = "INSERT INTO sensor_data VALUES ('%s', '%s', '%s', '%s', '%s', %f, %f);" % (r, d_id, state, model, firmware_version, temperature, power_drain)
    cur.execute(sql)

    
conn.commit()
