
import sys
import psycopg2

import numpy as np
import scipy.io as sio
import matplotlib.pyplot as plt
import pandas as pd
from scipy import stats
import math
from os import system, environ

from collections import defaultdict

conn = psycopg2.connect("dbname=postgres")
times = pd.io.sql.read_sql('SELECT min(time), max(time) FROM heron_data;',con=conn)

st = times.iloc[0]["min"]
en = times.iloc[0]["max"]

ranges = pd.date_range(st, en, freq='6H')

BASE_JAVA_OPTS = environ["JAVA_OPTS"] if "JAVA_OPTS" in environ else ""

for i in range(0, len(ranges)-1):
    r_st = ranges[i]
    r_en = ranges[i+1]

    print r_st, r_en

    system("cd ../../; cp script/heron/mb_heron_container.yaml /tmp/container.yaml;")
    f = open("/tmp/container.yaml", "a")
    f.write('\nmacrobase.loader.db.baseQuery: \"SELECT * FROM heron_data WHERE time >= \'{}\' AND time < \'{}\';\"\n'.format(r_st, r_en))
    f.close()
    system('cd ../../; mkdir heron_output; bin/pipeline.sh /tmp/container.yaml | tee heron_output/from-{}-to-{}.out'.format(str(r_st).replace(':', '-').replace(' ', '_'), str(r_en).replace(':', '-').replace(' ', '_')))
    
