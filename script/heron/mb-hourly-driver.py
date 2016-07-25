import sys
import MySQLdb
from os import system, environ
from datetime import timedelta

conn = MySQLdb.connect(host="", db="", user="", passwd="")

cursor = conn.cursor()
cursor.execute("USE herondatabase;")

cursor.execute('SELECT min(time) FROM heron_data;')
st = cursor.fetchone()[0]
cursor.execute('SELECT max(time) FROM heron_data;')
en = cursor.fetchone()[0]
print st, en

# ranges = pd.date_range(st, en, freq='6H')
# print ranges

BASE_JAVA_OPTS = ""
if "JAVA_OPTS" in environ: BASE_JAVA_OPTS = environ["JAVA_OPTS"]

# for i in range(0, len(ranges)-1):
while (st < en):
    r_st = st
    r_en = st+timedelta(hours=4)

    print r_st, r_en

    system("cd ../../; cp script/heron/mb_heron_container.yaml /tmp/container.yaml;")
    f = open("/tmp/container.yaml", "a")
    f.write('\nmacrobase.loader.db.baseQuery: \"SELECT * FROM heron_data WHERE time >= \'{}\' AND time < \'{}\';\"\n'.format(r_st, r_en))
    f.close()
    system('cd ../../; mkdir heron_output; java -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*:target/test-classes" macrobase.MacroBase pipeline /tmp/container.yaml | tee heron_output/from-{}-to-{}.out'.format(str(r_st).replace(':', '-').replace(' ', '_'), str(r_en).replace(':', '-').replace(' ', '_')))
    
    st = st+timedelta(hours=4)
