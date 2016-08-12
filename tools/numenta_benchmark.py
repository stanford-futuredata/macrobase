import psycopg2
import sys
import os
import os.path
import csv
import dateutil.parser
from datetime import datetime
import itertools
import re
import json

# Note: initially need to run `python scripts/create_new_detector.py --detector macrobase_ma`
# This script also requires additional logging statements in MacroBase - you need to use the nab branch.
# Then run this script with an argument of the path to the numenta anomaly benchmark directory.

# TODO we should probably tune window size
# TODO should add config parameter to disable scoring, change training percentage (0.15 for NAB)
config = '''
macrobase.query.name: numenta
macrobase.loader.attributes: [value]
macrobase.loader.targetLowMetrics: []
macrobase.loader.targetHighMetrics: [value]
macrobase.loader.timeColumn: timestamp

macrobase.loader.db.dbUrl: postgres
macrobase.loader.db.baseQuery: SELECT * FROM nab;

macrobase.analysis.detectorType: MOVING_AVERAGE

macrobase.analysis.minSupport: 0.001
macrobase.analysis.minOIRatio: 1

macrobase.analysis.useZScore: false

macrobase.analysis.usePercentile: true
macrobase.analysis.targetPercentile: 0.99

macrobase.analysis.timeseries.tupleWindow: 100

logging:
  level: WARN

  loggers:
    macrobase.analysis.outlier: DEBUG
'''
conf_file = '/tmp/macrobase_nab.conf'
result_file = '/tmp/macrobase_results.out'
mb_detector_name = 'macrobase_ma'

def main():
    if len(sys.argv) != 2:
        print 'Usage: numenta_benchmark.py PATH_TO_NAB_ROOT'
        sys.exit(1)

    with open(conf_file, 'w') as f:
        f.write(config)

    bench_dir = sys.argv[1]

    with open(os.path.join(bench_dir, 'labels', 'combined_windows.json')) as jf:
            label_windows = json.load(jf)
            for path, labels in label_windows.iteritems():
                label_windows[path] = map(lambda pair: map(tounixdate, pair), labels)

    data_dir = os.path.join(bench_dir, 'data')
    for dirpath, dirnames, filenames in os.walk(data_dir):
        for f in filenames:
            if not f.endswith('.csv'):
                continue
            reldir = os.path.relpath(dirpath, data_dir)
            print reldir
            path = os.path.join(dirpath, f)
            relpath = os.path.join(reldir, f)
            outpath = os.path.join(bench_dir, 'results', mb_detector_name, reldir, '%s_%s' % (mb_detector_name, f))
            run(path, outpath, label_windows[relpath])

    print 'Done! Inside the NAB directory run `python run.py -d %s --optimize --score --normalize`' % mb_detector_name

def run(csvpath, outpath, labels):
    conn = psycopg2.connect("dbname='postgres' host='localhost'")
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS nab; CREATE TABLE nab ( timestamp bigint NOT NULL, value numeric NOT NULL );")

    print 'Loading ' + csvpath
    with open(csvpath, 'rb') as in_csv:
        reader = csv.reader(in_csv)
        for row in skip_first(reader):  # Skip header
            cur.execute("INSERT INTO nab VALUES ('%s', '%s');" % (tounixdate(row[0]), row[1]))

    conn.commit()

    with open(conf_file, 'w') as f:
        f.write(config)

    print 'Running macrobase...'
    # Quick n dirty
    cmd = '''java ${{JAVA_OPTS}} \\
            -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*" \\
            macrobase.MacroBase batch {conf_file} \\
            > {result_file}'''.format(conf_file=conf_file, result_file=result_file)
    os.system(cmd)

    with open(result_file) as f:
        # Remove all log superfluous text
        it = skip_first(itertools.dropwhile(lambda l: 'Starting scoring...' not in l, iter(f)))
        it = itertools.takewhile(lambda l: re.match(r'\d', l), it)
        out_csv = open(outpath, 'w')
        in_csv = open(csvpath, 'rb')

        reader = skip_first(csv.reader(in_csv))
        out_csv.write('timestamp,value,anomaly_score,label\n')
        for scoreline, inrow in zip(it, reader):
            [timestamp, per_diff] = scoreline.strip().split(',')
            timestamp = int(timestamp)
            label = 1 if any(map(lambda t: t[0] <= timestamp <= t[1], labels)) else 0
            out_csv.write('%s,%s,%f,%d\n' % (inrow[0], inrow[1], toscore(float(per_diff)), label))

        in_csv.close()
        out_csv.close()
    print 'Output to ' + outpath

def todate(sdt):
    return dateutil.parser.parse(sdt)
def tounixdate(sdt):
    return int((todate(sdt) - datetime(1970, 1, 1)).total_seconds())
def datetostr(dt):
    return datetime.strftime(t, "%Y-%m-%d %H:%M:%S")
def skip_first(it):
    return itertools.islice(it, 1, None)

def toscore(per_diff):
    # Arbitrary function to map differences in [0, inf] to [0, 1].
    return 1 / (-per_diff - 1) + 1

if __name__ == '__main__':
    main()
