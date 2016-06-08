import sys
import os
import os.path
import csv
import dateutil.parser
from datetime import datetime
import itertools
import re
import json

# Note: initially need to run `python scripts/create_new_detector.py --detector macrobase`
# Then run this script with an argument of the path to the numenta anomaly benchmark directory.

# TODO we should probably tune window size
# TODO should add config parameter to disable scoring, change training percentage (0.15 for NAB)

# rJava config information - see ARIMA.java for more information
R_HOME = '/Library/Frameworks/R.framework/Resources/'
JAVA_LIB_PATH = '/Library/Frameworks/R.framework/Resources/library/rJava/jri/'

# Paths to write out intermediate data to
CONF_FILE = '/tmp/macrobase_nab.conf'
CSV_TEMP_FILE = '/tmp/numenta.csv'
SCORE_FILE = 'numenta_scores.json'

CONFIG = '''
macrobase.query.name: numenta
macrobase.loader.attributes: []
macrobase.loader.targetLowMetrics: [timestamp]
macrobase.loader.targetHighMetrics: [value]
macrobase.loader.timeColumn: 0

macrobase.loader.loaderType: CSV_LOADER
macrobase.loader.csv.file: %s

macrobase.analysis.transformType: ARIMA

macrobase.analysis.minSupport: 0.001
macrobase.analysis.minOIRatio: 1

macrobase.analysis.useZScore: false

macrobase.analysis.usePercentile: true
macrobase.analysis.targetPercentile: 0.99

macrobase.analysis.timeseries.tupleWindow: 576
macrobase.analysis.arima.predictSize: 288
macrobase.analysis.arima.fftPeriod: 288
macrobase.analysis.arima.fftK: 20

macrobase.diagnostic.dumpScoreFile: %s

logging:
  level: WARN
''' % (CSV_TEMP_FILE, SCORE_FILE)
MB_DETECTOR_NAME = 'macrobase'

def main():
    if len(sys.argv) != 2:
        print 'Usage: numenta_benchmark.py PATH_TO_NAB_ROOT'
        sys.exit(1)

    with open(CONF_FILE, 'w') as f:
        f.write(CONFIG)

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
            path = os.path.join(dirpath, f)
            relpath = os.path.join(reldir, f)
            outpath = os.path.join(bench_dir, 'results', MB_DETECTOR_NAME, reldir, '%s_%s' % (MB_DETECTOR_NAME, f))
            run(path, outpath, label_windows[relpath])

    print 'Done! Inside the NAB directory run `python run.py -d %s --optimize --score --normalize`' % MB_DETECTOR_NAME

def run(csvpath, outpath, labels):
    print 'Loading ' + csvpath
    # Need to transform CSV since currently we only accept columns containing
    # numbers, whereas Numenta timestamps are strings.
    with open(csvpath, 'rb') as in_csv, open(CSV_TEMP_FILE, 'w') as out_csv:
        reader = csv.reader(in_csv)
        for i, row in enumerate(reader):
            if i == 0:
                out_csv.write(','.join(row) + '\n')
            else:
                out_csv.write('%s,%s\n' % (tounixdate(row[0]), row[1]))

    print 'Running macrobase...'
    if not (os.path.exists('target') and os.path.isdir('target')):
        print 'Error: ./target/ directory not found. Make sure you are running this from the root of the MacroBase directory (i.e. python script/numenta_benchmark.py PATH_TO_NAB_ROOT) and that MacroBase is compiled.'
        sys.exit(1)
    # Quick n dirty
    cmd = '''export R_HOME=%s; \\
            java ${JAVA_OPTS} -cp "src/main/resources/:target/classes:target/lib/*:target/dependency/*:target/test-classes" \\
            -Djava.library.path=%s \\
            macrobase.diagnostic.DiagnosticMain dump %s > /dev/null''' % (R_HOME, JAVA_LIB_PATH, CONF_FILE)
    exit_code = os.system(cmd)
    if exit_code != 0:
        print 'Error running MacroBase'
        sys.exit(1)

    with open(os.path.join('target', 'scores', SCORE_FILE)) as f:
        scores = json.load(f)
        out_csv = open(outpath, 'w')
        in_csv = open(csvpath, 'rb')

        reader = skip_first(csv.reader(in_csv))
        out_csv.write('timestamp,value,anomaly_score,label\n')
        for datum, inrow in zip(scores, reader):
            timestamp = tounixdate(inrow[0])
            label = 1 if any(map(lambda t: t[0] <= timestamp <= t[1], labels)) else 0
            out_csv.write('%s,%s,%f,%d\n' % (inrow[0], inrow[1], toscore(datum['score']), label))

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
