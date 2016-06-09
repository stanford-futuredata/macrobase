
'''
Polls the specified topology and writes specified bolt metrics
to output CSV file, optionally polling periodically.
'''

import requests
import csv
import time
import argparse

from pprint import pprint
from collections import defaultdict

parser = argparse.ArgumentParser()
parser.add_argument("-tracker", "--tracker_url",
                    required=True,
                    help="heron tracker url (or url:port)")
parser.add_argument("-c", "--cluster_name",
                    required=True,
                    help="heron cluster name")
parser.add_argument("-e", "--environ_name",
                    required=True,
                    help="heron environ name")
parser.add_argument("-topo", "--topology_name",
                    required=True,
                    help="heron topology name")
parser.add_argument("-o", "--output_file",
                    required=True,
                    help="output file for CSV")
parser.add_argument("-m", "--metric_names",
                    required=True,
                    nargs='+',
                    help="metrics to pull from each bolt")
parser.add_argument("-i", "--poll_interval",
                    default=None,
                    type=int,
                    help="polling interval (seconds; optional)")                   

args = parser.parse_args()

TRACKER_URL = args.tracker_url
CLUSTER_NAME = args.cluster_name
ENVIRON_NAME = args.environ_name
TOPOLOGY_NAME = args.topology_name
OUTPUT_FILE = args.output_file
METRIC_NAMES = [m.replace(",", "") for m in args.metric_names]
POLL_INTERVAL = args.poll_interval

# returns the last time queried
def poll(csv_output, last_time_queried=None):
    component_request = requests.get("http://"+TRACKER_URL+"/topologies/info", params={ "cluster": CLUSTER_NAME, "environ": ENVIRON_NAME, "topology": TOPOLOGY_NAME }).json()
    components = component_request['result']['logical_plan']['bolts'].keys()

    containers_to_hosts = {}
    stmgrs = component_request['result']['physical_plan']['stmgrs']
    for mgr in stmgrs:
        host = stmgrs[mgr]["host"]
        for container in stmgrs[mgr]["instance_ids"]:
            containers_to_hosts[container] = host

    query_time = int(time.time())
    # default: three hours ago
    start_time = last_time_queried if last_time_queried else query_time - 10800

    for component in components:
        metric_request = requests.get("http://"+TRACKER_URL+"/topologies/metricstimeline", params={ "cluster": CLUSTER_NAME, 
                                                                                                    "environ": ENVIRON_NAME,
                                                                                                    "topology": TOPOLOGY_NAME,
                                                                                                    "component": component,
                                                                                                    "metricname": METRIC_NAMES,
                                                                                                    "starttime": start_time,
                                                                                                    "endtime": query_time
                                                                                                }).json()
        # results contains a list of metric -> containers
        # we want to group by container instead: container -> metrics
        metrics_by_container = defaultdict(lambda:defaultdict(list))
        for m in METRIC_NAMES:
            containers_this_metric = metric_request['result']['timeline'][m]
            for container in containers_this_metric:
                metrics_by_container[container][m] = containers_this_metric[container]

        cnt = 0
        # convert container to CSV, once per timestamp
        for container in metrics_by_container:
            times = sorted(metrics_by_container[container].values()[0])
            flattened_metrics = zip(*[[str(float(m[1])) for m in sorted(metrics_by_container[container][metric].items())] for metric in METRIC_NAMES])
            for i in range(0, len(flattened_metrics)):
                cnt += 1
                out.writerow(tuple([times[i], component, container, containers_to_hosts[container]]) + flattened_metrics[i])

        print "{}: wrote {} metric{}".format(query_time, cnt, "s" if cnt > 1 else "")

        return query_time

print "writing to {}".format(OUTPUT_FILE)
f = open(OUTPUT_FILE, 'w')
out = csv.writer(f)
out.writerow(["time", "component", "container", "hostname"] + METRIC_NAMES)

poll_time = poll(out)
if POLL_INTERVAL:
    while True:
        time.sleep(POLL_INTERVAL)
        poll_time = poll(out, last_time_queried = poll_time)
        f.flush()
