import argparse
import json
from execute_workflows import default_args
from execute_workflows import run_workload


def run_all_workloads(configurations):
    for dashboard_config_parameters in configurations:
        config_parameters = {}
        for key in default_args:
            config_parameters[key] = default_args[key]
        for key in dashboard_config_parameters:
            config_parameters[key] = dashboard_config_parameters[key]
        run_workload(config_parameters, 5, print_itemsets=False)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dashboard-config',
                        type=argparse.FileType('r'),
                        default='conf/dashboard_config.json',
                        help='File with a list of configuration parameters')
    args = parser.parse_args()
    args.workflows = json.load(args.dashboard_config)
    return args

if __name__ == '__main__':
    args = parse_args()
    run_all_workloads(args.workflows)
