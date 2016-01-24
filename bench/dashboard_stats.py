from execute_workflows import default_args
from execute_workflows import run_workload

all_dashboard_config_parameters = \
[
  {
    "isBatchJob": True,
    "taskName": "cmtDatasetComplex",
    "targetAttributes": ["build_version", "app_version", "deviceid", "hardware_carrier",
                         "state", "hardware_model"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["data_count_minutes", "data_count_accel_samples",
                          "data_count_netloc_samples", "data_count_gps_samples",
                          "distance_mapmatched_km", "distance_gps_km",
                          "battery_drain_rate_per_hour"],

    "baseQuery": "SELECT * FROM mapmatch_history H, sf_datasets D where H.dataset_id = D.id LIMIT 1000000;"
  },
  {
    "isBatchJob": True,
    "taskName": "campaignExpendituresComplex",
    "targetAttributes": ["cand_nm", "contbr_nm", "contbr_zip", "contbr_employer",
                         "contbr_employer"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["contb_receipt_amt"],

    "baseQuery": "SELECT * FROM campaign_expenditures LIMIT 1000000;"
  },
  {
    "isBatchJob": False,
    "taskName": "cmtDatasetComplexStreaming",
    "targetAttributes": ["build_version", "app_version", "deviceid", "hardware_carrier",
                         "state", "hardware_model"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["data_count_minutes", "data_count_accel_samples",
                          "data_count_netloc_samples", "data_count_gps_samples",
                          "distance_mapmatched_km", "distance_gps_km"],

    "baseQuery": "SELECT * FROM mapmatch_history H, sf_datasets D where H.dataset_id = D.id LIMIT 1000000;"
  },
  {
    "isBatchJob": False,
    "taskName": "campaignExpendituresComplexStreaming",
    "targetAttributes": ["cand_nm", "contbr_nm", "contbr_zip", "contbr_employer",
                         "contbr_employer"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["contb_receipt_amt"],

    "baseQuery": "SELECT * FROM campaign_expenditures LIMIT 1000000;"
  },
]

def run_all_workloads():
  for dashboard_config_parameters in all_dashboard_config_parameters:
    config_parameters = {}
    for key in default_args:
      config_parameters[key] = default_args[key]
    for key in dashboard_config_parameters:
      config_parameters[key] = dashboard_config_parameters[key]
    run_workload(config_parameters, print_itemsets=False)


if __name__ == '__main__':
  run_all_workloads()
