all_config_parameters = \
[
  {
    "isBatchJob": True,
    "taskName": "testTasks",
    "targetAttributes": ["device_id", "state", "model", "firmware_version"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["power_drain"],

    "baseQuery": "SELECT * FROM sensor_data;"
  },
  {
    "isBatchJob": True,
    "taskName": "cmtDatasetSimple",
    "targetAttributes": ["build_version", "app_version", "deviceid"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["data_count_minutes"],

    "baseQuery": "SELECT * FROM mapmatch_history H, sf_datasets D where H.dataset_id = D.id LIMIT 100000;"
  },
  {
    "isBatchJob": True,
    "taskName": "cmtDatasetComplex",
    "targetAttributes": ["build_version", "app_version", "deviceid", "hardware_carrier",
                         "state", "hardware_model"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["data_count_minutes", "data_count_accel_samples",
                          "data_count_netloc_samples", "data_count_gps_samples",
                          "distance_mapmatched_km", "distance_gps_km"],

    "baseQuery": "SELECT * FROM mapmatch_history H, sf_datasets D where H.dataset_id = D.id LIMIT 100000;"
  },
  {
    "isBatchJob": True,
    "taskName": "accidentsSimple",
    "targetAttributes": ["road_surface_conditions", "urban_or_rural_area", "speed_limit"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["number_of_casualties"],

    "baseQuery": "SELECT * FROM uk_road_accidents LIMIT 100000;"
  },
  {
    "isBatchJob": True,
    "taskName": "accidentsComplex",
    "targetAttributes": ["road_surface_conditions", "urban_or_rural_area", "speed_limit",
                         "road_type", "did_police_officer_attend_scene_of_accident"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["number_of_casualties", "number_of_vehicles", "accident_severity"],

    "baseQuery": "SELECT * FROM uk_road_accidents LIMIT 100000;"
  },
  {
    "isBatchJob": True,
    "taskName": "milanTelecomSimple",
    "targetAttributes": ["square_id", "country_code"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["sms_in_activity", "sms_out_activity"],

    "baseQuery": "SELECT * FROM milan_telecom;"
  },
  {
    "isBatchJob": True,
    "taskName": "milanTelecomComplex",
    "targetAttributes": ["square_id", "country_code"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["sms_in_activity", "sms_out_activity", "call_in_activity",
                          "call_out_activity", "internet_traffic_activity"],

    "baseQuery": "SELECT * FROM milan_telecom;"
  },
  {
    "isBatchJob": False,
    "taskName": "testTasks2",
    "targetAttributes": ["device_id", "state", "model", "firmware_version"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["power_drain"],

    "inputReservoirSize": 10000,
    "scoreReservoirSize": 10000,
    "inlierItemSummarySize": 1000,
    "outlierItemSummarySize": 10000,
    "summaryRefreshPeriod": 100000,
    "modelRefreshPeriod": 10000,
    "warmupCount": 1000,

    "baseQuery": "SELECT * FROM sensor_data;"
  }
]
