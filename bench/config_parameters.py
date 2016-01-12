all_config_parameters = \
[
  {
    "isBatchJob": True,
    "taskName": "testTasks",
    "targetAttributes": ["device_id", "state", "model", "firmware_version"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["power_drain"],

    "baseQuery": "SELECT * FROM sensor_data_demo;"
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
                          "distance_mapmatched_km", "distance_gps_km",
                          "battery_drain_rate_per_hour"],

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
    "taskName": "milanTelecomSimple",
    "targetAttributes": ["square_id", "country_code"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["sms_in_activity", "sms_out_activity"],

    "baseQuery": "SELECT * FROM milan_telecom LIMIT 100000;"
  },
  {
    "isBatchJob": True,
    "taskName": "milanTelecomComplex",
    "targetAttributes": ["square_id", "country_code"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["sms_in_activity", "sms_out_activity", "call_in_activity",
                          "call_out_activity", "internet_traffic_activity"],

    "baseQuery": "SELECT * FROM milan_telecom LIMIT 100000;"
  },
  {
    "isBatchJob": True,
    "taskName": "campaignExpendituresSimple",
    "targetAttributes": ["cand_nm", "contbr_nm"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["contb_receipt_amt"],

    "baseQuery": "SELECT * FROM campaign_expenditures LIMIT 100000;"
  },
  {
    "isBatchJob": True,
    "taskName": "campaignExpendituresComplex",
    "targetAttributes": ["cand_nm", "contbr_nm", "contbr_zip", "contbr_employer",
                         "contbr_employer"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["contb_receipt_amt"],

    "baseQuery": "SELECT * FROM campaign_expenditures LIMIT 100000;"
  },
  {
    "isBatchJob": True,
    "taskName": "fedDisbursementsSimple",
    "targetAttributes": ["can_id", "com_id", "rec_nam"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["dis_amo"],

    "baseQuery": "SELECT * FROM fed_disbursements LIMIT 100000;"
  },
  {
    "isBatchJob": True,
    "taskName": "fedDisbursementsComplex",
    "targetAttributes": ["can_id", "com_id", "rec_nam", "rec_zip", "com_nam"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["dis_amo"],

    "baseQuery": "SELECT * FROM fed_disbursements LIMIT 100000;"
  },
  {
    "isBatchJob": False,
    "taskName": "testTasksStreaming",
    "targetAttributes": ["device_id", "state", "model", "firmware_version"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["power_drain"],

    "baseQuery": "SELECT * FROM sensor_data_demo;"
  },
  {
    "isBatchJob": False,
    "taskName": "cmtDatasetSimpleStreaming",
    "targetAttributes": ["build_version", "app_version", "deviceid"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["data_count_minutes"],

    "baseQuery": "SELECT * FROM mapmatch_history H, sf_datasets D where H.dataset_id = D.id LIMIT 100000;"
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

    "baseQuery": "SELECT * FROM mapmatch_history H, sf_datasets D where H.dataset_id = D.id LIMIT 100000;"
  },
  {
    "isBatchJob": False,
    "taskName": "accidentsSimpleStreaming",
    "targetAttributes": ["road_surface_conditions", "urban_or_rural_area", "speed_limit"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["number_of_casualties"],

    "baseQuery": "SELECT * FROM uk_road_accidents LIMIT 100000;"
  },
  {
    "isBatchJob": False,
    "taskName": "milanTelecomSimpleStreaming",
    "targetAttributes": ["square_id", "country_code"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["sms_in_activity", "sms_out_activity"],

    "baseQuery": "SELECT * FROM milan_telecom LIMIT 100000;"
  },
  {
    "isBatchJob": False,
    "taskName": "milanTelecomComplexStreaming",
    "targetAttributes": ["square_id", "country_code"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["sms_in_activity", "sms_out_activity", "call_in_activity",
                          "call_out_activity", "internet_traffic_activity"],

    "baseQuery": "SELECT * FROM milan_telecom LIMIT 100000;"
  },
  {
    "isBatchJob": False,
    "taskName": "campaignExpendituresSimpleStreaming",
    "targetAttributes": ["cand_nm", "contbr_nm"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["contb_receipt_amt"],

    "baseQuery": "SELECT * FROM campaign_expenditures LIMIT 100000;"
  },
  {
    "isBatchJob": False,
    "taskName": "campaignExpendituresComplexStreaming",
    "targetAttributes": ["cand_nm", "contbr_nm", "contbr_zip", "contbr_employer",
                         "contbr_employer"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["contb_receipt_amt"],

    "baseQuery": "SELECT * FROM campaign_expenditures LIMIT 100000;"
  },
  {
    "isBatchJob": False,
    "taskName": "fedDisbursementsSimpleStreaming",
    "targetAttributes": ["can_id", "com_id", "rec_nam"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["dis_amo"],

    "baseQuery": "SELECT * FROM fed_disbursements LIMIT 100000;"
  },
  {
    "isBatchJob": False,
    "taskName": "fedDisbursementsComplexStreaming",
    "targetAttributes": ["can_id", "com_id", "rec_nam", "rec_zip", "com_nam"],
    "targetLowMetrics": [],
    "targetHighMetrics": ["dis_amo"],

    "baseQuery": "SELECT * FROM fed_disbursements LIMIT 100000;"
  },
]
