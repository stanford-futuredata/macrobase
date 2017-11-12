#!/usr/bin/env python
import subprocess
import sys

proc = "mvn test -Dtest=macrobase.analysis.classify.MultiMADOptimizationTest -Dinput={} -Dmetrics={} -Dattr={} -Dtrials={} -Ddataset={}"
trials = 2

if sys.argv[1] == "cmt_sm":
    #CMT Test Query
    input = "/data/pbailis/preagg/cmt_sm.csv"
    metrics = "data_count_accel_samples,data_count_netloc_samples"
    attr = "build_version"

elif sys.argv[1] == "cmt":
    input = "/data/pbailis/preagg/cmt.csv"
    metrics = "data_count_minutes,data_count_accel_samples,data_count_netloc_samples,data_count_gps_samples,distance_mapmatched_km,distance_gps_km,battery_drain_rate_per_hour"
    attr = "build_version"

else:
    input = "src/test/resources/shuttle.csv"
    metrics = "A0,A1,A2,A3,A4,A5,A6,A7,A8"
    attr = "A9"

subprocess.call(proc.format(input, metrics, attr, trials, sys.argv[1]), shell=True)

