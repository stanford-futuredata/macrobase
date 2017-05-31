from __future__ import print_function, unicode_literals

import os
import random
import numpy as np
import pandas as pd


class GaussianGenerator(object):
    def __init__(self, mu=0.0, sigma=1.0, floor=None, roof=None):
        self.mu = mu
        self.sigma = sigma
        self.floor = floor
        self.roof = roof

    def get(self, n):
        a = np.random.normal(self.mu, self.sigma, size=n)
        if self.floor is not None:
            a[a < self.floor] = self.floor
        if self.roof is not None:
            a[a > self.roof] = self.roof
        return a


class UniformGenerator(object):
    def __init__(self, values=list([0])):
        self.values = values

    def get(self, n):
        c = len(self.values)
        a = np.random.randint(0, c, size=n)
        av = np.empty(a.shape, dtype=object)
        for i in range(c):
            cur_value = self.values[i]
            av[a == i] = cur_value
        return av


def generate_data(
        generators,
        anomaly_match,
        anomaly_generators,
        num_rows=10000,
        baseline_rate=0.0, anomaly_precision=1.0,
        start_time=0,
        seed=0
):
    np.random.seed(seed)
    df = pd.DataFrame()
    df["time"] = np.arange(start_time, start_time+num_rows, 1)
    for col_name, col_gen in generators.items():
        df[col_name] = col_gen.get(num_rows)

    match_filter = (np.random.binomial(n=1, p=anomaly_precision, size=num_rows) > 0)
    for match_column, match_value in anomaly_match.items():
        match_filter &= (df[match_column] == match_value)
    match_filter |= (np.random.binomial(n=1, p=baseline_rate, size=num_rows) > 0)

    num_anomalies = np.sum(match_filter)

    for col_name, col_gen in anomaly_generators.items():
        df.loc[match_filter, col_name] = col_gen.get(num_anomalies)

    return df


FOLDER_PATH = os.path.expanduser("src/test/resources/")
FILE_PATH = os.path.join(FOLDER_PATH, "data3.csv")


ATTRIBUTE_VALUES = {
    "android_version": ["4.1", "4.2", "4.3", "5.0", "5.1", "6.0", "7.0", "7.1"],
    "hardware_model": ["GT-I9505", "SCH-I545", "SM-G920V", "SM-G950F", "Nexus 9", "XT1097"],
    "cache_server": ["us-west-1", "us-west-2", "us-east-1", "eu-west-1"],
    "origin_country": ["USA", "CAN", "RUS", "CHN", "JPN", "IND"],
    "power_saving": [True, False],
    "network_type": ["4G", "LTE", "3G", "WiFi"],
}


def write_datafile(version_no=2, k=2):
    # Define normal column value generators
    app_versions = list(range(1, version_no+1))
    generators = {
        "response_latency": GaussianGenerator(10, 10, floor=0),
        "cpu_usage": GaussianGenerator(30, 20, floor=0),
        "app_version": UniformGenerator(app_versions),
    }
    for attr_column, attr_values in ATTRIBUTE_VALUES.items():
        generators[attr_column] = UniformGenerator(attr_values)

    # Randomly generate anomaly match columns
    attribute_names = list(ATTRIBUTE_VALUES.keys())
    np.random.shuffle(attribute_names)
    # anomaly_attributes = attribute_names[:k]
    # anomaly_match = {
    #     "app_version": version_no,
    # }
    anomaly_attributes = ['hardware_model']
    anomaly_match = {}
    if k == 0:
        anomaly_match = {}
    for col_name in anomaly_attributes:
        # match_idx = random.randint(0, len(ATTRIBUTE_VALUES[col_name]) - 1)
        match_idx = 3
        anomaly_match[col_name] = ATTRIBUTE_VALUES[col_name][match_idx]
    print(anomaly_match)
    f = open("bug.txt", "w")
    f.write(str(anomaly_match))
    f.close()

    df = generate_data(
        generators=generators,
        anomaly_match=anomaly_match,
        anomaly_generators={
            "cpu_usage": GaussianGenerator(30, 0.1, floor=0)
            # "cpu_usage": GaussianGenerator(75, 10, floor=0)
        },
        baseline_rate=0.0001,
        num_rows=100000,
    )
    df.to_csv(
        FILE_PATH,
        header=True,
        index=False,
        float_format="%.4g"
    )

def main():
    write_datafile(k=1)

if __name__ == "__main__":
    main()
