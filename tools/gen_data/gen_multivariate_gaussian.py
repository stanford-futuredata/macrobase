import numpy as np
import pandas as pd
import scipy.stats

def gen_data(n, fname):
    mean = np.array([10,10])
    cov = np.array([
        [10,8],
        [8,10],
    ])
    d = len(mean)
    dist = scipy.stats.multivariate_normal(mean, cov)
    points = dist.rvs(size=n, random_state=0)
    noise = (np.random.rand(int(0.05 * n), d)-0.5) * 15 + 7
    combined = np.vstack([
        np.hstack([points, [["A"]] * len(points)]),
        np.hstack([noise, [["B"]] * len(noise)])
    ])
    np.random.shuffle(combined)

    points = pd.DataFrame(
        combined,
        columns = ["m1", "m2", "class"],
    )
    points.to_csv(fname, index=False)


gen_data(2000000, "target/mv_gaussian_noise.csv")
