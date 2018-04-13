export class Query{
    pipeline: string;
    inputURI: string;
    classifier: string;
    metric: string;
    cutoff: number;
    includeHi: boolean;
    includeLo: boolean;
    summarizer: string;
    attributes: string[];
    ratioMetric: string;
    minRatioMetric: number;
    minSupport: number;
}

// {
//   "pipeline": "BasicBatchPipeline",
//   "inputURI": "csv://core/demo/sample.csv",
//   "classifier": "percentile",
//   "metric": "usage",
//   "cutoff": 1.0,
//   "includeHi": true,
//   "includeLo": true,
//   "summarizer": "aplinear",
//   "attributes": [
//     "location",
//     "version"
//   ],
//   "ratioMetric": "globalratio",
//   "minRatioMetric": 10.0,
//   "minSupport": 0.2
// }