export class Query{
  constructor(
    public pipeline: string,
    public inputURI: string,
    public classifier: string,
    public metrics = new Set(),
    public cutoff: number,
    public includeHi: boolean,
    public includeLo: boolean,
    public summarizer: string,
    public attributes = new Set(),
    public ratioMetric: string,
    public minRatioMetric: number,
    public minSupport: number,
  ) { }
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