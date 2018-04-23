class Result{
  metric: Object;
  matcher: Object;
  matcherString: string;
  aggregate: Object;
}

export class QueryResult{
  numTotal: number;
  outliers: number;
  results: Result[];
}

  // {"numTotal":1057.0,"outliers":18.0,"results":[{"metric":{"global_ratio":"29.361","support":"0.556"},"matcher":{"location":"CAN","version":"v3"},"aggregate":{"Outliers":"10.000","Count":"20.000"}}]}