import {Injectable} from '@angular/core';
import {HttpClient, HttpHeaders, HttpErrorResponse} from '@angular/common/http';

import {Observable} from 'rxjs/Observable';

import {QueryResult} from "./query-result"
import {Query} from "./query"
import { MessageService } from "./message.service"

@Injectable()
export class QueryService {
  private queryURL = 'http://0.0.0.0:4567/query';

  private httpOptions = {
    headers: new HttpHeaders({ 'Access-Control-Allow-Headers': 'Accept, Content-Type',
                               'Accept': 'application/json',
                               'Content-Type': 'application/json' })
  };

  constructor(private http: HttpClient, private messageService: MessageService) { }

  bogusResult: QueryResult = {
    numTotal: -1,
    outliers: -1,
    results: [{"metric":{"global_ratio":"-1","support":"-1"},
               "matcher":{"location":"None","version":"None"},
               "aggregate":{"Outliers":"-1","Count":"-1"}}]
    };

  queryResult: QueryResult;
  queryString: string = "Result Here";

  runQuery(query: Query) {
    this.messageService.add("Running query on: " + JSON.stringify(query));
    this.http.post<QueryResult>(this.queryURL, query, this.httpOptions)
      .subscribe(
        data => this.queryResult = data,
        err => this.handleError('runQuery()', err)
      )
    // this.http.post<QueryResult>(this.queryURL, '{"pipeline":"BasicBatchPipeline","inputURI":"csv://core/demo/sample.csv","classifier":"percentile","metric":"usage","cutoff":1.0,"includeHi":true,"includeLo":true,"summarizer":"aplinear","attributes":["location","version"],"ratioMetric":"globalratio","minRatioMetric":10.0,"minSupport":0.2}')
    //   .subscribe(
    //     data => this.queryResult = data,
    //     err => this.handleError('runQuery()', err)
    //   )
    this.updateString();
  }

  updateString() {
    this.queryString = JSON.stringify(this.queryResult, undefined, 3);
  }

  private handleError(fname: string, err: HttpErrorResponse) {
    if (err.error instanceof Error) {
      this.messageService.add(fname + ": An error occurred: " + err.error.message);
    } else {
      this.messageService.add(fname + ": Backend returned code " + err.status + ": " + JSON.stringify(err.error));
    }
  }
}

// curl -H "Accept: application/json" -H "Content-type: application/json" -d '{"pipeline":"BasicBatchPipeline","inputURI":"csv://core/demo/sample.csv","classifier":"percentile","metric":"usage","cutoff":1.0,"includeHi":true,"includeLo":true,"summarizer":"aplinear","attributes":["location","version"],"ratioMetric":"globalratio","minRatioMetric":10.0,"minSupport":0.2}' 0.0.0.0:4567/query; echo