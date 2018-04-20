import {Injectable} from '@angular/core';
import {HttpClient, HttpHeaders, HttpErrorResponse} from '@angular/common/http';

import {Observable} from 'rxjs/Observable';
import { catchError, map, tap } from 'rxjs/operators';

import {QueryResult} from "./query-result";
import {Query} from "./query";
import { MessageService } from "./message.service";

import * as $ from "jquery";

@Injectable()
export class QueryService {
  private queryURL = 'http://0.0.0.0:4567/query';

  private httpOptions = {
    headers: new HttpHeaders({ 'Accept': 'application/json',
                               'Content-Type': 'application/json' })
  };

  constructor(private http: HttpClient, private messageService: MessageService) { }

  queryResult: QueryResult;
  resultsString: string;

  runQuery(query: Query) {
    this.messageService.add("Running query on: " + JSON.stringify(query));
    this.http.post<QueryResult>(this.queryURL, JSON.stringify(query))
      .subscribe(
        data => {this.queryResult = data;
                 this.updateString();},
        err => {this.handleError('runQuery()', err);}
      );
    // this.http.post<QueryResult>(this.queryURL, '{"pipeline":"BasicBatchPipeline","inputURI":"csv://core/demo/sample.csv","classifier":"percentile","metric":"usage","cutoff":1.0,"includeHi":true,"includeLo":true,"summarizer":"aplinear","attributes":["location","version"],"ratioMetric":"globalratio","minRatioMetric":10.0,"minSupport":0.2}')
    //   .subscribe(
    //     data => {this.queryResult = data;
    //              this.updateString();},
    //     err => {this.handleError('runQuery()', err);}
    //   )
  }

  updateString() {
    this.resultsString = JSON.stringify(this.queryResult.results, undefined, 3);
  }

  private handleError(fname: string, err: HttpErrorResponse) {
    if (err.error instanceof Error) {
      this.messageService.add(fname + ": An error occurred: " + err.error.message);
    } else {
      this.messageService.add(fname + ": Backend returned code " + err.status + ": " + JSON.stringify(err.error));
    }
  }
}