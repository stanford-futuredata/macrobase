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

    // var ajaxCall = $.ajax({
    //   type: "POST",
    //   url: this.queryURL,
    //   data: JSON.stringify(query),
    //   // headers: {"Access-Control-Request-Methods": "Accept": "application/json", "Content-type": "application/json"},
    //   success: function(data, textStatus, jqXHR){
    //     alert("Success");
    //   },
    //   error: function(jqXHR, textStatus, errorThrown){
    //     alert(textStatus);
    //     alert("Error thrown: " + errorThrown);
    //     alert("Status: " + jqXHR.status);
    //     alert(jqXHR.getResponseHeader('Access-Control-Allow-Origin'));
    //   }
    // });

    this.http.post<QueryResult>(this.queryURL, JSON.stringify(query))
      .subscribe(
        data => {this.queryResult = data;
                 this.updateString();},
        err => {this.handleError('runQuery()', err);}
      );
    // this.messageService.add(JSON.stringify(query))
    // this.http.post<QueryResult>(this.queryURL, '{"pipeline":"BasicBatchPipeline","inputURI":"csv://core/demo/sample.csv","classifier":"percentile","metric":"usage","cutoff":1.0,"includeHi":true,"includeLo":true,"summarizer":"aplinear","attributes":["location","version"],"ratioMetric":"globalratio","minRatioMetric":10.0,"minSupport":0.2}', this.httpOptions)
    //   .subscribe(
    //     data => {this.queryResult = data;},
    //     err => {this.handleError('runQuery()', err);}
    //   )
  }

  // runQuery(query: Query) {
  //   this.messageService.add("Running query on: " + JSON.stringify(query));
  //   // this.queryResult = this.http.post<QueryResult>(this.queryURL, query, this.httpOptions)
  //   this.queryResult = this.http.post<Object>(this.queryURL, '{"pipeline":"BasicBatchPipeline","inputURI":"csv://core/demo/sample.csv","classifier":"percentile","metric":"usage","cutoff":1.0,"includeHi":true,"includeLo":true,"summarizer":"aplinear","attributes":["location","version"],"ratioMetric":"globalratio","minRatioMetric":10.0,"minSupport":0.2}')
  //     .pipe(
  //       catchError(this.handleError('getHeroes'))
  //     );
  //   this.updateString();
  // }

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

  // private handleError<T> (operation: string) {
  // return (error: any): Observable<T> => {
  //   this.messageService.add(`${operation} failed: ${error.message}`);
  // };
}

// curl -H "Accept: application/json" -H "Content-type: application/json" -d '{"pipeline":"BasicBatchPipeline","inputURI":"csv://core/demo/sample.csv","classifier":"percentile","metric":"usage","cutoff":1.0,"includeHi":true,"includeLo":true,"summarizer":"aplinear","attributes":["location","version"],"ratioMetric":"globalratio","minRatioMetric":10.0,"minSupport":0.2}' 0.0.0.0:4567/query; echo