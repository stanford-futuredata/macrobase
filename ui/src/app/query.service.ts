import {Injectable} from '@angular/core';
import {HttpClient, HttpHeaders, HttpErrorResponse} from '@angular/common/http';
//import {Http, Response} from '@angular/http';
//import {Headers, RequestOptions} from '@angular/http';

import {Observable} from 'rxjs/Observable';
import { catchError, map, tap } from 'rxjs/operators';
import { of } from 'rxjs/observable/of';
// import 'rxjs/add/operator/catch';
// import 'rxjs/add/operator/map';

import {QueryResult} from "./query-result"
import {Query} from "./query"
import { MessageService } from "./message.service"

@Injectable()
export class QueryService {
  private queryURL = 'http://0.0.0.0:4567/query';

  private httpOptions = {
    headers: new HttpHeaders({ 'Accept': 'application/json', 'Content-Type': 'application/json' })
  };

  constructor(private http: HttpClient, private messageService: MessageService) { }

  queryResult: QueryResult = {
    numTotal: -1,
    outliers: -1,
    results: [{"metric":{"global_ratio":"-1","support":"-1"},
               "matcher":{"location":"None","version":"None"},
               "aggregate":{"Outliers":"-1","Count":"-1"}}]
    };
  queryString : string;

  runQuery(query: Query) {
    this.http.post<QueryResult>(this.queryURL, query, this.httpOptions)
      .subscribe(
        result => {this.queryResult = result;
                   this.updateString();
                   this.messageService.add("Success!");},
        err => {this.handleError('runQuery()', err);
                this.updateString();}
      )
  }

  updateString() {
    this.queryString = JSON.stringify(this.queryResult);
  }

  private handleError(fname: string, err: HttpErrorResponse) {
    if (err.error instanceof Error) {
      this.messageService.add(fname + ": An error occurred: " + err.error.message);
    } else {
      this.messageService.add(fname + ": Backend returned code " + err.status + ": " + JSON.stringify(err.error));
    }
  }
}
