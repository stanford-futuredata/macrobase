import {Injectable, EventEmitter} from '@angular/core';
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
  private rowsURL = 'http://0.0.0.0:4567/rows';

  //notify components that response has been received from server
  queryResponseReceived = new EventEmitter();
  rowsResponseReceived = new EventEmitter();

  queries = new Map();
  queryResults = new Map();
  rows = new Map();

  constructor(private http: HttpClient, private messageService: MessageService) { }

  runQuery(query: Query, id: number) {
    this.messageService.add("Query " + id + ": Running query on: " + JSON.stringify(query));
    this.http.post<QueryResult>(this.queryURL, JSON.stringify(query))
      .subscribe(
        data => {
                 this.queryResults.set(id, data);
                 this.queries.set(id, query);
                 this.queryResponseReceived.emit();
                },
        err => {this.handleError('runQuery()', err);}
      );
  }

  updateMatchingAttributes(id: number) {
    this.queryResults.get(id).results.forEach( (result) => {
      result.matcherString = JSON.stringify(result.matcher);
    })
  }

  getRows(query: Query, queryID: number, itemsetID: number) {
    this.messageService.add("Getting rows from query " + queryID + ", itemset " + itemsetID + ": Sending request: " + JSON.stringify(query));
    this.http.post(this.rowsURL, JSON.stringify(query))
      .subscribe(
        data => {
                 this.rows.set(queryID, data);
                 this.rowsResponseReceived.emit();
                },
        err => {this.handleError('runQuery()', err);}
      );
  }

  removeID(queryID: number) {
    this.queries.delete(queryID);
    this.queryResults.delete(queryID);
    this.rows.delete(queryID);
  }

  private handleError(fname: string, err: HttpErrorResponse) {
    if (err.error instanceof Error) {
      this.messageService.add(fname + ": An error occurred: " + err.error.message);
    } else {
      this.messageService.add(fname + ": Backend returned code " + err.status + ": " + JSON.stringify(err.error));
    }
  }
}