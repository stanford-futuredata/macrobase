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
  private dataURL = 'http://0.0.0.0:4567/rows';

  //notify components that response has been received from server
  queryResponseReceived = new EventEmitter();
  dataResponseReceived = new EventEmitter();

  queries = new Map();
  queryResults = new Map();
  itemsetData = new Map(); //map of [queryID, itemsetID] to rows of data, itemsetID of -1 = all
  // queryData = new Map();

  constructor(private http: HttpClient, private messageService: MessageService) { }

  runQuery(query: Query, id: number) {
    this.messageService.add("Query " + id + ": Running query on: " + JSON.stringify(query));
    this.http.post<QueryResult>(this.queryURL, JSON.stringify(query))
      .subscribe(
        data => {
                 this.queryResults.set(id, data);
                 this.queries.set(id, query);
                 this.queryResponseReceived.emit(id);
                },
        err => {this.handleError('runQuery()', err);}
      );
  }

  /*
   Sends POST request to server to get sample data over rows of the original query that match up with the given itemset.
  */
  getItemsetData(query: Query, queryID: number, itemsetID: number) {
    this.messageService.add("Getting data from query " + queryID + ", itemset " + itemsetID + ": Sending request: " + JSON.stringify(query));
    this.http.post(this.dataURL, JSON.stringify(query))
      .subscribe(
        data => {
                 let key = queryID.toString() + "," + itemsetID.toString()
                 this.itemsetData.set(key, data);
                 this.dataResponseReceived.emit();
                },
        err => {this.handleError('runQuery()', err);}
      );
  }

  // /*
  //  Sends POST request to server to get sample data over all rows of the original query.
  //  */
  // getQueryData(query: Query, queryID: number) {
  //   this.messageService.add("Getting data from query " + queryID + ": Sending request: " + JSON.stringify(query));
  //   this.http.post(this.dataURL, JSON.stringify(query))
  //     .subscribe(
  //       data => {
  //                this.queryData.set(queryID, data);
  //                this.dataResponseReceived.emit();
  //               },
  //       err => {this.handleError('runQuery()', err);}
  //     );
  // }

  removeID(queryID: number) {
    this.queries.delete(queryID);
    this.queryResults.delete(queryID);
    this.itemsetData.delete(queryID);
    // this.queryData.delete(queryID);
  }

  private handleError(fname: string, err: HttpErrorResponse) {
    if (err.error instanceof Error) {
      this.messageService.add(fname + ": An error occurred: " + err.error.message);
    } else {
      this.messageService.add(fname + ": Backend returned code " + err.status + ": " + JSON.stringify(err.error));
    }
  }
}