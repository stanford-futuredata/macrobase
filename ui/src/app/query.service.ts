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
  private sqlURL = 'http://0.0.0.0:4567/sql'

  //notify components that response has been received from server
  dataResponseReceived = new EventEmitter();
  sqlResponseReceived = new EventEmitter();

  queries = new Map();
  sqlResults = new Map();

  constructor(private http: HttpClient, private messageService: MessageService) { }

  runSQL(query, key: string)  {
    this.messageService.add(JSON.stringify(query));
    this.messageService.add(key + ": " + query["sql"]);
    this.http.post(this.sqlURL, query["sql"])
      .subscribe(
        data => {
          this.sqlResults.set(key, data);
          this.queries.set(key, query);
          this.sqlResponseReceived.emit(key);
        },
        err => { this.handleError('runSQL()', err); }
      );
  }

  removeID(queryID: number) {
    let key = queryID.toString()
    this.queries.delete(key);
    this.sqlResults.delete(key);
  }

  private handleError(fname: string, err: HttpErrorResponse) {
    if (err.error instanceof Error) {
      this.messageService.add(fname + ": An error occurred: " + err.error.message);
    } else {
      this.messageService.add(fname + ": Backend returned code " + err.status + ": " + JSON.stringify(err.error));
    }
  }
}