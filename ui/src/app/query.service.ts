/*
 * Service - Query
 * ###############
 * This service connects to the server running MacroBase SQL in order to issue and receive
 * the results from SQL commands. Results are stored in data structures in this service
 * that multiple components access.
 */

import {Injectable, EventEmitter} from '@angular/core';
import {HttpClient, HttpHeaders, HttpErrorResponse} from '@angular/common/http';

import {Observable} from 'rxjs/Observable';
import { catchError, map, tap } from 'rxjs/operators';

import { MessageService } from "./message.service";

import * as $ from "jquery";

@Injectable()
export class QueryService {
  private sqlURL = 'http://0.0.0.0:4567/sql'

  //notify components that response has been received from server
  public sqlResponseReceived = new EventEmitter();
  public importResponseReceived = new EventEmitter();

  public queries = new Map();
  public sqlResults = new Map();

  constructor(private http: HttpClient, private messageService: MessageService) { }

  /*
   * Run a SQL command stored in a given query object
   */
  public runSQL(query: Object, key: string)  {
    this.messageService.add(JSON.stringify(query));
    this.messageService.add(key + ": " + query["sql"]);
    this.http.post(this.sqlURL, query["sql"])
      .subscribe(
        data => {
          this.sqlResults.set(key, data);
          this.queries.set(key, query);
          if(key == "import") {
            this.importResponseReceived.emit();
          }
          else{
            this.sqlResponseReceived.emit(key);
          }
        },
        err => { this.handleError('runSQL()', err); }
      );
  }

  /*
   * Remove data associated with a given queryID
   */
  public removeID(queryID: number) {
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