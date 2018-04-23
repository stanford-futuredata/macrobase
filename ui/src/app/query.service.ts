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

  queryResults = new Map();

  runQuery(query: Query, id: number) {
    this.messageService.add("Query " + id + ": Running query on: " + JSON.stringify(query));
    this.http.post<QueryResult>(this.queryURL, JSON.stringify(query))
      .subscribe(
        data => {this.queryResults.set(id, data);
                 this.updateMatchingAttributes(id);},
        err => {this.handleError('runQuery()', err);}
      );
  }

  updateMatchingAttributes(id: number) {
    this.queryResults.get(id).results.forEach( (result) => {
      result.matcherString = JSON.stringify(result.matcher);
    })
  }

  private handleError(fname: string, err: HttpErrorResponse) {
    if (err.error instanceof Error) {
      this.messageService.add(fname + ": An error occurred: " + err.error.message);
    } else {
      this.messageService.add(fname + ": Backend returned code " + err.status + ": " + JSON.stringify(err.error));
    }
  }
}