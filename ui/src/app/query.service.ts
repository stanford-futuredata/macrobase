import {Injectable} from '@angular/core';
import {Http, Response} from '@angular/http';
import {Headers, RequestOptions} from '@angular/http';

import {Observable} from 'rxjs/Observable';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';

@Injectable()
export class QueryService {
  private queryURL = '0.0.0.0:4567';

  constructor(private http: Http) { }

  getQuery(): Observable<string> {
    return this.http.get(this.queryURL)
        .catch(this.handleError);
  }

  private handleError(error: Response | any) {
        let errorMessage: string;

        errorMessage = error.message ? error.message : error.toString();

        return Observable.throw(errorMessage);
    }
}
