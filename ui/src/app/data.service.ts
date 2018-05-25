import { Injectable, EventEmitter } from '@angular/core';

@Injectable()
export class DataService {

  dataSource: string;

  dataSourceChanged = new EventEmitter();

  constructor() { }

  getDataSource() {
    return this.dataSource;
  }

  setDataSource(pathname: string) {
    this.dataSource = pathname;
    this.dataSourceChanged.emit();
  }
}
