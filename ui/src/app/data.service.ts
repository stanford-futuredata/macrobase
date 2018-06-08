import { Injectable, EventEmitter } from '@angular/core';

@Injectable()
export class DataService {

  dataSource: string;
  tableName: string;
  port: string;

  dataSourceChanged = new EventEmitter();

  constructor() { }

  getDataSource() {
    return this.dataSource;
  }

  setDataSource(pathname: string) {
    this.dataSource = pathname;
  }

  getTableName() {
    return this.tableName;
  }

  setTableName(name: string) {
    this.tableName = name;
  }

  getPort() {
    return this.port;
  }

  setPort(port: string) {
    this.port = port;
  }
}
