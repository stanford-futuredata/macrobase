import { Injectable, EventEmitter } from '@angular/core';

@Injectable()
export class DataService {

  dataSource: string;
  tableName = "NONE";
  port: string;

  types = new Map();
  colNames = new Array();
  attributeColumns: Array<string>;
  metricColumns: Array<string>;

  dataSourceChanged = new EventEmitter();

  constructor() { }

  getDataSource() {
    return this.dataSource;
  }

  setDataSource(pathname: string) {
    this.dataSource = pathname;
    this.dataSourceChanged.emit();
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

  setTypes(colNames: Array<string>, types: Map<string, string>) {
    this.types = types;
    this.colNames = colNames;

    this.attributeColumns = new Array();
    this.metricColumns = new Array();
    for (let colName of colNames) {
      if(types.get(colName) == "attribute") {
        this.attributeColumns.push(colName);
      }
      else if (types.get(colName) == "metric") {
        this.metricColumns.push(colName);
      }
    }
  }

  getTypes() {
    return this.types;
  }

  getColNames() {
    return this.colNames;
  }

  getAttributeColumns() {
    return this.attributeColumns
  }

  getMetricColumns() {
    return this.metricColumns;
  }
}
