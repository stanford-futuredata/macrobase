/*
 * Service - Data
 * ##############
 * This service allows communication between components about the source of data / SQL table
 * on which to run queries.
 */

import { Injectable, EventEmitter } from '@angular/core';

@Injectable()
export class DataService {

  private dataSource: string;
  private tableName = "NONE";
  private port: string;

  private types = new Map();
  private colNames = new Array();
  private attributeColumns: Array<string>;
  private metricColumns: Array<string>;

  public dataSourceChanged = new EventEmitter();

  constructor() { }

  public getDataSource() {
    return this.dataSource;
  }

  public setDataSource(pathname: string) {
    this.dataSource = pathname;
    this.dataSourceChanged.emit();
  }

  public getTableName() {
    return this.tableName;
  }

  public setTableName(name: string) {
    this.tableName = name;
    this.dataSourceChanged.emit();
  }

  public getPort() {
    return this.port;
  }

  public setPort(port: string) {
    this.port = port;
    this.dataSourceChanged.emit();
  }

  /*
   * Given a list of column names and a map of column names to column type (attribute / metric / none),
   * generates lists of attribute and metric columns
   */
  public setTypes(colNames: Array<string>, types: Map<string, string>) {
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

  public getTypes() {
    return this.types;
  }

  public getColNames() {
    return this.colNames;
  }

  public getAttributeColumns() {
    return this.attributeColumns
  }

  public getMetricColumns() {
    return this.metricColumns;
  }
}
