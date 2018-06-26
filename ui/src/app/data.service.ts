/*
 * Service - Data
 * ##############
 * This service allows communication between components about the source of data / SQL table
 * on which to run queries.
 */

import { Injectable, EventEmitter } from '@angular/core';
import { ColType } from './coltype'

@Injectable()
export class DataService {

  private dataSource: string;
  private tableName = "NONE";
  private port: string;

  private colTypes = new Array();
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
  public setTypes(colTypes: Array<ColType>) {
    this.colTypes = colTypes;

    this.attributeColumns = new Array();
    this.metricColumns = new Array();
    for (let i = 0; i < colTypes.length; i++) {
      if(colTypes[i].isAttribute) {
        this.attributeColumns.push(colTypes[i].name);
      }
      else if(colTypes[i].isMetric) {
        this.metricColumns.push(colTypes[i].name);
      }
    }
  }

  public getColTypes() {
    return this.colTypes;
  }

  public getAttributeColumns() {
    return this.attributeColumns
  }

  public getMetricColumns() {
    return this.metricColumns;
  }
}
