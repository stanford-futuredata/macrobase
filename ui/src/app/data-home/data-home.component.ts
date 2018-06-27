/*
 * Component - DataHome
 * ####################
 * The data home represents the Data tab and allows the user to import data from a given
 * source and visualize and change column types (attribute / metric).
 */

import { Component, OnInit } from '@angular/core';
import { DataService } from '../data.service';
import { QueryService } from '../query.service';
import { DisplayService } from '../display.service';
import { ColType } from '../coltype';

@Component({
  selector: 'app-data-home',
  templateUrl: './data-home.component.html',
  styleUrls: ['./data-home.component.css']
})
export class DataHomeComponent implements OnInit {
  private dataSource = "core/demo/sample.csv";
  private tableName = "data";
  private port = "4567";
  private query = new Object();

  private displayTypes = false;
  private typesChanged = false;
  private colTypes: Array<ColType>;

  constructor(private dataService: DataService,
              private queryService: QueryService) { }

  ngOnInit() {
    this.queryService.importResponseReceived.subscribe(
        () => {
           this.setTypes();
        }
      );

    if(this.dataService.getTableName() != "NONE") {
      this.dataSource = this.dataService.getDataSource();
      this.tableName = this.dataService.getTableName();
      this.port = this.dataService.getPort();
    }

    this.colTypes = this.dataService.getColTypes();
    if(this.colTypes.length > 0) {
      this.displayTypes = true;
    }
  }

  /*
   * Send a request to server to run IMPORT... SQL command given user input of data source
   */
  private importData() {
    this.dataService.setDataSource(this.dataSource);
    this.dataService.setTableName(this.tableName);
    this.dataService.setPort(this.port);

    this.generateImportSQLString();
    this.queryService.runSQL(this.query, "import")
  }

  /*
   * Generate IMPORT... SQL command
   */
  private generateImportSQLString() {
    this.query["sql"] =
      `IMPORT FROM CSV FILE "${ this.dataSource }" INTO ${ this.tableName }`;
  }

  /*
   * Load in the type data on column names and which should be attributes / metrics that
   * is found in response from server
   */
  private setTypes() {
    this.colTypes = new Array();

    let data = this.queryService.sqlResults.get('import');
    let numRows = data["numRows"];
    for (let i = 0; i < numRows; i++) {
      let colType = new ColType();
      colType.name = data["stringCols"][0][i];

      if (data["stringCols"][1][i] == 'attribute') {
        colType.isAttribute = true;
      }
      else {
        colType.isAttribute = false;
      }

      if (data["stringCols"][1][i] == 'metric') {
        colType.isMetric = true;
      }
      else {
        colType.isMetric = false;
      }

      this.colTypes.push(colType);
    }

    this.displayTypes = true;
    this.dataService.setTypes(this.colTypes);

    this.typesChanged = false;
  }

  /*
   * Change column type based on user input
   */
  private updateType(i: number, type: string) {
    if(type == 'attribute') {
      if(this.colTypes[i].isAttribute) {
        this.colTypes[i].isAttribute = false;
      }
      else{
        this.colTypes[i].isMetric = false;
        this.colTypes[i].isAttribute = true;
      }
    }

    if(type == 'metric') {
      if(this.colTypes[i].isMetric) {
        this.colTypes[i].isMetric = false;
      }
      else{
        this.colTypes[i].isAttribute = false;
        this.colTypes[i].isMetric = true;
      }
    }

    this.dataService.setTypes(this.colTypes);
    this.typesChanged = true;
  }
}
