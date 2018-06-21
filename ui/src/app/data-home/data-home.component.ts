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
  private colNames: Array<string>;
  private types: Map<string, string>;

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

    this.types = this.dataService.getTypes();
    this.colNames = this.dataService.getColNames();
    if(this.colNames.length > 0) {
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
    this.colNames = new Array();
    this.types = new Map();

    let data = this.queryService.sqlResults.get('import');
    let numRows = data["numRows"];
    for (let i = 0; i < numRows; i++) {
      this.colNames.push(data["stringCols"][0][i])
      this.types.set(data["stringCols"][0][i], data["stringCols"][1][i]);
    }
    this.displayTypes = true;
    this.dataService.setTypes(this.colNames, this.types);
  }

  /*
   * Change column type based on user input
   */
  private setType(colName: string, type: string) {
    if (this.types.get(colName) == type) {
      this.types.set(colName, "none");
    }
    else {
      this.types.set(colName, type);
    }
    this.updateColor(colName);
    this.dataService.setTypes(this.colNames, this.types);
  }

  /*
   * Update visualization of column types
   */
  private updateColor(colName: string) {
    if (this.types.get(colName) == "attribute") {
      document.getElementById(colName + " attribute").style.backgroundColor = "lightgray";
      document.getElementById(colName + " metric").style.backgroundColor = "white";
    }
    else if (this.types.get(colName) == "metric") {
      document.getElementById(colName + " attribute").style.backgroundColor = "white";
      document.getElementById(colName + " metric").style.backgroundColor = "lightgray";
    }
    else{
      document.getElementById(colName + " attribute").style.backgroundColor = "white";
      document.getElementById(colName + " metric").style.backgroundColor = "white";
    }
  }

  /*
   * Return appropriate color for visualization of column types
   */
  private getColor(colName: string, type: string) {
    if (this.types.get(colName) == type) {
      return "lightgray";
    }
    else{
      return "white";
    }
  }
}
