import { Component, OnInit, ChangeDetectorRef } from '@angular/core';
import { DataService } from '../data.service';
import { QueryService } from '../query.service';
import { DisplayService } from '../display.service';

@Component({
  selector: 'app-data-home',
  templateUrl: './data-home.component.html',
  styleUrls: ['./data-home.component.css']
})
export class DataHomeComponent implements OnInit {
  private dataSource = "core/demo/sample.csv"
  private tableName = "data"
  private port = "4567"
  private query = new Object();

  private displayTypes = false;
  private colNames: Array<string>;
  private types: Map<string, string>;

  constructor(private dataService: DataService, private queryService: QueryService, private cd: ChangeDetectorRef) {
    this.queryService.importResponseReceived.subscribe(
        () => {
           this.setTypes()
        }
      );
  }

  ngOnInit() {
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

  private importData() {
    this.dataService.setDataSource(this.dataSource);
    this.dataService.setTableName(this.tableName);
    this.dataService.setPort(this.port);

    this.generateImportSQLString();
    this.queryService.runSQL(this.query, "import")
  }

  private generateImportSQLString() {
    this.query["sql"] =
      `IMPORT FROM CSV FILE "${ this.dataSource }" INTO ${ this.tableName }`;
  }

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

  private getColor(colName: string, type: string) {
    if (this.types.get(colName) == type) {
      return "lightgray";
    }
    else{
      return "white";
    }
  }
}
