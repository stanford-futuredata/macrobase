import { Component, OnInit } from '@angular/core';
import { DataService } from '../data.service'
import { QueryService } from '../query.service'

@Component({
  selector: 'app-data-home',
  templateUrl: './data-home.component.html',
  styleUrls: ['./data-home.component.css']
})
export class DataHomeComponent implements OnInit {
  dataSource = "../data/wikiticker.csv"
  tableName = "wiki"
  port = "4567"
  query = new Object();

  displayTypes = false;
  colNames: Array<string>;
  types: Map<string, string>;

  constructor(private dataService: DataService, private queryService: QueryService) { }

  ngOnInit() {
    this.types = this.dataService.getTypes();
    this.colNames = this.dataService.getColNames();
    if(this.colNames.length > 0) {
      this.displayTypes = true;
    }

    this.queryService.sqlResponseReceived.subscribe(
      () => {
          this.setTypes();
        }
      );
  }

  importData() {
    this.dataService.setDataSource(this.dataSource);
    this.dataService.setTableName(this.tableName);
    this.dataService.setPort(this.port);

    this.generateImportSQLString();
    this.queryService.runSQL(this.query, "import")
  }

  generateImportSQLString() {
    this.query["sql"] =
      `IMPORT FROM CSV FILE "${ this.dataSource }" INTO ${ this.tableName }`;
  }

  setTypes() {
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

  setType(colName: string, type: string) {
    if (this.types.get(colName) == type) {
      this.types.set(colName, "none");
    }
    else {
      this.types.set(colName, type);
    }
    this.updateColor(colName);
    this.dataService.setTypes(this.colNames, this.types);
  }

  updateColor(colName: string) {
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

  getColor(colName: string, type: string) {
    if (this.types.get(colName) == type) {
      return "lightgray";
    }
    else{
      return "white";
    }
  }
}
