import { Component, OnInit } from '@angular/core';
import { DataService } from '../data.service'
import { QueryService } from '../query.service'

@Component({
  selector: 'app-data-home',
  templateUrl: './data-home.component.html',
  styleUrls: ['./data-home.component.css']
})
export class DataHomeComponent implements OnInit {
  dataSource = "csv://../data/wikiticker.csv"
  tableName = "wiki"
  port = "4567"
  importSQLString: string;

  constructor(private dataService: DataService, private queryService: QueryService) { }

  ngOnInit() {
    this.dataService.setDataSource(this.dataSource);
  }

  importData() {
    this.dataService.setDataSource(this.dataSource);
    this.dataService.setTableName(this.tableName);
    this.dataService.setPort(this.port);
    this.dataService.dataSourceChanged.emit();

    this.generateImportSQLString();
    this.queryService.runSQL(this.importSQLString, "import")
  }

  generateImportSQLString() {
    let colTypes = `time string, user string, page
  string, channel string, namespace string, comment string, metroCode string,
  cityName string, regionName string, regionIsoCode string, countryName string,
  countryIsoCode string, isAnonymous string, isMinor string, isNew string,
  isRobot string, isUnpatrolled string, delta double, added double, deleted
  double`;
    this.importSQLString =
      `IMPORT FROM CSV FILE '${ this.dataSource }'
        INTO ${ this.tableName }(${ colTypes });`;
  }

}
