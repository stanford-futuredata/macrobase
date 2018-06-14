import { Component, OnInit } from '@angular/core';
import { DisplayService } from '../display.service'
import { DataService } from '../data.service'

@Component({
  selector: 'app-data-header',
  templateUrl: './data-header.component.html',
  styleUrls: ['./data-header.component.css']
})
export class DataHeaderComponent implements OnInit {
  constructor(private displayService: DisplayService, private dataService: DataService) { }

  dataSource: string;
  tableName: string;

  ngOnInit() {
    this.updateDisplay(this.displayService.getDisplayType());
    this.displayService.displayChanged.subscribe(
        () => {this.updateDisplay(this.displayService.getDisplayType());}
      )

    this.dataSource = this.dataService.getDataSource();
    this.tableName = this.dataService.getTableName();
    this.dataService.dataSourceChanged.subscribe(
        () => {
          this.dataSource = this.dataService.getDataSource();
          this.tableName = this.dataService.getTableName();
        }
      );
  }

  setDisplayType(type: string){
    this.displayService.setDisplayType(type);
    this.updateDisplay(type);
  }

  updateDisplay(type: string){
    this.clearColors();
    document.getElementById(type).style.backgroundColor = "lightblue";
  }

  clearColors() {
    document.getElementById('DataHomepage').style.backgroundColor = "gray";
    document.getElementById('History').style.backgroundColor = "gray";
    document.getElementById('Edit').style.backgroundColor = "gray";
    document.getElementById('Explore').style.backgroundColor = "gray";
  }
}
