/*
 * Component - DataHeader
 * ######################
 * The data header stays at the top of the screen and provides access to the different tabs
 * as well as the data source.
 */

import { Component, OnInit } from '@angular/core';
import { DisplayService } from '../display.service'
import { DataService } from '../data.service'

@Component({
  selector: 'app-data-header',
  templateUrl: './data-header.component.html',
  styleUrls: ['./data-header.component.css']
})
export class DataHeaderComponent implements OnInit {
  private dataSource: string;

  constructor(private displayService: DisplayService,
              private dataService: DataService) {}

  ngOnInit() {
    this.displayService.displayChanged.subscribe(
        () => {this.updateDisplay(this.displayService.getDisplayType());}
      );

    this.dataService.dataSourceChanged.subscribe(
        () => {
          this.dataSource = this.dataService.getDataSource();
        }
      );

    this.updateDisplay(this.displayService.getDisplayType());
    this.dataSource = this.dataService.getDataSource();
  }

  /*
   * Change tab based on user click
   */
  private setDisplayType(type: string){
    this.displayService.setDisplayType(type);
    this.updateDisplay(type);
  }

  /*
   * Change which tab appears to be selected
   */
  private updateDisplay(type: string){
    this.clearColors();
    document.getElementById(type).style.backgroundColor = "#90caf9";
  }

  /*
   * Clear all tabs of appearing to be selected
   */
  private clearColors() {
    document.getElementById('DataHomepage').style.backgroundColor = "gray";
    document.getElementById('Explore').style.backgroundColor = "gray";
  }
}
