import { Component, OnInit } from '@angular/core';
import { DisplayService } from '../display.service'

@Component({
  selector: 'app-data-header',
  templateUrl: './data-header.component.html',
  styleUrls: ['./data-header.component.css']
})
export class DataHeaderComponent implements OnInit {
  dataSource = "csv://../data/wikiticker.csv";

  setDisplayType(type: string){
    this.displayService.setDisplayType(type);
    this.displayService.setIDs([]);

    this.clearColors();
    document.getElementById(type).style.backgroundColor = "lightblue";
  }

  clearColors() {
    document.getElementById('DataHomepage').style.backgroundColor = "gray";
    document.getElementById('Dashboard').style.backgroundColor = "gray";
    document.getElementById('QueryWizard').style.backgroundColor = "gray";
  }

  constructor(private displayService: DisplayService) { }

  ngOnInit() {
  }

}
