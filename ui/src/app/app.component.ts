import { Component, OnInit } from '@angular/core';
import { QueryService } from './query.service'
import { DisplayService } from './display.service'

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  displayMessages = false;
  newID = 0;
  validIDs = new Set();
  displayIDs: number[];
  displayType = this.displayService.getDisplayType(); //DataHomepage, GenerateQuery, Dashboard, Explore

  constructor(private queryService: QueryService, private displayService: DisplayService) { }

  ngOnInit() {
    this.displayService.displayChanged.subscribe(
        () => {this.updateDisplayType(this.displayService.getDisplayType(), this.displayService.getIDs());}
      )
  }

  updateDisplayType(type: string, ids: number[]) {
    if(type == 'GenerateQuery'){
      if(ids.length == 0){
        this.newQuery();
        this.displayIDs = [this.newID];
      }
      else{ //editing an existing query
        this.displayIDs = ids;
      }
    }

    this.displayType = type;
  }

  newQuery() {
    this.newID++;
    this.validIDs.add(this.newID);
  }

  deleteQuery(id: number) {
    this.validIDs.delete(id);
    this.queryService.removeID(id);
  }

  minimizeCell(id: number){
    document.getElementById("cell"+id).style.display = "none";
    document.getElementById("expandCell"+id).style.display = "block";
  }

  expandCell(id: number){
    document.getElementById("cell"+id).style.display = "block";
    document.getElementById("expandCell"+id).style.display = "none";
  }

}
