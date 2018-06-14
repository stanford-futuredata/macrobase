import { Component, OnInit } from '@angular/core';
import { QueryService } from './query.service'
import { DisplayService } from './display.service'
import { DataService } from './data.service'

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent implements OnInit {
  displayMessages = true;
  newID = 0;
  validIDs = new Set();
  editID = 0;
  displayType = this.displayService.getDisplayType(); //DataHomepage, Edit, History, Explore
  selectedIDs = new Set();
  exploreIDs = new Set();
  plotIDsByMetric = new Map(); // map of metricName to (map of queryID to attributeID)
  isPlot = false;

  constructor(private queryService: QueryService, private displayService: DisplayService, private dataService: DataService) { }

  ngOnInit() {
    this.updateDisplayType(this.displayService.getDisplayType());
    this.displayService.displayChanged.subscribe(
        () => {this.updateDisplayType(this.displayService.getDisplayType());}
      )

    this.queryService.sqlResponseReceived.subscribe(
        (key) => {this.updateValidIDs(key);}
      )

    this.displayService.selectedResultsChanged.subscribe(
        () => {if(this.displayType == "Explore") {this.refreshPlot();} }
      )
  }

  updateDisplayType(type: string) {
    this.displayType = type;
  }

  updateValidIDs(key) {
    if(isNaN(key)) return; //not a diff query (import or rows query)
    let id = Number(key);
    this.validIDs.add(id);
    if(id == this.newID){
      this.newID++;
      this.editID = this.newID;
    }
  }

  getSelectedColor(id: number) {
    if(this.selectedIDs.has(id)){
      return "lightgray";
    }
    else{
      return "white";
    }
  }

  selectID(id: number) {
    if(this.selectedIDs.has(id)){
      this.selectedIDs.delete(id);
      document.getElementById('summary'+id).style.backgroundColor = "white";
    }
    else{
      this.selectedIDs.add(id);
      document.getElementById('summary'+id).style.backgroundColor = "lightgray";
    }
  }

  exploreSelected() {
    this.exploreIDs = this.selectedIDs;
    this.displayService.setDisplayType('Explore');
  }

  newQuery(){
    this.editID = this.newID;
    this.displayService.setDisplayType('Edit');
  }

  editSelected() {
    this.editID = Array.from(this.selectedIDs)[0];
    this.displayService.setDisplayType('Edit')
  }

  deleteSelected() {
    this.selectedIDs.forEach( (id) => {
      this.validIDs.delete(id);
      this.queryService.removeID(id);
      this.exploreIDs.delete(id);
      this.selectedIDs.delete(id)
    });
  }

  refreshPlot() {
    this.togglePlot();
    this.togglePlot();
  }

  togglePlot() {
    if(this.isPlot){
      this.isPlot = false;
      this.plotIDsByMetric = new Map();
      document.getElementById('plotButton').style.backgroundColor = "#eee";
    }
    else{
      this.createPlotIDs();
      this.isPlot = true;
      document.getElementById('plotButton').style.backgroundColor = "lightblue";
    }
  }

  createPlotIDs(){
    for(let queryID of Array.from(this.exploreIDs)) {
      let key = queryID.toString();
      if(this.displayService.selectedResultsByID.has(key) &&
        this.displayService.selectedResultsByID.get(key).size != 0){
        let metric = this.queryService.queries.get(key)["metric"];

        if(!this.plotIDsByMetric.has(metric)){
          this.plotIDsByMetric.set(metric, new Map());
        }

        if(!this.plotIDsByMetric.get(metric).has(key)){
          this.plotIDsByMetric.get(metric).set(key, [-1]);
        }

        alert("ok")
        for(let itemsetID of Array.from(this.displayService.selectedResultsByID.get(key).keys())){
          this.plotIDsByMetric.get(metric).get(queryID).push(itemsetID);
          alert(itemsetID);
        }
        alert("done")
      }
    }
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
