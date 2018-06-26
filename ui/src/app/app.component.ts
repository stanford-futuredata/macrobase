/*
 * Component - App
 * ###############
 * The main component of MacroBase UI.
 * Organizes query IDs, switching tabs, selecting queries and explanations.
 */

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
  private displayMessages = false;
  private newID = 0;
  private validIDs = new Array();
  private editID = 0;
  private displayType: string; //DataHomepage,Explore
  private selectedIDs = new Array();
  private exploreIDs = new Array();
  private plotIDsByMetric = new Map(); // map of metricName to (map of queryID to attributeID)
  private isPlot = false;
  private isLoaded = false;

  constructor(private queryService: QueryService,
              private displayService: DisplayService,
              private dataService: DataService) {}

  ngOnInit() {
    this.queryService.sqlResponseReceived.subscribe(
        (key) => {
          this.updateValidIDs(key);
          this.clearSelected(key);
        }
      );

    this.displayService.displayChanged.subscribe(
        () => {
          this.updateDisplayType(this.displayService.getDisplayType());
        }
      );

    this.displayService.selectedResultsChanged.subscribe(
        () => {
          if(this.displayType == "Explore") {
            this.refreshPlot();
          }
        }
      );

    this.dataService.dataSourceChanged.subscribe(
        () => {
          this.deleteAll();
          this.isLoaded = true;
        }
      );

    this.updateDisplayType(this.displayService.getDisplayType());
  }

  /*
   * Change tab
   */
  private updateDisplayType(type: string) {
    this.displayType = type;
    if(this.displayType == "Explore") {
      this.isPlot = false;
    }
  }

  /*
   * If a query was just run, update valid IDs to contain the query ID and explore the query.
   */
  private updateValidIDs(key) {
    if(isNaN(key)) return; //not a DIFF query (import or histogram query)
    let id = Number(key);
    if(!this.validIDs.includes(id)) {
      this.validIDs.push(id);
    }
    if(id == this.newID){
      this.newID++;
    }

    this.exploreIDs = new Array([id]);
  }

  /*
   * Get color of ID in history tab
   */
  private getSelectedColor(id: number) {
    if(this.selectedIDs.includes(id)){
      return "lightgray";
    }
    else{
      return "white";
    }
  }

  /*
   * Select ID in history tab
   */
  private selectID(id: number) {
    if(this.selectedIDs.includes(id)){
      this.selectedIDs.splice(this.selectedIDs.indexOf(id), 1);
      document.getElementById('summary'+id).style.backgroundColor = "white";
    }
    else{
      this.selectedIDs.push(id);
      document.getElementById('summary'+id).style.backgroundColor = "lightgray";
    }
  }

  /*
   * Explore selected IDs
   */
  private exploreSelected() {
    this.exploreIDs = this.selectedIDs;
    this.displayService.setDisplayType('Explore');
  }

  /*
   * Start a new query
   */
  private newQuery(){
    this.editID = this.newID;
    this.displayService.openEditor(this.editID);
  }

  /*
   * Edit the first selected ID
   */
  private editSelected(id: number) {
    if(id < 0) {
      this.editID = Array.from(this.selectedIDs)[0];
    }
    else{
      this.editID = id;
    }
    this.displayService.openEditor(this.editID);
  }

  /*
   * Remove ID from selected IDs and from set of explanations to plot
   */
  private clearSelected(key) {
    if(isNaN(key)) {
      return;
    }
    let id = Number(key);
    this.selectedIDs.splice(this.selectedIDs.indexOf(id), 1);
    this.displayService.updateSelectedResults(id, new Set());
    this.plotIDsByMetric = new Map();
  }

  /*
   * Delete selected queries
   */
  private deleteSelected() {
    this.selectedIDs.forEach( (id) => {
      this.delete(id);
    });
  }

  /*
   * Delete all queries
   */
  private deleteAll() {
    this.validIDs.forEach( (id) => {
      this.delete(id);
    });
    this.newID = 0;
    this.editID = 0;
  }

  /*
   * Delete query with given ID
   */
  private delete(id: number) {
    this.validIDs.splice(this.validIDs.indexOf(id), 1);
    this.queryService.removeID(id);
    this.exploreIDs.splice(this.exploreIDs.indexOf(id), 1);
    this.selectedIDs.splice(this.selectedIDs.indexOf(id), 1);
  }

  /*
   * Refresh histogram plotting
   */
  private refreshPlot() {
    this.togglePlot();
    this.togglePlot();
  }

  /*
   * Toggle histogram plotting
   */
  private togglePlot() {
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

  /*
   * Build IDs of explanations to plot based on selected explanations in selected queries
   */
  private createPlotIDs(){
    for(let queryID of Array.from(this.exploreIDs)) {
      let key = queryID.toString();
      if(this.displayService.selectedResultsByID.has(key) &&
        this.displayService.selectedResultsByID.get(key).size != 0){
        let metric = this.queryService.queries.get(key)["metric"];

        if(!this.plotIDsByMetric.has(metric)){
          this.plotIDsByMetric.set(metric, new Map());
          this.plotIDsByMetric.get(metric).set(queryID, [-1]); //plot metric without any filtering
        }
        else if(!this.plotIDsByMetric.get(metric).has(queryID)){
          this.plotIDsByMetric.get(metric).set(queryID, new Array());
        }

        for(let itemsetID of Array.from(this.displayService.selectedResultsByID.get(key))){
          this.plotIDsByMetric.get(metric).get(queryID).push(itemsetID);
        }
      }
    }
  }
}
