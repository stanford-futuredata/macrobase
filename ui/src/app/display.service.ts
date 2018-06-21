import { Injectable, EventEmitter } from '@angular/core';

@Injectable()
export class DisplayService {

  private displayType = "DataHomepage"; //DataHomepage, Edit, Explore, History

  displayChanged = new EventEmitter();

  getDisplayType(): string {
    return this.displayType
  }

  setDisplayType(type: string) {
    this.displayType = type;
    this.displayChanged.emit()
  }

  selectedResultsChanged = new EventEmitter
  selectedResultsByID = new Map();

  updateSelectedResults(queryID: number, selections) {
    this.selectedResultsByID.set(queryID, selections);
    this.selectedResultsChanged.emit()
  }

  axisBounds = new Map();

  updateAxisBounds(metric: string, min: number, max: number){
    this.axisBounds.set(metric, [min, max])
  }

  constructor() { }

}
