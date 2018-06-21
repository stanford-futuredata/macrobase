import { Injectable, EventEmitter } from '@angular/core';

@Injectable()
export class DisplayService {

  private displayType = "DataHomepage"; //DataHomepage, Edit, Explore, History

  public displayChanged = new EventEmitter();

  public selectedResultsChanged = new EventEmitter();
  public selectedResultsByID = new Map();

  constructor() { }

  public getDisplayType(): string {
    return this.displayType
  }

  public setDisplayType(type: string) {
    this.displayType = type;
    this.displayChanged.emit()
    alert("display emitted");
  }

  public updateSelectedResults(queryID: number, selections) {
    alert("emitted");
    this.selectedResultsByID.set(queryID, selections);
    this.selectedResultsChanged.emit();
  }
}
