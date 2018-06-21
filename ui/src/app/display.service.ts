/*
 * Service - Display
 * #################
 * This service allows communication between components about which tab to display and
 * which explanations are currently selected by the user.
 */

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
  }

  public updateSelectedResults(queryID: number, selections) {
    this.selectedResultsByID.set(queryID, selections);
    this.selectedResultsChanged.emit();
  }
}
