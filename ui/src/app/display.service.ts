import { Injectable, EventEmitter } from '@angular/core';

@Injectable()
export class DisplayService {

  private displayType = "Dashboard"; //DataHomepage, QueryWizard, Dashboard, Explore

  displayChanged = new EventEmitter();

  getDisplayType(): string {
    return this.displayType
  }

  setDisplayType(type: string) {
    this.displayType = type;
    this.displayChanged.emit()
  }

  constructor() { }

}
