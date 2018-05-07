import { Injectable, EventEmitter } from '@angular/core';

@Injectable()
export class DisplayService {

  private displayType = "Dashboard"; //DataHomepage, GenerateQuery, Dashboard, Explore
  private ids: number[];

  displayChanged = new EventEmitter();

  getDisplayType(): string {
    return this.displayType
  }

  setDisplayType(type: string) {
    this.displayType = type;
    this.displayChanged.emit()
  }

  clearIDs() {
    this.ids = [];
  }

  setIDs(ids: number[]) {
    this.ids = ids;
  }

  getIDs(): number[] {
    return this.ids;
  }

  constructor() { }

}
