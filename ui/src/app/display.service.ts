/*
 * Service - Display
 * #################
 * This service allows communication between components about which tab to display and
 * which explanations are currently selected by the user.
 */

import { Injectable, EventEmitter } from '@angular/core';
import { MatDialog, MatDialogConfig, MatDialogRef } from '@angular/material';
import { QueryEditorComponent } from './query-editor/query-editor.component'

@Injectable()
export class DisplayService {

  private displayType = "DataHomepage"; //DataHomepage, Edit, Explore, History

  public displayChanged = new EventEmitter();

  public selectedResultsChanged = new EventEmitter();
  public selectedResultsByID = new Map();

  constructor(private dialog: MatDialog) { }

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

  public openEditor(id: number, oldID: number) {
    const dialogConfig = new MatDialogConfig();

    dialogConfig.disableClose = true;
    dialogConfig.autoFocus = false;
    dialogConfig.width = "70%";
    dialogConfig.height = "700px";
    dialogConfig.data = {id: id, oldID: oldID};

    let dialogRef = this.dialog.open(QueryEditorComponent, dialogConfig);

    dialogRef.afterClosed().subscribe(
        (isSuccess) => {
          if(isSuccess){
            this.setDisplayType("Explore");
          }
        }
      );
  }
}
