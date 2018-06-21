/*
 * The cell represents a table of explanations that make up the response to a given query.
 */

import { Component, OnInit, Input } from '@angular/core';
import { QueryService } from '../query.service';
import { MessageService } from "../message.service";
import { DisplayService } from "../display.service";

@Component({
  selector: 'app-cell',
  templateUrl: './cell.component.html',
  styleUrls: ['./cell.component.css']
})
export class CellComponent implements OnInit {
  @Input() id: number;

  displayItemsets = false;

  totalEvents;
  totalOutliers;
  queryResult;

  selectedResults;

  constructor(private queryService: QueryService, private messageService: MessageService, private displayService: DisplayService) {
  }

  ngOnInit() {
    this.updateQuery();
    this.updateSelectedResults();

    this.queryService.sqlResponseReceived.subscribe(
        (key) => {
          if(key == this.id.toString()) {
            this.updateQuery();
            this.clearSelected();
          }
        }
      );
  }

  /*
   * After query response is received from server, update cell information to contain explanations.
   */
  private updateQuery() {
    let key = this.id.toString()
    if(!this.queryService.queries.has(key)) return;

    let result = this.queryService.sqlResults.get(key);
    let nAttribute = result.stringCols.length;

    this.queryResult = new Array();
    for(let i = 0; i < result.numRows; i++) {
      let itemset = new Object();
      itemset["support"] = result.doubleCols[0][i].toFixed(3);
      itemset["ratio"] = result.doubleCols[1][i].toFixed(3);
      itemset["nOutlier"] = result.doubleCols[2][i];
      itemset["nTotal"] = result.doubleCols[3][i];

      itemset["attributes"] = new Array();
      for(let j = 0; j < nAttribute; j++) {
        if(result.stringCols[j][i] != null) {
          itemset["attributes"].push(result.schema.columnNames[j] + ": " + result.stringCols[j][i])
        }
      }
      this.queryResult.push(itemset);
    }

    this.displayItemsets = true;
  }

  /*
   * Clear selection of explanations
   */
  private clearSelected() {
    this.selectedResults = new Set();
    this.displayService.updateSelectedResults(this.id, new Set());
  }

  /*
   * Update selection of explanations based on display service
   */
  private updateSelectedResults() {
    if(this.displayService.selectedResultsByID.has(this.id)) {
      this.selectedResults = this.displayService.selectedResultsByID.get(this.id);
    }
    else{
      this.selectedResults = new Set();
    }
  }

  /*
   * Select a given explanation
   */
  private selectResult(i: number) {
    if(this.selectedResults.has(i)){
      this.selectedResults.delete(i);
      document.getElementById('result'+ " " + this.id + " " + i).style.backgroundColor = "white";
    }
    else{
      this.selectedResults.add(i);
      document.getElementById('result'+ " " + this.id + " " + i).style.backgroundColor = "lightgray";
    }

    this.displayService.updateSelectedResults(this.id, this.selectedResults);
  }

  /*
   * Return the highlight color of a given explanation
   */
  private getSelectedColor(i: number) {
    if(this.selectedResults.has(i)){
      return "lightgray";
    }
    else{
      return "white";
    }
  }
}