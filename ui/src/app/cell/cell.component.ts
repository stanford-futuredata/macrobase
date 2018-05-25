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

  query;
  totalEvents;
  totalOutliers;
  queryResult;

  selectedResults;

  constructor(private queryService: QueryService, private messageService: MessageService, private displayService: DisplayService) {
  }

  ngOnInit() {
    if(this.queryService.queries.has(this.id)){
      this.updateQuery();
    }
    this.updateSelectedResults();

    this.queryService.queryResponseReceived.subscribe(
        () => {this.updateQuery();}
      )
  }

  updateQuery() {
    this.query = this.queryService.queries.get(this.id)
    this.queryResult = this.queryService.queryResults.get(this.id);
    this.totalEvents = this.queryResult.numTotal;
    this.totalOutliers = this.queryResult.outliers;
    let numItemsets = this.queryResult.results.length;

    //JSON to string for itemset attributes
    for(let result of this.queryResult.results) {
      result.matcherString = []
      for(let attribute in result.matcher) {
        let itemset = attribute + ": " + result.matcher[attribute];
        result.matcherString.push(itemset);
      }
    }

    this.displayItemsets = true;
  }

  updateSelectedResults() {
    if(this.displayService.selectedResultsByID.has(this.id)) {
      this.selectedResults = this.displayService.selectedResultsByID.get(this.id);
    }
    else{
      this.selectedResults = new Set();
    }
  }

  selectResult(i: number) {
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

  getSelectedColor(i: number) {
    if(this.selectedResults.has(i)){
      return "lightgray";
    }
    else{
      return "white";
    }
  }
}