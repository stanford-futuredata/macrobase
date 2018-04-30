import { Component, OnInit, Input } from '@angular/core';
import { QueryService } from '../query.service';
import { MessageService } from "../message.service";

@Component({
  selector: 'app-cell',
  templateUrl: './cell.component.html',
  styleUrls: ['./cell.component.css']
})
export class CellComponent implements OnInit {
  @Input() id: number;

  constructor(private queryService: QueryService, private messageService: MessageService) {
  }

  ngOnInit() {
    this.queryService.queryResponseReceived.subscribe(
        () => {this.updateQuery();}
      )
    this.queryService.rowsResponseReceived.subscribe(
        () => {this.updateRows();}
      )
  }

  getRows(itemsetID: number) {
    let itemset = this.queryResult.results[itemsetID];
    let itemsetAttributes = itemset.matcher;
    this.messageService.add(JSON.stringify(itemsetAttributes));

    let query = this.queryService.queries.get(this.id);
    query.numRows = 20;
    query.columnFilters = "";

    this.queryService.getRows(query, this.id, itemsetID);
  }

  queryResult;
  updateQuery() {
    this.queryResult = this.queryService.queryResults.get(this.id);
    //JSON to string for itemset attributes
    this.queryResult.results.forEach( (result) => {
      result.matcherString = JSON.stringify(result.matcher);
    })
  }

  rowStr;
  updateRows() {
    let rows = this.queryService.rows.get(this.id);
    this.rowStr = JSON.stringify(rows);
  }

}
