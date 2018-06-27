/*
 * Component - Plot
 * ################
 * This component creates a histogram for a given explanation from a given query.
 * Itemset/explanation ID = -1 creates a histogram of the metric of the given query over all rows.
 */

import { Component, OnInit, Input } from '@angular/core';
import { QueryService } from '../query.service';
import { MessageService } from "../message.service";
import { DisplayService } from "../display.service";
import { DataService } from "../data.service";

@Component({
  selector: 'app-plot',
  templateUrl: './plot.component.html',
  styleUrls: ['./plot.component.css']
})
export class PlotComponent implements OnInit {
  @Input() queryID: number;
  @Input() itemsetID: number;

  private query;
  private queryResult;

  private itemsetQuery = new Object();
  private itemsetData;

  private tableName;

  private dataLoaded = false;

  constructor(private queryService: QueryService,
              private dataService: DataService,
              private messageService: MessageService,
              private displayService: DisplayService) {}

  ngOnInit() {
    this.queryService.sqlResponseReceived.subscribe(
        () => {this.updateData();}
      );

    let key = this.queryID.toString();
    this.query = this.queryService.queries.get(key);
    this.queryResult = this.queryService.sqlResults.get(key);

    this.tableName = this.dataService.getTableName();

    this.requestData();
  }

  /*
   * Send a request for metric column data to server via SQL command
   */
  private requestData() {
    this.generateSQL();
    let key = this.queryID.toString() + "-" + this.itemsetID.toString();
    this.queryService.runSQL(this.itemsetQuery, key);
  }

  /*
   * Generate the SQL command for retreiving metric column data
   */
  private generateSQL() {
    let metric = this.query["metric"]
    let attributeFilter = ""
    if(this.itemsetID >= 0) {
      attributeFilter = this.getAttributeFilter();
      this.itemsetQuery["sql"] = `SELECT ${ metric } FROM ${ this.tableName } WHERE ${ attributeFilter }`
    }
    else{
      this.itemsetQuery["sql"] = `SELECT ${ metric } FROM ${ this.tableName }`
    }
  }

  /*
   * Generate string of attributes joined by " AND "
   */
  private getAttributeFilter(): string {
    let attributes = new Array();
    let nAttribute = this.queryResult.stringCols.length;
    for(let j = 0; j < nAttribute; j++) {
      if(this.queryResult.stringCols[j][this.itemsetID] != null) {
        attributes.push(this.queryResult.schema.columnNames[j] + '="' + this.queryResult.stringCols[j][this.itemsetID] + '"');
      }
    }
    return attributes.join(" AND ");

  }

  /*
   * Retreive data from response from server after running SQL command
   */
  private updateData() {
    if(this.dataLoaded) {
      return;
    }

    let key = this.queryID.toString() + "-" + this.itemsetID.toString()
    if(this.queryService.sqlResults.has(key)) {
      this.itemsetData = this.queryService.sqlResults.get(key);
      this.dataLoaded = true;
      this.makeHistogram();
    }
  }

  /*
   * Generate the histogram
   */
  private makeHistogram() {
    let metricName = this.query.metric;
    let metricData = this.itemsetData["doubleCols"][0];

    let histName = "";
    if(this.itemsetID < 0) {
      histName = "All rows";
    }
    else{
      histName = this.getAttributeFilter();
    }

    let data = [
      {
        x: metricData,
        type:'histogram',
        histnorm:'count'
      }
    ];

    var layout = {
      title: histName,
      // xaxis: {title: this.query.metric},
      yaxis: {title: 'Count'},
      margin: {
        l: 100,
        r: 50,
        b: 40,
        t: 40,
        pad: 4
      }
    };

    let div = "histogram" + " " + this.queryID.toString() + " " + this.itemsetID.toString();
    Plotly.newPlot(div, data=data, layout, {displaylogo: false});
  }
}
