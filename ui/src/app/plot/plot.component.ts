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

  query;
  queryResult;

  itemsetQuery = new Object();
  itemsetData;

  tableName;

  dataLoaded = false;

  constructor(private queryService: QueryService,
              private dataService: DataService,
              private messageService: MessageService,
              private displayService: DisplayService) { }

  ngOnInit() {
    let key = this.queryID.toString();
    this.query = this.queryService.queries.get(key);
    this.queryResult = this.queryService.sqlResults.get(key);

    this.tableName = this.dataService.getTableName();
    this.dataService.dataSourceChanged.subscribe(
        () => {
          this.tableName = this.dataService.getTableName();
        }
      );

    this.requestData();

    this.queryService.sqlResponseReceived.subscribe(
        () => {this.updateData();}
      )
  }

  requestData() {
    this.generateSQL();
    let key = this.queryID.toString() + "-" + this.itemsetID.toString();
    this.queryService.runSQL(this.itemsetQuery, key);
  }

  generateSQL() {
    let metric = this.query["metric"]
    let attributeFilter = ""
    if(this.itemsetID >= 0) {
      attributeFilter = this.getAttributeFilter();
    }
    this.itemsetQuery["sql"] = `SELECT ${ metric } FROM ${ this.tableName } ${ attributeFilter }`
  }

  getAttributeFilter(): string {
    let attributes = new Array();
    let nAttribute = this.queryResult.stringCols.length;
    for(let j = 0; j < nAttribute; j++) {
      if(this.queryResult.stringCols[j][this.itemsetID] != null) {
        attributes.push(this.queryResult.schema.columnNames[j] + '="' + this.queryResult.stringCols[j][this.itemsetID] + '"');
      }
    }
    return "WHERE " + attributes.join(" AND ");

  }

  updateData() {
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

  makeHistogram() {
    let metricName = this.query.metric;
    let metricData = this.itemsetData["doubleCols"][0];

    // if(this.itemsetID < 0) {
    //   this.displayService.updateAxisBounds(metricName,
    //                       Math.min.apply(Math, metricData), Math.max.apply(Math, metricData));
    // }

    let histName = "";
    if(this.itemsetID < 0) {
      histName = "all";
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
      // xaxis: {title: this.query.metric,
      //         range: this.displayService.axisBounds.get(metricName)},
      xaxis: {title: this.query.metric},
      yaxis: {title: 'Count'}
    };

    let div = "histogram" + " " + this.queryID.toString() + " " + this.itemsetID.toString();
    Plotly.newPlot(div, data=data, layout);
  }
}
