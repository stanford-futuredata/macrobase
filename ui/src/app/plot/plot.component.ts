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

  itemsetQuery;
  itemsetData;

  tableName;

  dataLoaded = false;

  constructor(private queryService: QueryService,
              private dataService: DataService,
              private messageService: MessageService,
              private displayService: DisplayService) { }

  ngOnInit() {
    alert(this.queryID)
    alert(this.itemsetID)
    let key = this.queryID.toString();
    this.query = this.queryService.queries.get(key);
    this.queryResult = this.queryService.sqlResults.get(key);

    alert("requesting")
    this.requestData();

    this.queryService.sqlResponseReceived.subscribe(
        () => {this.updateData();}
      )

    this.tableName = this.dataService.getTableName();
    this.dataService.dataSourceChanged.subscribe(
        () => {
          this.tableName = this.dataService.getTableName();
        }
      );
  }

  requestData() {
    this.generateSQL();
    let key = this.queryID.toString() + ":" + this.itemsetID.toString();
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
        attributes.push(this.queryResult.columnNames[j] + "=" + this.queryResult.stringCols[j][this.itemsetID]);
      }
    }
    return "WHERE " + attributes.join(" AND ");

  }

  updateData() {
    if(this.dataLoaded) {
      return;
    }

    let key = this.queryID.toString() + ":" + this.itemsetID.toString()
    if(this.queryService.sqlResults.has(key)) {
      this.itemsetData = this.queryService.sqlResults.get(key);
      this.dataLoaded = true;
      // this.makeHistogram();
    }
  }

  makeHistogram() {
    let metricData = this.getMetricData(this.itemsetData);

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
      xaxis: {title: this.query.metric, range: this.displayService.axisBounds.get(this.query.metric)},
      yaxis: {title: 'Count'}
    };

    let div = "histogram" + " " + this.queryID.toString() + " " + this.itemsetID.toString();
    Plotly.newPlot(div, data=data, layout);
  }

  /*
   Return an array of just the metric column of a dataframe.
  */
  getMetricData(data){
    let metricName = this.query.metric;
    let metricCol = -1;

    for(let i = 0; i < data.schema.numColumns; i++){
      if(data.schema.columnNames[i] == metricName){
        metricCol = i;
        break;
      }
    }
    if(metricCol == -1){
      this.messageService.add("Bad metric column name");
    }

    let metricData = [];

    let max = Number.NEGATIVE_INFINITY
    let min = Number.POSITIVE_INFINITY

    for(let i = 0; i < data.numRows; i++){
      let val = data.rows[i].vals[metricCol]
      metricData.push(val);
      if(this.itemsetID < 0) {
        if(val < min) {
          min = val;
        }
        if(val > max) {
          max = val;
        }
      }
    }

    if(this.itemsetID < 0) {
      this.displayService.updateAxisBounds(metricName, min, max);
    }

    return metricData;
  }
}
