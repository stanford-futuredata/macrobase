import { Component, OnInit, Input } from '@angular/core';
import { QueryService } from '../query.service';
import { MessageService } from "../message.service";
import { DisplayService } from "../display.service";

@Component({
  selector: 'app-plot',
  templateUrl: './plot.component.html',
  styleUrls: ['./plot.component.css']
})
export class PlotComponent implements OnInit {
  @Input() queryID: number;
  @Input() itemsetID: number;

  query;
  itemsetData;

  dataLoaded = false;

  constructor(private queryService: QueryService, private messageService: MessageService, private displayService: DisplayService) { }

  ngOnInit() {
    this.query = this.queryService.queries.get(this.queryID);

    this.requestData();

    this.queryService.dataResponseReceived.subscribe(
        () => {this.updateData();}
      )
  }

  requestData() {
    let newQuery = this.query;
    newQuery.numRows = -1; //select all rows
    newQuery.columnFilters = this.getItemsetAttributes();

    this.queryService.getItemsetData(newQuery, this.queryID, this.itemsetID);
  }

  getItemsetAttributes() {
    if(this.itemsetID < 0) {
      return "";
    }
    let itemset = this.queryService.queryResults.get(this.queryID).results[this.itemsetID];
    return JSON.stringify(itemset.matcher);
  }

  updateData() {
    if(this.dataLoaded) {
      return;
    }

    let key = this.queryID.toString() + "," + this.itemsetID.toString()
    if(this.queryService.itemsetData.has(key)) {
      this.itemsetData = this.queryService.itemsetData.get(key);
      this.dataLoaded = true;
      this.makeHistogram();
    }
  }

  makeHistogram() {
    let metricData = this.getMetricData(this.itemsetData);

    let histName = "";
    if(this.itemsetID < 0) {
      histName = "all";
    }
    else{
      histName = this.getItemsetAttributes();
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
