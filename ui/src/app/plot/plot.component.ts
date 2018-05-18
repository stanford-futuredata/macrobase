import { Component, OnInit, Input } from '@angular/core';
import { QueryService } from '../query.service';
import { MessageService } from "../message.service";

@Component({
  selector: 'app-plot',
  templateUrl: './plot.component.html',
  styleUrls: ['./plot.component.css']
})
export class PlotComponent implements OnInit {
  @Input() id;

  query;
  queryData;
  queryResult
  itemsetData;

  displayHistogram = 0;
  curItemsetID;
  isPlot = false;

  constructor(private queryService: QueryService, private messageService: MessageService) { }

  ngOnInit() {
    this.updateData();

    this.queryService.dataResponseReceived.subscribe(
        () => {this.updateData();
               this.updateHistogram();}
      )

    this.makeHistogram(this.id);
  }


  updateData() {
    this.query = this.queryService.queries.get(this.id)
    this.queryData = this.queryService.queryData.get(this.id);
    this.queryResult = this.queryService.queryResults.get(this.id);
    this.itemsetData = this.queryService.itemsetData.get(this.id);
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

    let metricData = new Array();

    for(let i = 0; i < data.numRows; i++){
      metricData.push(data.rows[i].vals[metricCol]);
    }

    return metricData;
  }

  getItemsetData(itemsetID: number, numRows: number, isSample: boolean) {
    this.curItemsetID = itemsetID;
    let query = this.queryService.queries.get(this.id);
    query.numRows = numRows;
    query.columnFilters = this.getItemsetAttributes(itemsetID);

    this.queryService.getItemsetData(query, this.id, itemsetID);
  }

  getItemsetAttributes(itemsetID: number) {
    let itemset = this.queryResult.results[itemsetID];
    return JSON.stringify(itemset.matcher);
  }

  getQueryData(numRows: number) {
    let query = this.query;
    query.numRows = numRows;
    query.columnFilters = "";

    this.queryService.getQueryData(query, this.id);
  }

  makeHistogram(itemsetID: number) {
    this.curItemsetID = itemsetID;
    this.getItemsetData(itemsetID, -1, false);
    if(this.isPlot){ //no need to wait for query data
      this.displayHistogram = 2;
    }
    else{
      this.getQueryData(-1);
      this.displayHistogram = 1;
    }
  }

  clearHistogram(){
    Plotly.purge(document.getElementById("histogram"));
    this.displayHistogram = 0;
    this.isPlot = false;
  }

  updateHistogram() {
    if(this.displayHistogram == 0){
      return;
    }
    if(this.displayHistogram == 1){
      this.displayHistogram++;
      return;
    }

    let data = []

    if(!this.isPlot){
      let queryMetricData = this.getMetricData(this.queryData);
      data.push({
              y: queryMetricData,
              opacity:0.75,
              type:'histogram',
              histnorm:'probability density',
              name:'all'});
    }

    let itemsetMetricData = this.getMetricData(this.itemsetData);
    data.push({
            y: itemsetMetricData,
            opacity:0.75,
            type:'histogram',
            histnorm:'probability density',
            name:this.getItemsetAttributes(this.curItemsetID)});

    var layout = {
            xaxis: {title: this.query.metric},
            yaxis: {title: 'Probability Density'},
            barmode:'overlay'};

    Plotly.plot(document.getElementById("histogram"), data=data, layout);

    this.isPlot = true;
  }

}
