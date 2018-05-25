import { Component, OnInit, Input } from '@angular/core';
import { Query } from '../query';
import { QueryResult } from '../query-result'

import { QueryService } from '../query.service'
import { MessageService } from '../message.service'

@Component({
  selector: 'app-query-wizard',
  templateUrl: './query-wizard.component.html',
  styleUrls: ['./query-wizard.component.css']
})
export class QueryWizardComponent implements OnInit {
  @Input() id: number;
  query: Query;

  possibleAttributes: string[];
  possibleMetrics: string[];

  attributeSet = new Set();
  selectedMetric;
  minSupport;
  minRatioMetric;

  checkAll = false;

  dataSource = "csv://../data/wikiticker.csv";

  constructor(private queryService: QueryService, private messageService: MessageService) { }

  ngOnInit() {
    this.loadSchema();
    this.getBaseQuery();
  }

  //to be implemented
  loadSchema(): void {
    this.possibleAttributes = [
      "time",
      "user",
      "page",
      "channel",
      "namespace",
      "comment",
      "metroCode",
      "cityName",
      "regionName",
      "regionIsoCode",
      "countryName",
      "countryIsoCode",
      "isAnonymous",
      "isMinor",
      "isNew",
      "isRobot",
      "isUnpatrolled"
    ]

    this.possibleMetrics = [
      "delta",
      "added",
      "deleted"
    ]
  }

  getBaseQuery(): void {
    this.query = {
      pipeline: "BasicBatchPipeline",
      inputURI: this.dataSource,
      classifier: "percentile",
      metric: "added",
      cutoff: 1.1,
      includeHi: true,
      includeLo: true,
      summarizer: "aplinear",
      attributes: [],
      ratioMetric: "globalratio",
      minRatioMetric: 1.0,
      minSupport: 0.01,
      numRows: -1,
      columnFilters: ""
    };

    for(var i in this.query.attributes){
      this.attributeSet.add(this.query.attributes[i]);
    }
    this.selectedMetric = this.query.metric;
    this.minSupport = this.query.minSupport;
    this.minRatioMetric = this.query.minRatioMetric;
  }

  updateAll() {
    for(let attribute of this.possibleAttributes) {
      if(this.checkAll) {
        if(!this.checkAttribute(attribute)) {
          this.addAttribute(attribute)
        }
      }
      else{
        if(this.checkAttribute(attribute)) {
          this.removeAttribute(attribute)
        }
      }
    }
  }

  checkAttribute(attribute: string) {
    if(this.attributeSet.has(attribute)){
      return true;
    }

    return false;
  }

  updateAttribute(attribute: string) {
    if(this.checkAttribute(attribute)){
      this.removeAttribute(attribute)
    }
    else{
      this.addAttribute(attribute)
    }
  }

  addAttribute(attribute: string): void {
    this.attributeSet.add(attribute);
  }

  removeAttribute(attribute: string): void {
    this.attributeSet.delete(attribute);
  }

  updateQuery(): void {
    this.query.metric = this.selectedMetric;
    this.query.attributes = Array.from(this.attributeSet);
    this.query.minSupport = parseFloat(this.minSupport);
    this.query.minRatioMetric = parseFloat(this.minRatioMetric);
  }

  runQuery(query: Query) {
    this.updateQuery();
    this.queryService.runQuery(this.query, this.id);
  }

}