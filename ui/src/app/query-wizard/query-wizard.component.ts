import { Component, OnInit } from '@angular/core';
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
  query: Query;
  metricSet = new Set();
  attributeSet = new Set();
  selectedMetric;
  selectedAttribute;
  minSupport;
  minRatioMetric;

  constructor(private queryService: QueryService, private messageService: MessageService) { }

  addMetric(): void {
    if(this.selectedMetric) this.metricSet.add(this.selectedMetric);
  }

  addAttribute(): void {
    if(this.selectedAttribute) this.attributeSet.add(this.selectedAttribute);
  }

  removeMetric(metric: string): void {
    this.metricSet.delete(metric);
  }

  removeAttribute(attribute: string): void {
    this.attributeSet.delete(attribute);
  }

  updateQuery(): void {
    this.query.metric = Array.from(this.metricSet)[0];
    this.query.attributes = Array.from(this.attributeSet);
    this.query.minSupport = parseFloat(this.minSupport);
    this.query.minRatioMetric = parseFloat(this.minRatioMetric);
  }

  possibleMetrics = [
    'usage',
    'latency'
  ];

  possibleAttributes = [
    'location',
    'version'
  ];

  getBaseQuery(): void {
    this.query = {
      pipeline: "BasicBatchPipeline",
      inputURI: "csv://core/demo/sample.csv",
      classifier: "percentile",
      metric: "usage",
      cutoff: 1.1,
      includeHi: true,
      includeLo: true,
      summarizer: "aplinear",
      attributes: [
        "location",
        "version"
      ],
      ratioMetric: "globalratio",
      minRatioMetric: 1.0,
      minSupport: 0.01
    };

    this.metricSet.add(this.query.metric);
    for(var i in this.query.attributes){
      this.attributeSet.add(this.query.attributes[i]);
    }
    this.minSupport = this.query.minSupport;
    this.minRatioMetric = this.query.minRatioMetric;
  }

  runQuery(query: Query) {
    this.updateQuery();
    this.queryService.runQuery(this.query);
  }

  ngOnInit() {
    this.getBaseQuery();
  }

}