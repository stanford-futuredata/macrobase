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

  constructor(private queryService: QueryService, private messageService: MessageService) { }

  addMetric(): void {
    if(this.selectedMetric) this.metricSet.add(this.selectedMetric);
    this.updateQuery();
  }

  addAttribute(): void {
    if(this.selectedAttribute) this.attributeSet.add(this.selectedAttribute);
    this.updateQuery();
  }

  removeMetric(metric: string): void {
    this.metricSet.delete(metric);
    this.updateQuery();
  }

  removeAttribute(attribute: string): void {
    this.attributeSet.delete(attribute);
    this.updateQuery();
  }

  updateQuery(): void {
    this.query.metric = Array.from(this.metricSet)[0]
    this.query.attributes = Array.from(this.attributeSet)
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
      minRatioMetric: 10.1,
      minSupport: 0.2
    };

    this.metricSet.add("usage");
    this.attributeSet.add("location");
    this.attributeSet.add("version");
  }

  runQuery(query: Query) {
    this.queryService.runQuery(this.query);
  }

  ngOnInit() {
    this.getBaseQuery();
  }

}