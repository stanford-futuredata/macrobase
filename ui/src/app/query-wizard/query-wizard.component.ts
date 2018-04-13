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
  selectedMetric;
  selectedAttribute;
  sqlQuery;

  constructor(private queryService: QueryService, private messageService: MessageService) { }

  addMetric(): void {
    if(this.selectedMetric) this.query.metrics.add(this.selectedMetric);
  }

  addAttribute(): void {
    if(this.selectedAttribute) this.query.attributes.add(this.selectedAttribute);
  }

  removeMetric(metric: string): void {
    this.query.metrics.delete(metric)
  }

  removeAttribute(attribute: string): void {
    this.query.attributes.delete(attribute)
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
      metrics: new Set(["usage"]),
      cutoff: 1.0,
      includeHi: true,
      includeLo: true,
      summarizer: "aplinear",
      attributes: new Set([
        "location",
        "version"
      ]),
      ratioMetric: "globalratio",
      minRatioMetric: 10.0,
      minSupport: 0.2
    };
  }

  runQuery(query: Query) {
    this.queryService.runQuery(this.query);
    this.messageService.add("Running query on: " + JSON.stringify(this.query));
  }

  ngOnInit() {
    this.getBaseQuery();
  }

}