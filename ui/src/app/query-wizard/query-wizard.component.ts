import { Component, OnInit } from '@angular/core';
import { Query } from '../query';

@Component({
  selector: 'app-query-wizard',
  templateUrl: './query-wizard.component.html',
  styleUrls: ['./query-wizard.component.css'],
})
export class QueryWizardComponent implements OnInit {
  query: Query;
  selectedMetric;
  selectedAttr;
  sqlQuery

  constructor() { }

  addMetric(): void {
    if(this.selectedMetric) this.query.metrics.add(this.selectedMetric);
  }

  addAttr(): void {
    if(this.selectedAttr) this.query.attrs.add(this.selectedAttr);
  }

  removeMetric(metric: string): void {
    this.query.metrics.delete(metric)
  }

  removeAttr(attr: string): void {
    this.query.attrs.delete(attr)
  }

  // updateSql(): void {
  //   let dbStr = "wiki";

  //   let metricStr = "";
  //   for(let metric in this.query.metrics){
  //     metricStr += "percentile(" + metric + ") > 0.95";
  //   }

  //   let attrStr = "";
  //   for(let attr in this.query.attrs){
  //     attrStr += attr + ",";
  //   }
  //   if(attrStr) attrStr = substr(0, attrStr.length-2);

  //   this.sqlQuery = "SELECT * FROM DIFF (SPLIT " + dbStr + " WHERE " metricStr + " ON " + attrStr + ";";
  // }

  possibleMetrics = [
    'added',
    'deleted',
    'delta'
  ];

  possibleAttrs = [
    'isMinor',
    'isRobot',
    'channel',
    'namespace',
    'isUnpatrolled',
    'isNew'
  ];

  getBaseQuery(): void {
    this.query = {
      metrics: new Set(['deleted']),
      attrs: new Set(['isRobot', 'isNew', 'channel']),
      support: .1,
      rratio: .1
    };
  }

  ngOnInit() {
    this.getBaseQuery();
    // this.updateSql();
  }

}
