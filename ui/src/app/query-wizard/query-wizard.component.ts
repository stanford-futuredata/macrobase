import { Component, OnInit, Input } from '@angular/core';
import { Query } from '../query';
import { QueryResult } from '../query-result'

import { QueryService } from '../query.service'
import { DataService } from '../data.service'
import { MessageService } from '../message.service'

@Component({
  selector: 'app-query-wizard',
  templateUrl: './query-wizard.component.html',
  styleUrls: ['./query-wizard.component.css']
})
export class QueryWizardComponent implements OnInit {
  @Input() id: number;
  query = new Object();

  tableName;

  possibleAttributes: Array<string>;
  possibleMetrics: Array<string>;

  attributeSet = new Set();
  selectedMetric;
  minSupport: number;
  minRatioMetric: number;
  percentile: number;

  checkAll = false;

  constructor(private queryService: QueryService, private dataService: DataService, private messageService: MessageService) { }

  ngOnInit() {
    this.loadSchema();
    if(this.queryService.queries.has(this.id.toString())) {
      this.loadQuery();
    }
    else{
      this.loadBaseQuery();
    }

    this.tableName = this.dataService.getTableName();
  }

  //to be implemented
  loadSchema(): void {
    this.possibleAttributes = this.dataService.getAttributeColumns();

    this.possibleMetrics = this.dataService.getMetricColumns();
  }

  loadQuery() {
    this.query = this.queryService.queries.get(this.id.toString());
    this.selectedMetric = this.query["metric"];
    this.attributeSet = this.query["attributes"];
    this.minSupport = this.query["minSupport"];
    this.minRatioMetric = this.query["minRatioMetric"];
    this.percentile = this.query["percentile"];
  }

  loadBaseQuery(): void {
    this.checkAll = true;
    this.updateAll(); //select all attributes
    this.selectedMetric = this.possibleMetrics[0];
    this.minSupport = 0.01
    this.minRatioMetric = 1;
    this.percentile = 0.95;
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

  runQuery() {
    if(this.tableName == null) {
      alert("No data source given.");
      return;
    }
    this.minSupport = Number(this.minSupport);
    this.minRatioMetric = Number(this.minRatioMetric);
    this.percentile = Number(this.percentile);

    this.query["attributes"] = this.attributeSet;
    this.query["metric"] = this.selectedMetric;
    this.query["minSupport"] = this.minSupport;
    this.query["minRatioMetric"] = this.minRatioMetric;
    this.query["percentile"] = this.percentile;
    this.generateSQLString();
    let key = this.id.toString();
    this.queryService.runSQL(this.query, key);
  }

  generateSQLString() {
    let attributes = Array.from(this.attributeSet.values()).join(', ');
    this.query["sql"] =
      `SELECT * FROM DIFF
         (SPLIT (
           SELECT *, percentile(${ this.selectedMetric }) as percentile from ${ this.tableName })
         WHERE percentile > ${ this.percentile.toFixed(2) })
      ON ${ attributes }
      WITH MIN SUPPORT ${ this.minSupport.toFixed(2) } MIN RATIO ${ this.minRatioMetric.toFixed(2) }`;
  }

}