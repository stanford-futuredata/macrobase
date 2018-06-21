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
  possibleTypes: Array<string>

  attributeSet = new Set();
  attributeString;
  selectedMetric;
  queryType: string;
  percentile: number;
  minSupport: number;
  minRatioMetric: number;

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
    this.updateAttributeString()

    this.tableName = this.dataService.getTableName();

    this.possibleTypes = ["percentile"];
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
    this.queryType = this.query["queryType"];
    if(this.queryType == "percentile") {
      this.percentile = this.query["percentile"];
    }
    this.minSupport = this.query["minSupport"];
    this.minRatioMetric = this.query["minRatioMetric"];
  }

  loadBaseQuery(): void {
    this.checkAll = true;
    this.updateAll(); //select all attributes
    this.selectedMetric = this.possibleMetrics[0];
    this.queryType = "percentile";
    this.percentile = 0.95;
    this.minSupport = 0.01
    this.minRatioMetric = 1;
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
    this.updateAttributeString();
  }

  checkAttribute(attribute: string) {
    if(this.attributeSet.has(attribute)){
      return true;
    }

    return false;
  }

  updateAttribute(attribute: string) {
    if(this.checkAttribute(attribute)){
      this.removeAttribute(attribute);
    }
    else{
      this.addAttribute(attribute);
    }
    this.updateAttributeString();
  }

  updateAttributeString() {
    this.attributeString = Array.from(this.attributeSet.values()).join(', ');
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

    this.query["attributes"] = this.attributeSet;
    this.query["metric"] = this.selectedMetric;
    this.query["queryType"] = this.queryType;
    if(this.queryType == "percentile") {
      this.percentile = Number(this.percentile);
      this.query["percentile"] = this.percentile;
    }
    this.minSupport = Number(this.minSupport);
    this.query["minSupport"] = this.minSupport;
    this.minRatioMetric = Number(this.minRatioMetric);
    this.query["minRatioMetric"] = this.minRatioMetric;

    this.generateSQLString();
    let key = this.id.toString();
    this.queryService.runSQL(this.query, key);
  }

  generateSQLString() {
    this.query["sql"] =
      `SELECT * FROM DIFF
         (SPLIT (
           SELECT *, percentile(${ this.selectedMetric }) as percentile from ${ this.tableName })
         WHERE percentile > ${ this.percentile.toFixed(2) })
      ON ${ this.attributeString }
      WITH MIN SUPPORT ${ this.minSupport.toFixed(2) } MIN RATIO ${ this.minRatioMetric.toFixed(2) }`;
  }

}