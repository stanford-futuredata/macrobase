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
  sqlString: string;

  tableName: string;
  possibleAttributes: string[];
  possibleMetrics: string[];

  attributeSet = new Set();
  selectedMetric;
  minSupport;
  minRatioMetric;

  percentileCutoff = 0.95;

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
    this.checkAll = true;
    this.updateAll(); //select all attributes
    this.selectedMetric = this.possibleMetrics[0];
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

  runQuery(query: Query) {
    this.generateSQLString();
    let key = this.id.toString();
    this.queryService.runSQL(this.sqlString, key)
  }

  generateSQLString() {
    let attributes = Array.from(this.attributeSet.values()).join(', ');
    this.sqlString =
      `SELECT * FROM DIFF
         (SPLIT (
           SELECT *, percentile(${ this.selectedMetric }) as percentile from ${ this.tableName })
         WHERE percentile > ${ this.percentileCutoff })
      ON ${ attributes }
      WITH MIN SUPPORT ${ this.minSupport } MIN RATIO ${ this.minRatioMetric };`;
  }

}