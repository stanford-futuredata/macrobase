/*
 * Component - QueryWizard
 * #######################
 * The query wizard allows users to generate queries and send them to be run as SQL commands
 * on the server.
 * Used in the Edit tab.
 */

import { Component, OnInit, Input } from '@angular/core';

import { QueryService } from '../query.service';
import { DataService } from '../data.service';
import { MessageService } from '../message.service';

@Component({
  selector: 'app-query-wizard',
  templateUrl: './query-wizard.component.html',
  styleUrls: ['./query-wizard.component.css']
})
export class QueryWizardComponent implements OnInit {
  @Input() id: number;

  private query = new Object();

  private tableName;

  private possibleAttributes: Array<string>;
  private possibleMetrics: Array<string>;
  private possibleTypes: Array<string>

  private attributeSet = new Set();
  private attributeString;
  private selectedMetric;
  private queryType: string;
  private percentile: number;
  private minSupport: number;
  private minRatioMetric: number;

  private checkAll = false;

  constructor(private queryService: QueryService,
              private dataService: DataService,
              private messageService: MessageService) {}

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

  /*
   * Load column names for attributes and metrics
   */
  private loadSchema(): void {
    this.possibleAttributes = this.dataService.getAttributeColumns();
    this.possibleMetrics = this.dataService.getMetricColumns();
  }

  /*
   * Load a previously run query for editing an existing query
   */
  private loadQuery() {
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

  /*
   * Load the base parameters for a new query
   */
  private loadBaseQuery(): void {
    this.checkAll = true;
    this.updateAll(); //select all attributes
    this.selectedMetric = this.possibleMetrics[0];
    this.queryType = "percentile";
    this.percentile = 0.95;
    this.minSupport = 0.01
    this.minRatioMetric = 1;
  }

  /*
   * "Select all" functionality for attributes
   */
  private updateAll() {
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

  /*
   * Check if attribute is already selected
   */
  private checkAttribute(attribute: string) {
    if(this.attributeSet.has(attribute)){
      return true;
    }

    return false;
  }

  /*
   * "Check/uncheck" attribute functionality
   */
  private updateAttribute(attribute: string) {
    if(this.checkAttribute(attribute)){
      this.removeAttribute(attribute);
    }
    else{
      this.addAttribute(attribute);
    }
    this.updateAttributeString();
  }

  /*
   * Update comma separated string list of attributes
   */
  private updateAttributeString() {
    this.attributeString = Array.from(this.attributeSet.values()).join(', ');
  }

  /*
   * Add attribute to selection
   */
  private addAttribute(attribute: string): void {
    this.attributeSet.add(attribute);
  }

  /*
   * Remove attribute from selection
   */
  private removeAttribute(attribute: string): void {
    this.attributeSet.delete(attribute);
  }

  /*
   * Send SQL command as request to server to run query
   */
  private runQuery() {
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

  /*
   * Generate the SQL command for the query
   */
  private generateSQLString() {
    this.query["sql"] =
      `SELECT * FROM DIFF
         (SPLIT (
           SELECT *, percentile(${ this.selectedMetric }) as percentile from ${ this.tableName })
         WHERE percentile > ${ this.percentile.toFixed(2) })
      ON ${ this.attributeString }
      WITH MIN SUPPORT ${ this.minSupport.toFixed(2) } MIN RATIO ${ this.minRatioMetric.toFixed(2) }`;
  }

}