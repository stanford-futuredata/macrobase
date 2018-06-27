import { Component, OnInit, Inject } from '@angular/core';
import { NgModel } from '@angular/forms'
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';

import { QueryService } from '../query.service';
import { DataService } from '../data.service';

@Component({
  selector: 'app-query-editor',
  templateUrl: './query-editor.component.html',
  styleUrls: ['./query-editor.component.css']
})
export class QueryEditorComponent implements OnInit {
  private id: number;
  private oldID: number;

  private query = new Object();

  private tableName;

  private possibleAttributes: Array<string>;
  private possibleMetrics: Array<string>;
  private possibleTypes: Array<string>

  private attributeString;
  private selectedMetric: string;
  private selectedAttributes: Array<string>;
  private queryType: string;
  private percentileString: string;
  private percentile: number;
  private minSupportString: string;
  private minSupport: number;
  private minRatioMetricString: string;
  private minRatioMetric: number;

  private running = false;

  constructor(private queryService: QueryService,
              private dataService: DataService,
              public dialogRef: MatDialogRef<QueryEditorComponent>,
              @Inject(MAT_DIALOG_DATA) public data: any) {}

  ngOnInit() {
    this.id = this.data.id;
    this.oldID = this.data.oldID;

    this.loadSchema();
    if(this.queryService.queries.has(this.oldID.toString())) {
      this.loadQuery();
    }
    else{
      this.loadBaseQuery();
    }
    this.updateAttributeString()

    this.tableName = this.dataService.getTableName();
    this.possibleTypes = ["percentile"];

    this.queryService.sqlResponseReceived.subscribe(
      () => {
        if (this.running) {
          this.dialogRef.close(true);
        }
      }
    );
  }

  private cancel() {
    this.dialogRef.close(false);
  }

  /*
   * Load column names for attributes and metrics
   */
  private loadSchema() {
    this.possibleAttributes = this.dataService.getAttributeColumns();
    this.possibleMetrics = this.dataService.getMetricColumns();
  }

  /*
   * Load a previously run query for editing an existing query
   */
  private loadQuery() {
    let query = this.queryService.queries.get(this.oldID.toString());
    this.selectedMetric = query["metric"];
    this.selectedAttributes = query["attributes"];
    this.queryType = query["queryType"];
    if(this.queryType == "percentile") {
      this.percentileString = query["percentile"];
    }
    this.minSupportString = query["minSupport"];
    this.minRatioMetricString = query["minRatioMetric"];
  }

  /*
   * Load the base parameters for a new query
   */
  private loadBaseQuery(): void {
    this.queryType = "percentile";
    this.percentileString = "0.95";
    this.minSupportString = "0.01";
    this.minRatioMetricString = "1";
  }

  private selectAll(selection: NgModel) {
    selection.update.emit(this.possibleAttributes);
    this.updateAttributeString();
  }

  private clear(selection: NgModel) {
    selection.update.emit([]);
    this.updateAttributeString();
  }

  /*
   * Update comma separated string list of attributes
   */
  private updateAttributeString() {
    if(this.selectedAttributes) {
      this.attributeString = this.selectedAttributes.join(', ');
    }
    else{
      this.attributeString = '';
    }
  }

  /*
   * Send SQL command as request to server to run query
   */
  private runQuery() {
    if(this.queryType == "percentile") {
      this.percentile = Number(this.percentileString);
    }
    this.minSupport = Number(this.minSupportString);
    this.minRatioMetric = Number(this.minRatioMetricString);

    if(!this.isFormValid()) {
      return;
    }

    this.query["attributes"] = this.selectedAttributes;
    this.query["metric"] = this.selectedMetric;
    this.query["queryType"] = this.queryType;
    if(this.queryType == "percentile") {
      this.query["percentile"] = this.percentileString;
    }
    this.query["minSupport"] = this.minSupportString;
    this.query["minRatioMetric"] = this.minRatioMetricString;

    this.generateSQLString();
    let key = this.id.toString();

    this.queryService.runSQL(this.query, key);
    this.running = true;
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

  private isFormValid() {
    if(this.selectedAttributes == null ||
       this.selectedMetric == null ||
       this.queryType == null ||
       this.percentileString == null ||
       this.minSupportString == null ||
       this.minRatioMetricString == null ||
       this.selectedAttributes.length == 0 ||
       this.minSupport < 0 || this.minSupport > 1 ||
       this.percentile < 0 || this.percentile > 1 ||
       this.minRatioMetric < 0) {
      return false;
    }

    return true;
  }

  /*
   * Local wrapper of isNaN() function for use in ngIf
   */
  private isNaN(val) {
    return isNaN(val);
  }

  /*
   * Local wrapper of Number() function for use in ngIf
   */
  private Number(val) {
    return Number(val);
  }
}
