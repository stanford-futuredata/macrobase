import { Component, OnInit, Input } from '@angular/core';
import { Query } from '../query';
import { QueryService } from '../query.service'

@Component({
  selector: 'app-query-summary',
  templateUrl: './query-summary.component.html',
  styleUrls: ['./query-summary.component.css']
})
export class QuerySummaryComponent implements OnInit {
  @Input() id: number;
  metric: string;
  attributes: string;
  support: number;
  ratio: number;
  percentile: number;

  constructor(private queryService: QueryService) { }

  ngOnInit() {
    let key = this.id.toString();
    this.updateSummary(this.queryService.queries.get(key));
    this.queryService.sqlResponseReceived.subscribe(
        () => {this.updateSummary(this.queryService.queries.get(key));}
      )
  }

  updateSummary(query) {
    this.metric = query["metric"];
    this.attributes = Array.from(query["attributes"]).join(", ");
    this.support = query["minSupport"];
    this.ratio = query["minRatioMetric"];
    this.percentile = query["percentile"]
  }

}
