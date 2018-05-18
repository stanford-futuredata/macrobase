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

  constructor(private queryService: QueryService) { }

  ngOnInit() {
    this.updateSummary(this.queryService.queries.get(this.id));
    this.queryService.queryResponseReceived.subscribe(
        () => {this.updateSummary(this.queryService.queries.get(this.id));}
      )
  }

  updateSummary(query: Query) {
    this.metric = query.metric;
    this.attributes = query.attributes.join(", ");
    this.support = query.minSupport;
    this.ratio = query.minRatioMetric;
  }

}
