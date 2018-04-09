import { Component, OnInit } from '@angular/core';
import { QueryService } from '../query.service'

@Component({
  selector: 'app-cell',
  templateUrl: './cell.component.html',
  styleUrls: ['./cell.component.css'],
  providers: [QueryService]
})
export class CellComponent implements OnInit {
  errorMessage: string;
  query: string;

  constructor(private queryService: QueryService) { }

  ngOnInit() {
      this.getQuery();
  }

  getQuery() {
      this.queryService.getQuery()
          .subscribe(
              query => this.query = query,
              error => this.errorMessage = error
          )
  }

}
