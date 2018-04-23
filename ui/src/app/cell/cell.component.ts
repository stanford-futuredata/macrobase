import { Component, OnInit } from '@angular/core';
import { QueryService } from '../query.service'

@Component({
  selector: 'app-cell',
  templateUrl: './cell.component.html',
  styleUrls: ['./cell.component.css']
})
export class CellComponent implements OnInit {
  constructor(private queryService: QueryService) { }

  ngOnInit() {
  }

}
