import { Component, OnInit, Input } from '@angular/core';
import { QueryService } from '../query.service'

@Component({
  selector: 'app-cell',
  templateUrl: './cell.component.html',
  styleUrls: ['./cell.component.css']
})
export class CellComponent implements OnInit {
  @Input() id: number;

  constructor(private queryService: QueryService) {
  }

  ngOnInit() {
  }

}
