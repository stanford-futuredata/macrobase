import { Component } from '@angular/core';
import { QueryService } from './query.service'

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  displayMessages = true;
  id = 0;
  validIDs = new Set();

  constructor(private queryService: QueryService) { }

  newQuery() {
    this.validIDs.add(this.id);
    this.id++;
  }

  deleteQuery(id: number) {
    this.validIDs.delete(id);
    this.queryService.removeID(id);
  }

  minimizeCell(id: number){
    document.getElementById("cell"+id).style.display = "none";
    document.getElementById("expandCell"+id).style.display = "block";
  }

  expandCell(id: number){
    document.getElementById("cell"+id).style.display = "block";
    document.getElementById("expandCell"+id).style.display = "none";
  }

}
