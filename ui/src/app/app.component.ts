import { Component, OnInit } from '@angular/core';
import { QueryService } from './query.service'
import { DisplayService } from './display.service'

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  displayMessages = false;
  newID = 0;
  validIDs = new Set();
  editExisting = false;
  editID = 0;
  displayType = this.displayService.getDisplayType(); //DataHomepage, QueryWizard, Dashboard, Explore
  selectedIDs = new Set();
  exploreIDs = new Set();
  isPlot = false;

  constructor(private queryService: QueryService, private displayService: DisplayService) { }

  ngOnInit() {
    this.updateDisplayType(this.displayService.getDisplayType());
    this.displayService.displayChanged.subscribe(
        () => {this.updateDisplayType(this.displayService.getDisplayType());}
      )
    this.queryService.queryResponseReceived.subscribe(
        (id) => {this.updateValidIDs(id);}
      )
  }

  updateDisplayType(type: string) {
    if(type == 'QueryWizard'){
      if(!this.editExisting){
        this.editID = this.newID;
      }
      this.editExisting = false;
    }

    this.displayType = type;
  }

  highlightSelected() {
    for(let id in Array.from(this.selectedIDs)) {
      document.getElementById('summary'+id).style.backgroundColor = "gray";
    }
  }

  updateValidIDs(id: number) {
    this.validIDs.add(id);
    if(id == this.newID){
      this.newID++;
    }
  }

  getSelectedColor(id: number) {
    if(this.selectedIDs.has(id)){
      return "gray";
    }
    else{
      return "bisque";
    }
  }

  selectID(id: number) {
    if(this.selectedIDs.has(id)){
      this.selectedIDs.delete(id);
      document.getElementById('summary'+id).style.backgroundColor = "bisque";
    }
    else{
      this.selectedIDs.add(id);
      document.getElementById('summary'+id).style.backgroundColor = "gray";
    }
  }

  exploreSelected() {
    this.exploreIDs = this.selectedIDs;
    this.displayService.setDisplayType('Explore');
  }

  newQuery(){
    this.editExisting = false;
    this.displayService.setDisplayType('QueryWizard');
  }

  editSelected() {
    this.editID = Array.from(this.selectedIDs)[0];
    this.editExisting = true;
    this.displayService.setDisplayType('QueryWizard')
  }

  deleteSelected() {
    this.selectedIDs.forEach( (id) => {
      this.validIDs.delete(id);
      this.queryService.removeID(id);
      this.exploreIDs.delete(id);
      this.selectedIDs.delete(id)
    });
  }

  plotSelected(){
    this.isPlot = true;
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
