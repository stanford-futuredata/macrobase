import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';

import { AppComponent } from './app.component';
import { QueryWizardComponent } from './query-wizard/query-wizard.component';

import { FormsModule } from '@angular/forms';
import { CellComponent } from './cell/cell.component';

import { HttpClientModule } from '@angular/common/http';
import { MessagesComponent } from './messages/messages.component';
import { MessageService } from './message.service';
import { QueryService } from './query.service';
import { DataHeaderComponent } from './data-header/data-header.component';
import { DisplayService } from './display.service';
import { DataService } from './data.service';
import { QuerySummaryComponent } from './query-summary/query-summary.component';
import { PlotComponent } from './plot/plot.component';


@NgModule({
  declarations: [
    AppComponent,
    QueryWizardComponent,
    CellComponent,
    MessagesComponent,
    DataHeaderComponent,
    QuerySummaryComponent,
    PlotComponent
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpClientModule
  ],
  providers: [
    MessageService,
    QueryService,
    DisplayService,
    DataService
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }
