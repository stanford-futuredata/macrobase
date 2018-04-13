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


@NgModule({
  declarations: [
    AppComponent,
    QueryWizardComponent,
    CellComponent,
    MessagesComponent
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpClientModule
  ],
  providers: [
    MessageService,
    QueryService
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }
