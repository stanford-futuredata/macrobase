import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';

import { AppComponent } from './app.component';
import { QueryWizardComponent } from './query-wizard/query-wizard.component';

import { FormsModule } from '@angular/forms';
import { CellComponent } from './cell/cell.component';

import { HttpModule } from '@angular/http';


@NgModule({
  declarations: [
    AppComponent,
    QueryWizardComponent,
    CellComponent
  ],
  imports: [
    BrowserModule,
    FormsModule,
    HttpModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
