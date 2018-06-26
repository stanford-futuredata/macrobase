import { AppComponent } from './app.component';
import { QueryWizardComponent } from './query-wizard/query-wizard.component';
import { CellComponent } from './cell/cell.component';
import { MessagesComponent } from './messages/messages.component';
import { MessageService } from './message.service';
import { QueryService } from './query.service';
import { DataHeaderComponent } from './data-header/data-header.component';
import { DisplayService } from './display.service';
import { DataService } from './data.service';
import { QuerySummaryComponent } from './query-summary/query-summary.component';
import { PlotComponent } from './plot/plot.component';
import { DataHomeComponent } from './data-home/data-home.component';
import { QueryEditorComponent } from './query-editor/query-editor.component';

import '../polyfills'
import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { HttpClientModule } from '@angular/common/http';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { CdkTableModule } from '@angular/cdk/table';
import {
  MatAutocompleteModule,
  MatButtonModule,
  MatButtonToggleModule,
  MatCardModule,
  MatCheckboxModule,
  MatChipsModule,
  MatDatepickerModule,
  MatDialogModule,
  MatDividerModule,
  MatExpansionModule,
  MatGridListModule,
  MatIconModule,
  MatInputModule,
  MatListModule,
  MatMenuModule,
  MatNativeDateModule,
  MatPaginatorModule,
  MatProgressBarModule,
  MatProgressSpinnerModule,
  MatRadioModule,
  MatRippleModule,
  MatSelectModule,
  MatSidenavModule,
  MatSliderModule,
  MatSlideToggleModule,
  MatSnackBarModule,
  MatSortModule,
  MatStepperModule,
  MatTableModule,
  MatTabsModule,
  MatToolbarModule,
  MatTooltipModule,
} from '@angular/material';

@NgModule({
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    FormsModule,
    ReactiveFormsModule,
    HttpClientModule,
    CdkTableModule,
    MatAutocompleteModule,
    MatButtonModule,
    MatButtonToggleModule,
    MatCardModule,
    MatCheckboxModule,
    MatChipsModule,
    MatStepperModule,
    MatDatepickerModule,
    MatDialogModule,
    MatDividerModule,
    MatExpansionModule,
    MatGridListModule,
    MatIconModule,
    MatInputModule,
    MatListModule,
    MatMenuModule,
    MatNativeDateModule,
    MatPaginatorModule,
    MatProgressBarModule,
    MatProgressSpinnerModule,
    MatRadioModule,
    MatRippleModule,
    MatSelectModule,
    MatSidenavModule,
    MatSliderModule,
    MatSlideToggleModule,
    MatSnackBarModule,
    MatSortModule,
    MatTableModule,
    MatTabsModule,
    MatToolbarModule,
    MatTooltipModule
  ],
  declarations: [
    AppComponent,
    QueryWizardComponent,
    CellComponent,
    MessagesComponent,
    DataHeaderComponent,
    QuerySummaryComponent,
    PlotComponent,
    DataHomeComponent,
    QueryEditorComponent
  ],
  entryComponents: [
    QueryEditorComponent
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
