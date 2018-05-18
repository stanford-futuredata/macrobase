import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { QuerySummaryComponent } from './query-summary.component';

describe('QuerySummaryComponent', () => {
  let component: QuerySummaryComponent;
  let fixture: ComponentFixture<QuerySummaryComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ QuerySummaryComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(QuerySummaryComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
