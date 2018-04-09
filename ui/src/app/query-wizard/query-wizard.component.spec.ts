import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { QueryWizardComponent } from './query-editor.component';

describe('QueryWizardComponent', () => {
  let component: QueryWizardComponent;
  let fixture: ComponentFixture<QueryWizardComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ QueryWizardComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(QueryWizardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
