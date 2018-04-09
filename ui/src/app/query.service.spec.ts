import { TestBed, inject } from '@angular/core/testing';

import { QueryService } from './query.service';

describe('QueryService', () => {
  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [QueryService]
    });
  });

  it('should be created', inject([QueryService], (service: QueryService) => {
    expect(service).toBeTruthy();
  }));
});
