/*
 * Service - Message
 * #################
 * Used for debugging purposes via custom messages.
 */

import { Injectable } from '@angular/core';

@Injectable()
export class MessageService {
  messages: string[] = [];

  add(message: string) {
    this.messages.push(message);
  }

  clear() {
    this.messages = [];
  }
}