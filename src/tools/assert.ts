import { ExpNode } from './ExpTree.js';

export function assert(condition: boolean, msg?: string): asserts condition {
  if (!condition) {
    throw msg;
  }
}
