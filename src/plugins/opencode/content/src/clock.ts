/**
 * Clock interface for abstracting time
 *
 * This allows injecting a test clock for deterministic testing
 */

/** Convert a millisecond timestamp to Unix seconds */
export function msToSeconds(ms: number): number {
  return ms / 1000
}

/**
 * Interface for getting the current time
 */
export interface Clock {
  /** Get the current timestamp in milliseconds */
  now(): number

  /** Get the current timestamp in Unix seconds */
  nowSeconds(): number
}

/**
 * Real wall clock implementation using Date.now()
 */
export class WallClock implements Clock {
  now(): number {
    return Date.now()
  }

  nowSeconds(): number {
    return msToSeconds(this.now())
  }
}

/**
 * Test clock that allows setting and advancing time manually
 */
export class TestClock implements Clock {
  private currentTime: number

  constructor(initialTime: number = 1000000000000) {
    this.currentTime = initialTime
  }

  now(): number {
    return this.currentTime
  }

  nowSeconds(): number {
    return msToSeconds(this.currentTime)
  }

  /**
   * Set the current time to a specific value
   */
  set(time: number): void {
    this.currentTime = time
  }

  /**
   * Advance the clock by a number of milliseconds
   */
  advance(ms: number): void {
    this.currentTime += ms
  }

  /**
   * Advance by 1ms (convenience for getting unique timestamps)
   */
  tick(): void {
    this.advance(1)
  }
}

/** Default wall clock instance */
export const wallClock = new WallClock()
