import { describe, expect, it } from 'vitest'
import { RangeSet } from '../src/range-set'

function setOf(...ranges: ReadonlyArray<readonly [number, number]>): RangeSet {
  const set = new RangeSet()
  for (const [start, end] of ranges) set.add(start, end)
  return set
}

describe('RangeSet', () => {
  const overlapCases = [
    ['an empty set', [], [0, 5], false],
    ['a range before the only one', [[5, 10]], [0, 5], false],
    ['a range after the only one', [[5, 10]], [10, 12], false],
    ['a range inside one', [[0, 10]], [3, 4], true],
    ['a range across the start of one', [[5, 10]], [4, 6], true],
    ['a range across the end of one', [[5, 10]], [9, 11], true],
    ['a range spanning one', [[5, 10]], [0, 20], true],
    [
      'a gap between two',
      [
        [0, 5],
        [10, 15]
      ],
      [5, 10],
      false
    ],
    [
      'a range into the second of two',
      [
        [0, 5],
        [10, 15]
      ],
      [7, 11],
      true
    ]
  ] as const

  for (const [label, ranges, [start, end], want] of overlapCases) {
    it(`overlaps: ${label}`, () => {
      expect(setOf(...ranges).overlaps(start, end)).toBe(want)
    })
  }

  const coverCases = [
    ['an empty set covers zero bytes', [], 0, true],
    ['an empty set does not cover a byte', [], 1, false],
    [
      'ranges written in order',
      [
        [0, 3],
        [3, 6]
      ],
      6,
      true
    ],
    [
      'ranges written out of order',
      [
        [3, 6],
        [0, 3]
      ],
      6,
      true
    ],
    [
      'a range that closes a gap',
      [
        [0, 2],
        [4, 6],
        [2, 4]
      ],
      6,
      true
    ],
    [
      'ranges with a gap',
      [
        [0, 2],
        [4, 6]
      ],
      6,
      false
    ],
    ['ranges short of the end', [[0, 5]], 6, false],
    ['ranges missing the start', [[1, 6]], 6, false]
  ] as const

  for (const [label, ranges, size, want] of coverCases) {
    it(`covers: ${label}`, () => {
      expect(setOf(...ranges).covers(size)).toBe(want)
    })
  }
})
