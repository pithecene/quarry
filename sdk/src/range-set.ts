/** A half-open byte range [start, end). */
type ByteRange = { readonly start: number; readonly end: number }

/**
 * A set of byte ranges, kept as sorted, disjoint, half-open intervals in
 * which touching ranges are merged. An artifact written in order is one
 * interval however many ranges it took.
 *
 * @internal
 */
export class RangeSet {
  private readonly ranges: ByteRange[] = []

  /** Reports whether [start, end) shares a byte with a range in the set. */
  overlaps(start: number, end: number): boolean {
    const next = this.ranges[this.firstEndingAfter(start)]
    return next !== undefined && next.start < end
  }

  /**
   * Adds [start, end), which must not overlap a range in the set, merged
   * with the range that ends at start and the one that begins at end.
   */
  add(start: number, end: number): void {
    const first = this.firstEndingAfter(start - 1)
    let merged: ByteRange = { start, end }
    let count = 0
    for (let next = this.ranges[first]; next !== undefined && next.start <= end; ) {
      merged = { start: Math.min(merged.start, next.start), end: Math.max(merged.end, next.end) }
      count++
      next = this.ranges[first + count]
    }
    this.ranges.splice(first, count, merged)
  }

  /** Reports whether the set is exactly [0, size). */
  covers(size: number): boolean {
    if (size === 0) return this.ranges.length === 0
    const only = this.ranges[0]
    return this.ranges.length === 1 && only?.start === 0 && only.end === size
  }

  /** Returns the index of the first range whose end exceeds offset. */
  private firstEndingAfter(offset: number): number {
    let lo = 0
    let hi = this.ranges.length
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if ((this.ranges[mid]?.end ?? Number.POSITIVE_INFINITY) > offset) {
        hi = mid
      } else {
        lo = mid + 1
      }
    }
    return lo
  }
}
