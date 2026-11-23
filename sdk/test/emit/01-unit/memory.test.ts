/**
 * Unit tests for Memory Pressure API (createMemoryAPI).
 *
 * Goal: Validate node/browser/cgroup reading, pressure classification,
 * highest-pressure selection, threshold customization, and isAbove().
 *
 * Uses internal test hooks (_readNode, _readBrowser, _readCgroup) to
 * inject deterministic values without real v8/cgroup/CDP access.
 */
import { describe, expect, it } from 'vitest'
import type { MemoryPressureLevel, MemoryUsage } from '../../../src/memory'
import { createMemoryAPI } from '../../../src/memory'

/** Helper: create a MemoryUsage with a specific ratio. */
function usage(ratio: number, limit = 1_000_000_000): MemoryUsage {
  const used = Math.round(limit * ratio)
  return { used, limit, ratio }
}

/** Helper: stub node reader. */
function stubNode(ratio: number): () => MemoryUsage {
  return () => usage(ratio)
}

/** Helper: stub browser reader. */
function stubBrowser(ratio: number | null): () => Promise<MemoryUsage | null> {
  return async () => (ratio === null ? null : usage(ratio))
}

/** Helper: stub cgroup reader. */
function stubCgroup(ratio: number | null): () => MemoryUsage | null {
  return () => (ratio === null ? null : usage(ratio))
}

describe('createMemoryAPI', () => {
  // ── Node metrics ────────────────────────────────────────────────

  describe('node metrics', () => {
    it('always includes node field with valid used, limit, ratio', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.3),
        _readBrowser: stubBrowser(null),
        _readCgroup: stubCgroup(null)
      })

      const snap = await api.snapshot()

      expect(snap.node.used).toBeGreaterThan(0)
      expect(snap.node.limit).toBeGreaterThan(0)
      expect(snap.node.ratio).toBeCloseTo(0.3, 1)
    })
  })

  // ── Browser metrics ─────────────────────────────────────────────

  describe('browser metrics', () => {
    it('includes browser field when page is available', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.2),
        _readBrowser: stubBrowser(0.6),
        _readCgroup: stubCgroup(null)
      })

      const snap = await api.snapshot()

      expect(snap.browser).not.toBeNull()
      expect(snap.browser?.ratio).toBeCloseTo(0.6, 1)
    })

    it('returns browser: null when opted out', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.2),
        _readBrowser: stubBrowser(0.6),
        _readCgroup: stubCgroup(null)
      })

      const snap = await api.snapshot({ browser: false })

      expect(snap.browser).toBeNull()
    })

    it('returns browser: null when reader returns null', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.2),
        _readBrowser: stubBrowser(null),
        _readCgroup: stubCgroup(null)
      })

      const snap = await api.snapshot()

      expect(snap.browser).toBeNull()
    })

    it('returns browser: null when reader returns null (graceful degradation)', async () => {
      // The real readBrowserUsage wraps page.metrics() in try/catch and
      // returns null on failure. Test hooks bypass that, so we validate
      // the null return path directly.
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.2),
        _readBrowser: async () => null,
        _readCgroup: stubCgroup(null)
      })

      const snap = await api.snapshot()
      expect(snap.browser).toBeNull()
    })
  })

  // ── Cgroup metrics ──────────────────────────────────────────────

  describe('cgroup metrics', () => {
    it('includes cgroup field when available', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.2),
        _readBrowser: stubBrowser(null),
        _readCgroup: stubCgroup(0.75)
      })

      const snap = await api.snapshot()

      expect(snap.cgroup).not.toBeNull()
      expect(snap.cgroup?.ratio).toBeCloseTo(0.75, 2)
    })

    it('returns cgroup: null when not available', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.2),
        _readBrowser: stubBrowser(null),
        _readCgroup: stubCgroup(null)
      })

      const snap = await api.snapshot()

      expect(snap.cgroup).toBeNull()
    })
  })

  // ── Cgroup v1 unlimited sentinel ────────────────────────────────

  describe('cgroup v1 unlimited sentinel', () => {
    it('returns cgroup: null when v1 limit is the unlimited sentinel (~2^63)', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.3),
        _readBrowser: stubBrowser(null),
        _readCgroupFile: (path: string) => {
          // v2 paths unavailable
          if (path === '/sys/fs/cgroup/memory/memory.usage_in_bytes') return 500_000_000
          if (path === '/sys/fs/cgroup/memory/memory.limit_in_bytes') return 2 ** 63 - 4096
          return null
        }
      })

      const snap = await api.snapshot()

      expect(snap.cgroup).toBeNull()
    })

    it('returns cgroup: null when v1 limit is exactly at 2^62 threshold', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.3),
        _readBrowser: stubBrowser(null),
        _readCgroupFile: (path: string) => {
          if (path === '/sys/fs/cgroup/memory/memory.usage_in_bytes') return 500_000_000
          if (path === '/sys/fs/cgroup/memory/memory.limit_in_bytes') return 2 ** 62
          return null
        }
      })

      const snap = await api.snapshot()

      expect(snap.cgroup).toBeNull()
    })

    it('returns valid cgroup usage when v1 limit is below sentinel threshold', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.3),
        _readBrowser: stubBrowser(null),
        _readCgroupFile: (path: string) => {
          if (path === '/sys/fs/cgroup/memory/memory.usage_in_bytes') return 500_000_000
          if (path === '/sys/fs/cgroup/memory/memory.limit_in_bytes') return 1_000_000_000
          return null
        }
      })

      const snap = await api.snapshot()

      expect(snap.cgroup).not.toBeNull()
      expect(snap.cgroup?.ratio).toBeCloseTo(0.5, 2)
    })
  })

  // ── Pressure classification ─────────────────────────────────────

  describe('pressure classification', () => {
    const cases: Array<[number, MemoryPressureLevel]> = [
      [0.0, 'low'],
      [0.3, 'low'],
      [0.49, 'low'],
      [0.5, 'moderate'],
      [0.6, 'moderate'],
      [0.69, 'moderate'],
      [0.7, 'high'],
      [0.8, 'high'],
      [0.89, 'high'],
      [0.9, 'critical'],
      [0.95, 'critical'],
      [1.0, 'critical']
    ]

    for (const [ratio, expected] of cases) {
      it(`ratio ${ratio} → ${expected}`, async () => {
        const api = createMemoryAPI({
          page: null,
          _readNode: stubNode(ratio),
          _readBrowser: stubBrowser(null),
          _readCgroup: stubCgroup(null)
        })

        const snap = await api.snapshot()

        expect(snap.pressure).toBe(expected)
      })
    }
  })

  // ── Highest pressure across sources ─────────────────────────────

  describe('highest pressure across sources', () => {
    it('node=low + browser=critical → critical', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.1),
        _readBrowser: stubBrowser(0.95),
        _readCgroup: stubCgroup(null)
      })

      const snap = await api.snapshot()

      expect(snap.pressure).toBe('critical')
    })

    it('node=moderate + cgroup=high → high', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.55),
        _readBrowser: stubBrowser(null),
        _readCgroup: stubCgroup(0.75)
      })

      const snap = await api.snapshot()

      expect(snap.pressure).toBe('high')
    })

    it('all sources present, highest wins', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.3),
        _readBrowser: stubBrowser(0.55),
        _readCgroup: stubCgroup(0.92)
      })

      const snap = await api.snapshot()

      expect(snap.pressure).toBe('critical')
    })
  })

  // ── Custom thresholds ───────────────────────────────────────────

  describe('custom thresholds', () => {
    it('custom thresholds change classification', async () => {
      const api = createMemoryAPI({
        page: null,
        thresholds: { moderate: 0.3, high: 0.5, critical: 0.7 },
        _readNode: stubNode(0.6),
        _readBrowser: stubBrowser(null),
        _readCgroup: stubCgroup(null)
      })

      const snap = await api.snapshot()

      // With custom thresholds, 0.6 is >= 0.5 (high) but < 0.7 (critical)
      expect(snap.pressure).toBe('high')
    })

    it('partial threshold override merges with defaults', async () => {
      const api = createMemoryAPI({
        page: null,
        thresholds: { critical: 0.8 },
        _readNode: stubNode(0.85),
        _readBrowser: stubBrowser(null),
        _readCgroup: stubCgroup(null)
      })

      const snap = await api.snapshot()

      expect(snap.pressure).toBe('critical')
    })

    it('non-monotonic thresholds throw RangeError', () => {
      expect(() =>
        createMemoryAPI({
          page: null,
          thresholds: { moderate: 0.9, high: 0.5, critical: 0.95 }
        })
      ).toThrow(RangeError)
    })

    it('high >= critical throws RangeError', () => {
      expect(() =>
        createMemoryAPI({
          page: null,
          thresholds: { moderate: 0.3, high: 0.8, critical: 0.8 }
        })
      ).toThrow(RangeError)
    })

    it('threshold out of range throws RangeError', () => {
      expect(() =>
        createMemoryAPI({
          page: null,
          thresholds: { moderate: 0 }
        })
      ).toThrow(RangeError)

      expect(() =>
        createMemoryAPI({
          page: null,
          thresholds: { critical: 1.5 }
        })
      ).toThrow(RangeError)
    })
  })

  // ── isAbove() ───────────────────────────────────────────────────

  describe('isAbove()', () => {
    it('returns true when pressure is at the given level', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.75),
        _readBrowser: stubBrowser(null),
        _readCgroup: stubCgroup(null)
      })

      expect(await api.isAbove('high')).toBe(true)
    })

    it('returns true when pressure is above the given level', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.95),
        _readBrowser: stubBrowser(null),
        _readCgroup: stubCgroup(null)
      })

      expect(await api.isAbove('moderate')).toBe(true)
    })

    it('returns false when pressure is below the given level', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.2),
        _readBrowser: stubBrowser(null),
        _readCgroup: stubCgroup(null)
      })

      expect(await api.isAbove('high')).toBe(false)
    })

    it('isAbove("low") is always true', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.01),
        _readBrowser: stubBrowser(null),
        _readCgroup: stubCgroup(null)
      })

      expect(await api.isAbove('low')).toBe(true)
    })
  })

  // ── Snapshot metadata ───────────────────────────────────────────

  describe('snapshot metadata', () => {
    it('includes ISO 8601 UTC timestamp', async () => {
      const api = createMemoryAPI({
        page: null,
        _readNode: stubNode(0.3),
        _readBrowser: stubBrowser(null),
        _readCgroup: stubCgroup(null)
      })

      const snap = await api.snapshot()

      // ISO 8601 format: YYYY-MM-DDTHH:MM:SS.sssZ
      expect(snap.ts).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)
    })
  })
})
