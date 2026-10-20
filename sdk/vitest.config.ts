import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    // Include all test files
    include: ['test/**/*.test.ts'],

    // Exclude type-only tests (run separately via `vitest run --typecheck.only`)
    exclude: ['test/**/*.test-d.ts', 'node_modules'],

    // Type-level contract tests: compile-time expectTypeOf assertions
    typecheck: {
      tsconfig: './tsconfig.typecheck.json',
      include: ['test/**/*.test-d.ts']
    },

    // Test isolation
    isolate: true,

    // Timeouts
    testTimeout: 10000,
    hookTimeout: 10000,

    // Coverage configuration
    coverage: {
      provider: 'v8',
      reporter: ['text', 'html'],
      reportsDirectory: './coverage',
      include: ['src/**/*.ts'],
      exclude: ['src/**/*.d.ts', 'src/index.ts']
    }
  }
})
