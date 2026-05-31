// Re-export the leaf package so consumers that `@testable import cmux`
// (notably the cmuxTests target) can use ``CellGrid``, ``Cell``,
// ``SurfaceHandle``, etc. without an additional explicit
// `import CmuxTerminalAccess` — which on Xcode 26's stricter module
// dependency scanner trips an "Unable to find module dependency: 'cmux'"
// error when combined with `@testable import cmux` in the same test file.
@_exported import CmuxTerminalAccess
