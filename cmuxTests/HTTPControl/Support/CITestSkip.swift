import Foundation

/// Returns `true` when the HTTP integration suites should be skipped.
///
/// On github-hosted macOS runners the cmux DEV app's xctest host
/// saturates the libdispatch global queue and the DispatchIO read
/// callbacks for the loopback HTTP server fire 30+ seconds after the
/// TCP accept — by which time the test client has already timed out
/// and the test scope has deallocated the server. These tests pass on
/// the user's dev box and on warp.dev runners; only github-hosted
/// macos-latest reliably hits the contention.
///
/// CI sets `CMUX_SKIP_HTTP_TCP_INTEGRATION=1` for github-hosted
/// runs; locally the env var is unset and the tests run normally.
enum CITestSkip {
    /// True when the heavy TCP/loopback HTTP integration suites
    /// should be marked disabled for the current environment.
    static var httpTCPIntegrationDisabled: Bool {
        ProcessInfo.processInfo.environment["CMUX_SKIP_HTTP_TCP_INTEGRATION"] != nil
    }
}
