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
/// Detection uses a sentinel file at
/// `/tmp/cmux-ci-flags/skip-http-tcp-integration` rather than an env
/// var because env vars don't reliably propagate from xcodebuild into
/// the launched test runner process. CI touches that file before
/// invoking xcodebuild; locally the file is absent and the tests
/// run normally.
enum CITestSkip {
    /// True when the heavy TCP/loopback HTTP integration suites
    /// should be marked disabled for the current environment.
    static var httpTCPIntegrationDisabled: Bool {
        FileManager.default.fileExists(
            atPath: "/tmp/cmux-ci-flags/skip-http-tcp-integration"
        )
    }
}
