import Darwin
import Foundation

/// Minimal synchronous loopback HTTP client used by HTTPControl tests.
///
/// Sends one raw request through a raw POSIX `socket(AF_INET, SOCK_STREAM)`
/// connected to `127.0.0.1:<port>` and concatenates the full response
/// (including the status line and headers) into a single `String`.
/// Tests then `contains(...)` against the response to assert wire-level
/// expectations like the status code or `Allow:` header without
/// committing to a full HTTP-parsing helper.
///
/// Uses POSIX sockets rather than `NWConnection` to match the server's
/// POSIX listener (``HTTPControlTCPListener``) — `NWConnection` on the
/// github-hosted macOS runner family was observed to fail loopback
/// `connectx()` with `EADDRNOTAVAIL` even against a known-good
/// listener, so we sidestep Network.framework for the client side too.
enum LoopbackHTTPClient {
    static func send(
        port: UInt16,
        raw: String,
        timeout: TimeInterval = 4
    ) throws -> String {
        let fd = socket(AF_INET, SOCK_STREAM, 0)
        guard fd >= 0 else {
            throw LoopbackHTTPClientError.socketFailed(errno)
        }
        defer { close(fd) }

        var addr = sockaddr_in()
        memset(&addr, 0, MemoryLayout<sockaddr_in>.size)
        addr.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        addr.sin_family = sa_family_t(AF_INET)
        addr.sin_port = in_port_t(port).bigEndian
        var loopback = in_addr()
        guard inet_pton(AF_INET, "127.0.0.1", &loopback) == 1 else {
            throw LoopbackHTTPClientError.addressParseFailed
        }
        addr.sin_addr = loopback

        let connectRC = withUnsafePointer(to: &addr) { ptr in
            ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sa in
                Darwin.connect(
                    fd, sa, socklen_t(MemoryLayout<sockaddr_in>.size)
                )
            }
        }
        guard connectRC == 0 else {
            throw LoopbackHTTPClientError.connectFailed(errno, port: port)
        }

        // Set send/recv timeouts so a hung server doesn't block the
        // test indefinitely.
        var tv = timeval(
            tv_sec: Int(timeout),
            tv_usec: Int32((timeout - Double(Int(timeout))) * 1_000_000)
        )
        _ = setsockopt(
            fd, SOL_SOCKET, SO_RCVTIMEO,
            &tv, socklen_t(MemoryLayout<timeval>.size)
        )
        _ = setsockopt(
            fd, SOL_SOCKET, SO_SNDTIMEO,
            &tv, socklen_t(MemoryLayout<timeval>.size)
        )

        let reqBytes = Array(raw.utf8)
        let sent = reqBytes.withUnsafeBufferPointer { buf -> Int in
            Darwin.send(fd, buf.baseAddress!, buf.count, 0)
        }
        guard sent == reqBytes.count else {
            throw LoopbackHTTPClientError.sendShort(
                sent: sent, expected: reqBytes.count
            )
        }

        var received = Data()
        var buf = [UInt8](repeating: 0, count: 16 * 1024)
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            let n = buf.withUnsafeMutableBufferPointer { p -> Int in
                Darwin.recv(fd, p.baseAddress!, p.count, 0)
            }
            if n <= 0 { break }
            received.append(contentsOf: buf.prefix(n))
        }
        return String(decoding: received, as: UTF8.self)
    }
}

/// Failure modes for ``LoopbackHTTPClient/send(port:raw:timeout:)``.
enum LoopbackHTTPClientError: Error {
    case socketFailed(Int32)
    case addressParseFailed
    case connectFailed(Int32, port: UInt16)
    case sendShort(sent: Int, expected: Int)
}
