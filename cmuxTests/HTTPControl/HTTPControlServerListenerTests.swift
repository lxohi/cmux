import Darwin
import Foundation
import Testing
@testable import cmux

@Suite struct HTTPControlServerListenerTests {
    @Test func startsOnEphemeralPortAndCloses() throws {
        let server = HTTPControlServer(
            routeTable: RouteTable(),
            auth: HTTPAuth(expectedToken: "t"),
            hostAllowlistFor: { p in HostAllowlist(port: Int(p)) }
        )
        let port = try server.startTCP(port: 0)
        #expect(port > 0)
        server.stop()
    }

    @Test func boundPortIsLoopbackReachable() throws {
        let server = HTTPControlServer(
            routeTable: RouteTable(),
            auth: HTTPAuth(expectedToken: "t"),
            hostAllowlistFor: { p in HostAllowlist(port: Int(p)) }
        )
        let port = try server.startTCP(port: 0)
        defer { server.stop() }
        #expect(port > 0, "startTCP should resolve a non-zero port")

        // Reachability check via a raw POSIX connect — matches the
        // server's POSIX listener and avoids Network.framework
        // client-side issues seen on github-hosted runners.
        let fd = socket(AF_INET, SOCK_STREAM, 0)
        #expect(fd >= 0)
        defer { close(fd) }
        var addr = sockaddr_in()
        memset(&addr, 0, MemoryLayout<sockaddr_in>.size)
        addr.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        addr.sin_family = sa_family_t(AF_INET)
        addr.sin_port = in_port_t(port).bigEndian
        var loopback = in_addr()
        #expect(inet_pton(AF_INET, "127.0.0.1", &loopback) == 1)
        addr.sin_addr = loopback
        let rc = withUnsafePointer(to: &addr) { ptr in
            ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) { sa in
                Darwin.connect(
                    fd, sa, socklen_t(MemoryLayout<sockaddr_in>.size)
                )
            }
        }
        #expect(rc == 0, "connect to 127.0.0.1:\(port) failed (errno=\(errno))")
    }
}
