import Darwin
import Foundation

/// AF_INET HTTP listener bound to 127.0.0.1.
///
/// Mirrors ``HTTPControlUDSListener`` for the TCP transport: raw POSIX
/// `socket(2)` / `bind(2)` / `listen(2)` rather than ``NWListener``. The
/// POSIX path avoids subtle Network.framework state-machine issues
/// (e.g. listeners silently never reaching `.ready` on the
/// github-hosted macOS runner family) and gives us the same accept-loop
/// shape we already use for the UDS path. Same dispatch source and
/// `onAccept(fd)` callback contract; the owning ``HTTPControlServer``
/// runs ``acceptRawFD(_:)`` on the accepted descriptor.
final class HTTPControlTCPListener: @unchecked Sendable {
    /// Port the listener bound to. Resolved during ``start()`` —
    /// pass `0` to request an ephemeral port and read the resolved
    /// value from this property after ``start()`` returns.
    private(set) var port: UInt16 = 0

    private var fd: Int32 = -1
    private var source: DispatchSourceRead?
    private let queue: DispatchQueue
    private let requestedPort: UInt16
    private let onAccept: (Int32) -> Void
    private let lock = NSLock()

    /// Build a listener bound to `127.0.0.1:<port>`.
    ///
    /// - Parameters:
    ///   - port: TCP port to bind. Pass `0` for an ephemeral port
    ///     (the resolved value will be available via ``port`` after
    ///     ``start()`` returns).
    ///   - queue: Dispatch queue the accept handler runs on.
    ///   - onAccept: Closure invoked with each accepted client fd.
    ///     Ownership of `fd` transfers to the closure — it MUST
    ///     eventually `close()` the descriptor.
    init(
        port: UInt16,
        queue: DispatchQueue,
        onAccept: @escaping (Int32) -> Void
    ) {
        self.requestedPort = port
        self.queue = queue
        self.onAccept = onAccept
    }

    /// Binds the socket to 127.0.0.1:port and begins accepting clients.
    func start() throws {
        let s = socket(AF_INET, SOCK_STREAM, 0)
        guard s >= 0 else {
            throw HTTPControlTCPListenerError.socketFailed(errno)
        }
        var yes: Int32 = 1
        _ = setsockopt(
            s, SOL_SOCKET, SO_REUSEADDR, &yes, socklen_t(MemoryLayout<Int32>.size)
        )

        var addr = sockaddr_in()
        memset(&addr, 0, MemoryLayout<sockaddr_in>.size)
        addr.sin_family = sa_family_t(AF_INET)
        addr.sin_port = in_port_t(requestedPort).bigEndian
        // 127.0.0.1 in network byte order.
        addr.sin_addr.s_addr = inet_addr("127.0.0.1")

        let bindRC = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                bind(s, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard bindRC == 0 else {
            let e = errno
            close(s)
            throw HTTPControlTCPListenerError.bindFailed(e)
        }
        guard listen(s, 16) == 0 else {
            let e = errno
            close(s)
            throw HTTPControlTCPListenerError.listenFailed(e)
        }

        // Resolve the actual bound port (may differ from requestedPort
        // when 0 was passed).
        var bound = sockaddr_in()
        memset(&bound, 0, MemoryLayout<sockaddr_in>.size)
        var blen = socklen_t(MemoryLayout<sockaddr_in>.size)
        let nameRC = withUnsafeMutablePointer(to: &bound) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                getsockname(s, $0, &blen)
            }
        }
        guard nameRC == 0 else {
            let e = errno
            close(s)
            throw HTTPControlTCPListenerError.getsocknameFailed(e)
        }
        let resolvedPort = UInt16(bigEndian: bound.sin_port)

        let src = DispatchSource.makeReadSource(fileDescriptor: s, queue: queue)
        src.setEventHandler { [weak self] in
            guard let self else { return }
            while true {
                var caddr = sockaddr_in()
                memset(&caddr, 0, MemoryLayout<sockaddr_in>.size)
                var clen = socklen_t(MemoryLayout<sockaddr_in>.size)
                let cfd = withUnsafeMutablePointer(to: &caddr) {
                    $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                        accept(self.fd, $0, &clen)
                    }
                }
                if cfd < 0 {
                    break
                }
                self.onAccept(cfd)
            }
        }
        src.resume()
        lock.lock()
        self.fd = s
        self.source = src
        self.port = resolvedPort
        lock.unlock()
    }

    /// Cancels the accept source and closes the listener fd.
    func stop() {
        lock.lock()
        let s = source
        let f = fd
        source = nil
        fd = -1
        lock.unlock()
        s?.cancel()
        if f >= 0 { close(f) }
    }
}

/// Failure modes for ``HTTPControlTCPListener/start()``.
enum HTTPControlTCPListenerError: Error {
    case socketFailed(Int32)
    case bindFailed(Int32)
    case listenFailed(Int32)
    case getsocknameFailed(Int32)
}
