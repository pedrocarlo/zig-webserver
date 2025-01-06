const std = @import("std");
const mem = std.mem;
const IO = @import("../io.zig").IO;
const posix = std.posix;
const assert = std.debug.assert;
const TCP = @import("../socket/tcp.zig").TCP;
const log = std.log.scoped(.server);
const Time = @import("../time.zig").Time;
const RingBufferType = @import("../ring_buffer.zig").RingBufferType;
const stdx = @import("../stdx.zig");

pub const Server = struct {
    const Self = @This();

    allocator: mem.Allocator,
    io: *IO,
    tcp: *TCP,
    time: Time = .{},

    /// This slice is allocated with a fixed size in the init function and never reallocated.
    connections: []Connection,
    /// Number of connections currently in use (i.e. connection.peer != .none).
    connections_used: usize = 0,

    /// Accept Completion that stores the results of the callback when accepting
    /// a new connection
    accept_completion: IO.Completion = undefined,

    pub fn init(allocator: mem.Allocator, io: *IO, addr: std.net.Address) !Self {
        log.debug("Init server at address {}", .{addr});

        var tcp = try TCP.init(io, addr);

        // TODO arbitrary connection max number
        const connections = try allocator.alloc(Connection, 10);
        errdefer allocator.free(connections);
        @memset(connections, .{});

        return .{ .allocator = allocator, .io = io, .tcp = &tcp, .connections = connections };
    }

    pub fn deinit(
        self: *Self,
    ) void {
        self.tcp.deinit();
        self.allocator.free(self.connections);
    }

    pub fn start(self: *Self) !void {
        self.accept();
        while (true) {
            // TODO should catch the error and log not return
            try self.io.tick();
        }
    }

    fn accept(
        self: *Self,
    ) void {
        self.tcp.accept(*Server, self, Server.on_accept, &self.accept_completion);
    }

    fn on_accept(
        self: *Self,
        completion: *IO.Completion,
        result: IO.AcceptError!posix.socket_t,
    ) void {
        _ = completion;
        _ = self;

        // assert(self.accept_connection != null);
        // Sinalize that server can accept a new connection
        // defer self.accept_connection = null;
        const fd = result catch |err| {
            // TODO: some errors should probably be fatal
            log.warn("accept failed: {}", .{err});
            return;
        };

        log.debug("accept succeed. socket file descriptor: {}", .{fd});

        // TODO do something with connection
    }

    const SendQueue = RingBufferType(*[]u8, .{
        // TODO for now arbitrary number
        .array = 2,
    });

    // Tigerbeetle
    /// Used to send/receive messages to/from a client or fellow replica.
    const Connection = struct {
        state: enum {
            /// The connection is not in use, with peer set to `.none`.
            free,
            /// The connection has been reserved for an in progress accept operation,
            /// with peer set to `.none`.
            accepting,
            /// The peer is a replica and a connect operation has been started
            /// but not yet completed.
            connecting,
            /// The peer is fully connected and may be a client, replica, or unknown.
            connected,
            /// The connection is being terminated but cleanup has not yet finished.
            terminating,
        } = .free,
        /// This is guaranteed to be valid only while state is connected.
        /// It will be reset to IO.INVALID_SOCKET during the shutdown process and is always
        /// IO.INVALID_SOCKET if the connection is unused (i.e. peer == .none). We use
        /// IO.INVALID_SOCKET instead of undefined here for safety to ensure an error if the
        /// invalid value is ever used, instead of potentially performing an action on an
        /// active fd.
        fd: posix.socket_t = IO.INVALID_SOCKET,

        /// This completion is used for all recv operations.
        /// It is also used for the initial connect when establishing a replica connection.
        recv_completion: IO.Completion = undefined,
        /// True exactly when the recv_completion has been submitted to the IO abstraction
        /// but the callback has not yet been run.
        recv_submitted: bool = false,
        /// The Message with the buffer passed to the kernel for recv operations.
        recv_message: ?*[]u8 = null,
        /// The number of bytes in `recv_message` that have been received and need parsing.
        recv_progress: usize = 0,
        /// The number of bytes in `recv_message` that have been parsed.
        recv_parsed: usize = 0,
        /// True if we have already checked the header checksum of the message we
        /// are currently receiving/parsing.
        recv_checked_header: bool = false,

        /// This completion is used for all send operations.
        send_completion: IO.Completion = undefined,
        /// True exactly when the send_completion has been submitted to the IO abstraction
        /// but the callback has not yet been run.
        send_submitted: bool = false,
        /// Number of bytes of the current message that have already been sent.
        send_progress: usize = 0,
        /// The queue of messages to send to the client or replica peer.
        send_queue: SendQueue = SendQueue.init(),

        /// Given a newly accepted fd, start receiving messages on it.
        /// Callbacks will be continuously re-registered until terminate() is called.
        pub fn on_accept(connection: *Connection, server: *Server, fd: posix.socket_t) void {
            assert(connection.peer == .none);
            assert(connection.state == .accepting);
            assert(connection.fd == IO.INVALID_SOCKET);

            connection.state = .connected;
            connection.fd = fd;
            server.connections_used += 1;

            connection.assert_recv_send_initial_state(server);
            connection.get_recv_message_and_recv(server);
            assert(connection.send_queue.empty());
        }

        fn assert_recv_send_initial_state(connection: *Connection, bus: *Server) void {
            assert(bus.connections_used > 0);

            assert(connection.state == .connected);
            assert(connection.fd != IO.INVALID_SOCKET);

            assert(connection.recv_submitted == false);
            assert(connection.recv_message == null);
            assert(connection.recv_progress == 0);
            assert(connection.recv_parsed == 0);

            assert(connection.send_submitted == false);
            assert(connection.send_progress == 0);
        }

        /// Acquires a free message if necessary and then calls `recv()`.
        /// If the connection has a `recv_message` and the message being parsed is
        /// at pole position then calls `recv()` immediately, otherwise copies any
        /// partially received message into a new Message and sets `recv_message`,
        /// releasing the old one.
        fn get_recv_message_and_recv(connection: *Connection, server: *Server) void {
            connection.recv(server);
        }

        fn recv(connection: *Connection, server: *Server) void {
            assert(connection.peer != .none);
            assert(connection.state == .connected);
            assert(connection.fd != IO.INVALID_SOCKET);

            assert(!connection.recv_submitted);
            connection.recv_submitted = true;

            // assert(connection.recv_progress < constants.message_size_max);
            const message_size_max: comptime_int = 2E+6;

            server.io.recv(
                *Server,
                server,
                on_recv,
                &connection.recv_completion,
                connection.fd,
                connection.recv_message.?,
                .buffer[connection.recv_progress..message_size_max],
            );
        }

        fn on_recv(
            server: *Server,
            completion: *IO.Completion,
            result: IO.RecvError!usize,
        ) void {
            const connection: *Connection = @alignCast(
                @fieldParentPtr("recv_completion", completion),
            );
            assert(connection.recv_submitted);
            connection.recv_submitted = false;
            if (connection.state == .terminating) {
                connection.maybe_close(server);
                return;
            }
            assert(connection.state == .connected);
            const bytes_received = result catch |err| {
                // TODO: maybe don't need to close on *every* error
                log.warn("error receiving from {}: {}", .{ connection.peer, err });
                connection.terminate(server, .shutdown);
                return;
            };
            // No bytes received means that the peer closed its side of the connection.
            if (bytes_received == 0) {
                log.info("peer performed an orderly shutdown: {}", .{connection.peer});
                connection.terminate(server, .close);
                return;
            }
            connection.recv_progress += bytes_received;
            connection.parse_messages(server);
        }
    };
};
