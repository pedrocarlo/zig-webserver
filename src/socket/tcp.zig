const std = @import("std");
const IO = @import("../io.zig").IO;
const posix = std.posix;
const log = std.log.scoped(.tcp);

/// Non blocking tcp client
pub const TCP = struct {
    const Self = @This();

    io: *IO,
    fd: posix.socket_t,
    addr: std.net.Address,

    /// Init with ip to create socket for
    pub fn init(io: *IO, addr: std.net.Address) !Self {
        log.debug("Init tcp at address {}", .{addr});

        // Flags from libxev
        const flags = flags: {
            const flags: u32 = posix.SOCK.STREAM | posix.SOCK.CLOEXEC;
            // TODO did not get to linux io_uring yet, so not using this
            // if (xev.backend != .io_uring) flags |= posix.SOCK.NONBLOCK;
            break :flags flags;
        };

        // Domain here is specified when address is created with std.net.Address
        // Protocol is 0 as it should be inferred by socket flags and family
        const fd = try io.open_socket(addr, flags);
        errdefer io.close_socket(fd);

        // OPT with value 1 means enable
        // posix.SO.REUSEADDR allows the port to be rebinded when closed
        // so we dont have to wait to rebind
        // https://stackoverflow.com/questions/3229860/what-is-the-meaning-of-so-reuseaddr-setsockopt-option-linux
        try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
        try posix.bind(fd, &addr.any, addr.getOsSockLen());

        // TODO for now arbitrary backlog value for tcp connections
        try posix.listen(fd, 10);

        return .{ .fd = fd, .io = io, .addr = addr };
    }

    pub fn deinit(self: Self) void {
        self.io.close_socket(self.fd);
    }

    /// Just a wrapper for the underlying IO struct accept call
    pub fn accept(self: *Self, comptime Context: anytype, context: Context, callback: fn (
        context: Context,
        completion: *IO.Completion,
        result: IO.AcceptError!posix.socket_t,
    ) void, accept_completion: *IO.Completion) void {
        self.io.accept(Context, context, callback, accept_completion, self.fd);
    }

    pub fn recv(self: *Self, comptime Context: anytype, context: Context, callback: fn (
        context: Context,
        completion: *IO.Completion,
        result: IO.RecvError!posix.socket_t,
    ) void, recv_completion: *IO.Completion, buffer: []u8) void {
        self.io.recv(Context, context, callback, recv_completion, self.fd, buffer);
    }
};
