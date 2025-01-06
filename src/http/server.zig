const std = @import("std");
const IO = @import("../io.zig").IO;
const posix = std.posix;
const assert = std.debug.assert;
const TCP = @import("../socket/tcp.zig").TCP;
const log = std.log.scoped(.server);
const Time = @import("../time.zig").Time;

pub const Server = struct {
    const Self = @This();

    io: *IO,
    tcp: *TCP,
    time: Time = .{},

    /// Accept Completion that stores the results of the callback when accepting
    /// a new connection
    accept_completion: IO.Completion = undefined,

    pub fn init(io: *IO, addr: std.net.Address) !Self {
        log.debug("Init server at address {}", .{addr});

        var tcp = try TCP.init(io, addr);

        return .{ .io = io, .tcp = &tcp };
    }

    pub fn deinit(
        self: *Self,
    ) void {
        self.tcp.deinit();
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
};
