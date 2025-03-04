const std = @import("std");
const mem = std.mem;
const IO = @import("io.zig").IO;
const Server = @import("http/server.zig").Server;
const log = std.log.scoped(.main);

pub fn main() !void {
    var io = try IO.init();
    defer io.deinit();

    const addr = try std.net.Address.parseIp4("127.0.0.1", 5000);

    log.info("Server Address {}", .{addr});

    const gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer gpa.deinit();

    var server = try Server.init(allocator, &io, addr);
    defer server.deinit();

    try server.start();

    return;
}
