const std = @import("std");
const posix = std.posix;
const builtin = @import("builtin");

// const IO_Linux = @import("io/linux.zig").IO;
const IO_Darwin = @import("io/darwin.zig").IO;
// const IO_Windows = @import("io/windows.zig").IO;

pub const IO = switch (builtin.target.os.tag) {
    // .linux => IO_Linux,
    // .windows => IO_Windows,
    .macos, .tvos, .watchos, .ios => IO_Darwin,
    else => @compileError("IO is not supported for platform"),
};

