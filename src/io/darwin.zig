//! Uses Kqueue for darwin
//! Inspired by TigerBeetle darwin.zig and Mitchell libxev kqueue.zig
//! From what I understand, kqueue notifies you of system events,
//! it does not execute syscalls.

const std = @import("std");
const posix = std.posix;
const mem = std.mem;
const assert = std.debug.assert;
const queue = @import("../queue.zig");

// TODO start with sockets only than see how to handle
// files and other io operation

pub const IO = struct {
    /// File descriptor for kqueue
    kq: posix.fd_t,

    /// Number of Io operations that have been submitted to kqueue
    /// These operations are not in any queue
    io_inflight: usize = 0,

    /// Queue that holds completed completions
    completed: queue.Intrusive(Completion) = .{},

    /// Queue that holds pending completions
    io_pending: queue.Intrusive(Completion) = .{},

    // code from tigerbeetle
    // Cannot specify type of error here as this method needs to be same to all
    // other implementations in other architectures
    pub fn init() !IO {
        // Creates kqueue
        const kq = try posix.kqueue();
        assert(kq > -1);

        return IO{ .kq = kq };
    }

    // Code from tigerbeetle
    // TODO probably need to stop and notify all callbacks the event loop ended
    pub fn deinit(self: *IO) void {
        assert(self.kq > -1);
        posix.close(self.kq);
        self.kq = -1;
    }

    // Tigerbeetle code
    /// Pass all queued submissions to the kernel and peek for completions.
    pub fn tick(self: *IO) !void {
        try self.flush();
    }

    // Tigerbeetle code without timeouts
    // TODO ignoring timeouts for now
    fn flush(self: *IO) !void {
        var io_pending = self.io_pending.peek();

        // Saw this both in Tigerbeetle and Libxev
        // Why 256 events?
        // Better number for batching?
        var events: [256]posix.Kevent = undefined;

        const change_events = self.flush_io(&events, &io_pending);

        // Only call kevent() if we need to submit io events or if we need to wait for completions.
        if (change_events > 0 or self.completed.empty()) {
            // Zero timeouts for kevent() implies a non-blocking poll
            var ts = std.mem.zeroes(posix.timespec);

            // We need to wait (not poll) on kevent if there's nothing to submit or complete.
            // We should never wait indefinitely (timeout_ptr = null for kevent) given:
            // - tick() is non-blocking (wait_for_completions = false)
            // - run_for_ns() always submits a timeout
            // TODO timeouts not implemented yet
            // if (change_events == 0 and self.completed.empty()) {
            //     if (wait_for_completions) {
            //         const timeout_ns = next_timeout orelse @panic("kevent() blocking forever");
            //         ts.tv_nsec = @as(@TypeOf(ts.tv_nsec), @intCast(timeout_ns % std.time.ns_per_s));
            //         ts.tv_sec = @as(@TypeOf(ts.tv_sec), @intCast(timeout_ns / std.time.ns_per_s));
            //     } else if (self.io_inflight == 0) {
            //         return;
            //     }
            // }

            // Batch events
            const new_events = try posix.kevent(
                self.kq,
                events[0..change_events],
                events[0..events.len],
                &ts,
            );

            // Mark the io events submitted only after kevent() successfully processed them
            self.io_pending.head = io_pending;
            if (io_pending == null) {
                self.io_pending.tail = null;
            }

            self.io_inflight += change_events;
            self.io_inflight -= new_events;

            for (events[0..new_events]) |event| {
                const completion: *Completion = @ptrFromInt(event.udata);
                completion.next = null;
                self.completed.push(completion);
            }
        }

        var completed = self.completed;
        self.completed.reset();
        while (completed.pop()) |completion| {
            (completion.callback)(self, completion);
        }
    }

    // Tigerbeetle code as well
    /// Creates the events and populates in the events slice
    fn flush_io(_: *IO, events: []posix.Kevent, io_pending_top: *?*Completion) usize {
        for (events, 0..) |*event, flushed| {
            const completion = io_pending_top.* orelse return flushed;
            io_pending_top.* = completion.next;

            const event_info = switch (completion.operation) {
                // Socket operations
                .accept => |op| [2]c_int{ op.socket, posix.system.EVFILT_READ },
                .connect => |op| [2]c_int{ op.socket, posix.system.EVFILT_WRITE },
                .recv => |op| [2]c_int{ op.socket, posix.system.EVFILT_READ },
                .send => |op| [2]c_int{ op.socket, posix.system.EVFILT_WRITE },

                // File operations
                .read => |op| [2]c_int{ op.fd, posix.system.EVFILT_READ },
                .write => |op| [2]c_int{ op.fd, posix.system.EVFILT_WRITE },
                else => @panic("invalid completion operation queued for io"),
            };

            event.* = .{
                .ident = @as(u32, @intCast(event_info[0])),
                .filter = @as(i16, @intCast(event_info[1])),
                // Add to queue, enable it and run once
                .flags = posix.system.EV_ADD | posix.system.EV_ENABLE | posix.system.EV_ONESHOT,
                .fflags = 0,
                .data = 0,
                // User data pointer to send together
                .udata = @intFromPtr(completion),
            };
        }
        return events.len;
    }

    // Tigerbeetle
    /// Completion is passed from caller so that IO can created it for the caller
    fn submit(
        self: *IO,
        context: anytype,
        comptime callback: anytype,
        completion: *Completion,
        comptime operation_tag: std.meta.Tag(Operation),
        operation_data: anytype,
        comptime OperationImpl: type,
    ) void {
        const onCompleteFn = struct {
            fn onComplete(io: *IO, _completion: *Completion) void {
                // Perform the actual operation
                const op_data = &@field(_completion.operation, @tagName(operation_tag));
                const result = OperationImpl.do_operation(op_data);

                // Requeue onto io_pending if error.WouldBlock
                switch (operation_tag) {
                    .accept, .connect, .read, .write, .send, .recv => {
                        _ = result catch |err| switch (err) {
                            error.WouldBlock => {
                                _completion.next = null;
                                io.io_pending.push(_completion);
                                return;
                            },
                            else => {},
                        };
                    },
                    else => {},
                }

                // Complete the Completion

                return callback(
                    @ptrCast(@alignCast(_completion.context)),
                    _completion,
                    result,
                );
            }
        }.onComplete;

        completion.* = .{
            .next = null,
            .context = context,
            .callback = onCompleteFn,
            .operation = @unionInit(Operation, @tagName(operation_tag), operation_data),
        };

        switch (operation_tag) {
            .timeout => self.timeouts.push(completion),
            else => self.completed.push(completion),
        }
    }

    pub fn open_socket(_: *IO, addr: std.net.Address, flags: u32) !posix.socket_t {
        return try posix.socket(addr.any.family, flags, 0);
    }

    pub fn close_socket(self: *IO, socket: posix.socket_t) void {
        _ = self;
        posix.close(socket);
    }

    pub const AcceptError = posix.AcceptError || posix.SetSockOptError;

    // Tigerbeetle accept
    pub fn accept(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: AcceptError!posix.socket_t,
        ) void,
        completion: *Completion,
        socket: posix.socket_t,
    ) void {
        self.submit(
            context,
            callback,
            completion,
            .accept,
            .{
                .socket = socket,
            },
            struct {
                fn do_operation(op: anytype) AcceptError!posix.socket_t {
                    const fd = try posix.accept(
                        op.socket,
                        null,
                        null,
                        posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC,
                    );
                    errdefer posix.close(fd);

                    // Darwin doesn't support posix.MSG_NOSIGNAL to avoid getting SIGPIPE on
                    // socket send(). Instead, it uses the SO_NOSIGPIPE socket option which does
                    // the same for all send()s.
                    posix.setsockopt(
                        fd,
                        posix.SOL.SOCKET,
                        posix.SO.NOSIGPIPE,
                        &mem.toBytes(@as(c_int, 1)),
                    ) catch |err| return switch (err) {
                        error.TimeoutTooBig => unreachable,
                        error.PermissionDenied => error.NetworkSubsystemFailed,
                        error.AlreadyConnected => error.NetworkSubsystemFailed,
                        error.InvalidProtocolOption => error.ProtocolFailure,
                        else => |e| e,
                    };

                    return fd;
                }
            },
        );
    }

    /// This struct holds the data needed for a single IO operation
    pub const Completion = struct {
        next: ?*Completion,
        context: ?*anyopaque,
        callback: *const fn (*IO, *Completion) void,
        operation: Operation,
    };

    const Operation = union(enum) {
        accept: struct {
            socket: posix.socket_t,
        },
        close: struct {
            fd: posix.fd_t,
        },
        connect: struct {
            socket: posix.socket_t,
            address: std.net.Address,
            initiated: bool,
        },
        fsync: struct {
            fd: posix.fd_t,
        },
        read: struct {
            fd: posix.fd_t,
            buf: [*]u8,
            len: u32,
            offset: u64,
        },
        recv: struct {
            socket: posix.socket_t,
            buf: [*]u8,
            len: u32,
        },
        send: struct {
            socket: posix.socket_t,
            buf: [*]const u8,
            len: u32,
        },
        timeout: struct {
            expires: u64,
        },
        write: struct {
            fd: posix.fd_t,
            buf: [*]const u8,
            len: u32,
            offset: u64,
        },
    };
};
