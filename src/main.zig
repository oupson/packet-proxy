const std = @import("std");
const IO_Uring = std.os.linux.IO_Uring;
const iovec = std.os.iovec;

const Config = struct {
    endpoints: []const EndPointConfig,
};

const EndPointConfig = struct {
    type: EndPointType,
    from: []const u8,
    to: []const u8,
};

const EndPointType = enum {
    udp,
    tcp,
};

const QUEUE_DEPTH = 64;
const BUFFER_SIZE = 512;

const UserData = union(UserDataType) {
    no_data: void,
    accept: std.net.Address,
    open: struct {
        from_fd: i32,
        to_fd: i32,
    },
    splice_in: struct {
        from_fd: i32,
        to_fd: i32,
        pipes: [2]std.os.fd_t,
    },
};

const UserDataType = enum {
    no_data,
    accept,
    open,
    splice_in,
};

const ENTRY_SIZE = 10;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var config_file = try std.fs.cwd().openFile("config.json", .{});
    defer config_file.close();

    var reader = std.json.reader(allocator, config_file.reader());
    defer reader.deinit();

    const config_parsed = try std.json.parseFromTokenSource(Config, allocator, &reader, .{});
    defer config_parsed.deinit();

    const config = config_parsed.value;

    std.log.info("{any}", .{config});

    var ring = try IO_Uring.init(QUEUE_DEPTH, 0);
    defer ring.deinit();

    var iov = try allocator.alloc(iovec, ENTRY_SIZE * 2);
    defer allocator.free(iov);
    for (0..ENTRY_SIZE * 2) |i| {
        var buffer = try allocator.alloc(u8, BUFFER_SIZE);
        iov[i].iov_base = buffer.ptr;
        iov[i].iov_len = BUFFER_SIZE;
    }

    defer {
        for (0..ENTRY_SIZE * 2) |i| {
            allocator.free(iov[i].iov_base[0..iov[i].iov_len]);
        }
    }

    try ring.register_buffers(iov);

    for (config.endpoints, 0..) |endpoint, i| {
        std.log.debug("{} : {any}", .{ i, endpoint });

        const address = try getAddress(allocator, endpoint.from);
        const to_address = try getAddress(allocator, endpoint.to);

        if (endpoint.type == .tcp) {
            var socket = try std.os.socket(address.any.family, std.os.SOCK.STREAM, 0);
            try std.os.bind(socket, &address.any, address.getOsSockLen());

            try std.os.listen(socket, 10);

            const data = try allocator.create(UserData);
            data.* = UserData{ .accept = to_address };

            std.log.info("{}", .{@as(u64, @intFromPtr(data))});

            var sqe = try ring.accept(@as(u64, @intFromPtr(data)), socket, null, null, 0);
            sqe.ioprio |= std.os.linux.IORING_ACCEPT_MULTISHOT;
        } else {
            std.log.warn("unsupported udp", .{});
        }
    }

    var entry: u16 = 0;
    _ = try ring.submit();

    while (true) {
        const count = ring.cq_ready();
        std.log.debug("ready {}", .{count});

        if (count > 0) {
            var head = ring.cq.head.*;
            var tail = head +% count;
            while (head != tail) {
                var cqe = &ring.cq.cqes[head & ring.cq.mask];
                head +%= 1;

                const data: *UserData = @ptrFromInt(cqe.user_data);

                std.log.info("{}", .{cqe});
                if (cqe.res < 0) {
                    std.log.err("async request failed : {}, {}", .{ data, cqe.err() });
                    if (cqe.err() == .CANCELED) {} else {
                        return error.asyncRequestFailed;
                    }
                }

                std.log.info("{}", .{cqe});
                std.log.info("{}", .{data});

                switch (data.*) {
                    UserData.no_data => {
                        unreachable;
                    },
                    UserData.accept => |address| {
                        std.log.debug("connecting to {}", .{address});
                        const socketfd = try std.os.socket(address.any.family, std.os.SOCK.STREAM, 0);

                        const user_data = try allocator.create(UserData);
                        user_data.* = UserData{ .open = .{ .from_fd = cqe.res, .to_fd = socketfd } };

                        _ = try ring.connect(@as(u64, @intFromPtr(user_data)), socketfd, &address.any, address.getOsSockLen());
                    },
                    UserData.open => |d| {
                        data.* = UserData{ .splice_in = .{ .from_fd = d.from_fd, .to_fd = d.to_fd, .pipes = try std.os.pipe() } };

                        const user_data_to = try allocator.create(UserData);
                        user_data_to.* = UserData{ .splice_in = .{ .from_fd = d.to_fd, .to_fd = d.from_fd, .pipes = try std.os.pipe() } };

                        _ = std.os.linux.fcntl(data.splice_in.pipes[0], 1031, 4096);
                        _ = try prepSplice(&ring, d.from_fd, -1, data.splice_in.pipes[1], -1, 4096, data);

                        _ = std.os.linux.fcntl(user_data_to.splice_in.pipes[0], 1031, 4096);
                        _ = try prepSplice(&ring, d.to_fd, -1, user_data_to.splice_in.pipes[1], -1, 4096, user_data_to);

                        entry += 1;
                    },
                    UserData.splice_in => |d| {
                        const size = cqe.res;

                        if (size == 0) {
                            std.log.debug("connection closed", .{});
                        } else {
                            var sqe = try prepSplice(&ring, d.pipes[0], -1, d.to_fd, -1, @intCast(size), data);
                            sqe.flags |= std.os.linux.IOSQE_IO_LINK | std.os.linux.IOSQE_CQE_SKIP_SUCCESS;
                            _ = try prepSplice(&ring, d.from_fd, -1, d.pipes[1], -1, 4096, data);
                        }
                    },
                }
            }
            ring.cq_advance(count);
            _ = try ring.submit();
        } else {
            std.log.debug("waiting ...", .{});
            _ = try ring.enter(0, 1, std.os.linux.IORING_ENTER_GETEVENTS);
        }
    }
}

fn getAddress(allocator: std.mem.Allocator, hostAndPort: []const u8) !std.net.Address {
    var addr_split = std.mem.splitAny(u8, hostAndPort, ":");
    const host = addr_split.next().?;
    const port = try std.fmt.parseUnsigned(u16, addr_split.next().?, 10);
    var addr_list = try std.net.getAddressList(allocator, host, port);
    const address = addr_list.addrs[0];
    addr_list.deinit();
    return address;
}

fn prepSplice(ring: *IO_Uring, from_fd: i32, from_offset: i64, to_fd: i32, to_offset: i64, len: u64, data: anytype) !*std.os.linux.io_uring_sqe {
    var sqe = try ring.get_sqe();
    std.os.linux.io_uring_prep_rw(.SPLICE, sqe, to_fd, undefined, len, @bitCast(to_offset));
    sqe.splice_fd_in = from_fd;
    sqe.addr = @bitCast(from_offset); // splice_off_in
    //  sqe.rw_flags = SPLICE_F_MORE;
    sqe.user_data = @as(u64, @intFromPtr(data));

    return sqe;
}
