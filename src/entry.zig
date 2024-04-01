const std = @import("std");

const Allocator = std.mem.Allocator;

pub fn entryList(comptime E: type) type {
    return struct {
        allocator: Allocator,
        entries: []?E,
        free_index: ?usize,

        const Self = @This();

        pub fn init(allocator: Allocator, size: usize) !Self {
            const entries = try allocator.alloc(?E, size);
            for (0..size) |i| {
                entries[i] = null;
            }

            return Self{
                .allocator = allocator,
                .entries = entries,
                .free_index = 0,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.entries);
        }

        pub fn insert(self: *Self, entry: E) !usize {
            var entry_index: usize = 0;
            if (self.free_index) |e| {
                entry_index = e;
            } else {
                for (0..self.entries.len) |i| {
                    if (self.entries[i] == null) {
                        entry_index = i;
                        break;
                    }
                }
                return error.full;
            }

            self.entries[entry_index] = entry;

            if (entry_index + 1 < self.entries.len and self.entries[entry_index + 1] == null) {
                self.free_index = entry_index + 1;
            } else {
                self.free_index = null;
            }

            return entry_index;
        }

        pub fn remove(self: *Self, index: usize) void {
            self.entries[index] = null;
            self.free_index = index;
        }

        pub fn getPtr(self: *Self, index: usize) !*E {
            if (self.entries[index]) |*e| {
                return e;
            } else {
                return error.noEntry;
            }
        }
    };
}

test "test insert 4 elements should not fail" {
    const allocator = std.testing.allocator;
    const expect = std.testing.expect;

    var entry_list = try entryList(usize).init(allocator, 4);
    defer entry_list.deinit();

    for (0..4) |i| {
        _ = try entry_list.insert(i);
    }

    for (0..4) |i| {
        try expect((try entry_list.getPtr(i)).* == i);
    }
}

test "test insert when full should fail" {
    const allocator = std.testing.allocator;

    var entry_list = try entryList(usize).init(allocator, 4);
    defer entry_list.deinit();

    for (0..4) |i| {
        _ = try entry_list.insert(i);
    }

    const r = entry_list.insert(5);
    try std.testing.expectError(error.full, r);
}

test "test insert after removing should not fail" {
    const allocator = std.testing.allocator;
    const expect = std.testing.expect;

    var entry_list = try entryList(usize).init(allocator, 4);
    defer entry_list.deinit();

    const e1 = try entry_list.insert(1);
    const e2 = try entry_list.insert(2);
    const e3 = try entry_list.insert(3);
    const e4 = try entry_list.insert(4);

    entry_list.remove(e2);

    const inserted_id = try entry_list.insert(5);
    try expect((try entry_list.getPtr(e1)).* == 1);
    try expect((try entry_list.getPtr(inserted_id)).* == 5);
    try expect((try entry_list.getPtr(e3)).* == 3);
    try expect((try entry_list.getPtr(e4)).* == 4);
}

test "test getPtr should return ptr to element" {
    const allocator = std.testing.allocator;
    const expect = std.testing.expect;

    var entry_list = try entryList(usize).init(allocator, 4);
    defer entry_list.deinit();

    const e1 = try entry_list.insert(1);
    const e2 = try entry_list.insert(2);
    const e3 = try entry_list.insert(3);
    const e4 = try entry_list.insert(4);

    const ptr = try entry_list.getPtr(e2);
    ptr.* = 5;

    try expect((try entry_list.getPtr(e1)).* == 1);
    try expect((try entry_list.getPtr(e2)).* == 5);
    try expect((try entry_list.getPtr(e3)).* == 3);
    try expect((try entry_list.getPtr(e4)).* == 4);
}
