# Zig library for working with msgpack messages

Example usage:

```zig
const std = @import("std");
const msgpack = @import("msgpack");

const Message = struct {
    name: []const u8,
    age: u8,
};

var buffer = std.ArrayList(u8).init(allocator);
defer buffer.deinit();

try msgpack.pack(buffer.writer(), Message, .{
    .name = "John",
    .age = 20,
});

const message = try msgpack.unpack(buffer.reader(), Message, allocator);
defer allocator.free(message.name);
```