# Zig library for working with msgpack messages

Simple usage:

```zig
const std = @import("std");
const msgpack = @import("msgpack");

const Message = struct {
    name: []const u8,
    age: u8,
};

var buffer = std.ArrayList(u8).init(allocator);
defer buffer.deinit();

try msgpack.encode(Message{
    .name = "John",
    .age = 20,
}, buffer.writer());

const decoded = try msgpack.decodeFromSlice(Message, allocator, buffer.items);
defer decoded.deinit();

std.debug.assert(std.mem.eql(u8, decoded.name, "John"));
std.debug.assert(decoded.age == 20);
```

Change the default format from using field names to field indexes:

```zig
const std = @import("std");
const msgpack = @import("msgpack");

const Message = struct {
    name: []const u8,
    age: u8,

    pub fn msgpackFormat() msgpack.StructFormat {
        return .{ .as_map = .{ .key = .field_index } };
    }
};
```

Completely custom format:

```zig
const std = @import("std");
const msgpack = @import("msgpack");

const Message = struct {
    items: []u32,

    pub fn msgpackWrite(self: Message, packer: anytype) !void {
        try packer.writeArray(u32, self.items);
    }

    pub fn msgpackRead(unpacker: anytype) !Message {
        const items = try unpacker.readArray(u32);
        return Message{ .items = items };
    }
};
```

