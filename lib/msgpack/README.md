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

try msgpack.encode(buffer.writer(), Message, .{
    .name = "John",
    .age = 20,
});

var stream = std.io.fixedBufferStream(buffer.items);
const message = try decode(stream.reader(), allocator, Message);
defer allocator.free(message.name);
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

