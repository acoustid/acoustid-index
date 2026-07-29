// Peer discovery for snapshot bootstrap.
//
// A node whose position falls below the changelog's retention floor cannot replay —
// those positions are gone. It fetches a snapshot from a peer instead and resumes
// from that snapshot's watermark. This module answers the only question that needs
// answering: which peer to fetch from.
//
// Configuration is always a list of base URLs. Their hostnames are resolved on every
// lookup, and a name that resolves to several addresses expands to that many peers —
// so a Kubernetes headless Service is just one URL, and membership changes as pods
// come and go without any config change or restart. Discovery is a *list*, never a
// membership protocol: a bootstrapping node needs a lookup ("who has a snapshot I can
// use"), not consensus about who is alive, so gossip and failure detection would buy
// nothing here.
//
// The coordinator is deliberately not involved: it owns the log, not the nodes. A
// registry there would make the log service stateful and therefore a singleton,
// because two instances behind a Service each see a different subset of heartbeats.
//
// The other half of this protocol is `GET /:index/_status` (server.zig), which
// reports what a node holds, and `GET /:index/_snapshot`, which serves it.
//
// Only cleartext http:// peers are supported: each resolved address is turned back
// into a URL, so a hostname would not survive into the request.

const std = @import("std");
const zio = @import("zio");
const http = @import("dusty");
const msgpack = @import("msgpack");
const api = @import("api.zig");

const log = std.log.scoped(.peers);

// Cap on addresses taken from one configured URL. Sized so the resolver can never
// overflow it (see the lookup call below), not as a real limit on cluster size —
// note zio's own resolver already caps a DNS answer at 8 addresses per family, so on
// Linux that, not this, is what bounds how many peers one URL can reveal. Harmless
// here: donor selection only needs one usable peer, not the full membership.
pub const max_addrs_per_url = 64;

// Peer URLs are rebuilt per resolved address, so only cleartext HTTP is supported.
const http_default_port: u16 = 80;

// How long one peer gets to answer a status probe before we give up on it. A peer that
// is merely down fails fast (ECONNREFUSED); this bounds the cases that don't — a
// black-holed address, or a peer that accepts the connection and then wedges — so one
// sick peer can't hold up a bootstrap indefinitely. In-cluster this call is a few ms,
// so the bound is loose on purpose: it is a backstop, not a latency target.
pub const default_probe_timeout: zio.Duration = .fromMilliseconds(5_000);

/// A peer that can serve a usable snapshot, and the watermark it would land us on.
pub const Donor = struct {
    base_url: []const u8, // allocated in the arena passed to findDonors
    file_version: u64,
};

// Configured peer base URLs ("http://host:port"). Borrowed — must outlive this. Empty
// means bootstrap is unavailable: a node that falls below retention can only wait for
// an operator, which is fine for a single-node cluster.
urls: []const []const u8 = &.{},
io: std.Io,
allocator: std.mem.Allocator,
probe_timeout: zio.Duration = default_probe_timeout,

const Self = @This();

/// Expand the configured URLs into one base URL per resolved address, in `arena`.
/// Hostnames are resolved on every call: DNS is the membership source, so a stale
/// answer is exactly what we must avoid caching.
pub fn resolve(self: Self, arena: std.mem.Allocator) ![]const []const u8 {
    var out: std.ArrayListUnmanaged([]const u8) = .empty;
    for (self.urls) |url| {
        self.expandUrl(arena, url, &out) catch |err| {
            // One unresolvable name must not hide the peers that did resolve.
            log.warn("peer URL '{s}' did not resolve: {}", .{ url, err });
        };
    }
    return out.toOwnedSlice(arena);
}

fn expandUrl(self: Self, arena: std.mem.Allocator, url: []const u8, out: *std.ArrayListUnmanaged([]const u8)) !void {
    _ = self;
    const uri = try std.Uri.parse(url);
    // Each resolved address is turned back into a URL, so the hostname does not
    // survive into the request — which would break TLS name verification and any
    // Host-based routing. Rather than fail those silently (a peer that is never a
    // donor, with only a debug line to show for it), refuse the scheme outright.
    if (!std.mem.eql(u8, uri.scheme, "http")) return error.PeerUrlSchemeUnsupported;
    const host_component = uri.host orelse return error.PeerUrlMissingHost;
    const raw_host = try host_component.toRawMaybeAlloc(arena);
    // std.Uri keeps the brackets of an IPv6 literal in the host component, and they
    // are not part of the address — HostName rejects them.
    const host_str = if (raw_host.len >= 2 and raw_host[0] == '[' and raw_host[raw_host.len - 1] == ']')
        raw_host[1 .. raw_host.len - 1]
    else
        raw_host;
    const port = uri.port orelse http_default_port;

    // HostName accepts a numeric address as well as a name, and lookup resolves it to
    // itself — so a literal-IP peer needs no special case here.
    const host = try zio.net.HostName.init(host_str);
    var storage: [max_addrs_per_url]zio.net.HostName.LookupResult = undefined;
    // Deliberately NOT `catch error.TooManyAddresses => storage.len`. How much of
    // `storage` is valid on that error depends on which resolver answered: the
    // getaddrinfo path documents "the buffer is fully populated" (dns/posix.zig), but
    // the custom resolver's DNS path returns before its copy-back
    // (dns/resolver/resolver.zig:352), leaving `storage` untouched. Since `lookup`
    // returns no count on error, there is no way to tell — so don't guess.
    //
    // Unreachable in practice at this size, which is the point of sizing it here:
    // zio's custom resolver caps answers at 8 addresses per family (silently — see
    // `@min(result.count, parse_addrs.len)` at resolver.zig:718), and the paths that
    // can exceed 64 fill before erroring anyway.
    const n = try host.lookup(&storage, .{ .port = port });

    for (storage[0..n]) |entry| switch (entry) {
        // IpAddress.format renders host:port, bracketing IPv6.
        .address => |addr| try out.append(arena, try std.fmt.allocPrint(arena, "http://{f}", .{addr})),
        .canonical_name => {},
    };
}

// One peer's probe slot. `ok` stays false on any failure — an unreachable or
// mid-restart peer is not an error, it just isn't a donor.
const Probe = struct {
    base_url: []const u8,
    ok: bool = false,
    generation: u64 = 0,
    file_version: u64 = 0,
};

/// Every peer able to donate a snapshot of (`index_name`, `generation`) to a consumer
/// stuck at `after`, freshest first. Empty if none can. Probes all peers concurrently.
///
/// A *ranked* list rather than one pick: the best peer's `_snapshot` can still fail
/// (it may be mid-bootstrap itself, or its disk may be failing), and re-probing would
/// only rank it first again — one sick-but-freshest peer would wedge bootstrap
/// indefinitely. The caller walks the list instead.
pub fn findDonors(
    self: Self,
    arena: std.mem.Allocator,
    index_name: []const u8,
    generation: u64,
    after: u64,
) ![]const Donor {
    const urls = try self.resolve(arena);
    if (urls.len == 0) {
        log.warn("no peers resolved for '{s}': cannot bootstrap", .{index_name});
        return &.{};
    }

    const probes = try arena.alloc(Probe, urls.len);
    for (probes, urls) |*p, url| p.* = .{ .base_url = url };

    // Probe concurrently, each bounded by its own AutoCancel: a sick peer costs the
    // fan-out one probe_timeout, not a serial one each, and never an unbounded stall.
    var group: zio.Group = .init;
    errdefer group.cancel();
    for (probes) |*p| try group.spawn(probeOne, .{ self.io, self.allocator, index_name, self.probe_timeout, p });
    try group.wait();

    const donors = try rankDonors(arena, probes, generation, after);
    // Bootstrap only runs during an incident, so say enough to diagnose one: without
    // this, "no donor" is indistinguishable from DNS returning nothing, every peer
    // refusing, a generation mismatch, or every peer simply being too far behind.
    if (donors.len == 0) {
        var answered: usize = 0;
        var best_seen: u64 = 0;
        for (probes) |p| {
            if (!p.ok) continue;
            answered += 1;
            if (p.generation == generation and p.file_version > best_seen) best_seen = p.file_version;
        }
        log.warn(
            "no donor for '{s}' gen {d} at {d}: {d}/{d} peers answered, best usable file_version {d}",
            .{ index_name, generation, after, answered, probes.len, best_seen },
        );
    }
    return donors;
}

/// The selection rule, split out from the I/O so it can be tested directly.
///
/// Requiring `file_version > after` does two things at once: it guarantees forward
/// progress (a snapshot at or below where we already are would re-trigger the same
/// below-retention read), and it excludes this node from its own peer list for free —
/// a node's `file_version` never exceeds its applied version, and `after` IS that
/// applied version, so a bootstrapping node can never rank itself. That is why no node
/// identity or self-address configuration is needed anywhere.
///
/// Freshest first: the highest `file_version` has the best chance of clearing a
/// retention floor we cannot see. If it doesn't clear it, the next read re-signals and
/// we try again from further along, so the loop still converges.
fn rankDonors(arena: std.mem.Allocator, probes: []const Probe, generation: u64, after: u64) ![]const Donor {
    var list: std.ArrayListUnmanaged(Donor) = .empty;
    for (probes) |p| {
        if (!p.ok) continue;
        if (p.generation != generation) continue; // different lineage, not our data
        if (p.file_version <= after) continue; // no progress (excludes ourselves)
        try list.append(arena, .{ .base_url = p.base_url, .file_version = p.file_version });
    }
    const donors = try list.toOwnedSlice(arena);
    std.mem.sort(Donor, donors, {}, struct {
        fn desc(_: void, a: Donor, b: Donor) bool {
            return a.file_version > b.file_version;
        }
    }.desc);
    return donors;
}

// Ask one peer what it holds for `index_name`. Errors are swallowed by design: this
// runs against every configured peer, and peers being down is the normal case this
// whole mechanism exists to survive.
//
// The AutoCancel bounds every suspension point in the probe, connect and read alike, by
// canceling this task's I/O when it fires. `check` is what separates our own deadline
// from a real shutdown cancel — the latter must stay quiet and let the group unwind.
fn probeOne(io: std.Io, allocator: std.mem.Allocator, index_name: []const u8, timeout: zio.Duration, p: *Probe) void {
    var deadline: zio.AutoCancel = .init;
    defer deadline.clear();
    deadline.set(.{ .duration = timeout });

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();

    probeOneFallible(io, a, index_name, p) catch |err| {
        if (err == error.Canceled) {
            if (!deadline.check(error.Canceled)) return; // shutdown, not our deadline
            log.debug("peer {s} timed out for '{s}'", .{ p.base_url, index_name });
            return;
        }
        log.debug("peer {s} did not answer for '{s}': {}", .{ p.base_url, index_name, err });
    };
}

fn probeOneFallible(io: std.Io, arena: std.mem.Allocator, index_name: []const u8, p: *Probe) !void {
    const url = try std.fmt.allocPrint(arena, "{s}/{s}/_status", .{ p.base_url, index_name });

    // A client per probe: they run concurrently and dusty's connection pool is not
    // shared-safe. Same pattern as RemoteCoordinator's per-call clients.
    var client = http.Client.init(arena, io, .{});
    defer client.deinit();

    // Without an Accept header the server answers a bodyless GET in JSON.
    var headers = try http.Headers.init(arena, 1);
    defer headers.deinit(arena);
    try headers.add("Accept", comptime http.ContentType.msgpack.toContentType());

    var resp = try client.fetch(url, .{ .method = .get, .headers = &headers });
    defer resp.deinit();
    // 404 is routine: the peer simply doesn't hold this index yet.
    if (resp.status() != .ok) return error.PeerStatusFailed;

    const body = (try resp.body()) orelse return error.EmptyPeerStatus;
    const status = try msgpack.decodeFromSliceLeaky(api.PeerStatusResponse, arena, body);

    p.generation = status.generation;
    p.file_version = status.file_version;
    p.ok = true;
}

const testing = std.testing;

// Resolution needs a runtime: zio's resolver suspends, and HostName.lookup must run
// on a coroutine. Literal addresses still go through it, which is the point — one
// code path for names and IPs.
fn testResolve(urls: []const []const u8, out: *[]const []const u8, arena: std.mem.Allocator, err: *?anyerror) void {
    const self: Self = .{ .urls = urls, .io = undefined, .allocator = testing.allocator };
    out.* = self.resolve(arena) catch |e| {
        err.* = e;
        return;
    };
}

fn resolveInRuntime(urls: []const []const u8, arena: std.mem.Allocator) ![]const []const u8 {
    const rt = try zio.Runtime.init(testing.allocator, .{ .executors = .exact(1) });
    defer rt.deinit();

    var out: []const []const u8 = &.{};
    var err: ?anyerror = null;
    var task = try zio.spawn(testResolve, .{ urls, &out, arena, &err });
    task.join();
    if (err) |e| return e;
    return out;
}

test "resolve expands literal addresses, keeping scheme and port" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const urls = try resolveInRuntime(&.{ "http://127.0.0.1:8080", "http://[::1]:9090" }, arena.allocator());
    try testing.expectEqual(@as(usize, 2), urls.len);
    try testing.expectEqualStrings("http://127.0.0.1:8080", urls[0]);
    try testing.expectEqualStrings("http://[::1]:9090", urls[1]);
}

test "resolve defaults the port from the scheme" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const urls = try resolveInRuntime(&.{"http://127.0.0.1"}, arena.allocator());
    try testing.expectEqual(@as(usize, 1), urls.len);
    try testing.expectEqualStrings("http://127.0.0.1:80", urls[0]);
}

test "resolve skips a bad URL rather than losing the peers that did resolve" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    // "not a url" has no host; the literal after it must still come through.
    const urls = try resolveInRuntime(&.{ "not a url", "http://127.0.0.1:8080" }, arena.allocator());
    try testing.expectEqual(@as(usize, 1), urls.len);
    try testing.expectEqualStrings("http://127.0.0.1:8080", urls[0]);
}

test "no peers configured resolves to nothing" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const urls = try resolveInRuntime(&.{}, arena.allocator());
    try testing.expectEqual(@as(usize, 0), urls.len);
}

test "resolve refuses https, which cannot survive the rebuild from an address" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    // Silently dropping it would leave a peer that is never a donor and no clue why.
    const urls = try resolveInRuntime(&.{ "https://127.0.0.1:8443", "http://127.0.0.1:8080" }, arena.allocator());
    try testing.expectEqual(@as(usize, 1), urls.len);
    try testing.expectEqualStrings("http://127.0.0.1:8080", urls[0]);
}

// --- donor selection (the rule both design claims rest on) ---

fn probe(url: []const u8, generation: u64, file_version: u64) Probe {
    return .{ .base_url = url, .ok = true, .generation = generation, .file_version = file_version };
}

test "rankDonors returns usable donors, freshest first" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const probes = [_]Probe{
        probe("http://a", 1, 5),
        probe("http://b", 1, 9),
        probe("http://c", 1, 7),
    };
    const donors = try rankDonors(arena.allocator(), &probes, 1, 0);
    try testing.expectEqual(@as(usize, 3), donors.len);
    try testing.expectEqualStrings("http://b", donors[0].base_url);
    try testing.expectEqualStrings("http://c", donors[1].base_url);
    try testing.expectEqualStrings("http://a", donors[2].base_url);
}

test "rankDonors drops peers that cannot help" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const probes = [_]Probe{
        .{ .base_url = "http://silent", .ok = false, .generation = 1, .file_version = 99 },
        probe("http://wrong-lineage", 2, 99), // delete+recreate: not our data
        probe("http://behind", 1, 4), // at or below where we're stuck: no progress
        probe("http://equal", 1, 5),
        probe("http://good", 1, 6),
    };
    const donors = try rankDonors(arena.allocator(), &probes, 1, 5);
    try testing.expectEqual(@as(usize, 1), donors.len);
    try testing.expectEqualStrings("http://good", donors[0].base_url);
}

test "rankDonors excludes the bootstrapping node itself" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    // A node's file_version never exceeds its applied version, and `after` IS that
    // applied version — so its own probe can never satisfy `file_version > after`.
    // This is what makes self-address configuration unnecessary; if the comparison
    // were ever loosened to >=, a node could "bootstrap" from itself and spin.
    const after: u64 = 12;
    const probes = [_]Probe{probe("http://self", 1, after)};
    const donors = try rankDonors(arena.allocator(), &probes, 1, after);
    try testing.expectEqual(@as(usize, 0), donors.len);
}

test "a peer that accepts but never answers is given up on, not waited for" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const rt = try zio.Runtime.init(testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();

    const Ctx = struct {
        io: std.Io,
        url: []const u8 = &.{},
        arena: std.mem.Allocator,
        elapsed_ms: u64 = 0,
        donors: []const Donor = &.{},
        err: ?anyerror = null,

        fn run(ctx: *@This()) void {
            // A listening socket we never accept on: connect() completes out of the
            // backlog, then the HTTP response never comes. This is the stall that a
            // plain "is the peer up?" check cannot catch.
            var server = zio.net.IpAddress.parseIp4("127.0.0.1", 0) catch |e| {
                ctx.err = e;
                return;
            };
            var listener = server.listen(.{ .reuse_address = true }) catch |e| {
                ctx.err = e;
                return;
            };
            defer listener.close();
            // bind() writes the actually-bound address back, so port 0 resolves here.
            const bound = listener.socket.address.ip;

            const url = std.fmt.allocPrint(ctx.arena, "http://{f}", .{bound}) catch |e| {
                ctx.err = e;
                return;
            };
            ctx.url = url;

            const urls = [_][]const u8{url};
            const peers: Self = .{
                .urls = &urls,
                .io = ctx.io,
                .allocator = testing.allocator,
                .probe_timeout = .fromMilliseconds(300),
            };

            var sw = zio.Stopwatch.start();
            ctx.donors = peers.findDonors(ctx.arena, "main", 1, 0) catch |e| {
                ctx.err = e;
                return;
            };
            ctx.elapsed_ms = @intCast(@divTrunc(sw.read().toNanoseconds(), std.time.ns_per_ms));
        }
    };

    var ctx: Ctx = .{ .io = rt.io(), .arena = a };
    var task = try zio.spawn(Ctx.run, .{&ctx});
    task.join();

    if (ctx.err) |e| return e;
    try testing.expectEqual(@as(usize, 0), ctx.donors.len);
    // It really stalled — the connect succeeded out of the backlog and the read hung,
    // so anything near-instant would mean the test wasn't exercising the timeout.
    try testing.expect(ctx.elapsed_ms >= 250);
    // And the stall was ended by probe_timeout, not by the OS read timeout (minutes).
    try testing.expect(ctx.elapsed_ms < 3000);
}
