const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const zio_dep = b.dependency("zio", .{ .target = target, .optimize = optimize });
    const dusty_dep = b.dependency("dusty", .{ .target = target, .optimize = optimize });
    const msgpack_dep = b.dependency("msgpack", .{ .target = target, .optimize = optimize });
    const metrics_dep = b.dependency("metrics", .{ .target = target, .optimize = optimize });

    // Inject the real zio module into dusty, replacing dusty's built-in zio stub.
    // Without this, request timeouts panic (the stub's AutoCancel.set() @panics).
    dusty_dep.module("dusty").addImport("zio", zio_dep.module("zio"));

    const exe = b.addExecutable(.{
        .name = "fpindex",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.root_module.addImport("zio", zio_dep.module("zio"));
    exe.root_module.addImport("dusty", dusty_dep.module("dusty"));
    exe.root_module.addImport("msgpack", msgpack_dep.module("msgpack"));
    exe.root_module.addImport("metrics", metrics_dep.module("metrics"));

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    const run_step = b.step("run", "Run the server");
    run_step.dependOn(&run_cmd.step);

    const unit_tests = b.addTest(.{
        .root_module = exe.root_module,
        .test_runner = .{ .path = b.path("test_runner.zig"), .mode = .simple },
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    run_unit_tests.has_side_effects = true;
    const test_step = b.step("unit-tests", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
}
